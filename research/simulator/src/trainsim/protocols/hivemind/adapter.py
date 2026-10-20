"""Node-local, event-driven approximation of Hivemind group matchmaking."""

from __future__ import annotations

from dataclasses import asdict
import math
import random
from typing import Any, Mapping

from ...policies import HonestPolicy, PolicyObservation, policy_from_config
from ...randomness import rng
from ...types import Advertisement, AttemptState, GroupRoster, Message, Outcome, WorkItem
from ..base import ProtocolAdapter
from .state import JoinRequest, NodeState, PendingJoin


class HivemindAdapter(ProtocolAdapter):
    """Coordinate independent work items using local DHT-style views and RPCs.

    The adapter stores one matchmaking state per participant. Discovery results
    arrive through the shared simulated network; group formation is therefore
    decided from each node's local advertisements and message history.
    """

    PREFIX = "trainsim"
    CONTROL_BYTES = {
        "join_request": 256,
        "join_response": 128,
        "group_announcement": 128,
        "cancel_join": 128,
    }

    def __init__(
        self,
        engine,
        network,
        discovery,
        profiles,
        config,
        on_terminal,
        policies=None,
    ) -> None:
        self.engine = engine
        self.network = network
        self.discovery = discovery
        self.profiles = dict(profiles)
        self.config = config
        self.on_terminal = on_terminal
        self.prefix = config.get("prefix", self.PREFIX)
        self.states: dict[str, NodeState] = {}
        self.groups: dict[str, Any] = {}
        self._group_sequence = 0
        self._policies: dict[str, Any] = {}
        configured = config.get("policies", {})
        for node_id in self.profiles:
            policy = None
            if isinstance(policies, Mapping):
                policy = policies.get(node_id)
            elif policies is not None:
                policy = policies
            if policy is None:
                policy = policy_from_config(configured.get(node_id))
            self._policies[node_id] = policy or HonestPolicy()

    def start(self, node_id: str) -> None:
        """Register one participant's protocol state."""
        if node_id not in self.profiles:
            raise KeyError(f"Unknown simulated node {node_id!r}")
        if node_id in self.states:
            return
        profile = self.profiles[node_id]
        self.states[node_id] = NodeState(profile, profile.initial_group_bits)
        self._phase(self.states[node_id], "idle")

    def on_work_ready(self, node_id: str, work: WorkItem) -> None:
        """Begin one bounded search for a group for the ready work item."""
        state = self._state(node_id)
        if not state.active:
            return
        if state.work is not None:
            raise RuntimeError(f"Node {node_id} received work while another item is active")
        state.work = work
        state.retry_sequence = 0
        state.last_terminal_work_id = None
        state.generation += 1
        deadline = self.engine.now + float(self.config["search_timeout"])
        self._begin_search(state, deadline, reset_attempts=True)

    def on_message(self, node_id: str, message: Message) -> None:
        """Deliver a network message to the addressed local protocol state."""
        state = self.states.get(node_id)
        if state is None or not state.active:
            return
        handlers = {
            "join_request": self._on_join_request,
            "join_response": self._on_join_response,
            "cancel_join": self._on_cancel_join,
            "begin_allreduce": self._on_begin_allreduce,
            "reduce_input": self._on_reduce_input,
            "reduce_result": self._on_reduce_result,
        }
        handler = handlers.get(message.kind)
        if handler is not None:
            handler(state, message)

    def stop(self, node_id: str) -> None:
        """Cancel local pending activity when the experiment reaches its horizon."""
        state = self.states.get(node_id)
        if state is None or not state.active:
            return
        state.active = False
        state.generation += 1
        state.cancel_all_timers()
        # The registry is virtual and leases expire naturally. Horizon cleanup
        # must not launch a withdrawal RPC after measurement has stopped.
        state.declared_key = None
        state.declared_expiration = math.inf
        state.phase = "stopped"

    def _state(self, node_id: str) -> NodeState:
        try:
            return self.states[node_id]
        except KeyError as exc:
            raise RuntimeError(f"Node {node_id!r} has not been started") from exc

    def _phase(self, state: NodeState, phase: str, **data: Any) -> None:
        state.phase = phase
        data.setdefault("work_id", state.work.work_id if state.work else "")
        self.engine.emit("node_phase", state.node_id, phase=phase, **data)

    def _schedule(self, state: NodeState, name: str, when: float, callback, *args) -> None:
        """Replace a named timer and ignore callbacks from an old attempt."""
        state.cancel_timer(name)
        expected_generation = state.generation
        expected_attempt = state.attempt.attempt_id if state.attempt else None
        def fire():
            if state.timers.get(name) is not handle or not state.active:
                return
            del state.timers[name]
            if state.generation != expected_generation:
                return
            if expected_attempt is not None and (
                state.attempt is None or state.attempt.attempt_id != expected_attempt
            ):
                return
            callback(*args)
        handle = self.engine.call_at(max(self.engine.now, when), fire)
        state.timers[name] = handle

    def _begin_search(self, state: NodeState, deadline: float, *, reset_attempts: bool) -> None:
        """Publish the current key and begin local advertisement discovery."""
        if state.work is None or not state.active:
            return
        if reset_attempts:
            state.attempted_candidates.clear()
        state.generation += 1
        retry = state.retry_sequence
        attempt_id = f"{state.work.work_id}:attempt:{retry}"
        state.attempt = AttemptState(attempt_id, state.work.work_id,
                                     state.work.compute_finish + float(self.config["min_matchmaking_time"]), deadline)
        state.current_leader = None
        state.current_followers.clear()
        state.pending_join = None
        state.lookup_inflight = False
        state.group_id = None
        state.group_run = None
        state.declared_key = self._key(state)
        state.declared_expiration = min(
            max(state.work.compute_finish, self.engine.now + float(self.config["min_matchmaking_time"])),
            deadline,
        )
        self._phase(state, "searching", attempt_id=attempt_id, key=state.declared_key,
                    expiration=state.declared_expiration)
        self._publish(state, active=True)
        self._schedule(state, "search_deadline", deadline, self._search_deadline, state.node_id, attempt_id)
        self._schedule(state, "leader_expiry", state.declared_expiration,
                       self._leader_expired, state.node_id, attempt_id, state.declared_expiration)
        self._lookup(state)

    def _key(self, state: NodeState) -> str:
        return f"{self.prefix}.0b{state.group_bits}"

    def _publish(self, state: NodeState, *, active: bool) -> None:
        if state.declared_key is None:
            return
        expiry = state.declared_expiration
        advertisement = Advertisement(state.declared_key, state.node_id, active, expiry)
        action = self._apply_policy(state, "advertisement", asdict(advertisement), {"delay_seconds": 0.0})
        delay = float(action.get("delay_seconds", 0.0))
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("policy advertisement delay must be finite and nonnegative")
        if delay:
            self.engine.schedule(delay, self.discovery.publish, state.node_id, advertisement)
        else:
            self.discovery.publish(state.node_id, advertisement)
        self.engine.emit("advertisement", state.node_id, key=advertisement.key,
                         active=active, expiry=expiry)

    def _withdraw(self, state: NodeState) -> None:
        if state.declared_key is not None and math.isfinite(state.declared_expiration):
            self._publish(state, active=False)
        state.declared_key = None
        state.declared_expiration = math.inf

    def _lookup(self, state: NodeState) -> None:
        if (not state.active or state.attempt is None or state.phase != "searching"
                or state.current_leader is not None or state.declared_key is None
                or state.lookup_inflight or self.engine.now >= state.attempt.search_deadline):
            return
        state.lookup_inflight = True
        attempt_id = state.attempt.attempt_id
        generation = state.generation
        key = state.declared_key
        def arrived(advertisements):
            if (not state.active or state.generation != generation or state.attempt is None
                    or state.attempt.attempt_id != attempt_id):
                return
            state.lookup_inflight = False
            candidates = self._eligible_candidates(state, advertisements)
            if candidates and state.pending_join is None and state.current_leader is None:
                self._request_candidate(state, candidates)
            elif state.phase == "searching" and state.pending_join is None:
                active_same_key = [ad for ad in advertisements
                                   if ad.active and ad.peer_id != state.node_id
                                   and ad.key == state.declared_key and ad.expiry > self.engine.now
                                   and ad.expiry <= state.attempt.search_deadline]
                # PotentialLeaders triggers another DHT read when its local
                # candidate queue is empty. If visible peers all outrank this
                # node, the next declaration/expiry event will wake the search.
                blocked_by_priority = active_same_key and all(
                    (ad.expiry, self.profiles[ad.peer_id].peer_id)
                    > (state.declared_expiration, state.peer_id_bytes)
                    for ad in active_same_key if ad.peer_id in self.profiles
                )
                if not blocked_by_priority:
                    self._schedule(state, "discovery_refresh", self.engine.now,
                                   self._lookup, state)
        self.discovery.lookup(state.node_id, key, arrived)

    def _poll(self, node_id: str, attempt_id: str) -> None:
        state = self.states.get(node_id)
        if state is not None and state.attempt is not None and state.attempt.attempt_id == attempt_id:
            self._lookup(state)

    def _eligible_candidates(self, state: NodeState, advertisements) -> list[tuple[str, float]]:
        own_priority = (state.declared_expiration, state.peer_id_bytes)
        candidates = []
        if state.attempt is None:
            return candidates
        for ad in advertisements:
            peer = ad.peer_id
            profile = self.profiles.get(peer)
            pair = (peer, float(ad.expiry))
            if (not ad.active or peer == state.node_id or profile is None
                    or ad.key != state.declared_key or ad.expiry <= self.engine.now
                    or ad.expiry > state.attempt.search_deadline
                    or pair in state.attempted_candidates
                    or (float(ad.expiry), profile.peer_id) > own_priority):
                continue
            candidates.append(pair)
        candidates.sort(key=lambda item: (item[1], self.profiles[item[0]].peer_id))
        return candidates

    def _request_candidate(self, state: NodeState, candidates: list[tuple[str, float]]) -> None:
        local = {"candidates": [peer for peer, _ in candidates],
                 "expirations": [expiry for _, expiry in candidates]}
        proposed = {"peer_id": candidates[0][0]}
        decision = self._apply_policy(state, "candidate_selection", local, proposed)
        peer_id = decision.get("peer_id", candidates[0][0])
        selected = next((pair for pair in candidates if pair[0] == peer_id), candidates[0])
        self._send_join_request(state, selected[0], selected[1])

    def _send_join_request(self, state: NodeState, peer_id: str, peer_expiry: float) -> None:
        if state.work is None or state.attempt is None or state.declared_key is None:
            return
        pair = (peer_id, float(peer_expiry))
        state.attempted_candidates.add(pair)
        request_expiry = state.declared_expiration
        pending = PendingJoin(peer_id, peer_expiry, request_expiry,
                              state.attempt.attempt_id, state.generation)
        state.pending_join = pending
        self._phase(state, "joining", attempt_id=state.attempt.attempt_id,
                    candidate=peer_id, candidate_expiry=peer_expiry)
        self._send(state, peer_id, "join_request", {
            "work_id": state.work.work_id,
            "group_key": state.declared_key,
            "request_expiration": request_expiry,
        }, self.CONTROL_BYTES["join_request"], attempt_id=state.attempt.attempt_id)
        self._schedule(state, "join_reply_timeout", self.engine.now + float(self.config["request_timeout"]),
                       self._join_reply_timeout, state.node_id, pending)

    def _join_reply_timeout(self, node_id: str, pending: PendingJoin) -> None:
        state = self.states.get(node_id)
        if state is None or state.pending_join != pending:
            return
        state.pending_join = None
        state.current_leader = None
        self._phase(state, "searching", timeout="join_reply", candidate=pending.peer_id)
        self._lookup(state)

    def _on_join_request(self, leader: NodeState, message: Message) -> None:
        body = message.body
        reason = self._join_rejection(leader, message.sender, body)
        proposed = {"accept": reason is None, "reason": reason or "accepted"}
        local = {"peer_id": message.sender, "work_id": body.get("work_id", ""),
                 "group_key": body.get("group_key", ""),
                 "expiration": body.get("request_expiration", -math.inf),
                 "current_group_size": len(leader.current_followers) + 1,
                 "target_group_size": self.config["target_group_size"]}
        if reason is None:
            decision = self._apply_policy(leader, "incoming_join", local, proposed)
            if not decision.get("accept", False):
                reason = decision.get("reason", "policy_rejected")
        if reason is not None:
            self._send(leader, message.sender, "join_response", {"code": reason},
                       self.CONTROL_BYTES["join_response"], attempt_id=message.attempt_id)
            return
        follower = JoinRequest(message.sender, str(body["work_id"]),
                               float(body["request_expiration"]), str(body["group_key"]),
                               message.attempt_id)
        leader.current_followers[message.sender] = follower
        self._send(leader, message.sender, "join_response", {"code": "accepted"},
                   self.CONTROL_BYTES["join_response"], attempt_id=message.attempt_id)
        self.engine.emit("join_accepted", leader.node_id, peer_id=message.sender,
                         work_id=follower.work_id, group_size=len(leader.current_followers) + 1)
        if len(leader.current_followers) + 1 >= int(self.config["target_group_size"]):
            self._assemble_group(leader)

    def _join_rejection(self, leader: NodeState, peer_id: str, body: dict) -> str | None:
        if leader.phase not in ("searching", "joining") or leader.attempt is None or leader.work is None:
            return "not_looking_for_group"
        if leader.current_leader is not None:
            return "not_a_leader"
        if leader.declared_key is None or body.get("group_key") != leader.declared_key:
            return "bad_group_key"
        try:
            request_expiry = float(body["request_expiration"])
        except (KeyError, TypeError, ValueError):
            return "protocol_violation"
        if not math.isfinite(request_expiry) or request_expiry <= self.engine.now:
            return "bad_expiration_time"
        if leader.declared_expiration > request_expiry:
            return "bad_expiration_time"
        if peer_id == leader.node_id or peer_id in leader.current_followers:
            return "duplicate_peer_id"
        if len(leader.current_followers) + 1 >= int(self.config["target_group_size"]):
            return "group_is_full"
        return None

    def _on_join_response(self, state: NodeState, message: Message) -> None:
        pending = state.pending_join
        if (pending is None or pending.peer_id != message.sender or state.attempt is None
                or pending.attempt_id != message.attempt_id
                or pending.attempt_id != state.attempt.attempt_id
                or self.engine.now >= state.attempt.search_deadline):
            return
        code = message.body.get("code")
        state.cancel_timer("join_reply_timeout")
        if code == "accepted":
            state.current_leader = message.sender
            deadline = pending.request_expiration + float(self.config["request_timeout"])
            state.pending_join = pending
            self._schedule(state, "leader_wait_timeout", deadline,
                           self._leader_wait_timeout, state.node_id, pending)
            self._disband_followers(state, suggested_leader=message.sender)
            self._phase(state, "following", leader=message.sender,
                        wait_deadline=deadline)
            return
        state.cancel_timer("leader_wait_timeout")
        state.current_leader = None
        state.pending_join = None
        suggested = message.body.get("suggested_leader")
        if suggested and suggested in self.profiles and suggested != state.node_id:
            self._send_join_request(state, suggested, self.engine.now + float(self.config["min_matchmaking_time"]))
        else:
            self._phase(state, "searching", rejection=code)
            self._lookup(state)

    def _leader_wait_timeout(self, node_id: str, pending: PendingJoin) -> None:
        state = self.states.get(node_id)
        if state is None or state.pending_join != pending or state.group_id is not None:
            return
        self._cancel_remote_join(state, pending)
        state.pending_join = None
        state.current_leader = None
        if state.attempt is not None and self.engine.now < state.attempt.search_deadline:
            self._phase(state, "searching", timeout="leader_wait")
            self._lookup(state)

    def _on_cancel_join(self, leader: NodeState, message: Message) -> None:
        if leader.group_id is None:
            request = leader.current_followers.get(message.sender)
            if request is not None and request.attempt_id == message.attempt_id:
                leader.current_followers.pop(message.sender, None)

    def _cancel_remote_join(self, state: NodeState, pending: PendingJoin) -> None:
        self._send(state, pending.peer_id, "cancel_join", {},
                   self.CONTROL_BYTES["cancel_join"], attempt_id=pending.attempt_id)

    def _disband_followers(self, state: NodeState, suggested_leader: str | None) -> None:
        followers = list(state.current_followers.values())
        state.current_followers.clear()
        for follower in followers:
            body = {"code": "group_disbanded", "suggested_leader": suggested_leader}
            self._send(state, follower.peer_id, "join_response", body,
                       self.CONTROL_BYTES["join_response"], attempt_id=follower.attempt_id)

    def _leader_expired(self, node_id: str, attempt_id: str, expiration: float) -> None:
        state = self.states.get(node_id)
        if (state is None or state.attempt is None or state.attempt.attempt_id != attempt_id
                or state.phase != "searching" or state.current_leader is not None
                or state.declared_expiration != expiration):
            return
        minimum = int(self.config["min_group_size"])
        if len(state.current_followers) + 1 >= minimum:
            self._assemble_group(state)
            return
        self._disband_followers(state, suggested_leader=None)
        self._withdraw(state)
        if self.engine.now < state.attempt.search_deadline:
            self._refresh_declaration(state)
        else:
            self._finish_work(state, Outcome.FAILED)

    def _refresh_declaration(self, state: NodeState) -> None:
        if state.work is None or state.attempt is None:
            return
        state.declared_key = self._key(state)
        state.declared_expiration = min(
            max(state.work.compute_finish, self.engine.now + float(self.config["min_matchmaking_time"])),
            state.attempt.search_deadline,
        )
        self._publish(state, active=True)
        self._schedule(state, "leader_expiry", state.declared_expiration,
                       self._leader_expired, state.node_id, state.attempt.attempt_id,
                       state.declared_expiration)
        self._lookup(state)

    def _search_deadline(self, node_id: str, attempt_id: str) -> None:
        state = self.states.get(node_id)
        if state is None or state.attempt is None or state.attempt.attempt_id != attempt_id:
            return
        if state.phase == "searching" or state.phase == "joining" or state.phase == "following":
            if state.pending_join is not None:
                self._cancel_remote_join(state, state.pending_join)
            self._disband_followers(state, suggested_leader=None)
            self._finish_work(state, Outcome.FAILED)

    def _assemble_group(self, leader: NodeState) -> None:
        if (leader.work is None or leader.attempt is None or leader.current_leader is not None
                or leader.phase != "searching" or leader.group_id is not None):
            return
        follower_records = list(leader.current_followers.values())
        member_ids = [leader.node_id, *(item.peer_id for item in follower_records)]
        if len(member_ids) < int(self.config["min_group_size"]):
            return
        # Preserve immutable request records for the roster, then release the
        # leader's mutable pending-join table without sending disband replies.
        leader.current_followers.clear()
        work_by_peer = {leader.node_id: leader.work.work_id}
        work_by_peer.update({item.peer_id: item.work_id for item in follower_records})
        group_rng = rng(self.engine.seed, "hivemind_group_id", leader.node_id, self._group_sequence)
        group_id = group_rng.randbytes(16).hex()
        self._group_sequence += 1
        ordered = list(member_ids)
        random.Random(bytes.fromhex(group_id)).shuffle(ordered)
        roster = GroupRoster(group_id, leader.node_id, tuple(ordered),
                             tuple(work_by_peer[peer] for peer in ordered), self.engine.now)
        # Group membership is now committed. A leader that reached the target
        # assembles immediately; otherwise this is the expiry-time minimum group.
        self._withdraw(leader)
        self._update_group_bits(leader, roster)
        leader.group_id = group_id
        leader.current_followers.clear()
        self._phase(leader, "allreduce", group_id=group_id, group_size=len(ordered))
        from .allreduce import GroupAllReduce
        attempt_ids = {leader.node_id: leader.attempt.attempt_id}
        attempt_ids.update({item.peer_id: item.attempt_id for item in follower_records})
        group = GroupAllReduce(self, roster, attempt_ids=attempt_ids)
        self.groups[group_id] = group
        leader.group_run = group
        self.engine.emit("group_formed", leader.node_id, roster=asdict(roster))
        group.start_member(leader.node_id)
        for follower in follower_records:
            self._send(leader, follower.peer_id, "begin_allreduce", {"roster": roster},
                       self.CONTROL_BYTES["group_announcement"] + 64 * len(ordered),
                       attempt_id=follower.attempt_id, group_id=group_id,
                       policy_hook="group_announcement")

    def _update_group_bits(self, state: NodeState, roster: GroupRoster) -> None:
        target = self.config.get("target_group_size")
        num_buckets = target if target is not None else 1 << math.ceil(math.log2(len(roster.members)))
        index = roster.members.index(state.node_id)
        sample = random.Random(bytes.fromhex(roster.group_id)).sample(range(num_buckets), len(roster.members))
        nbits = math.ceil(math.log2(num_buckets))
        new_bits = format(sample[index], f"0{nbits}b") if nbits else ""
        bits = state.group_bits
        state.group_bits = (bits + new_bits)[-len(bits):] if bits else ""

    def _on_begin_allreduce(self, state: NodeState, message: Message) -> None:
        if state.attempt is None or state.work is None or state.phase != "following":
            return
        pending = state.pending_join
        if (pending is None or pending.peer_id != message.sender
                or pending.attempt_id != message.attempt_id
                or message.group_id == ""
                or self.engine.now >= pending.request_expiration + float(self.config["request_timeout"])
                or self.engine.now >= state.attempt.search_deadline):
            return
        roster = message.body.get("roster")
        if not isinstance(roster, GroupRoster) or state.node_id not in roster.members:
            return
        if roster.group_id != message.group_id or roster.leader != message.sender:
            return
        expected_work = roster.work_ids[roster.members.index(state.node_id)]
        if expected_work != state.work.work_id:
            return
        state.cancel_timer("leader_wait_timeout")
        self._withdraw(state)
        self._update_group_bits(state, roster)
        state.group_id = roster.group_id
        state.group_run = self.groups.get(roster.group_id)
        self._phase(state, "allreduce", group_id=roster.group_id, group_size=len(roster.members))
        if state.group_run is not None:
            state.group_run.start_member(state.node_id)

    def _on_reduce_input(self, state: NodeState, message: Message) -> None:
        group = self.groups.get(message.group_id)
        if group is not None:
            group.input_received(message)

    def _on_reduce_result(self, state: NodeState, message: Message) -> None:
        group = self.groups.get(message.group_id)
        if group is not None:
            group.result_received(message)

    def _group_member_complete(self, node_id: str, outcome: Outcome, result) -> None:
        """Commit one recipient's complete set of reducer outputs."""
        state = self.states.get(node_id)
        if state is not None and state.group_id == result.group_id:
            self._finish_work(state, outcome, result)

    def _group_member_failed(self, node_id: str, result) -> None:
        """Retry a failed all-reduce only while the original step deadline remains."""
        state = self.states.get(node_id)
        if state is None or state.work is None or state.group_id != result.group_id:
            return
        deadline = state.attempt.search_deadline if state.attempt is not None else self.engine.now
        if self.engine.now < deadline:
            self.engine.emit("aggregate_result", node_id, result=asdict(result))
            state.retry_sequence += 1
            self._begin_search(state, deadline, reset_attempts=True)
        else:
            self._finish_work(state, Outcome.FAILED, result)

    def _finish_work(self, state: NodeState, outcome: Outcome, result=None) -> None:
        if state.work is None or state.last_terminal_work_id == state.work.work_id:
            return
        work_id = state.work.work_id
        for group in tuple(self.groups.values()):
            if group.work_by_member.get(state.node_id) == work_id:
                group.member_unavailable(state.node_id)
        if result is not None:
            self.engine.emit("aggregate_result", state.node_id, result=asdict(result))
        state.last_terminal_work_id = work_id
        self._withdraw(state)
        state.cancel_all_timers()
        self._phase(state, "terminal", outcome=outcome.value, work_id=work_id)
        self.engine.emit("work_terminal", state.node_id, work_id=work_id, outcome=outcome.value)
        state.generation += 1
        state.work = None
        state.attempt = None
        state.pending_join = None
        state.current_leader = None
        state.current_followers.clear()
        state.group_id = None
        state.group_run = None
        self.on_terminal(state.node_id, outcome)

    def _apply_policy(self, state: NodeState, hook: str, local: dict, proposed: dict) -> dict:
        policy = self._policies[state.node_id]
        decision = policy.decide(PolicyObservation(state.node_id, hook, dict(local)), dict(proposed))
        if decision != proposed:
            self.engine.emit("policy_action", state.node_id, hook=hook,
                             observation=local, proposed=proposed, selected=decision)
        return decision

    def _send(
        self,
        state: NodeState,
        recipient: str,
        kind: str,
        body: dict,
        wire_bytes: int,
        *,
        attempt_id: str = "",
        group_id: str = "",
        policy_hook: str | None = None,
    ):
        message = self.build_message(state.node_id, recipient, kind, body, wire_bytes,
                                     attempt_id=attempt_id, group_id=group_id)
        delay = 0.0
        if policy_hook is not None:
            action = self._apply_policy(state, policy_hook,
                                        {"recipient": recipient, "wire_bytes": wire_bytes,
                                         "group_id": group_id, "kind": kind},
                                        {"delay_seconds": 0.0})
            delay = float(action.get("delay_seconds", 0.0))
            if delay < 0:
                raise ValueError("policy delay cannot be negative")
        def send_now():
            if not state.active:
                return
            if kind == "reduce_input":
                hook = "input_transfer"
                local = {"recipient": recipient, "wire_bytes": wire_bytes, "group_id": group_id}
                action = self._apply_policy(state, hook, local, {"delay_seconds": 0.0})
                extra = float(action.get("delay_seconds", 0.0))
                if extra > 0:
                    self.engine.schedule(extra, self._network_send, message)
                    return
            elif kind == "reduce_result":
                hook = "result_transfer"
                local = {"recipient": recipient, "wire_bytes": wire_bytes, "group_id": group_id}
                action = self._apply_policy(state, hook, local, {"delay_seconds": 0.0})
                extra = float(action.get("delay_seconds", 0.0))
                if extra > 0:
                    self.engine.schedule(extra, self._network_send, message)
                    return
            self._network_send(message)
        if delay > 0:
            self.engine.schedule(delay, send_now)
        else:
            send_now()
        return message

    def build_message(
        self,
        sender: str,
        recipient: str,
        kind: str,
        body: dict,
        wire_bytes: int,
        *,
        attempt_id: str = "",
        group_id: str = "",
    ) -> Message:
        """Create a uniquely identified protocol message for a local sender."""
        state = self.states[sender]
        state.message_sequence += 1
        return Message(f"{sender}:{state.message_sequence}", sender, recipient, kind,
                       body, int(wire_bytes), attempt_id, group_id)

    def send_group_message(self, node_id: str, message: Message, *, policy_hook: str | None = None):
        """Send an all-reduce stream message through this participant's policy hook."""
        state = self.states.get(node_id)
        if state is None or not state.active:
            return None
        if policy_hook is None and message.kind == "reduce_input":
            policy_hook = "input_transfer"
        elif policy_hook is None and message.kind == "reduce_result":
            policy_hook = "result_transfer"
        if policy_hook is not None:
            local = {"recipient": message.recipient, "wire_bytes": message.wire_bytes,
                     "group_id": message.group_id, "kind": message.kind}
            action = self._apply_policy(state, policy_hook, local, {"delay_seconds": 0.0})
            delay = float(action.get("delay_seconds", 0.0))
            if delay < 0:
                raise ValueError("policy delay cannot be negative")
            if delay:
                self.engine.schedule(delay, self._network_send, message)
                return None
        return self._network_send(message)

    def _network_send(self, message: Message):
        """Submit a fully formed message to the shared network model."""
        deliver = lambda delivered: self.on_message(delivered.recipient, delivered)
        if message.sender == message.recipient:
            self.engine.schedule(0, deliver, message)
            return None
        return self.network.send(message, deliver)
