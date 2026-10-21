"""Research-only discovery/leader policy on actual Optimizer averaging prefixes."""
from __future__ import annotations

import asyncio
import time

CONDITIONS = ('baseline', 'capture_control', 'gradient_exclusion', 'dual_exclusion')


def attacked_prefixes(condition, gradient_prefix, state_prefix):
    if condition not in CONDITIONS:
        raise ValueError(f'Unknown condition: {condition}')
    return ({gradient_prefix, state_prefix} if condition == 'dual_exclusion' else
            {gradient_prefix} if condition == 'gradient_exclusion' else set())


def install_capture_policy(*, condition, gradient_prefix, state_prefix, identities, emit):
    from hivemind.averaging.averager import DecentralizedAverager
    from hivemind.averaging.group_info import GroupInfo
    from hivemind.averaging.key_manager import GroupKeyManager
    from hivemind.averaging.matchmaking import Matchmaking
    from hivemind.p2p import PeerID
    from hivemind.proto import averaging_pb2
    from compute_native_hooks import _filter_aligned_roster

    attacked = attacked_prefixes(condition, gradient_prefix, state_prefix)
    channels = {gradient_prefix: 'gradient', state_prefix: 'state'}
    originals = []

    def label(peer):
        return identities.get(str(peer), str(peer))

    def prefix(obj):
        return str(getattr(obj, '_prefix', getattr(obj, 'prefix', '')))

    def record(kind, p, **fields):
        emit(kind, channel=channels.get(p, 'other'), prefix=p, **fields)

    def patch(owner, method, replacement):
        original = getattr(owner, method)
        replacement.__annotations__ = dict(getattr(original, '__annotations__', {}) or {})
        originals.append((owner, method, original, replacement))
        setattr(owner, method, replacement)

    get_averagers = GroupKeyManager.get_averagers

    async def candidates(self, group_key, only_active):
        result = await get_averagers(self, group_key, only_active)
        p = prefix(self)
        if p in channels:
            record('capture_candidates', p, peer=label(self.peer_id), group_key=group_key,
                   only_active=only_active, candidates=[label(peer) for peer, _ in result])
        return result

    patch(GroupKeyManager, 'get_averagers', candidates)

    leader_assemble = Matchmaking.leader_assemble_group
    follower_assemble = Matchmaking.follower_assemble_group

    def remember(self, group, leader):
        if not hasattr(self, '_capture_leaders'):
            self._capture_leaders = {}
        self._capture_leaders[group.group_id] = leader
        p = prefix(self)
        if p in channels:
            record('capture_assembly', p, peer=label(self.peer_id), leader=label(leader),
                   group_id=group.group_id.hex(), members=[label(peer) for peer in group.peer_ids])

    async def assembled_leader(self):
        group = await leader_assemble(self)
        remember(self, group, self.peer_id)
        return group

    async def assembled_follower(self, leader, message):
        group = await follower_assemble(self, leader, message)
        remember(self, group, leader)
        return group

    patch(Matchmaking, 'leader_assemble_group', assembled_leader)
    patch(Matchmaking, 'follower_assemble_group', assembled_follower)
    rpc_join = Matchmaking.rpc_join_group

    async def join(self, request, context):
        p, sender, recipient = prefix(self), label(self.peer_id), label(context.remote_id)
        if condition != 'baseline' and p in channels and sender == 'X' and recipient in ('D', 'C'):
            until = time.monotonic() + 3.5
            while not any(label(peer) == 'A' for peer in self.current_followers):
                if time.monotonic() >= until:
                    record('capture_admission', p, peer=sender, recipient=recipient,
                           admitted=False, reason='A_missing_at_hold_deadline')
                    yield averaging_pb2.MessageFromLeader(code=averaging_pb2.GROUP_IS_FULL)
                    return
                await asyncio.sleep(.01)
        async for message in rpc_join(self, request, context):
            if message.code == averaging_pb2.BEGIN_ALLREDUCE and p in channels:
                ids = tuple(PeerID(raw) for raw in message.ordered_peer_ids)
                gathered = tuple(message.gathered)
                native_members = [label(peer) for peer in ids]
                native_payload = message.SerializeToString().hex()
                native_ids = [peer.to_bytes().hex() for peer in ids]
                native_data = [blob.hex() for blob in gathered]
                removed = False
                if p in attacked and sender == 'X' and recipient != 'A':
                    ids, gathered, removed = _filter_aligned_roster(ids, gathered, 'A', label)
                    message = averaging_pb2.MessageFromLeader(
                        code=message.code, group_id=message.group_id,
                        suggested_leader=message.suggested_leader,
                        ordered_peer_ids=[peer.to_bytes() for peer in ids], gathered=gathered)
                record('capture_announcement', p, sender=sender, recipient=recipient,
                       group_id=message.group_id.hex(), native_members=native_members,
                       members=[label(peer) for peer in ids], omitted=removed,
                       native_payload_hex=native_payload, payload_hex=message.SerializeToString().hex(),
                       native_peer_ids_hex=native_ids, native_gathered_hex=native_data,
                       peer_ids_hex=[peer.to_bytes().hex() for peer in ids],
                       gathered_hex=[blob.hex() for blob in gathered])
            yield message

    patch(Matchmaking, 'rpc_join_group', join)
    aggregate = DecentralizedAverager._aggregate_with_group

    async def aggregate_group(self, group, min_vector_size, **kwargs):
        p = prefix(self)
        leader_peer = getattr(self._matchmaking, '_capture_leaders', {}).pop(group.group_id, None)
        leader = label(leader_peer) if leader_peer is not None else None
        native_members = [label(peer) for peer in group.peer_ids]
        removed = False
        if p in attacked and label(self.peer_id) == leader == 'X':
            ids, gathered, removed = _filter_aligned_roster(group.peer_ids, group.gathered, 'A', label)
            group = GroupInfo(group.group_id, ids, gathered)
        if p in channels:
            record('capture_local_roster', p, peer=label(self.peer_id), leader=leader,
                   group_id=group.group_id.hex(), native_members=native_members,
                   members=[label(peer) for peer in group.peer_ids], omitted=removed)
        return await aggregate(self, group, min_vector_size, **kwargs)

    patch(DecentralizedAverager, '_aggregate_with_group', aggregate_group)

    def uninstall():
        for owner, method, original, replacement in reversed(originals):
            if getattr(owner, method) is replacement:
                setattr(owner, method, original)

    return uninstall
