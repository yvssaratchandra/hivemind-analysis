## Issue Description
A leader can accept a follower and then send nothing. The follower waits, gives up, retries, and gets stalled again, so it spends its whole averaging step without ever averaging.
Component: hivemind.averaging
Version: 1.2.0.dev0 (Fork point: `4bd43b7`)
Effect: denial of averaging against a chosen peer, at the cost of one identity and no gradients. This is Deny, not capture: the attacker gains nothing, the victim just loses the round.

## Expected Behavior
A leader that accepts a follower and then goes silent should cost that follower roughly what an outright refusal costs, so that stalling is not cheaper for the attacker than being honest. Instead a refusal costs the follower `request_timeout` (3 s default) and it can go find another leader, while a stall costs it its own full matchmaking window.

## Related code
An accepted follower waits here:

```python
# hivemind/averaging/matchmaking.py:216
message = await asyncio.wait_for(anext(stream), time_to_expiration + self.request_timeout)
```

`time_to_expiration` comes from `get_request_expiration_time()`, which is the follower's **own**
declared expiration, i.e. its own `min_matchmaking_time`. So the wait is set by the victim, not by
the attacker. The attacker cannot lengthen it. It does not need to, because it can simply do it again
on the next attempt.

The stream has to stay **open**. Returning from the handler closes it, `anext` raises
`StopAsyncIteration`, and the follower fails fast (`:241`) — the opposite of a stall. Holding it open
is the whole attack.

## Steps to Reproduce
```
.venv/bin/python research/step9_stall.py --no-attack   # baseline
.venv/bin/python research/step9_stall.py               # stall
```

One attacker and one honest averager. The attacker wins leadership by declaring the earlier deadline
(`min_matchmaking_time` 6 s against the honest peer's 20 s), then forwards `ACCEPTED` and withholds
everything after it:

```python
async for response in super().rpc_join_group(request, context):
    yield response
    if self._stall and response.code == averaging_pb2.ACCEPTED:
        await asyncio.sleep(600)   # do not return: returning closes the stream
```

Predicted stall = follower's `min_matchmaking_time` + `request_timeout` = 20 + 3 = 23 s.

## Result
Baseline, leader honest:
```
honest peer 0: waited   0.03s  ->  AVERAGED
```

Stall:
```
honest peer 0: waited 120.04s  ->  no group: AllreduceException

leader ACCEPTED 12D3KooW9wQXvqVS...
  -> follower held on for 23.00s before giving up (predicted 23.0s)
  -> follower held on for 22.98s before giving up (predicted 23.0s)
  -> follower held on for 22.97s before giving up (predicted 23.0s)
  -> follower held on for 22.97s before giving up (predicted 23.0s)
  -> follower held on for 22.97s before giving up (predicted 23.0s)
  -> follower held on for  4.96s before giving up (predicted 23.0s)
```

Measured 22.97 to 23.00 s against a predicted 23.0 s, so the timeout law holds exactly: the victim's
patience is its own `min_matchmaking_time + request_timeout`.

The part that matters is the repetition. Giving up on a stalling leader does not help, because the
victim goes straight back to `pop_next_leader`, picks the same attacker again (it still holds the
earliest declared deadline) and is stalled again. Five full stalls plus one truncated by the step
deadline consumed the entire 120 s step. Against a baseline that averaged in 0.03 s.

## Notes
Cost to the attacker: one identity, one accepted stream, and a sleep. No Sybils, no crafted
gradients, no DHT work.

Not yet tested, and the report should not be read as covering them: whether refusal really is cheaper
for the victim than stalling (the control that decides whether this is a finding at all); whether
`pause_search` (`:214`) also silences the victim's own advertisement while it waits; and the harm
over a real training run rather than a single step.

Gotcha for anyone extending this: the RPC handler name embeds the servicer **class name**, so a swarm
mixing `DecentralizedAverager` with a subclass fails to connect in *both* directions with
`protocols not supported: [<prefix>::DecentralizedAverager.rpc_join_group]`. That presents as a
matchmaking failure, not a naming one. Every peer has to be the same class, with behaviour switched
by a flag.
