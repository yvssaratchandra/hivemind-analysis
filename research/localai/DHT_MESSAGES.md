# LocalAI: short private DHT control-message capture

Goal: identify DHT coordination messages during startup, advertisement, maintenance and restart, alongside the already captured EdgeVPN ledger gossip. Two model-free LocalAI peers and one diagnostic DHT server are sufficient.

## Implementation status

| Step | Status |
| --- | --- |
| Inspect pinned DHT discovery path | Complete: provider advertisements/lookups; mDNS remains enabled |
| Add endpoint DHT request instrumentation | Complete: inbound request hook; normal DHT sender retained |
| Run bounded private capture | Complete: two LocalAI peers and one diagnostic server, including restart |
| Classify messages and preserve evidence | Complete: raw JSONL, source archive, configuration and analysis retained |

LocalAI revision: `a3d555653c0cbfd73ab59ab15beab80903e96484`; EdgeVPN `v0.34.0`; libp2p Kademlia DHT `v0.40.0`.

The lab uses an internal Docker bridge, explicit private bootstrap addresses, no host mounts or published ports, no models and no inference requests. The diagnostic peer explicitly serves DHT requests; unmodified LocalAI peers retain their automatic DHT mode. LocalAI also enables mDNS, so successful peer discovery or ledger convergence alone cannot establish that DHT caused it.

The endpoint request hook records incoming decoded DHT protobuf messages. Saved bytes are re-encodings and labeled as such; responses, outbound queries and exact encrypted or length-delimited frames are outside its coverage. The normal DHT sender retains its existing timeouts and retries. This is one diagnostic endpoint's request inventory, not a network-wide tap.

## Measured messages

Run **passed**, 2026-10-07 14:32:29–14:35:10 UTC. The observer captured **108 DHT requests**, all using `/ipfs/kad/1.0.0`:

| Request | Count | Purpose | First raw example |
| --- | ---: | --- | --- |
| `FIND_NODE` | 50 | Find peers near a routing key; bootstrap/refresh and provider placement lookups | `observer.jsonl:23` |
| `ADD_PROVIDER` | 29 | Advertise a peer's ID and addresses under a rendezvous key | `observer.jsonl:52` |
| `GET_PROVIDERS` | 29 | Request peers advertising the rendezvous key | `observer.jsonl:53` |

Counts by observed lifecycle phase:

| Phase | `FIND_NODE` | `ADD_PROVIDER` | `GET_PROVIDERS` |
| --- | ---: | ---: | ---: |
| Bootstrap: first peer | 6 | 2 | 2 |
| Second peer joins / advertises | 13 | 8 | 8 |
| Steady maintenance | 11 | 11 | 11 |
| One peer crashes; survivor continues | 5 | 2 | 2 |
| Restart / rejoin | 15 | 6 | 6 |
| Shutdown | 0 | 0 | 0 |

Advertisements and queries continue after admission; they are not exclusively join messages. All 29 provider advertisements used the same rendezvous key and carried a provider ID matching the sending peer. Addresses included container IPv4/TCP addresses and loopback. Restart produced a new libp2p identity: three distinct LocalAI identities were seen over the run, with only two service peers running concurrently. No `PUT_VALUE`, `GET_VALUE` or `PING` request was observed at this endpoint.

The observer also received **58 ledger gossip envelopes** containing `services`, `<NetworkID>_federated`, `healthcheck`, and later `shared_state` model metadata (`[]`). It captured **90 mDNS packets** and six connection callbacks. Thus, in this inspected discovery path, **DHT locates peers; application service, group, liveness and model metadata travel in separate ledger gossip**. Provider receipt alone does not prove storage, a successful lookup response, or acceptance by other peers. See the [pinned discovery path](https://github.com/mudler/edgevpn/blob/v0.34.0/pkg/discovery/dht.go) and [LocalAI settings](https://github.com/mudler/LocalAI/blob/a3d555653c0cbfd73ab59ab15beab80903e96484/core/p2p/p2p.go).

## Controls and limits

Both live-peer APIs passed the full fresh-membership checks after join, steady observation and restart. The 15-second crash window was too short for the 40-second freshness threshold: the survivor still listed the departed peer as fresh. This does not establish an explicit DHT leave message or provider-record expiry; neither was tested. The steady window was 54 seconds, excluding the longer heartbeat cleanup cycle.

All three configured/loaded-model checks were empty. Maximum sampled Docker RAM was **74.2 MiB / 68.2 MiB** for the two LocalAI peers and **29.5 MiB** for the observer; lowest host available RAM was **18.5 GiB**. These are model-free measurements. No OOM, capture/decode/hash errors, or cleanup failures occurred. All lab containers and networks were removed; the upstream clone is unchanged. Focused checks: **39 Python tests and 10 Go tests passed**.

Two early attempts stopped during token generation with `--network none`; LocalAI's hardcoded mDNS requires a multicast-capable interface. [Diagnostic logs](results/dht-token-network-none-diagnostic-20261007T1432/token-helper-failure-logs.json) preserve the fatal `no supported interface` error. The completed runner generates its token with DHT disabled on the fresh internal bridge and removes that helper before measurement. Failed attempts remain separately labeled and are excluded from the counts above.

Evidence: [analysis](results/dht-messages-private-20261007T1433/dht-analysis.json), [raw observer trace](results/dht-messages-private-20261007T1433/observer.jsonl), [phase log](results/dht-messages-private-20261007T1433/phases.jsonl), [API/RAM samples](results/dht-messages-private-20261007T1433/samples.jsonl), and [run summary](results/dht-messages-private-20261007T1433/summary.json). The summary pins images, token hash and archived instrumentation. This private LAN experiment does not establish WAN behavior or DHT-only discovery because LocalAI also enables mDNS and direct bootstrap gossip connections.

## Repeat

```sh
docker build -t localai-control-observer:dht-edgevpn034-a3d5556 research/localai/control-observer
python3 research/localai/run_dht_messages.py --label dht-repeat-001
python3 research/localai/analyze_dht_messages.py research/localai/results/dht-repeat-001
```
