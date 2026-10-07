# LocalAI control observer

This small diagnostic joins a **private LocalAI shared-token EdgeVPN network** and records control-plane evidence. It uses EdgeVPN v0.34.0 and libp2p v0.48.0, matching LocalAI commit `a3d555653c0cbfd73ab59ab15beab80903e96484`.

It logs inbound, decrypted EdgeVPN gossip messages before EdgeVPN applies them to its ledger. Each `gossip_message` JSONL event includes the observer ID, the peer ID EdgeVPN reports as the immediate Gossipsub delivery source, UTC receive time, SHA-256 of the exact decrypted compressed body, the exact body as base64, and the complete decompressed block payload when it fits the safety limit. The payload's block index/hash form a stable block identity. EdgeVPN v0.34.0 overwrites its message sender field with Gossipsub `ReceivedFrom`; that ID may identify a relay, not the original block publisher.

The observer also reports listen-address and peer connection open/close events. libp2p v0.48.0's public `Network.Notify` callbacks do not include stream open/close callbacks, so this build cannot log per-stream events. By default it captures UDP/5353 packets with `AF_PACKET` on the observer container's `eth0`, and decodes mDNS DNS questions and resource records. This needs `CAP_NET_RAW` (included in Docker's default capability set); if unavailable, it emits `packet_capture_error` and continues recording gossip. Packet capture is limited to the observer container's own network namespace and UDP/5353.

The default observer does not register a LocalAI model, worker, service, or health endpoint. It keeps DHT off and mDNS on, matching the prior diagnostic build. EdgeVPN's `Node.Start` always starts periodic ledger synchronization, so the observer publishes its own genesis/current ledger snapshot about every 10 seconds. That is an intentional participation effect; `observer_started` records the observer peer ID and this behavior. It does not announce an `alive` service record.

An opt-in diagnostic mode enables EdgeVPN's Kademlia DHT and installs the pinned `go-libp2p-kad-dht` inbound request hook. For inbound requests, each `dht_message` records the decoded operation, exact key bytes and hash, closer/provider peer metadata, remote peer and protocol, and the canonical protobuf body re-encoded from the hook's message. The public hook does not expose the original length-delimited wire frame or the response generated after the hook; the JSON field `capture_fidelity=decoded_protobuf_message_reencoded_by_hook` makes that limit explicit. This first-pass observer does not claim to capture all network DHT messages, outbound queries, or server replies.

Use `--dht=true --mdns=false --dht-mode=server` to run the diagnostic observer as a DHT server while the LocalAI peers keep their normal `ModeAuto` behavior. The forced server mode and private-address allowance apply only to this diagnostic observer. When no `--bootstrap-peer` is provided, the observer inserts a generated loopback sentinel into EdgeVPN's bootstrap list so EdgeVPN v0.34.0 does not substitute its public bootstrap peers; the Kademlia bootstrap list remains explicitly empty. The sentinel is not a real peer and only causes a harmless local connection attempt to `127.0.0.1:1`. The observer emits `bootstrap_policy=private_sentinel_no_external_bootstrap` so this setup is visible. Optional `--bootstrap-peer` flags must each name a literal private or loopback IP, TCP or UDP/QUIC multiaddr, and valid `/p2p/<peer-id>`; public and DNS bootstrap addresses are rejected. A supplied private bootstrap list is used by both EdgeVPN discovery and Kademlia.

## Build

Build the image from this directory. The build stage pins all modules using `go.sum`; the final image is a static binary in `scratch`.

```sh
docker build -t localai-control-observer:edgevpn034-a3d5556 .
docker build -t localai-control-observer:dht-edgevpn034-a3d5556 .
```

Run the focused unit tests in the pinned Go builder stage:

```sh
docker build --target test -t localai-control-observer:test .
```

To generate or refresh `go.sum` without installing Go on the host, first build the `tidy` stage and copy its generated checksums out:

```sh
docker build --target tidy -t localai-control-observer:tidy .
container_id=$(docker create localai-control-observer:tidy)
docker cp "$container_id":/src/go.mod ./go.mod
docker cp "$container_id":/src/go.sum ./go.sum
docker rm "$container_id"
docker build -t localai-control-observer:edgevpn034-a3d5556 .
```

## Run on a private test network

Pass the shared token through `LOCALAI_P2P_TOKEN`; it is never written to logs. Use the same token as the LocalAI test peers, and attach the observer only to the experiment's internal bridge. Do not publish ports or mount host paths.

```sh
docker run --rm \
  --network <private-internal-bridge> \
  --name localai-control-observer \
  -e LOCALAI_P2P_TOKEN \
  localai-control-observer:edgevpn034-a3d5556
```

For a short DHT-enabled capture, build and run the separate opt-in tag on the same private internal Docker bridge as the LocalAI test peers. `peer_multiaddr` in the observer's `listen_address` events includes `/p2p/<observer-peer-id>` and can be passed to LocalAI peers as their private bootstrap address. With no `--bootstrap-peer`, the observer uses the loopback sentinel behavior described above and does not fall back to public bootstrappers.

```sh
docker run --rm \
  --network <private-internal-bridge> \
  --name localai-control-observer-dht \
  -e LOCALAI_P2P_TOKEN \
  localai-control-observer:dht-edgevpn034-a3d5556 \
  --dht=true --mdns=false --dht-mode=server --run-for=120s
```

`LOCALAI_P2P_LISTEN_MADDRS` can override the ephemeral TCP/QUIC listen addresses. The mDNS packet capture can be disabled with `--capture-mdns=false`, or pointed at another interface with `--interface eth0`. `--run-for 15m` is useful for a bounded capture; the default runs until SIGTERM/SIGINT. stdout is JSONL; EdgeVPN/library diagnostics and startup errors go to stderr.

The main event categories are:

| Category | Events | Meaning |
| --- | --- | --- |
| `startup` | `observer_started`, `listen_address` | Observer identity and active listen addresses |
| `peer_discovery` | `mdns_packet`, `packet_capture_started`, `packet_capture_error` | LAN discovery questions and answers |
| `connectivity` | `peer_connection` | libp2p peer connect/disconnect metadata |
| `ledger_sync` | `gossip_message` | Decrypted EdgeVPN ledger block gossip received from other peers |
| `dht_rpc` | `dht_message`, `dht_rpc_error` | When `--dht=true`, decoded inbound Kademlia requests seen by the observer's server hook |
| `shutdown` | `observer_stopping`, closed `listen_address` | Observer termination |

`message_base64` is the exact decrypted, gzip-compressed EdgeVPN message body, not ciphertext. `block_payload` is the full decompressed JSON block. A body that exceeds `--max-decompressed-bytes` is marked `payload_truncated`; the default is 64 MiB. This cap protects the observer from an unexpectedly large compressed body.
