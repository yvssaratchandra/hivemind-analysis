# Two local Exo peers with Docker

Runs two real Exo processes on one Linux machine, each with its own network namespace, IPv6 interface, random node ID, and state volume. They use Exo's unmodified discovery, Zenoh transport, and election code. The shared Docker bridge is private; the processes share the host's CPU and RAM.

This first setup runs routing, election, and the master with `--no-worker --no-downloads --no-api --offline`. It loads no model and serves no dashboard/API. Each peer has a **2 GiB memory cap**, configurable with `EXO_PEER_MEMORY`; this is a chosen budget, not measured Exo usage. Image compilation is outside that runtime budget.

**Validation status:** the shared image, IPv6 discovery, and election convergence were validated in the [three-peer model-free audit](../audit/README.md) on local Exo `21a54c5e`, with two honest peers and a bounded Router publisher. That audit ran with the API enabled. This standalone two-peer `--no-api` recipe has not been run, so its exact startup path remains unverified. The inspected image is 4.595 GiB logical size; the peer runtime cap remains 2 GiB each.

## Start

Requires Linux Docker Engine with IPv6 support and Docker Compose v2. Image building needs Internet access for Python and Rust dependencies. Both peers then run on an internal network. From this repository's root:

```bash
cd research/exo/two-peer
export EXO_REVISION="$(git -C ../../../exo rev-parse HEAD)"
docker compose -p exo-baseline-001 config --quiet
docker compose -p exo-baseline-001 build peer-a
docker compose -p exo-baseline-001 up -d --no-build
docker compose -p exo-baseline-001 logs -f
```

`peer-b` reuses the image built for `peer-a`. All peers use the same Exo namespace. Their separate network namespaces let both listen on TCP 52414 and UDP 52413; neither port is published to the host. Separate state volumes avoid the PID-file collision of two ordinary same-user processes.

## Establish a connected baseline

Two running containers alone do not prove a connected swarm. In each peer's logs, record its `Starting node <id>` line. Confirm that:

1. The node IDs differ.
2. Each log contains `discovered: Some("<other-id>")`, followed by election messages involving that peer. Ignore discovery notices for the process's own ID.
3. After mutual discovery, the latest `Current session:` or `Election finished, new SessionId(...)` lines converge to the same master ID and session election clock. One peer maintains/promotes itself as master; the other follows/demotes. The initial solo-master lines at startup are insufficient evidence.

Read either peer's complete logs separately:

```bash
docker compose -p exo-baseline-001 logs --no-color peer-a
docker compose -p exo-baseline-001 logs --no-color peer-b
```

If they remain independent masters, inspect IPv6 addresses and multicast membership in both containers:

```bash
docker compose -p exo-baseline-001 exec peer-a cat /proc/net/if_inet6
docker compose -p exo-baseline-001 exec peer-b cat /proc/net/if_inet6
docker compose -p exo-baseline-001 exec peer-a cat /proc/net/igmp6
docker compose -p exo-baseline-001 exec peer-b cat /proc/net/igmp6
```

There should be non-loopback IPv6 addresses and membership in Exo's group `ff12::e0a1:de89`. Exo joins that group on eligible interfaces; successful joins do not by themselves prove multicast delivery across the bridge. The removed bootstrap-peer option cannot bypass failed discovery. If network creation reports an overlapping subnet, choose another unused ULA `/64` with `EXO_IPV6_SUBNET` before retrying.

## Preserve a run and test a failure

Use a fresh project name for each experiment; it separates networks and volumes. From the `two-peer` directory, save baseline evidence before changing either peer:

```bash
mkdir -p ../results/exo-baseline-001
git -C ../../../exo rev-parse HEAD > ../results/exo-baseline-001/source-commit.txt
git -C ../../../exo status --short > ../results/exo-baseline-001/source-status.txt
git -C ../../../exo diff > ../results/exo-baseline-001/source.patch
docker compose -p exo-baseline-001 config > ../results/exo-baseline-001/compose.yaml
docker compose -p exo-baseline-001 logs --no-color --timestamps > ../results/exo-baseline-001/baseline.log
docker image inspect exo-control-plane:local > ../results/exo-baseline-001/image.json
docker compose -p exo-baseline-001 ps -a > ../results/exo-baseline-001/containers.txt
```

Once convergence is established, choose the current master from its node ID and stop that service. For example, **only if `peer-a` is the elected master**:

```bash
docker compose -p exo-baseline-001 stop peer-a
docker compose -p exo-baseline-001 logs -f peer-b
```

Check whether the survivor elects itself; do not assume recovery succeeded. A graceful stop is a baseline failure condition, distinct from a crash or packet partition. Save post-stop logs separately. Exo generates a new node ID on restart in this checkout, even when the state volume persists.

Stop the experiment while retaining its volumes:

```bash
docker compose -p exo-baseline-001 down
```

Later experiments can add an attacker peer and controlled delay/drop rules. Model placement and inference require a worker-enabled extension; these two coordinator-only peers do not advertise normal worker resource profiles.

Sources: [Exo startup flags](../../../exo/src/exo/main.py), [IPv6 discovery](../../../exo/rust/networking/src/discovery.rs), [Docker IPv6 bridge configuration](https://docs.docker.com/engine/network/drivers/bridge/#use-ipv6-in-a-user-defined-bridge-network), [Compose network isolation](https://docs.docker.com/reference/compose-file/networks/#internal).
