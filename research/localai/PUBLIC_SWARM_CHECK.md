# LocalAI public P2P population check

Checked **2026-10-07, 07:06 UTC** using passive public HTTP GETs from the research container. **Current public peer population: unknown.** No P2P network was joined and no inference was requested.

| Official endpoint | Observed response |
| --- | --- |
| `https://localai.io/` (connectivity control) | HTTP 200 |
| `https://explorer.localai.io/` | HTTP 521, Cloudflare |
| `https://explorer.localai.io/networks` | HTTP 521, Cloudflare |
| `https://demo.localai.io/` | HTTP 530; body `error code: 1033` |

These observations establish endpoint unavailability from this environment at this time, **not zero peers**, permanent shutdown, or global unavailability.

The [official August 2024 announcement](https://github.com/mudler/LocalAI/discussions/3363) advertised community pools and a public demo. [Current documentation](https://localai.io/docs/features/distribute/index.print.html) still points to the same explorer. Searches for a replacement directory or recent public worker counts found no verifiable alternative. Historical announcements establish a feature, not its current adoption.

Source inspection at commit `5537f4b1efbe4ec65702be0b80cff6e6b0875ea5`:

- [`GET /networks`](https://github.com/mudler/LocalAI/blob/5537f4b1efbe4ec65702be0b80cff6e6b0875ea5/core/http/routes/explorer.go) serves a directory of separate submitted networks, not one universal public swarm.
- [Directory handler](https://github.com/mudler/LocalAI/blob/5537f4b1efbe4ec65702be0b80cff6e6b0875ea5/core/http/endpoints/explorer/dashboard.go) lists stored networks with worker entries; it does not freshly test workers during a GET.
- [Discovery code](https://github.com/mudler/LocalAI/blob/5537f4b1efbe4ec65702be0b80cff6e6b0875ea5/core/explorer/discovery.go) periodically joins registered networks and collects advertised online worker IDs from their ledgers. Failed checks increment a counter and eventually remove networks. Even a functioning directory would provide cached advertised worker entries, not a verified count of distinct physical hosts or responsive inference workers.

**Research decision:** retain LocalAI as a candidate for local control-plane experiments; do not rank it as a demonstrated large, active public P2P deployment without a working directory or operator evidence. The next useful evidence is a functioning registry with timestamped counts and confirmation of an operator's public research scope.

## Alternative publication search (2026-10-07)

No alternative maintained pool directory or explicit current public joining token was found in the material inspected:

| Place checked | Finding |
| --- | --- |
| [GitHub announcement](https://github.com/mudler/LocalAI/discussions/3363), [pool issue](https://github.com/mudler/LocalAI/issues/3113), and repository issue/PR searches | Historical feature descriptions point back to the explorer; no alternate listing found. |
| Maintainer's [r/LocalLLaMA announcement and replies](https://www.reddit.com/r/LocalLLaMA/comments/1f3f1qj/) and [r/selfhosted announcement](https://www.reddit.com/r/selfhosted/comments/1f3f2w1/) | August 2024 posts refer to the same explorer; the maintainer describes the demo's P2P pool but directs joiners to the explorer. No separate usable joining information found. |
| Web searches for public swarms, shared tokens, pool tokens, and joining invitations | Primarily documentation and repeated release announcements; no verifiable current pool invitation found. |
| [Official Discord](https://discord.gg/uJAeKSAGDy), linked in the [README](https://github.com/mudler/LocalAI/blob/5537f4b1efbe4ec65702be0b80cff6e6b0875ea5/README.md) | Possible next place to ask operators; channel history was not searched and nobody was contacted. |

Search coverage is limited to accessible/indexed pages and GitHub issue/PR search, not all Discord messages or private communities. Publicly indexed LocalAI web UIs are not evidence that operators have offered their P2P networks for public use; their token APIs were not queried.

Evidence: [`results/public-directory-20261007-001/http_checks.json`](results/public-directory-20261007-001/http_checks.json), including status, response headers, body excerpts, and timestamps. Probe: [`check_public_directory.py`](check_public_directory.py). Successful directory responses are summarized without retaining network tokens.

Repeat with a **new output directory**:

```sh
python3 research/localai/check_public_directory.py research/localai/results/public-directory-YYYYMMDD-002
```
