# Record-holder exclusion smoke: rh_excl_20261021_a

Status: **complete**

Two private loopback arms use the same controlled-holder filtering, X leadership policy, and X admission hold. The exclusion arm additionally removes A from X's own roster and from announcements to D/C. Each channel uses a fixed unsharded native averaging key and 192-coordinate markers reset before every round.

| Arm | Channel | Step successes / attempts | X announcements with all four native peers | D/C complete effective means | D/C zero A-origin outputs | Holder Find requests | Actual filtered holder responses |
|---|---|---:|---:|---:|---:|---:|---:|
| capture_control | gradient_probe | 12 / 12 | 3 | 6 | 0 | 1892 | 351 |
| capture_control | state_probe | 12 / 12 | 3 | 6 | 0 | 1890 | 338 |
| capture_exclusion | gradient_probe | 12 / 12 | 3 | 6 | 6 | 1660 | 214 |
| capture_exclusion | state_probe | 12 / 12 | 3 | 6 | 6 | 1707 | 223 |

The DHT filter is installed only in the dedicated local holder process and checks the exact channel key plus authenticated requester PeerID. The DHT-only identities at XOR distances 0–19 make a fixed nearest-first fixture; the first five are the designed replicas under the recorded five-replica setting. Raw holder responses distinguish stored values from cached values, and requests from each participant establish which controlled identities actually answered.

Read D/C outcomes only when a successful native step, a full four-peer native group, and its actual effective roster are all recorded. A zero A coefficient is insufficient by itself: each complete D/C output is also checked coordinate by coordinate against the expected equal-weight mean of the effective roster's fresh inputs. No-group attempts, errors, malformed outputs, and incomplete groups remain in the denominator.

Scope: native decentralized averagers with fixed keys, default DHT caching, local loopback, and a cold prepositioned holder topology. This does not measure acquiring holder control, training convergence, scheduled Optimizer state averaging, recovery, WAN behavior, or public-swarm success rates.
