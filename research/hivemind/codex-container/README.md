# Containerized Codex and local Docker experiments

Run this launcher on your Arch host as your normal user. Without `--docker`, it
keeps the existing project mount and separate persistent Codex home. Docker access
is enabled explicitly; the same home volume preserves your login and sessions.

## Start with Docker access

If Docker or its host runtime is missing:

```bash
sudo pacman -Syu --needed docker crun
sudo systemctl enable --now docker.service
sudo usermod -aG docker "$USER"
```

After changing group membership, log out and back in. From the host repository
root, check the connection and resume your session:

```bash
./research/hivemind/codex-container/run.sh --docker command docker version
./research/hivemind/codex-container/run.sh --docker resume --all --cd .
```

The first Docker launch automatically builds a separate
`localhost/hivemind-codex-docker:latest` image containing the Docker client,
Compose, and Buildx. No Docker daemon runs inside the Codex container. To rebuild
later, use `./research/hivemind/codex-container/run.sh --docker build`.
`resume --all` shows sessions previously recorded under `/workspace`; `--cd .`
keeps the resumed session at the host-matching project path.

Docker mode requires a local Unix socket, defaulting to `/var/run/docker.sock`.
For a different local socket, set `CODEX_DOCKER_SOCKET=/absolute/path/docker.sock`
before the command. The launcher checks socket type/permissions and checks daemon
connectivity inside the container before starting your command.

Only this repository, a Podman-managed Codex home volume, and the socket are
mounted. Docker mode also mounts the **same repository** at its canonical host
path and uses that as the working directory: Docker resolves Compose bind sources
on the daemon host. Run experiments using relative project paths from that
directory; `/workspace` remains an alias unless that would overlap the canonical
project mount (a host checkout below `/workspace`). Host home directories,
other projects, host PID/network namespaces, and privileged mode are not added.

**The host Docker socket does not enforce a restriction to project files.** The
agent must not request Docker bind mounts, reads, or directory listings outside
this project. Rootful daemon access carries host root permissions. See
[Docker daemon security](https://docs.docker.com/engine/security/),
[bind-source resolution](https://docs.docker.com/engine/storage/bind-mounts/), and
[Podman supplementary groups](https://docs.podman.io/en/latest/markdown/podman-run.1.html#group-add-group-keep-groups).

## Initial Exo audit

Inside the restarted Docker-enabled session, use the
[three-peer audit](../../exo/audit/README.md). These peers have no models or
workers and use a private Docker bridge. First check prerequisites:

```bash
python3 research/exo/audit/run.py preflight
```

Then build and run with distinct, unused evidence labels:

```bash
python3 research/exo/audit/run.py build --label exo-audit-build-20261006-002
python3 research/exo/audit/run.py run --label exo-audit-20261006-002 --seed 1901
```

The existing live session cannot acquire a new host mount retroactively: restart
through the launcher on the host. Script checks with mock utilities do not
validate the image build, actual socket access, or Exo runtime.

Launcher checks (10 mock tests passed during setup):

```bash
python3 -B -m unittest discover -s research/hivemind/codex-container/tests -v
```

## Socket mounted, but Codex reports permission denied

If the launcher's Docker connection check succeeds but agent commands fail, use
`/status` and `/permissions` in the active Codex CLI to inspect its execution
permissions. A session with networking disabled can reject Unix socket connects
even when the socket's file permissions allow access. Enable an appropriate
Docker socket permission profile or Full access inside this Podman session if
your client policy permits it. See the official
[permissions documentation](https://learn.chatgpt.com/docs/permissions) and
[sandbox guidance](https://learn.chatgpt.com/docs/agent-approvals-security).

If the session policy cannot allow Docker, run the existing bounded audit directly
from your **host terminal**, without starting Codex inside the helper container:

```bash
./research/hivemind/codex-container/run.sh --docker command python3 research/exo/audit/run.py preflight
./research/hivemind/codex-container/run.sh --docker command python3 research/exo/audit/run.py build --label exo-audit-build-20261006-003
./research/hivemind/codex-container/run.sh --docker command python3 research/exo/audit/run.py run --label exo-audit-20261006-003 --seed 1901
```

Proceed to each command only after the previous one succeeds. Use fresh labels
on retries. Results stay under this project's `research/exo/results/` for review.
