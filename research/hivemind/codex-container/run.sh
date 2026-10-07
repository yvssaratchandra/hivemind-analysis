#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_dir=$(cd -- "$script_dir/../../.." && pwd -P)
image=localhost/hivemind-codex:latest
home_volume=hivemind-codex-home
docker_enabled=false

if [[ ${1:-} == --docker ]]; then
    docker_enabled=true
    image=localhost/hivemind-codex-docker:latest
    shift
fi

if [[ $(id -u) == 0 ]]; then
    echo 'Run this launcher as your normal user, without sudo.' >&2
    exit 1
fi

build_image() {
    # This context contains only the container setup, not repository contents.
    podman build --tag "$image" --file "$script_dir/Containerfile" "$script_dir"
}

case "${1:-}" in
    build)
        build_image
        exit
        ;;
    login)
        shift
        set -- codex login --device-auth "$@"
        ;;
    shell)
        shift
        set -- bash "$@"
        ;;
    command)
        shift
        if [[ $# == 0 ]]; then
            echo 'Usage: run.sh [--docker] command PROGRAM [ARGS...]' >&2
            exit 2
        fi
        ;;
    *)
        # The default mode uses Podman as the isolation boundary. Docker mode
        # additionally grants daemon access, including its host permissions.
        set -- codex --no-daemon --sandbox danger-full-access --ask-for-approval never "$@"
        ;;
esac

runtime_flags=()
working_dir=/workspace
project_mount="$repo_dir:/workspace:rw,Z"
if $docker_enabled; then
    docker_socket=${CODEX_DOCKER_SOCKET:-/var/run/docker.sock}
    if [[ $docker_socket != /* ]]; then
        echo 'CODEX_DOCKER_SOCKET must be an absolute Unix socket path.' >&2
        exit 2
    fi
    socket_type=$(LC_ALL=C stat -L -c %F -- "$docker_socket" 2>/dev/null || true)
    if [[ $socket_type != socket ]]; then
        echo "Docker socket unavailable: $docker_socket. Start Docker on the host first." >&2
        exit 1
    fi
    if [[ ! -r $docker_socket || ! -w $docker_socket ]]; then
        echo "Docker socket is inaccessible: $docker_socket. Check your host user's Docker permissions and log in again after changing groups." >&2
        exit 1
    fi
    if ! command -v crun >/dev/null 2>&1; then
        echo 'Docker mode requires crun on the host for --group-add keep-groups.' >&2
        exit 1
    fi
    runtime_flags+=(
        --runtime crun
        --group-add keep-groups
        --env DOCKER_HOST=unix:///var/run/docker.sock
        --volume "$docker_socket:/var/run/docker.sock:rw"
    )
    # Docker resolves bind sources on its host. Run Compose from the same
    # absolute project path while retaining /workspace as a project alias.
    working_dir=$repo_dir
    if [[ $repo_dir == /workspace/* ]]; then
        # Avoid nesting a second repo mount inside the /workspace alias.
        project_mount="$repo_dir:$repo_dir:rw,Z"
    elif [[ $repo_dir != /workspace ]]; then
        runtime_flags+=(--volume "$repo_dir:$repo_dir:rw,Z")
    fi
    # Check the connection inside the actual UID/group/mount configuration
    # before starting Codex or a requested experiment command.
    set -- bash -c '
        if ! docker info --format "{{.ServerVersion}}" >/dev/null; then
            echo "Cannot reach the host Docker daemon from this container." >&2
            exit 1
        fi
        exec "$@"
    ' docker-check "$@"
fi

if ! podman image exists "$image"; then
    build_image
fi

terminal_flags=(-i)
if [[ -t 0 && -t 1 ]]; then
    terminal_flags+=(-t)
fi

# Mount only this project, a separate persistent container home, and (when
# requested) the Docker socket. Socket permissions are broader than these binds.
# keep-id makes repository edits belong to the host user.
exec podman run --rm "${terminal_flags[@]}" \
    --userns=keep-id:uid=1000,gid=1000 \
    --user=1000:1000 \
    --cap-drop=all \
    --security-opt=no-new-privileges \
    --pids-limit=512 \
    --env HOME=/home/node \
    --workdir "$working_dir" \
    --volume "$project_mount" \
    --volume "$home_volume:/home/node:U" \
    "${runtime_flags[@]}" \
    "$image" "$@"
