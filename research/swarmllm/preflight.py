#!/usr/bin/env python3
"""Record local resources and tool availability without launching peers."""
import datetime
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys


def snapshot():
    memory = {}
    for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
        key, value = line.split(':', 1)
        if key in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree'):
            memory[key] = int(value.split()[0]) * 1024
    limits = {}
    for name in ('memory.max', 'memory.current', 'cpu.max', 'memory.swap.max'):
        path = pathlib.Path('/sys/fs/cgroup') / name
        if path.exists():
            limits[name] = path.read_text().strip()
    result = {
        'checked_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'architecture': platform.machine(),
        'memory_bytes': memory,
        'visible_cgroup': limits,
        'logical_cpu_count': os.cpu_count(),
        'workspace_disk_free_bytes': shutil.disk_usage('.').free,
        'tools': {name: shutil.which(name) for name in ('docker', 'podman', 'rustc', 'cargo', 'uv', 'python3')},
        'limitations': 'Host-visible memory is not a guarantee of process allowance. No peer started; no per-node memory measured.',
    }
    if result['tools']['docker']:
        try:
            command = subprocess.run(['docker', 'version', '--format', '{{json .}}'],
                                     capture_output=True, text=True, timeout=10)
            result['docker_version'] = {'exit_code': command.returncode,
                                        'stdout': command.stdout, 'stderr': command.stderr}
        except subprocess.TimeoutExpired:
            result['docker_version'] = {'error': 'Timed out after 10 seconds'}
    return result


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python3 preflight.py NEW_OUTPUT_DIRECTORY')
    output = pathlib.Path(sys.argv[1])
    output.mkdir(parents=True, exist_ok=False)
    result = snapshot()
    (output / 'preflight.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
