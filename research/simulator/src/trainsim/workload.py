"""One independent batch per compute/coordination cycle."""
from dataclasses import asdict

from .randomness import rng
from .types import WorkItem


class Workload:
    """Own per-node work sequences; protocol completion starts fresh computation."""

    def __init__(self, engine, profiles, config, adapter):
        self.engine, self.profiles, self.config, self.adapter = engine, profiles, config, adapter
        self.sequence = {node: 0 for node in profiles}
        self.pending = {}
        self.active = True

    def start(self):
        for node in self.profiles:
            self.adapter.start(node)
            offset = rng(self.engine.seed, "readiness", node).uniform(0, self.config["initial_offset_seconds"])
            self.engine.schedule(offset, self.compute, node)

    def compute(self, node):
        if not self.active:
            return
        if node in self.pending:
            raise RuntimeError(f"Double computation at {node}")
        seq = self.sequence[node]
        self.sequence[node] += 1
        start = self.engine.now
        work = WorkItem(f"{node}:{seq}", node, seq, start,
                        start+self.profiles[node].compute_seconds, 1., self.config["payload_bytes"])
        self.pending[node] = work
        self.engine.emit("node_phase", node, phase="compute", work_id=work.work_id)
        self.engine.schedule(self.profiles[node].compute_seconds, self.ready, node, work)

    def ready(self, node, work):
        if self.active:
            self.engine.emit("work_ready", node, work=asdict(work))
            self.adapter.on_work_ready(node, work)

    def terminal(self, node, outcome):
        if node not in self.pending:
            raise RuntimeError(f"Duplicate terminal callback at {node}")
        del self.pending[node]
        if self.active:
            self.engine.schedule(0, self.compute, node)

    def stop(self):
        self.active = False
        for node in self.profiles:
            self.adapter.stop(node)
