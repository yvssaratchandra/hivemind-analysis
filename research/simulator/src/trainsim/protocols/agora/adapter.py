"""Assemble one Agora stage, with an explicit startup-only population barrier."""
from .aggregation import AgoraAggregator
from .matchmaking import AgoraMatcher, TimedRegistry
from .partitioning import stage_namespace, stage_record_key
from .workload import AgoraWorkload


class AgoraAdapter:
    def __init__(self, engine, network, profiles, config, seed):
        self.engine, self.network, self.profiles, self.config = engine, network, profiles, config
        self.seed = seed
        self.registry = TimedRegistry(engine, network)
        self.matcher = AgoraMatcher(engine, self.registry, profiles, config,
                                    config['stage'], config['stage']+'_avg', self._unexpected_roster)
        self.aggregator = AgoraAggregator(engine, network, profiles, config)
        self.workload = AgoraWorkload(engine, profiles, config, self.matcher, self.aggregator)
        self.active = False
        self.measurement_started = False
        self.setup_observers = set()
        self.indices = {node: index for index, node in enumerate(sorted(profiles))}

    @staticmethod
    def _unexpected_roster(*args):
        raise RuntimeError('A workload averaging operation must provide its own roster callback')

    def start(self):
        self.active = True
        self.setup_started_at = self.engine.now
        for node in self.profiles:
            self._publish_stage(node, initial=True)
        self.engine.schedule(self.config['setup_timeout_seconds'], self._setup_deadline)

    def _publish_stage(self, node, initial=False):
        if not self.active:
            return
        expiry = self.engine.now+self.config['stage_ttl_seconds']
        key = stage_record_key(self.config['stage'], self.indices[node])
        role = 'NODE' if self.indices[node] < self.config['reducers'] else 'CLIENT'
        value = dict(node_id=node, role=role, stage=self.config['stage'], weight=1.0)
        self.engine.emit('agora_stage_publish_started', node, key=key, expiry=expiry, role=role)
        def acknowledged(accepted):
            if not self.active:
                return
            self.engine.emit('agora_stage_publish_ack', node, key=key, expiry=expiry, accepted=accepted)
            if initial:
                self._check_population(node)
            self.engine.schedule(self.config['stage_refresh_seconds'], self._publish_stage, node)
        self.registry.publish(node, key, self.profiles[node].peer_id.hex(), value, expiry, acknowledged)

    def _check_population(self, node):
        if not self.active or self.measurement_started or node in self.setup_observers:
            return
        def received(records):
            if not self.active or self.measurement_started:
                return
            self.engine.emit('agora_setup_population_observed', node, count=len(records),
                             peer_ids=[record.peer_id for record in records])
            if len(records) == self.config['population']:
                self.setup_observers.add(node)
                self.engine.emit('agora_setup_node_ready', node, observed_count=len(records))
                # This global check is only the experimental setup barrier. The
                # matcher never consults this set or any global group membership.
                if len(self.setup_observers) == len(self.profiles):
                    self.measurement_started = True
                    self.engine.emit('measurement_started', stage=self.config['stage'],
                                     population=len(self.profiles),
                                     setup_seconds=self.engine.now-self.setup_started_at)
                    self.workload.start()
            else:
                self.engine.schedule(self.config['poll_seconds'], self._check_population, node)
        self.registry.lookup(node, stage_namespace(self.config['stage']), received, prefix=True)

    def _setup_deadline(self):
        if self.active and not self.measurement_started:
            raise RuntimeError('Agora startup population check exceeded setup deadline')

    def stop(self):
        self.active = False
        self.workload.stop()
        self.aggregator.stop()
        # The shared runner stops Engine before cleanup, suppressing all pending
        # timers and network callbacks. There is no post-run protocol activity.
