"""Offline native-class regressions for actual training capture policy."""
import asyncio
import multiprocessing as mp
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import run_compute_exclusion as base
import capture_training_hooks as policy


class CapturePolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base.native_imports()
        from hivemind.p2p import PeerID
        cls.peers={n:PeerID(('peer-'+n).encode()) for n in ('A','X','D','C')}
        cls.identities={str(peer):n for n,peer in cls.peers.items()}

    def test_exact_attack_channels(self):
        self.assertEqual(policy.attacked_prefixes('baseline','g','s'),set())
        self.assertEqual(policy.attacked_prefixes('capture_control','g','s'),set())
        self.assertEqual(policy.attacked_prefixes('gradient_exclusion','g','s'),{'g'})
        self.assertEqual(policy.attacked_prefixes('dual_exclusion','g','s'),{'g','s'})
        with self.assertRaises(ValueError):
            policy.attacked_prefixes('typo','g','s')

    def test_observation_only_hooks_keep_honest_advertisements(self):
        from compute_native_hooks import install_hooks
        from hivemind.averaging.key_manager import GroupKeyManager
        from hivemind.averaging.matchmaking import Matchmaking
        events=[]
        dht=SimpleNamespace(store=AsyncMock(return_value=True))
        key=SimpleNamespace(prefix='g',dht=dht)
        native_expiry=Matchmaking.get_request_expiration_time
        calls=[]
        def expiry(obj):
            calls.append(obj)
            return 9.
        with patch.object(Matchmaking,'get_request_expiration_time',expiry):
            uninstall=install_hooks(node='C',condition='baseline',emit=lambda k,**v:events.append((k,v)),
                gradient_prefix='g',identities=self.identities,leader_fixture=False)
            try:
                self.assertTrue(asyncio.run(GroupKeyManager.declare_averager(key,'g.0b',self.peers['C'],7.)))
                obj=SimpleNamespace(_prefix='g',peer_id=self.peers['C'],step_control=SimpleNamespace(deadline=12.))
                self.assertEqual(Matchmaking.get_request_expiration_time(obj),9.)
                self.assertEqual(calls,[obj])
                self.assertFalse(any(k=='leader_advertisement_suppressed' for k,v in events))
            finally:
                uninstall()
        self.assertIs(Matchmaking.get_request_expiration_time,native_expiry)

    def test_local_omission_requires_actual_x_leadership_and_scoped_channel(self):
        from hivemind.averaging.averager import DecentralizedAverager
        from hivemind.averaging.group_info import GroupInfo
        group=GroupInfo(b'group',tuple(self.peers.values()),(b'A',b'X',b'D',b'C'))
        for leader,prefix,removed in (('X','s',True),('D','s',False),(None,'s',False),('X','other',False)):
            instance=object.__new__(DecentralizedAverager)
            mp.Process.__init__(instance)
            instance.prefix=prefix
            leaders={} if leader is None else {b'group':self.peers[leader]}
            instance._matchmaking=SimpleNamespace(_capture_leaders=leaders)
            with patch.object(DecentralizedAverager,'peer_id',self.peers['X']), \
                 patch.object(DecentralizedAverager,'_aggregate_with_group',AsyncMock()) as aggregate:
                uninstall=policy.install_capture_policy(condition='dual_exclusion',gradient_prefix='g',
                    state_prefix='s',identities=self.identities,emit=lambda *a,**k:None)
                try:
                    asyncio.run(instance._aggregate_with_group(group,1))
                    effective=aggregate.call_args.args[1]
                    self.assertEqual(self.peers['A'] not in effective.peer_ids,removed)
                    self.assertEqual(effective.gathered,(b'X',b'D',b'C') if removed else group.gathered)
                finally:
                    uninstall()

    def test_actual_state_announcement_omits_a_only_for_other_recipients(self):
        from hivemind.averaging.matchmaking import Matchmaking
        from hivemind.proto import averaging_pb2
        original=averaging_pb2.MessageFromLeader(code=averaging_pb2.BEGIN_ALLREDUCE,group_id=b'g',
            ordered_peer_ids=[p.to_bytes() for p in self.peers.values()],gathered=[b'A',b'X',b'D',b'C'])
        async def native_rpc(*args):
            yield original
        async def collect(obj,recipient):
            return [m async for m in Matchmaking.rpc_join_group(obj,None,SimpleNamespace(remote_id=self.peers[recipient]))]
        events=[]
        with patch.object(Matchmaking,'rpc_join_group',native_rpc):
            uninstall=policy.install_capture_policy(condition='dual_exclusion',gradient_prefix='g',
                state_prefix='s',identities=self.identities,emit=lambda k,**v:events.append((k,v)))
            try:
                obj=SimpleNamespace(_prefix='s',peer_id=self.peers['X'],current_followers={self.peers['A']:None})
                target=asyncio.run(collect(obj,'A'))[0]
                other=asyncio.run(collect(obj,'D'))[0]
                self.assertEqual(target.SerializeToString(),original.SerializeToString())
                self.assertEqual(list(other.gathered),[b'X',b'D',b'C'])
                self.assertEqual(len(other.ordered_peer_ids),3)
                self.assertEqual(events[-1][1]['channel'],'state')
                from analyze_capture_training import validate_announcement
                event=dict(events[-1][1], kind='capture_announcement')
                errors=[]
                validate_announcement(event,True,errors)
                self.assertEqual(errors,[])
                event['gathered_hex'][0]=b'tampered'.hex()
                validate_announcement(event,True,errors)
                self.assertTrue(errors)
            finally:
                uninstall()


class CaptureEvidenceTests(unittest.TestCase):
    def events(self):
        common=dict(node='A',prefix='run_grad_averager',channel='gradient',group_id='g')
        allocation=dict(common,kind='allocation',members=['A','X'],weight=224,
            part_ranges={'A':[dict(tensor_index=0,start=0,end=48,elements=48)],'X':[]})
        return [allocation,dict(common,kind='local_roster',members=['A','X']),
                dict(common,kind='aggregate_input',values=[[1.]*48]),
                dict(common,kind='aggregate_output',values=[[1.]*48]),
                dict(common,kind='processed_chunk',reducer='A',part_index=0,values=[0.]*48),
                dict(common,kind='input_chunk',direction='received',receiver='A',sender='X',
                     part_index=0,values=[7.]*48,weight=0)]

    def test_zero_weight_input_is_valid_when_total_weight_is_positive(self):
        from analyze_capture_training import _verify_group_outputs
        errors=[]
        result=_verify_group_outputs(self.events(),errors)
        self.assertEqual(errors,[])
        self.assertEqual(result['weighted_coordinates_checked'],48)

    def test_tampered_output_and_negative_weight_are_rejected(self):
        from analyze_capture_training import _verify_group_outputs
        for field in ('output','weight'):
            events=self.events()
            if field=='output':
                events[3]['values'][0][0]=2.
            else:
                events[-1]['weight']=-1
            errors=[]
            _verify_group_outputs(events,errors)
            self.assertTrue(errors)


class CaptureWorkloadTests(unittest.TestCase):
    def test_evaluator_includes_x_and_preserves_d_c_arithmetic(self):
        import numpy as np
        from compute_workload import evaluation, evaluate_models as original
        from capture_training_workload import evaluate_models
        x,y=evaluation(501)
        models={n:np.zeros(48,np.float32) for n in ('A','X','D','C')}
        actual=evaluate_models(models,x,y)
        expected=original({'B' if n=='X' else n:v for n,v in models.items()},x,y)
        self.assertEqual(actual['per_node']['X'],expected['per_node']['B'])
        self.assertEqual(actual['per_node']['D'],expected['per_node']['D'])
        self.assertEqual(set(actual['per_node']),set(models))


class LockedEvidenceTests(CaptureEvidenceTests):
    def test_locked_snapshot_and_accepted_inputs_override_late_observations(self):
        from analyze_capture_training import _verify_group_outputs
        events=self.events()
        common={k:events[0][k] for k in ('node','prefix','group_id')}
        # The outer buffer observation raced with a subsequent reset.
        events[3]['values']=[[0.]*48]
        events.extend([
            dict(common,kind='locked_aggregate_input',values=[[1.]*48]),
            dict(common,kind='locked_aggregate_output',values=[[1.]*48]),
            dict(common,kind='reducer_accepted',part_index=0,sender_peer_id='A',
                 weight=224.,values=[1.]*48,result=[1.]*48),
            dict(common,kind='reducer_accepted',part_index=0,sender_peer_id='X',
                 weight=0.,values=[7.]*48,result=[1.]*48),
        ])
        # Late wire data was not accepted into the reducer.
        events[-4]['values']=[[1.]*48]
        events[5]['weight']=1000.
        errors=[]
        proof=_verify_group_outputs(events,errors)
        self.assertEqual(errors,[])
        self.assertEqual(proof['weighted_coordinates_checked'],48)
        events[-1]['result'][0]=9.
        _verify_group_outputs(events,errors)
        self.assertTrue(errors)

    def test_reducer_rejection_does_not_emit_acceptance(self):
        from hivemind.averaging.partition import TensorPartReducer, BannedException
        from capture_training_evidence import install_evidence
        import torch
        async def rejected(*args,**kwargs):
            raise BannedException('late sender')
        events=[]
        with patch.object(TensorPartReducer,'accumulate_part',rejected):
            uninstall=install_evidence(lambda k,**v:events.append((k,v)))
            try:
                obj=SimpleNamespace(_capture_context=dict(prefix='g',group_id='a',senders=['A']))
                with self.assertRaises(BannedException):
                    asyncio.run(TensorPartReducer.accumulate_part(obj,0,0,torch.ones(4),1.))
                self.assertEqual(events,[])
            finally:
                uninstall()


if __name__ == '__main__':
    unittest.main()
