"""Check timing instrumentation preserves native wait arguments and results.

Run with the source-verified Step 24 runtime; no DHT peers are started.
"""
import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import step24_lifetime_sweep as sweep
from hivemind.averaging.partition import TensorPartContainer


class NativeWaitTests(unittest.IsolatedAsyncioTestCase):
    def test_grouping_continues_after_tensor_mismatch(self):
        checked = dict(incorrect_steps=['H0:1'], integrity_errors=['incorrect tensor'],
                       control_integrity_errors=[], pending=[], error=None)
        self.assertFalse(sweep.trial_should_halt(checked, 'grouping'))
        self.assertTrue(sweep.trial_should_halt(checked, 'averaging'))

    def test_grouping_stops_on_corrupt_control_evidence_or_infrastructure(self):
        for checked in [dict(control_integrity_errors=['spoofed leader']),
                        dict(pending=['H0:1']), dict(error='process failed')]:
            self.assertTrue(sweep.trial_should_halt(checked, 'grouping'))

    async def test_reducer_failure_tracing_preserves_native_exception(self):
        events = []
        error = RuntimeError('native reducer failure')
        runner = SimpleNamespace(peer_id='h', group_id=b'group',
                                 tensor_part_container=SimpleNamespace(failed_size=128))

        async def original(self, peer_id):
            raise error

        with patch.object(sweep, 'ORIGINAL_COMMUNICATE', original), \
             patch.object(sweep, 'emit', lambda *args, **kwargs: events.append((args, kwargs))):
            with self.assertRaises(RuntimeError) as caught:
                await sweep.traced_communicate(runner, 'x')
        self.assertIs(caught.exception, error)
        failure = next(detail for args, detail in events if args[0] == 'reducer_exchange_failed')
        self.assertEqual(failure['failed_elements'], 128)

    async def test_native_failed_reducer_fallback_returns_zero_delta(self):
        tensor = sweep.base.torch.full((128,), 10.0)
        container = TensorPartContainer([tensor], peer_fractions=[0.0, 1.0], return_deltas=True)
        try:
            container.register_failed_reducer(1)
            updates = [update async for update in container.iterate_output_tensors()]
            self.assertEqual(len(updates), 1)
            self.assertEqual(container.failed_size, 128)
            self.assertTrue(sweep.base.torch.equal(updates[0], sweep.base.torch.zeros_like(tensor)))
            self.assertTrue(sweep.base.torch.equal(tensor + updates[0], tensor))
        finally:
            container.finalize()

    async def test_native_wait_is_observed_without_replacing_timeout_or_futures(self):
        calls, events = [], []
        futures = {object(), object()}
        timeout = 0.75
        sentinel = object()
        mm = SimpleNamespace(peer_id='x', step_control=object(),
                             potential_leaders=SimpleNamespace(declared_expiration_time=102),
                             current_followers={'h': object()})
        context = SimpleNamespace(remote_id='h')

        async def native_call(self, context):
            return await sweep.traced_wait(futures, timeout=timeout,
                                           return_when=asyncio.FIRST_COMPLETED)

        async def original(fs, *, timeout=None, return_when=None):
            calls.append((fs, timeout, return_when))
            return sentinel

        with patch.object(sweep, 'ORIGINAL_JOIN_RPC_CODE', native_call.__code__), \
             patch.object(sweep, 'ORIGINAL_WAIT', original), \
             patch.object(sweep, 'emit', lambda *args, **kwargs: events.append((args, kwargs))), \
             patch.object(sweep, 'user_data', return_value={'step_id': 'X:1'}), \
             patch.object(sweep.base, 'get_dht_time', return_value=101.25):
            result = await native_call(mm, context)
        self.assertIs(result, sentinel)
        self.assertIs(calls[0][0], futures)
        self.assertEqual(calls[0][1:], (timeout, asyncio.FIRST_COMPLETED))
        self.assertEqual(events[0][0], ('native_leader_wait', 'x', 'h'))
        self.assertEqual(events[0][1]['timeout'], timeout)
        self.assertEqual(events[0][1]['captured_expiration'], 102)

    async def test_other_waits_are_forwarded_without_native_timer_event(self):
        sentinel = object()
        events = []

        async def original(fs, *, timeout=None, return_when=None):
            return sentinel

        with patch.object(sweep, 'ORIGINAL_WAIT', original), \
             patch.object(sweep, 'emit', lambda *args, **kwargs: events.append(args)):
            result = await sweep.traced_wait(set(), timeout=1)
        self.assertIs(result, sentinel)
        self.assertEqual(events, [])

    async def test_client_response_tracing_preserves_awaitable_and_message(self):
        events, calls = [], []
        awaitable = object()
        mm = SimpleNamespace(peer_id='h', current_leader=None, step_control=object())
        message = SimpleNamespace(code=sweep.base.averaging_pb2.ACCEPTED, group_id=b'')

        async def native_call(self, leader):
            return await sweep.traced_wait_for(awaitable, 3)

        async def original(value, timeout):
            calls.append((value, timeout))
            return message

        with patch.object(sweep, 'ORIGINAL_CLIENT_CODE', native_call.__code__), \
             patch.object(sweep, 'ORIGINAL_WAIT_FOR', original), \
             patch.object(sweep, 'emit', lambda *args, **kwargs: events.append((args, kwargs))), \
             patch.object(sweep, 'user_data', return_value={'step_id': 'H0:1'}):
            result = await native_call(mm, 'x')
        self.assertIs(result, message)
        self.assertIs(calls[0][0], awaitable)
        self.assertEqual(calls[0][1], 3)
        self.assertEqual(events[-1][1]['code'], 'ACCEPTED')

    async def test_client_exception_tracing_preserves_exception(self):
        events = []
        error = TimeoutError('native timeout')
        mm = SimpleNamespace(peer_id='h', current_leader='x', step_control=object())

        async def native_call(self, leader):
            return await sweep.traced_wait_for(object(), 1)

        async def original(value, timeout):
            raise error

        with patch.object(sweep, 'ORIGINAL_CLIENT_CODE', native_call.__code__), \
             patch.object(sweep, 'ORIGINAL_WAIT_FOR', original), \
             patch.object(sweep, 'emit', lambda *args, **kwargs: events.append((args, kwargs))), \
             patch.object(sweep, 'user_data', return_value={'step_id': 'H0:1'}):
            with self.assertRaises(TimeoutError) as caught:
                await native_call(mm, 'x')
        self.assertIs(caught.exception, error)
        self.assertEqual(events[-1][1]['error'], 'TimeoutError')


if __name__ == '__main__':
    unittest.main()
