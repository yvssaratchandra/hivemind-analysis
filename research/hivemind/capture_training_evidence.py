"""Observe locked tensor results and accepted reducer inputs without changing them."""


def install_evidence(emit):
    from hivemind.averaging.averager import DecentralizedAverager
    from hivemind.averaging.allreduce import AllReduceRunner
    from hivemind.averaging.partition import TensorPartReducer
    from compute_native_hooks import _tensor_values
    originals = []

    def patch(owner, method, replacement):
        original = getattr(owner, method)
        originals.append((owner, method, original, replacement))
        setattr(owner, method, replacement)

    inplace = DecentralizedAverager._run_allreduce_inplace_

    async def observed_inplace(self, tensors, group_info, group_id=None, **kwargs):
        fields = dict(prefix=self.prefix, group_id=(group_id or group_info.group_id).hex())
        # The native caller holds lock_averaged_tensors throughout this method.
        # Snapshot here: the outer _aggregate_with_group hook runs after unlock.
        emit('locked_aggregate_input', values=[_tensor_values(t) for t in tensors], **fields)
        result = await inplace(self, tensors, group_info, group_id=group_id, **kwargs)
        emit('locked_aggregate_output', values=[_tensor_values(t) for t in tensors], **fields)
        return result

    patch(DecentralizedAverager, '_run_allreduce_inplace_', observed_inplace)
    init = AllReduceRunner.__init__

    def observed_init(self, *args, **kwargs):
        init(self, *args, **kwargs)
        self.tensor_part_reducer._capture_context = dict(
            prefix=self._prefix, group_id=self.group_id.hex(),
            senders=list(self.sender_peer_ids))

    patch(AllReduceRunner, '__init__', observed_init)
    accumulate = TensorPartReducer.accumulate_part

    async def observed_accumulate(self, sender_index, part_index, tensor_part, weight=1.):
        context = getattr(self, '_capture_context', None)
        values = _tensor_values(tensor_part) if context else None
        result = await accumulate(self, sender_index, part_index, tensor_part, weight)
        if context:
            emit('reducer_accepted', prefix=context['prefix'], group_id=context['group_id'],
                 sender_peer_id=str(context['senders'][sender_index]),
                 part_index=part_index, weight=float(weight), values=values,
                 result=_tensor_values(result))
        return result

    patch(TensorPartReducer, 'accumulate_part', observed_accumulate)

    def uninstall():
        for owner, method, original, replacement in reversed(originals):
            if getattr(owner, method) is replacement:
                setattr(owner, method, original)

    return uninstall
