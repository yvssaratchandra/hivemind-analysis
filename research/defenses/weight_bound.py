"""D4 -- validate the averaging weight on receipt.

Track C's sharpest single finding: one peer in sixteen that simply claims
``weight = 1e6`` diverges the whole run. The weight travels on the wire in every
``AveragingData`` message and the reducer applies it verbatim::

    self.accumulator.add_(tensor_part, alpha=weight)   # partition.py:242
    self.denominator += weight

There is no non-negativity check, no finiteness check and no cap on the receiving
path (``allreduce.py:339-347``). The honest value is a peer's *self-reported* sample
count (``grad_averager.py:196``), so it is attacker-chosen by construction.

Hivemind already asserts exactly the right property -- but only locally, on the
sender's own value: ``StepControl.weight`` does ``assert weight >= 0 and
np.isfinite(weight)`` (``averaging/control.py:95``) and ``DecentralizedAverager.step``
repeats it (``averager.py:399``). Those are self-imposed constraints an honest client
applies to itself; nothing enforces them on incoming data. **D4 is simply moving that
existing check to the receiving side**, which makes it a small and uncontroversial
upstream patch.

Three levels, cheapest first:

* :func:`sanitize_weight` -- reject negative, NaN and infinite weights. Fixes the
  catastrophic cases at essentially zero cost and zero policy.
* :func:`clip_weight` -- additionally cap against a group-relative bound, so no single
  peer can outvote the rest.
* :func:`reconcile_with_progress` -- cross-check the claimed weight against the peer's
  *signed* progress record, which Hivemind already publishes and validates
  (``progress_tracker.py:108``) but never reconciles against the all-reduce weight.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Sequence

__all__ = ["sanitize_weight", "clip_weight", "reconcile_with_progress", "WeightPolicy"]

DEFAULT_MAX_WEIGHT_RATIO = 10.0


def sanitize_weight(weight: float) -> Optional[float]:
    """Return a usable weight, or ``None`` if the sender's value must be discarded.

    Mirrors the assertion an honest sender already applies to itself
    (``control.py:95``): finite and non-negative.
    """
    if weight is None or math.isnan(weight) or math.isinf(weight) or weight < 0:
        return None
    return float(weight)


def clip_weight(
    weight: float,
    peer_weights: Sequence[float],
    *,
    max_ratio: float = DEFAULT_MAX_WEIGHT_RATIO,
) -> Optional[float]:
    """Cap a weight relative to what the rest of the group claims.

    Bounds any one peer's influence to ``max_ratio`` times the group median, so a
    peer inflating its sample count gains a bounded advantage instead of unbounded
    control. The median (not the mean) is the reference precisely because the mean is
    what the attacker is trying to move.
    """
    sanitized = sanitize_weight(weight)
    if sanitized is None:
        return None

    others = [value for value in (sanitize_weight(w) for w in peer_weights) if value is not None and value > 0]
    if not others:
        return sanitized

    ordered = sorted(others)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    if median <= 0:
        return sanitized
    return min(sanitized, max_ratio * median)


def reconcile_with_progress(
    claimed_weight: float,
    signed_samples_accumulated: Optional[float],
    *,
    tolerance: float = 1.5,
) -> Optional[float]:
    """Check the claimed weight against the peer's signed progress record.

    ``ProgressTracker`` already publishes ``samples_accumulated`` per peer under an
    RSA-signed, schema-validated key (``progress_tracker.py:108-110``) -- the same
    quantity the all-reduce weight is supposed to represent (``grad_averager.py:196``).
    Today the signed record and the unsigned wire value are never compared. Doing so
    costs nothing extra: the data is already there and already authenticated.

    Returns the accepted weight, or ``None`` if the claim exceeds what the peer
    verifiably reported.
    """
    sanitized = sanitize_weight(claimed_weight)
    if sanitized is None:
        return None
    if signed_samples_accumulated is None:
        return sanitized  # no signed record to compare against; fall back to other checks
    if sanitized > tolerance * max(signed_samples_accumulated, 1.0):
        return None
    return sanitized


@dataclass
class WeightPolicy:
    """Composable receive-side policy, so each level can be evaluated separately."""

    sanitize: bool = True
    clip: bool = True
    max_ratio: float = DEFAULT_MAX_WEIGHT_RATIO
    rejected: int = 0
    clipped: int = 0

    def apply(self, weight: float, peer_weights: Sequence[float] = ()) -> Optional[float]:
        """Return the weight to actually use, or ``None`` to drop this contribution."""
        result: Optional[float] = weight
        if self.sanitize:
            result = sanitize_weight(result)
            if result is None:
                self.rejected += 1
                return None
        if self.clip and len(peer_weights):
            capped = clip_weight(result, peer_weights, max_ratio=self.max_ratio)
            if capped is None:
                self.rejected += 1
                return None
            if capped < result:
                self.clipped += 1
            result = capped
        return result

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(sanitize={self.sanitize}, clip={self.clip}, "
            f"max_ratio={self.max_ratio}, rejected={self.rejected}, clipped={self.clipped})"
        )
