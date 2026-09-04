"""D2 -- make averager declarations prove who wrote them.

Track B's attack is cheap because ``declare_averager`` writes an *unsigned* record
whose subkey is a bare ``peer_id.to_bytes()`` (``hivemind/averaging/key_manager.py:46``),
with no validator attached to averaging keys. One process wrote 400 fabricated
averagers into a victim's group key in the live experiment.

Hivemind already ships the machinery and already uses it elsewhere: ``ProgressTracker``
protects its records with ``RSASignatureValidator`` + ``SchemaValidator``
(``hivemind/optim/progress_tracker.py:108-110``). The averaging keys were simply never
given the same treatment.

**Why this needs a custom validator rather than ``SchemaValidator``.** ``SchemaValidator``
maps a *fixed* field name to a key (``prefix_field``, ``schema.py:44-46``), but averaging
group keys are dynamic -- ``{prefix}.0b{bits}``, a different key per bucket, with the bits
changing every round. So the constraint has to be expressed as a pattern over keys, which
is what :class:`SignedAveragingValidator` does.

**What this buys, stated honestly.** ``RSASignatureValidator`` is *fail-open*: a record
whose key/subkey lacks an ``[owner:...]`` marker is accepted from anyone
(``crypto.py:47``). This validator closes that hole for averaging keys, so every
declaration must carry a public key and a matching signature. That eliminates
**impersonation** (a peer can no longer declare on someone else's behalf) and raises the
price of a fabricated identity from *one DHT write* to *one keypair plus one DHT write*.

It does **not** by itself stop Sybils: signing proves possession of a keypair, not
distinctness of a person, and keypairs are cheap. D2 is therefore a necessary
precondition for -- not a substitute for -- an admission or cost mechanism such as D1's
identity binding.
"""

from __future__ import annotations

import re
from typing import List, Optional, Tuple

from hivemind.dht.crypto import RSASignatureValidator
from hivemind.dht.validation import DHTRecord, RecordValidatorBase
from hivemind.p2p import PeerID
from hivemind.utils.crypto import RSAPrivateKey

__all__ = ["SignedAveragingValidator", "signed_subkey", "build_averaging_validators"]

# matches the group keys produced by GroupKeyManager.current_key: "<prefix>.0b<bits>"
GROUP_KEY_PATTERN = re.compile(rb"^([^.]+)\.0b[01]*$")


def signed_subkey(peer_id: PeerID, validator: RSASignatureValidator) -> bytes:
    """The subkey a peer publishes under D2: its peer_id plus its ``[owner:...]`` marker.

    The marker is what switches ``RSASignatureValidator`` from "unprotected, accept from
    anyone" to "must carry a valid signature".
    """
    return peer_id.to_bytes() + validator.local_public_key


class SignedAveragingValidator(RecordValidatorBase):
    """Require every declaration under an averaging group key to be owner-tagged.

    Runs *before* the signature validator (higher ``priority``) and rejects any record
    on a matching key whose subkey lacks a public key. ``RSASignatureValidator`` then
    verifies the signature for the records this admits, so the two compose into
    "declarations must be signed by their owner".

    :param prefixes: run prefixes to protect. ``None`` protects every group-shaped key.
    """

    def __init__(self, prefixes: Optional[List[str]] = None) -> None:
        self._prefixes = {prefix.encode() for prefix in prefixes} if prefixes else None

    def _is_protected_key(self, key: bytes) -> bool:
        match = GROUP_KEY_PATTERN.match(key)
        if match is None:
            return False
        return self._prefixes is None or match.group(1) in self._prefixes

    def validate(self, record: DHTRecord) -> bool:
        if not self._is_protected_key(record.key):
            return True  # not an averaging declaration; other validators decide
        if record.subkey is None:
            return False
        # the owner marker must be present, otherwise the signature check is skipped
        return bool(
            RSASignatureValidator.PUBLIC_KEY_REGEX and RSASignatureValidator._PUBLIC_KEY_RE.search(record.subkey)
        )

    @property
    def priority(self) -> int:
        # must run before RSASignatureValidator (priority 10) so unsigned declarations
        # are rejected rather than silently accepted by the fail-open path
        return 5

    def merge_with(self, other: RecordValidatorBase) -> bool:
        if not isinstance(other, SignedAveragingValidator):
            return False
        if self._prefixes is None or other._prefixes is None:
            self._prefixes = None
        else:
            self._prefixes |= other._prefixes
        return True


def build_averaging_validators(
    prefixes: Optional[List[str]] = None,
    private_key: Optional[RSAPrivateKey] = None,
) -> Tuple[List[RecordValidatorBase], RSASignatureValidator]:
    """Validators that make a run's averaging declarations unforgeable.

    Returns ``(validators, signature_validator)``: pass ``validators`` to
    ``DHT(record_validators=...)`` and use the signature validator to build subkeys via
    :func:`signed_subkey`.
    """
    signature_validator = RSASignatureValidator(private_key)
    return [SignedAveragingValidator(prefixes), signature_validator], signature_validator
