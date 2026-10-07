# Archived observer trace

`observer.jsonl.gz` preserves the original observer trace byte-for-byte. The uncompressed
file is too large for GitHub's 100 MiB per-file limit, so its extracted copy is ignored.
The archive uses gzip with a fixed timestamp and no embedded filename.

- Uncompressed size: 199,946,461 bytes.
- Raw lines: 27,937; existing report line references address these original lines.
- Uncompressed SHA-256: `59b8416e9c3b251c25dffbf43e75b7c7bb739a6e8df51047c02249839b27c683`.
- Archive SHA-256: `fce580f74dd856c28c11d02e902a78688cb0c8efe2531f43a4bcf34541db392f`.

From the repository root, extract without removing the archive:

```sh
gzip -dk research/localai/results/control-messages-lifecycle32-20261007T1350/observer.jsonl.gz
sha256sum research/localai/results/control-messages-lifecycle32-20261007T1350/observer.jsonl
python3 research/localai/analyze_control_messages.py research/localai/results/control-messages-lifecycle32-20261007T1350
```

If `observer.jsonl` already exists, verify its checksum instead of overwriting it.
Compression was verified by hashing the decompressed archive against the original.
The analyzer and saved instrumentation continue to use the original JSONL format.
