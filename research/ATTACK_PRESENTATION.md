# Candidate attacks for presentation

Three findings qualify as concrete attacks and together form a strong **Select–Deny–Tamper** presentation.

## 1. Targeted matchmaking eclipse — Select

**Attacker goal:** Control who appears in a victim's averaging group.

### Attack

1. Create DHT identities close to the victim's matchmaking key.
2. Control every lookup responder the victim queries.
3. Hide honest advertisements from the victim.
4. Make the victim lead, then have malicious training peers join it.

### Measured result

At the default `bucket_size = 20`, 20 crafted DHT identities produced **100% captured rounds**; no honest peer appeared in the victim's group. With 18 identities, capture was only 14%. Doubling `bucket_size` to 40 also doubled the required identities, strongly supporting the causal explanation.

### Why it is compelling

This directly answers the main research question: the victim's local malicious fraction, **beta**, can be actively selected rather than simply reflecting the global malicious fraction, **alpha**.

### Qualification

The attack uses DHT-only Sybils for lookup control plus malicious training peers that join the victim. Five identities obtain record custody, but approximately `bucket_size` identities are needed for reliable lookup control when caching is enabled.

**Relevant findings:** `HANDOFF.md` section 4.6, and C7.S and C13.S in `CONTROL_DATA_AUDIT.md`.

---

## 2. Accept-then-stall — Deny

**Attacker goal:** Prevent a selected peer from completing averaging.

### Attack

1. The malicious peer becomes group leader.
2. It replies `ACCEPTED` to the victim's join request.
3. It never sends `BEGIN_ALLREDUCE`.
4. The victim waits until its timeout instead of immediately finding another group.

### Measured result

- Honest leader: approximately **0.03 seconds**.
- Malicious leader: approximately **23 seconds per stall**, matching the predicted timeout.
- Immediate rejection caused no lost steps, while stalling caused 24 lost steps out of 360 and about **31 times greater median latency**.
- With 15 honest alternatives, the attacker denied 6.7% of steps, so severity depends on how many alternative leaders are available.

### Why it is compelling

It is easy to explain and demonstrates a protocol-valid Byzantine attack: the attacker responds correctly enough to avoid omission-based defences but then wastes the victim's time.

### Qualification

The attacker must first become leader. Do not present it as total denial in a well-populated deployment; present it as a targeted delay/denial capability whose effect increases when alternatives are scarce.

**Relevant finding:** C13.D/E01 in `CONTROL_DATA_AUDIT.md`.

---

## 3. Weight-inflation poisoning — Tamper

**Attacker goal:** Magnify a malicious model contribution.

### Attack

1. Join an averaging group as an ordinary member.
2. Send a malicious gradient.
3. Claim an artificially large averaging weight.
4. Hivemind applies the sender-supplied weight without independently validating it.

### Measured result

With local malicious fraction **beta = 1/4**:

- A sign-flipped gradient alone was diluted by averaging.
- The same gradient combined with a dishonest weight moved the model **35–148 times farther from the optimum**.
- Coordinate-wise median reduced the result to approximately 0.5–2.3 times the control.

### Why it is compelling

It connects a control-message weakness directly to measurable learning harm. The decisive problem is not merely that an attacker can send a bad gradient; it is that the attacker controls the metadata determining how strongly that gradient counts.

### Qualification

The attacker must already be a group member. The experiment used three seeds, and the robust-aggregation result should be described as promising rather than conclusive.

**Relevant finding:** C17.T in `CONTROL_DATA_AUDIT.md`.

## Recommendation

Present the three attacks in lifecycle order:

```text
Control discovery -> Delay/deny averaging -> Corrupt averaging
      Select                 Deny                   Tamper
```

If only two are required, choose:

1. **Targeted matchmaking eclipse**, because it is central to the research question.
2. **Weight-inflation poisoning**, because it demonstrates concrete model harm.

The forged-withdrawal attack is also valid and particularly simple: one unsigned DHT write can durably hide another peer. It is narrower, however, because it censors discovery and does not by itself capture an existing group. Keep it as a backup example.

Do not yet headline C12.S as complete group capture. Winning the leader comparison was demonstrated, but it has not yet been reconciled with live end-to-end matchmaking. Present it as a powerful enabling result or proposed attack chain, not as the primary completed attack.
