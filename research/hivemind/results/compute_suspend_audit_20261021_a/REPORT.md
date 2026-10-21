# Confirmation suspension audit

Two system suspensions affected `exclusion_s302` and `absent_s302`. Exclude both from clean timing comparisons despite their original `checks.json` validity. Preserve all original artifacts.

| Case | Recorded training seconds | Wall seconds | Timing eligible |
|---|---:|---:|---|
| baseline_s302 | 300.00 | 300.00 | True |
| exclusion_s302 | 300.00 | 827.33 | False |
| absent_s302 | 300.00 | 3792.67 | False |

The system journal confirms sleeps at 11:36–11:44 and 11:52–12:50 Europe/Stockholm. The harness uses monotonic elapsed time; Hivemind DHT time uses wall time. Simply subtracting sleep would not repair protocol expiration effects. Baseline seed 302 completed before either sleep. The ongoing seed 303 exclusion run started after resume. Replace affected seed 302 under a fresh label before final confirmation claims.
