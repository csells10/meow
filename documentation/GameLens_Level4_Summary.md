# GameLens Level 4 Claim-Language Calibration

_Last updated: 2026-05-19_

## TLDR

Level 4 v0.1 is now packaged as a runnable backend calibration step after Level 3.

The completed v0.1 scope is:

> Use `two_way_context` to decide when certain claim/metric rows are allowed to receive stronger explanatory language.

This is **not** a winner-prediction layer.  
This is **not** a confidence-boosting layer.  
This is **not** frontend work yet.

Current status:

| Area | Status |
|---|---:|
| Level 4 registry rules | Complete |
| `/game` response annotation | Complete |
| Runtime `two_way_context` support | Complete |
| Backend smoke tests | Complete |
| Level 4 calibration worker | Complete |
| Dry-run local outputs | Complete |
| BigQuery table/write | Complete |
| Full Level 4 system | Ongoing |
| Frontend display | Not started |

---

## Milestone Completed

### Level 4 v0.1 — Claim-Language Calibration

We now have a working Level 4 v0.1 pipeline piece:

```text
Level 1 → build claim rows
Level 2 → validate claims
Level 3 → add engineered features
Level 4 → build claim-language calibration summary
```

Level 4 can now be run after Level 3 using:

```bash
python -m agg.gamelens_training.build_claim_language_calibration \
  --run-id larger_240_level3_qa_20260517 \
  --dry-run
```

And written to BigQuery using:

```bash
python -m agg.gamelens_training.build_claim_language_calibration \
  --run-id larger_240_level3_qa_20260517 \
  --write-bigquery \
  --replace-run
```

---

## What Level 4 v0.1 Does

Level 4 v0.1 answers this question:

> When GameLens makes a claim, is that claim allowed to speak more firmly based on historical validation and `two_way_context`?

The main rule:

```text
supportive two_way_context
+ allowlisted metric
+ allowlisted claim surface
= language_boost_allowed true
```

Everything else stays unboosted:

```text
available_mixed = no boost
unavailable = no boost
blocked metric = no boost
conditional disabled metric = no boost
```

---

## What Level 4 v0.1 Does Not Do

Level 4 v0.1 does **not**:

- Pick winners
- Change `matchup_lean`
- Change `outcome_confidence`
- Change `model_trust`
- Change `model_outcome`
- Override the model
- Add frontend UI by itself
- Make `two_way_context` visible as a user-facing concept

This boundary is important.

`two_way_context` is currently used as backend calibration context, not product copy.

---

## Files Added / Modified

### New / primary Level 4 files

```text
services/claim_language_support_registry.py
agg/gamelens_training/build_claim_language_calibration.py
```

### Existing files used / integrated

```text
services/claim_language_response.py
services/claim_language_features.py
services/game_service.py
```

### BigQuery output table

```text
Analytics.gamelens_claim_language_calibration
```

---

## Current Registry Rules

### Strong allowlist

These metrics can receive stronger support language when `two_way_context = supportive` and the claim surface is allowed:

```text
points_allowed_per_play
1st_down_rate
```

### Watch-level allowlist

This metric can receive measured support language, not overconfident language:

```text
points_per_play
```

### Conditional but disabled

This metric remains a candidate, but it is disabled in v0.1:

```text
third_down_pct
```

### Blocked from automatic stronger language

These metrics should not receive automatic stronger language, even when `two_way_context = supportive`:

```text
red_zone_efficiency
turnover_margin_per_game
td_rate
yards_per_play
yards_per_pass
yards_per_rush
```

---

## Runtime API Behavior

The `/game` API now includes response-only metadata such as:

```json
{
  "language_support": {
    "language_boost_allowed": true,
    "support_level": "strong",
    "reason": "points_allowed_per_play was the cleanest stable support metric in the 240-game QA run.",
    "rule_status": "allowed",
    "scope": "claim_language_support",
    "two_way_context": "supportive",
    "two_way_edge_score": 0.1915
  }
}
```

This metadata is currently informational. It should not alter core model outputs.

---

## Backend Smoke Tests Completed

| Game ID | Purpose | Result |
|---|---|---|
| `20231015_SF@CLE` | Supportive positive case | Passed |
| `20250113_MIN@LAR` | Supportive but bad model miss | Passed |
| `20251208_PHI@LAC` | Available-mixed no-boost case | Passed |
| `20240114_GB@DAL` | High-confidence miss / no-boost case | Passed |
| `20230907_DET@KC` | Week 1 unavailable-data case | Passed |

### Smoke-test read

The smoke tests confirmed:

```text
supportive + allowlisted metric/surface → boost allowed
available_mixed → no boost
unavailable → no boost
blocked metric → no boost
conditional-disabled metric → no boost
```

Most importantly:

> Picks, confidence, Model Trust, and model outcome logic stayed unchanged.

---

## Level 4 Calibration Worker

### File

```text
agg/gamelens_training/build_claim_language_calibration.py
```

### Purpose

This script reads the Level 1–3 claim training table after Level 3 is complete:

```text
Analytics.gamelens_claim_training_examples
```

It summarizes validation performance by:

```text
feature_formula_version
claim_type
claim_layer
metric
core_area
category
two_way_context
```

It then produces a calibration summary showing whether the current registry rule should be kept, blocked, reviewed, or considered for a future allowlist.

---

## Dry-Run Output

Command used:

```bash
python -m agg.gamelens_training.build_claim_language_calibration \
  --run-id larger_240_level3_qa_20260517 \
  --dry-run
```

Local outputs:

```text
qa/gamelens_calibration_runs/larger_240_level3_qa_20260517__level4_v0_1/calibration_summary.csv
qa/gamelens_calibration_runs/larger_240_level3_qa_20260517__level4_v0_1/calibration_summary.json
qa/gamelens_calibration_runs/larger_240_level3_qa_20260517__level4_v0_1/run_metadata.json
```

Dry-run summary:

| Field | Value |
|---|---:|
| Input rows | 5,328 |
| Summary rows | 135 |
| Overall validation rate | 0.4962 |
| Feature formula version | `offense_finish_v2__defensive_suppression_v3__two_way_context_v1` |

Note:

The input row count is lower than the full Level 3 row count because Level 4 v0.1 currently excludes rows where `metric IS NULL`. This is intentional for v0.1 because the current registry is metric-based.

---

## BigQuery Write

Command used:

```bash
python -m agg.gamelens_training.build_claim_language_calibration \
  --run-id larger_240_level3_qa_20260517 \
  --write-bigquery \
  --replace-run
```

This created/wrote to:

```text
Analytics.gamelens_claim_language_calibration
```

Validation query:

```sql
SELECT
  recommendation,
  COUNT(*) AS row_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_language_calibration`
WHERE calibration_run_id = 'larger_240_level3_qa_20260517__level4_v0_1'
GROUP BY recommendation
ORDER BY row_count DESC;
```

Result:

| Recommendation | Row Count |
|---|---:|
| `keep_blocked` | 78 |
| `no_boost_context_not_supportive` | 28 |
| `review_conditional_candidate` | 15 |
| `keep_v0_1_strong_allow` | 9 |
| `keep_v0_1_watch_allow` | 3 |
| `insufficient_sample` | 1 |
| `future_allowlist_candidate` | 1 |

---

## Interpretation of Calibration Results

The Level 4 v0.1 registry mostly held up.

The output confirmed:

- Strong allowlist rules are behaving as expected.
- Watch-level `points_per_play` support is reasonable in the approved surfaces.
- Blocked metrics should remain blocked.
- `available_mixed` and `unavailable` correctly prevent boosts.
- `third_down_pct` remains interesting but should stay conditional/disabled for now.
- One new candidate emerged, but should be reviewed later rather than enabled immediately.

---

## Future Allowlist Candidate

The one `future_allowlist_candidate` was:

```text
category_summary + supporting + points_per_play + supportive
```

Performance:

| Field | Value |
|---|---:|
| Row count | 37 |
| Validation rate | 0.6757 |
| Surface baseline validation rate | 0.4982 |
| Lift vs surface baseline | +0.1775 |

Recommended action:

```text
Do not enable yet.
Mark as Level 4 v0.2 review candidate.
```

Reason:

This is promising, but v0.1 should remain conservative until the behavior is reviewed across additional samples and surfaces.

---

## Current Level 4 Status

### Complete for v0.1

```text
Level 4 v0.1 — Two-Way Claim-Language Calibration
```

Completed:

- Registry rules
- Runtime API annotation
- Existing runtime `two_way_context` integration
- Smoke tests across known cases
- Calibration summary worker
- Dry-run local outputs
- BigQuery table output
- BigQuery validation query

### Not complete overall

Full Level 4 is still ongoing.

Future Level 4 work may include:

- Better calibration table usage inside `/game`
- Deciding whether runtime should read from BigQuery calibration instead of only static registry rules
- Level 4 v0.2 feature candidates
- Broader regression testing
- Frontend display decision
- Product copy rules

---

## Recommended Next Steps

### 1. Commit current backend state

Suggested commit message:

```text
Package Level 4 claim-language calibration worker
```

### 2. Keep frontend paused

Do not start frontend display work until the backend contract is documented and stable.

Current frontend principle:

```text
Frontend waits until Level 4 has a documented backend contract.
```

### 3. Review the future allowlist candidate later

Candidate:

```text
category_summary + supporting + points_per_play + supportive
```

Decision needed later:

```text
Should points_per_play be allowed in category_summary when two_way_context is supportive?
```

### 4. Decide if API should eventually read calibration table

Current behavior:

```text
/game uses runtime registry + runtime two_way_context annotation.
```

Possible future behavior:

```text
/game reads prepared calibration recommendations from Analytics.gamelens_claim_language_calibration.
```

No change needed yet.

### 5. Plan Level 4 v0.2

Possible feature candidates:

```text
Trap Door Profile
Drive Killer Index
Empty Yards Detector
Chaos Threat
```

These should be added iteratively, not all at once.

---

## Level 4 Mental Model

Level 4 is the editor.

It does not decide who wins.

It decides whether GameLens is allowed to say something more firmly, more cautiously, or normally.

Current v0.1 principle:

```text
Proven support may improve claim language.
It must not inflate prediction confidence.
```

---

## Current Pipeline Command Sequence

For future QA runs, the intended sequence is:

```bash
# Level 1
python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run <payload_run_name> \
  --run-id <run_id> \
  --write-bigquery \
  --replace-run

# Level 2
python -m agg.gamelens_training.update_claim_training_validation \
  --run-id <run_id> \
  --write-bigquery

# Level 3
python -m agg.gamelens_training.update_claim_training_features \
  --run-id <run_id> \
  --write-bigquery

# Level 4
python -m agg.gamelens_training.build_claim_language_calibration \
  --run-id <run_id> \
  --dry-run

# Level 4 BigQuery write, after reviewing dry-run output
python -m agg.gamelens_training.build_claim_language_calibration \
  --run-id <run_id> \
  --write-bigquery \
  --replace-run
```

---

## Backburner Cleanup

Eventually consider adding a table creation helper to:

```text
insert_bq.py
```

Possible function:

```text
create_gamelens_claim_language_calibration_table()
```

This is not required right now because the Level 4 worker can create the table if it does not exist.

---

## Final Note

This is a backend milestone, not a frontend milestone.

The frontend can eventually use `language_support`, but only after deciding:

- Where it should appear
- What wording is safe
- Whether it actually improves the user experience
- How to avoid making the page feel more engineering-heavy

For now, Level 4 v0.1 is best treated as a backend calibration layer.
