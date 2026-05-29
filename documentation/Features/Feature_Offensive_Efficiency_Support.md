# GameLens Level 4 Update — `offensive_efficiency_support_v1`

_Last updated: May 24, 2026_

## 1. Executive Summary

`offensive_efficiency_support_v1` is a new GameLens claim-language metadata feature.

Its purpose is to help answer:

> When GameLens makes an offensive-efficiency claim before a game, does the postgame data tend to agree?

This feature is **not winner prediction**.

It is also **not matchup lean**, **not confidence**, **not Model Trust**, and **not frontend wording control yet**.

Current status:

```text
Level 3 feature: built and validated
Level 4 calibration: aware of feature
/game API exposure: working
Frontend usage: future / metadata-only
Runtime boosting: disabled
```

The feature is currently exposed as structured metadata so the frontend and future QA tooling can inspect it safely.

The most important guardrail remains:

```json
"metadata_only": true,
"language_boost_allowed": false
```

That means the feature is visible, but it does **not** currently change product language or confidence.

---

## 2. Why This Feature Exists

GameLens already explains matchup shape using areas like:

- Offensive Output
- Scoring Efficiency
- Defensive Control
- Disruption and Turnovers
- Team Comparison
- Metric Highlights
- Category Summaries

But not every claim deserves the same language strength.

For example, these two claims should not be treated equally:

```text
BUF has a strong offensive-efficiency profile supported by points per play, yards per play, and yards per pass.
```

versus:

```text
BUF has a red-zone efficiency edge.
```

The second may sound exciting, but prior validation showed red-zone efficiency can be unstable and should not automatically create stronger language.

`offensive_efficiency_support_v1` gives GameLens a structured way to separate:

```text
repeat-positive offensive-efficiency support
```

from:

```text
context-only, mixed, or caution-only offensive metrics
```

---

## 3. Evidence Behind the Feature

Clean-path revalidation across these runs supported the feature idea:

```text
baseline_96_stage1_v2
fresh_96_features_qa_v2
larger_240_level3_qa_20260517
```

Findings:

| Metric | Finding |
|---|---|
| `points_per_play` | Repeat-positive across all 3 runs; best anchor metric |
| `yards_per_rush` | Useful, but scoped / weird-pocket behavior |
| `yards_per_play` | Useful overall, but had one negative run; safer as support input |
| `yards_per_pass` | Better than expected inside the feature, but not standalone auto-boost yet |
| `td_rate` | Caution-only / blocked |
| `red_zone_efficiency` | Caution-only / blocked |
| `turnover_margin_per_game` | Caution-only / blocked |
| `third_down_pct` | Context-only |
| `1st_down_rate` | Context-only |

---

## 4. Level 3 Feature Definition

### Feature name

```text
offensive_efficiency_support_v1
```

### Level 3 output fields

Added to dry-run CSV output, BigQuery temp schema, BigQuery MERGE, and summary logic:

```text
offensive_efficiency_support_score FLOAT
offensive_efficiency_support_bucket STRING
offensive_efficiency_support_strength STRING
offensive_efficiency_support_reason STRING
offensive_efficiency_support_metrics STRING
```

### Formula version

Current Level 3 formula version after this feature:

```text
clean_hierarchy_context_v1__offensive_efficiency_support_v1__offense_finish_v2__defensive_suppression_v3__two_way_context_v1
```

### Important boundary

This did **not** change existing behavior for:

```text
clean_hierarchy_context_v1
offense_finish_score
defensive_suppression_score
two_way_edge_score
two_way_context
```

It also did **not** change:

```text
winner logic
matchup lean
confidence
Model Trust
frontend behavior
Level 4 boost rules
```

---

## 5. Level 3 Bucket Meanings

| Bucket | Strength | Meaning |
|---|---|---|
| `repeat_positive_strong` | `strong_support` | Strongest offensive-efficiency support candidate |
| `repeat_positive_supportive` | `measured_support` | Useful support, but use measured wording |
| `mixed_near_even` | `mixed` | Mixed profile; no broad boost |
| `negative_caution` | `caution` | Claimed side has negative offensive-efficiency context |
| `opposing_efficiency_signal` | `caution` | Opponent has stronger efficiency signal |
| `caution_only` | `caution_only` | Metric is explicitly excluded from stronger-language support |
| `context_only` | `context_only` | Useful descriptive context, not stronger-language evidence |
| `anchor_unavailable` | `unavailable` | Missing `points_per_play` anchor |
| `not_relevant` | `not_applicable` | Feature does not apply to this claim family |

---

## 6. Validation Results

On `larger_240_level3_qa_20260517`, overall validation rate was approximately:

```text
3335 / 6775 = 49.2%
```

Bucket performance:

| Bucket | Rows | Validation Rate | Read |
|---|---:|---:|---|
| `repeat_positive_strong` | 780 | 59.7% | Strong candidate for future language support |
| `repeat_positive_supportive` | 784 | 57.1% | Useful, but measured |
| `mixed_near_even` | 354 | 53.4% | Mildly useful, not broadly boost-worthy |
| `not_relevant` | 1801 | 50.6% | Outside feature scope |
| `context_only` | 1016 | 48.8% | Baseline-ish context |
| `caution_only` | 1798 | 40.7% | Correctly caution/blocked |
| `anchor_unavailable` | 110 | 39.1% | Bad when anchor missing |
| `negative_caution` | 105 | 39.0% | Correctly caution |
| `opposing_efficiency_signal` | 27 | 37.0% | Correctly caution, small sample |

Combined positive buckets:

```text
repeat_positive_strong + repeat_positive_supportive
= 914 validated / 1564 rows
= 58.4% validation rate
```

This is about **+9 percentage points above overall baseline**.

### Interpretation

The feature appears to do two useful things:

1. Finds offensive-efficiency claim rows that validate above baseline.
2. Correctly demotes risky/caution-only metrics.

This makes it a successful Level 3 metadata feature and a strong Level 4 calibration candidate.

---

## 7. Metric-Level Findings

Positive buckets held up across multiple offensive-efficiency metrics.

| Metric | Bucket | Validation Read |
|---|---|---|
| `points_per_play` | strong/supportive | Correct anchor metric |
| `yards_per_pass` | strong/supportive | Better than expected inside feature |
| `yards_per_play` | strong/supportive | Useful when anchored by `points_per_play` |
| `yards_per_rush` | strong/supportive/mixed pockets | Useful but scoped; needs separate follow-up |

Caution logic was also validated:

| Metric | Result |
|---|---|
| `td_rate` | Keep blocked / caution-only |
| `red_zone_efficiency` | Keep blocked / caution-only |
| `turnover_margin_per_game` | Keep blocked / caution-only |

Context-only logic also looked correct:

| Metric | Result |
|---|---|
| `third_down_pct` | Context-only |
| `1st_down_rate` | Context-only |

---

## 8. Level 4 Role

### Current Level 4 status

Level 4 is now aware of `offensive_efficiency_support_v1`.

Calibration version used in dry-run:

```text
level4_v0_2_offensive_efficiency_language_calibration
```

The Level 4 dry-run showed the key safety split:

```text
api_exposure_allowed_v0_2 = true
api_use_allowed_v0_1 = false
```

### Meaning

| Field | Meaning |
|---|---|
| `api_exposure_allowed_v0_2 = true` | Safe to expose as API metadata |
| `api_use_allowed_v0_1 = false` | Not allowed to drive runtime language boosts yet |

This is the correct current stage.

The feature is exposed for inspection and future calibration, but it does not steer language yet.

---

## 9. API Exposure Shape

The `/game` response now includes `offensive_efficiency_support_v1` in top-level claim-language context.

Example:

```json
"claim_language_context": {
  "available": true,
  "feature_versions": {
    "offensive_efficiency_support": "offensive_efficiency_support_v1",
    "two_way_context": "two_way_context_v1"
  },
  "offensive_efficiency_support_by_side": {
    "away": {
      "anchor_available": true,
      "anchor_metric": "points_per_play",
      "bucket": "repeat_positive_strong",
      "components": {
        "points_per_play": 0.8064,
        "yards_per_pass": 0.2258,
        "yards_per_play": 0.3226,
        "yards_per_rush": 0.3549
      },
      "feature_version": "offensive_efficiency_support_v1",
      "language_boost_allowed": false,
      "metadata_only": true,
      "score": 0.6134,
      "strength": "strong_support"
    }
  },
  "scope": "claim_language_support"
}
```

### Runtime side-level fields

| Field | Meaning |
|---|---|
| `feature_version` | Feature identifier |
| `score` | Signed offensive-efficiency support score |
| `bucket` | Feature bucket |
| `strength` | Language support strength category |
| `components` | Metric components used in the runtime score |
| `anchor_metric` | Always `points_per_play` for v1 |
| `anchor_available` | Whether anchor metric was available |
| `metadata_only` | Must remain `true` for now |
| `language_boost_allowed` | Must remain `false` for now |

### Duplicate field removed

The API originally exposed both:

```json
"components": {...}
```

and:

```json
"component_metrics": "points_per_play:0.8064;..."
```

`component_metrics` was removed from the runtime API because it duplicated the structured `components` object.

Keep the Level 3 BigQuery field:

```text
offensive_efficiency_support_metrics
```

That flat string is still useful for training CSVs and BigQuery analysis.

---

## 10. Row-Level API Metadata

Rows inside sections like `metric_highlights`, `category_summaries`, and drivers now include nested metadata.

Example:

```json
"language_support": {
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "points_per_play",
  "language_boost_allowed": false,
  "offensive_efficiency_support": {
    "bucket": "repeat_positive_strong",
    "feature_version": "offensive_efficiency_support_v1",
    "language_boost_allowed": false,
    "metadata_only": true,
    "metrics": "points_per_play,yards_per_play,yards_per_pass",
    "reason": "metric:points_per_play; score anchored by repeat-positive points_per_play, with yards_per_play/yards_per_pass as watch inputs and yards_per_rush only when rushing-scoped",
    "score": 0.6134,
    "strength": "strong_support"
  }
}
```

Important:

```text
Row-level metadata does not currently rewrite summaries.
```

It is for inspection, future frontend affordances, and later calibration.

---

## 11. Files Changed

Expected changed files for this feature set:

```text
agg/gamelens_training/update_claim_training_features.py
agg/gamelens_training/build_claim_language_calibration.py
services/claim_language_features.py
services/claim_language_response.py
services/game_service.py
```

### `update_claim_training_features.py`

Adds Level 3 fields, scoring, buckets, BigQuery schema/MERGE support, CSV output, and summary logic.

### `build_claim_language_calibration.py`

Reads and summarizes offensive-efficiency support fields during Level 4 calibration.

Adds metadata concepts like:

```text
offensive_efficiency_language_signal
api_exposure_allowed_v0_2
```

### `claim_language_features.py`

Builds runtime side-level offensive-efficiency metadata for `/game`.

Important runtime behavior:

```text
Uses available ranking/team comparison/matchup rows to build side-level score.
Keeps metadata_only true.
Keeps language_boost_allowed false.
```

### `claim_language_response.py`

Attaches nested `offensive_efficiency_support` metadata to row-level `language_support` objects while preserving existing claim-strength metadata.

Important correction:

```text
Do not use any shorter version that removed claim_strength_* paths.
```

### `game_service.py`

Exposes top-level `claim_language_context.feature_versions` and `offensive_efficiency_support_by_side` after matchup breakdown context is available.

---

## 12. Smoke-Test Games

These were used as local `/game` smoke tests:

```text
20251013_BUF@ATL
20251009_PHI@NYG
20251208_PHI@LAC
```

Local URLs:

```text
http://127.0.0.1:5000/game/20251013_BUF%40ATL
http://127.0.0.1:5000/game/20251009_PHI%40NYG
http://127.0.0.1:5000/game/20251208_PHI%40LAC
```

### Expected results

For `20251013_BUF@ATL`:

```text
away bucket = repeat_positive_strong
away strength = strong_support
anchor_available = true
metadata_only = true
language_boost_allowed = false
```

For `20251009_PHI@NYG`:

```text
away bucket = repeat_positive_supportive
away strength = measured_support
anchor_available = true
metadata_only = true
language_boost_allowed = false
```

For `20251208_PHI@LAC`:

```text
away bucket = mixed_near_even
away strength = mixed
anchor_available = true
metadata_only = true
language_boost_allowed = false
```

---

## 13. Local QA Checklist

After changes, run:

```bash
python -m py_compile services/claim_language_features.py services/claim_language_response.py services/game_service.py
```

Then restart Flask and test:

```text
http://127.0.0.1:5000/game/20251013_BUF%40ATL
```

Search payload for:

```text
claim_language_context
feature_versions
offensive_efficiency_support_by_side
language_support.offensive_efficiency_support
```

Confirm:

```text
component_metrics is not present in runtime API
metadata_only is true
language_boost_allowed is false
points_per_play anchor missing does not appear when points_per_play exists
```

---

## 14. How To Analyze This Feature Later

### Bucket validation query

```sql
SELECT
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength,
  COUNT(*) AS row_count,
  COUNTIF(validation_result = 'validated') AS validated_count,
  SAFE_DIVIDE(COUNTIF(validation_result = 'validated'), COUNT(*)) AS validation_rate,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength
ORDER BY row_count DESC;
```

### Metric-level validation query

```sql
SELECT
  metric,
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength,
  COUNT(*) AS row_count,
  COUNTIF(validation_result = 'validated') AS validated_count,
  SAFE_DIVIDE(COUNTIF(validation_result = 'validated'), COUNT(*)) AS validation_rate,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  metric,
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength
HAVING row_count >= 20
ORDER BY
  validation_rate DESC,
  row_count DESC;
```

### Product-surface validation query

```sql
SELECT
  claim_type,
  claim_layer,
  metric,
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength,
  COUNT(*) AS row_count,
  COUNTIF(validation_result = 'validated') AS validated_count,
  SAFE_DIVIDE(COUNTIF(validation_result = 'validated'), COUNT(*)) AS validation_rate,
  COUNTIF(validation_result = 'not_validated') AS not_validated_count,
  COUNTIF(validation_result = 'actual_neutral_or_mixed') AS neutral_or_mixed_count
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
  AND offensive_efficiency_support_strength IN (
    'strong_support',
    'measured_support',
    'mixed'
  )
GROUP BY
  claim_type,
  claim_layer,
  metric,
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength
HAVING row_count >= 20
ORDER BY
  validation_rate DESC,
  row_count DESC;
```

---

## 15. How To Use In Frontend Later

### Current recommendation

Do **not** display this prominently to normal users yet.

This is currently best used as:

```text
admin/debug metadata
QA inspection
tooltip input later
future language restraint/support signal
```

### Safe frontend integration v0

If surfaced at all, keep it subtle and non-decisive.

Possible admin/debug display:

```text
Offensive efficiency support: Strong metadata signal
```

or:

```text
Claim-language metadata: repeat_positive_strong
```

Avoid user-facing language like:

```text
This team is more likely to win.
```

or:

```text
GameLens is confident because of offensive efficiency support.
```

That would be wrong at the current stage.

### Suggested frontend mapping

| API strength | Internal UI wording |
|---|---|
| `strong_support` | Offensive efficiency support detected |
| `measured_support` | Measured offensive efficiency support |
| `mixed` | Mixed offensive efficiency profile |
| `caution` | Offensive efficiency caution |
| `caution_only` | Metric is caution-only |
| `context_only` | Context only |
| `not_applicable` | Hide |
| `unavailable` | Hide |

### Suggested frontend location

Best future places:

1. Admin/debug panel
2. Metric tooltip
3. “Why this language?” explanation drawer
4. QA-only badge on Team Comparison / Metric Highlights

Avoid putting it in the main Matchup Lean card yet.

---

## 16. Future Frontend Integration Plan

### Phase A — Hidden / QA-only

Use it only for internal QA.

Frontend can inspect:

```text
row.language_support.offensive_efficiency_support
```

but avoid rendering anything publicly.

### Phase B — Tooltip metadata

For offensive metrics, a tooltip could say:

```text
This metric has supporting offensive-efficiency metadata, but it is not currently used to boost confidence.
```

Only show this when:

```text
metadata_only = true
language_boost_allowed = false
```

### Phase C — Product copy support

After more validation, Level 4 may allow limited copy like:

```text
This offensive edge is backed by multiple efficiency inputs.
```

But only if future rules explicitly allow it.

### Phase D — Runtime language support

Only after another calibration pass should `language_boost_allowed` ever become true for offensive-efficiency support.

That would require:

- stronger Level 4 rule design
- sample-size thresholds
- surface-specific allowlist
- metric-specific allowlist
- frontend copy review
- regression against bad-miss examples

---

## 17. What Not To Do Yet

Do not:

```text
use this feature to change matchup_lean
use this feature to change outcome_confidence
use this feature to change Model Trust
use this feature to rewrite visible summaries
use this feature as a winner predictor
allow frontend to treat strong_support as a confidence boost
```

The feature is useful, but it has not earned the steering wheel yet.

Current correct interpretation:

```text
This is claim-language metadata that may help future calibration.
```

Not:

```text
This is a live confidence driver.
```

---

## 18. Recommended Commit Message

```text
Add offensive efficiency claim-language metadata

Adds offensive_efficiency_support_v1 as a Level 3 metadata feature, wires it into Level 4 calibration summaries, and exposes safe metadata-only offensive-efficiency support context through /game. The feature remains non-steering: it does not change matchup lean, confidence, Model Trust, frontend wording, winner logic, or language_boost_allowed behavior.
```

---

## 19. Final Current-State Summary

`offensive_efficiency_support_v1` is now a successful metadata feature.

It found above-baseline offensive-efficiency claim rows, correctly identified caution-only metrics, and now appears in `/game` safely.

The current implementation is intentionally cautious:

```text
Visible to API: yes
Usable for analysis: yes
Frontend-ready as debug metadata: yes
Runtime language boost: no
Outcome confidence driver: no
Winner prediction: no
```

This is the right stopping point before documentation, commit, and future frontend exploration.
