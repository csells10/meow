# GameLens Feature Guide Book

_Last updated: 2026-05-24_

## Purpose

This guide captures the current GameLens feature-engineering discoveries, tested Level 3 features, Level 4 calibration direction, API exposure strategy, frontend integration ideas, and recommended next steps.

The main goal is **not** to build a forced pick machine.

The goal is:

> Help GameLens speak more truthfully about matchup claims.

That means these features should mainly support:

- claim-language calibration
- stronger or softer wording
- warning / caution metadata
- matchup-path explanation
- future `claim_strength_language_signal` improvements
- confidence restraint when the data is fragile
- frontend explanation aids that make users smarter

They should **not** directly drive:

- automatic winner prediction
- matchup lean override
- automatic High Confidence
- Model Trust override
- betting-style pick logic

---

# 1. Current Working Thesis

The discovery work now points to a cleaner product thesis:

> **GameLens should trust repeatable efficiency support more than volatile event outcomes.**

The data repeatedly favored efficient offensive production and broad offensive output, while warning against chaos-driven or fragile finishing signals.

## Clean support family

These are the strongest support patterns found so far:

- `Offensive Output`
- `Passing Game` inside `Offensive Output`
- `Rushing Game` inside `Offensive Output`
- `points_per_play`
- `yards_per_play`
- `yards_per_pass`
- `yards_per_rush`
- repeat-positive offensive-efficiency buckets from `offensive_efficiency_support_v1`

Football translation:

> Teams that create yards and points efficiently produce cleaner claim support.

## Warning / restraint family

These are the most dangerous patterns:

- `Disruption and Turnovers`
- `Turnovers`
- `Turnover Risk`
- `Pressure`
- `turnover_margin_per_game`
- `td_rate`
- `red_zone_efficiency`
- broad `Scoring Production`
- `Red Zone Finish`

Football translation:

> Chaos and finishing stats may explain football, but they should not create loud confidence by themselves.

## Context-only family

These help describe a matchup, but should not steer language alone:

- `third_down_pct`
- `1st_down_rate`
- `Drive Conversion`
- broad drive-sustainability ideas
- broad rushing-control ideas

Football translation:

> These stats help tell the story, but they should not drive the story.

---

# 2. Testing Philosophy

Every feature test should answer this question:

> When GameLens makes this kind of pregame claim, does the postgame data agree more often under this feature context?

Not:

> Did this feature pick the winner?

## Standard validation fields

Use these fields consistently:

| Field | Meaning |
|---|---|
| `Feature_Name` | Feature being tested |
| `Test_Run` | Baseline, fresh, larger, or all selected runs |
| `Scope_Checked` | Claim family being evaluated |
| `Feature_Bucket` | Bucket / condition being tested |
| `Row_Count` | Sample size |
| `Validation_Rate` | Percent of rows that validated |
| `Lift_vs_Baseline` | Bucket rate minus baseline rate |
| `Repeatable_Across_Runs` | Whether lift repeated across runs |
| `Decision` | Keep, warn, retest, hold, etc. |
| `Notes` | Plain-English interpretation |

## Decision labels

| Label | Meaning |
|---|---|
| `promote_scoped` | Useful, but only for specific claim families/language |
| `keep_as_warning` | Good caution/restraint feature |
| `keep_scoped_retest` | Directionally useful, but needs more validation |
| `retest` | Interesting but not ready |
| `hold_context_only` | Descriptive only |
| `hold_research` | Keep in notes, do not implement yet |
| `do_not_promote` | Do not use as a support feature |
| `accepted_metadata_only` | Safe to expose as metadata/API context, not language steering |
| `level4_candidate` | Earned Level 4 calibration review but not runtime boosting |

---

# 3. Pipeline Map

GameLens claim learning is best thought of as levels.

```text
Level 0: Table setup
  agg/gamelens_training/create_claim_training_examples_table.py

Level 1: Claim extraction
  agg/gamelens_training/build_claim_training_examples.py
  -> one row per pregame GameLens claim

Level 2: Postgame validation
  agg/gamelens_training/update_claim_training_validation.py
  -> validation_result, actual_side, actual_gap, qa_read_v2, etc.

Level 3: Pregame feature enrichment
  agg/gamelens_training/update_claim_training_features.py
  -> clean hierarchy, score features, context buckets

Level 4: Claim-language calibration
  agg/gamelens_training/build_claim_language_calibration.py
  -> summarizes which surfaces/metrics/features deserve stronger, softer, or blocked language

Runtime API exposure
  services/claim_language_features.py
  services/claim_language_response.py
  services/game_service.py
  -> exposes safe metadata to /game
```

Important boundary:

> Level 3 and Level 4 features are about claim quality and language calibration, not winner prediction.

---

# 4. Current Implemented Level 3 Features

## 4.1 `clean_hierarchy_context_v1`

### Question

Does each claim row have a clean, registry-backed hierarchy path?

```text
Core Area -> Category -> Metric
```

### Purpose

This feature attaches canonical metadata from `metric_registry.py` so claims can be analyzed through a consistent hierarchy instead of trusting legacy parser metadata.

### Fields

| Field | Meaning |
|---|---|
| `registry_core_area` | Canonical core area from registry |
| `registry_category` | Canonical category from registry |
| `registry_metric_label` | Human-readable metric label |
| `registry_signal_strength` | Registry signal strength |
| `registry_ranking_usage` | Registry ranking usage |
| `clean_hierarchy_path` | Readable hierarchy path |
| `clean_hierarchy_status` | How clean/recovered the hierarchy was |
| `clean_hierarchy_path_flag` | Boolean: usable hierarchy path |
| `missing_hierarchy_parent_flag` | Boolean: parent metadata was missing/recovered |

### Status

```text
Implemented / keep
```

### Product use

Use this for analysis and calibration grouping. Do not use it as a direct frontend concept yet unless building an internal/debug view.

---

## 4.2 `offense_finish_score`

### Question

Does this team have pregame support for moving the ball and finishing drives?

### Current use

Existing Level 3 score.

### Lesson

Useful only when scoped correctly. Do not attach it to defensive, turnover, or pressure claims.

### Status

```text
Implemented / keep / scoped
```

### Product use

Good ingredient for claim-language support, especially when combined with defensive context. It should not independently boost winner confidence.

---

## 4.3 `defensive_suppression_score`

### Question

Does this team have pregame support for limiting opponent scoring/offensive output?

### Current use

Existing Level 3 score.

### Lesson

It works best when Defensive Control exists. Metric-only fallback was dangerous. Strong positive defensive suppression appears useful; weak/mixed defensive suppression should not be treated as enough.

### Status

```text
Implemented / keep / scoped defensive context
```

### Product use

Use as defensive claim-language context, especially for defensive suppression claims. Do not turn it into a broad outcome confidence boost.

---

## 4.4 `two_way_edge_score`

### Question

Does the claimed side have both offensive finish and defensive suppression support?

### Current use

Existing Level 3 score.

### Lesson

The raw score is useful, but the bucketed `two_way_context` is more product-friendly.

### Status

```text
Implemented / keep as feature ingredient
```

---

## 4.5 `two_way_context`

### Question

Does the claimed team have both offensive finish support and defensive suppression support?

### Buckets

```text
supportive
available_mixed
unavailable
```

### Current lesson

`supportive` has been one of the most useful Level 3 context buckets for claim-language support.

### Status

```text
Implemented / accepted for Level 4 v0.1 claim-language support
```

### Product use

Use as claim-language support only.

Do not use as:

- winner confidence
- matchup lean override
- Model Trust override
- automatic High Confidence

---

## 4.6 `offensive_efficiency_support_v1` — New Feature

### Question

Can repeat-positive offensive efficiency metrics identify GameLens claims that deserve stronger claim-language review?

### Why this feature exists

Clean-path revalidation across multiple runs showed:

- `points_per_play` was repeat-positive across all 3 reviewed runs.
- `yards_per_rush` was useful but scoped.
- `yards_per_play` was useful overall but had one negative run.
- `yards_per_pass` had positive overall lift but was not clean repeat-positive under the first rule.
- `td_rate`, `red_zone_efficiency`, and `turnover_margin_per_game` should be excluded or caution-only.
- `third_down_pct` and `1st_down_rate` should remain context-only.

### Level 3 fields

| Field | Meaning |
|---|---|
| `offensive_efficiency_support_score` | Numeric support score, roughly -1 to +1 |
| `offensive_efficiency_support_bucket` | Bucketed result |
| `offensive_efficiency_support_strength` | Language strength classification |
| `offensive_efficiency_support_reason` | Plain-English/audit reason |
| `offensive_efficiency_support_metrics` | Compact metric list for CSV/BigQuery |

### Buckets

| Bucket | Meaning | Product read |
|---|---|---|
| `repeat_positive_strong` | Strong offensive-efficiency support | Level 4 candidate for stronger language review |
| `repeat_positive_supportive` | Measured positive support | Useful, but language should remain measured |
| `mixed_near_even` | Mixed profile | No general boost; possible scoped research pocket |
| `negative_caution` | Negative offensive-efficiency context | Caution / softener |
| `opposing_efficiency_signal` | Opponent has strong offensive-efficiency signal | Strong caution |
| `caution_only` | Metric is blocked/caution-only | Do not boost |
| `context_only` | Metric is descriptive only | Do not boost |
| `not_relevant` | Not an offensive-efficiency claim family | Ignore for this feature |
| `anchor_unavailable` | `points_per_play` anchor missing | Caution / no support |

### Feature logic summary

- `points_per_play` is the anchor.
- `yards_per_play` and `yards_per_pass` can support the score as watch inputs.
- `yards_per_rush` contributes only when the claim is rushing-scoped.
- `td_rate`, `red_zone_efficiency`, and `turnover_margin_per_game` are caution-only.
- `third_down_pct` and `1st_down_rate` are context-only.

### Validation result

In the larger Level 3 run:

| Bucket | Rows | Validation Rate | Read |
|---|---:|---:|---|
| `repeat_positive_strong` | 780 | 59.7% | Best support candidate |
| `repeat_positive_supportive` | 784 | 57.1% | Useful measured support |
| `mixed_near_even` | 354 | 53.4% | Interesting, not boost-worthy broadly |
| `context_only` | 1,016 | 48.8% | Baseline/context |
| `not_relevant` | 1,801 | 50.6% | Outside scope |
| `caution_only` | 1,798 | 40.7% | Correctly cautioned |
| `anchor_unavailable` | 110 | 39.1% | Bad when anchor missing |
| `negative_caution` | 105 | 39.0% | Correctly cautioned |
| `opposing_efficiency_signal` | 27 | 37.0% | Correctly cautioned, small sample |

Combined positive buckets:

```text
repeat_positive_strong + repeat_positive_supportive
= 914 validated / 1,564 rows
= 58.4% validation rate
```

That is meaningfully above the larger-run overall baseline of roughly 49.2%.

### Current decision

```text
offensive_efficiency_support_v1 = accepted_metadata_only / level4_candidate
```

It is useful enough to expose and analyze, but it should **not** directly change runtime language yet.

---

# 5. Runtime API Exposure

The new feature is exposed safely in `/game` under `claim_language_context`.

## Top-level shape

```json
"claim_language_context": {
  "available": true,
  "scope": "claim_language_support",
  "feature_versions": {
    "two_way_context": "two_way_context_v1",
    "offensive_efficiency_support": "offensive_efficiency_support_v1"
  },
  "two_way_context_by_side": {
    "away": {},
    "home": {}
  },
  "offensive_efficiency_support_by_side": {
    "away": {},
    "home": {}
  }
}
```

## Side-level offensive-efficiency shape

```json
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
}
```

## Row-level language support shape

Rows in `metric_highlights`, `category_summaries.drivers`, and Team Comparison can include nested metadata:

```json
"language_support": {
  "claim_type": "metric_highlight",
  "claim_layer": "headline",
  "metric": "points_per_play",
  "language_boost_allowed": false,
  "offensive_efficiency_support": {
    "feature_version": "offensive_efficiency_support_v1",
    "bucket": "repeat_positive_strong",
    "strength": "strong_support",
    "score": 0.6134,
    "metrics": "points_per_play,yards_per_play,yards_per_pass",
    "reason": "metric:points_per_play; score anchored by repeat-positive points_per_play...",
    "metadata_only": true,
    "language_boost_allowed": false
  }
}
```

## Important API guardrail

Runtime exposure should preserve:

```json
"metadata_only": true,
"language_boost_allowed": false
```

This means:

> The API can show and inspect the feature, but the feature is not allowed to rewrite summaries or boost language yet.

## Removed duplicate field

The runtime API originally exposed both:

```json
"component_metrics": "points_per_play:0.2258;yards_per_play:-0.1612"
```

and:

```json
"components": {
  "points_per_play": 0.2258,
  "yards_per_play": -0.1612
}
```

The string field was removed from the runtime payload because `components` is cleaner and frontend-friendly.

Do **not** remove the Level 3/BigQuery field:

```text
offensive_efficiency_support_metrics
```

That flat field still makes sense for CSV, BigQuery, and compact QA outputs.

---

# 6. Level 4 Role

## Current Level 4 interpretation

`offensive_efficiency_support_v1` is now visible to Level 4 as metadata.

Current safety split:

```text
api_exposure_allowed_v0_2 = true
api_use_allowed_v0_1 = false
```

Meaning:

- Expose the feature in calibration summaries and `/game` metadata.
- Do not let it automatically boost language.
- Do not use it for confidence or winner logic.

## What Level 4 should do with it now

Level 4 should group and summarize:

```text
claim_type
claim_layer
metric
core_area
category
offensive_efficiency_support_bucket
offensive_efficiency_support_strength
two_way_context
validation_result
```

The goal is to learn which combinations are safe enough for future language changes.

## Future Level 4 rule idea

Possible future rule after more testing:

```text
IF offensive_efficiency_support_strength = strong_support
AND claim_type / claim_layer / metric surface has enough sample
AND validation_rate clears threshold
AND metric is not blocked
AND two_way_context is not contradictory
THEN mark as stronger-language candidate
```

But this should be implemented only after another calibration pass, not immediately.

---

# 7. Frontend Integration Plan

## Current frontend recommendation

Do **not** show this as a loud user-facing badge yet.

The safest first UI use is internal/debug/admin visibility.

## Phase 1: Internal QA / debug display

Add an optional debug panel or expandable section showing:

- offensive efficiency bucket
- score
- strength
- components
- anchor availability
- metadata-only flag

Suggested label:

```text
Offensive Efficiency Support
```

Suggested display:

```text
Strong support · score 0.61
Anchor: points_per_play
Components: points_per_play + yards_per_play + yards_per_pass + yards_per_rush
Metadata only: no language boost active
```

## Phase 2: Subtle analyst-facing context

Once comfortable, show small restrained text on relevant metric rows:

| Bucket | UI phrase |
|---|---|
| `repeat_positive_strong` | Efficiency support present |
| `repeat_positive_supportive` | Measured efficiency support |
| `mixed_near_even` | Mixed efficiency profile |
| `negative_caution` | Efficiency caution |
| `caution_only` | Volatile metric |
| `context_only` | Context only |

Avoid flashy language such as:

```text
Confirmed Edge
Lock
Strong Prediction
High Confidence Boost
```

## Phase 3: Language wiring only after calibration

Eventually, the frontend may use backend-owned language modifiers to decide row copy or badges.

Recommended rule:

> Frontend should display backend-provided labels, not invent its own boost logic.

Frontend should not decide that `strong_support` means “boost.” The backend must make that decision later.

## Frontend fields to consume later

Safe fields:

```text
language_support.offensive_efficiency_support.bucket
language_support.offensive_efficiency_support.strength
language_support.offensive_efficiency_support.score
language_support.offensive_efficiency_support.reason
language_support.offensive_efficiency_support.metadata_only
language_support.offensive_efficiency_support.language_boost_allowed
```

Top-level fields:

```text
claim_language_context.feature_versions.offensive_efficiency_support
claim_language_context.offensive_efficiency_support_by_side.away.bucket
claim_language_context.offensive_efficiency_support_by_side.home.bucket
```

## Frontend guardrails

Do not let the frontend:

- boost language from this feature alone
- change confidence labels
- change Matchup Lean
- change Model Trust
- show a “confirmed” badge because `strength = strong_support`
- display raw debug scores in the main UI without explanation

---

# 8. Layered Discovery Results

## 8.1 Metric-level findings

### Best metric opportunities

| Metric | Interpretation |
|---|---|
| `points_per_play` | Clean scoring-efficiency support; current anchor for offensive-efficiency support |
| `yards_per_play` | Broad snap-to-snap offensive efficiency; useful when anchored |
| `yards_per_pass` | Passing efficiency support; better inside the new feature than expected |
| `yards_per_rush` | Rushing efficiency support; useful but scoped |

### Metric warnings

| Metric | Interpretation |
|---|---|
| `td_rate` | Major warning; very volatile |
| `red_zone_efficiency` | Warning; finishing is fragile |
| `turnover_margin_per_game` | Warning; turnover edge is unstable |

### Metric context only

| Metric | Interpretation |
|---|---|
| `third_down_pct` | High-volume but mostly neutral/context |
| `1st_down_rate` | Descriptive, not a strong driver |

---

## 8.2 Category-level findings

### Clean opportunity

| Category | Read |
|---|---|
| `Rushing Game` | Cleanest category-level opportunity |

### Warning categories

| Category | Read |
|---|---|
| `Pressure` | Strong warning category |
| `Turnovers` | Strong warning category |
| `Turnover Risk` | Strong warning category |
| `Scoring Production` | Warning parent category despite useful `points_per_play` inside it |
| `Red Zone Finish` | Suspicious / generally weak |

### Review / context categories

| Category | Read |
|---|---|
| `Passing Game` | Promising but needs clean scoping |
| `Scoring Suppression` | Positive but not fully repeat-clean |
| `Offensive Rhythm` | Mixed because it contains useful and weak signals |
| `Drive Conversion` | High-volume neutral/context |
| `Scoring Efficiency` | Positive in some rows, but shape needs caution |

---

## 8.3 Core Area findings

| Core Area | Decision | Read |
|---|---|---|
| `Offensive Output` | Opportunity | Cleanest broad Core Area support |
| `Disruption and Turnovers` | Warning | Cleanest broad Core Area warning |
| `Defensive Control` | Review | Directionally useful, but not clean enough alone |
| `Scoring Efficiency` | Review / mixed | Contains both strong and dangerous signals |

Football summary:

> Offensive Output is sturdy. Disruption and Turnovers is volatile. Defensive Control needs scoping. Scoring Efficiency is split.

---

## 8.4 Three-level hierarchy finding

The hierarchy query was the most useful exploratory view because it showed where the signal actually lives:

```text
Core Area
  -> Category
      -> Metric
```

### Clean opportunity paths

| Hierarchy Path | Read |
|---|---|
| `Offensive Output` | Clean broad opportunity |
| `Offensive Output > Passing Game` | Clean opportunity |
| `Offensive Output > Rushing Game` | Clean opportunity |
| `Offensive Output > Offensive Rhythm > yards_per_play` | Clean metric support |
| `Offensive Output > Passing Game > yards_per_pass` | Clean metric support |
| `Offensive Output > Rushing Game > yards_per_rush` | Clean metric support |
| `Scoring Efficiency > Scoring Production > points_per_play` | Clean metric support despite noisy parent category |

### Clean warning paths

| Hierarchy Path | Read |
|---|---|
| `Disruption and Turnovers` | Broad warning |
| `Disruption and Turnovers > Turnovers` | Warning |
| `Disruption and Turnovers > Turnovers > turnover_margin_per_game` | Warning |
| `Scoring Efficiency > Scoring Production` | Warning parent category |
| `Scoring Efficiency > Scoring Production > td_rate` | Very strong warning |
| `Scoring Efficiency > Red Zone Finish` | Warning |
| `Scoring Efficiency > Red Zone Finish > red_zone_efficiency` | Warning |

### Important hierarchy lesson

The data showed a split inside `Scoring Efficiency`:

```text
Scoring Efficiency = mixed overall
  Scoring Production = warning
    points_per_play = opportunity
    td_rate = warning
  Red Zone Finish = warning
    red_zone_efficiency = warning
  Drive Conversion = context / neutral
    third_down_pct = neutral/context
```

That means we should **not** say:

> Scoring Efficiency is good.

The better rule is:

> Some scoring-efficiency metrics are good, but finishing and touchdown-rate metrics are fragile.

---

# 9. Ten Tested Feature Candidates

These remain in the feature backlog/research guide. Some are implemented, some are candidates, and some should stay on ice.

## 1. `offense_vs_defense_collision_v0`

**Question:** Can this offense attack this defense, or is the offense running into resistance?

**Best use:** Offensive/rushing/scoring matchup-path language.

**Important buckets:**

- `offense_clear_path`
- `offense_supported`
- `strength_on_strength`
- `opponent_defense_warning`
- `offense_stressed`

**Decision:** `promote_scoped / retest with clean hierarchy`

**Product language:**

> The offensive claim has a clearer matchup path because the opponent defense does not show matching resistance.

**Implementation note:** This is one of the most GameLens-like features, but it should be implemented through language calibration, not winner confidence.

---

## 2. `matchup_fragility_warning_v0`

**Question:** Is a claim stable, or fragile because support is mixed?

**Best use:** Language restraint.

**Finding:** Stable two-way support was the real winner. Mixed support was useful as a soft caution, not a hard failure.

**Decision:** `promote_scoped`

**Implementation note:** Use this to decide whether language can be stronger, measured, or softened.

---

## 3. `passing_efficiency_claim_support_v0`

**Question:** Does passing-related language validate better when the claimed team has a real passing-efficiency edge?

**Finding:** Passing efficiency is useful, especially when cleanly scoped under `Offensive Output > Passing Game > yards_per_pass`.

**Caution:** Mild passing support should not be treated as support.

**Decision:** `keep_scoped_retest`

**Implementation note:** Good candidate after clean-path validation. `offensive_efficiency_support_v1` currently keeps `yards_per_pass` as a support input rather than a standalone boost.

---

## 4. `defensive_resistance_support_v0`

**Question:** Does defensive language validate better when the defense has strong suppression support?

**Finding:** Strong defensive support is useful directionally, but clear/moderate support was not enough.

**Decision:** `retest / supporting context only`

**Implementation note:** Do not make this a language boost yet. Use it as supporting context.

---

## 5. `defense_vs_dynamite_v0`

**Question:** Can a claimed defense still be trusted when the opponent has offensive firepower?

**Finding:** Strong defense works best when the opponent does not have matching firepower. Strength-on-strength should soften defensive language.

**Decision:** `promote_scoped`

**Implementation note:** Use for defensive claim language calibration.

---

## 6. `explosive_offense_warning_v0`

**Question:** Does opponent offensive firepower make claims against that opponent more fragile?

**Finding:** Good warning feature, especially for defensive and Core Area claims.

**Decision:** `promote_scoped`

**Implementation note:** This is a restraint feature, not a winner-prediction feature.

---

## 7. `scoring_claim_support_v0`

**Question:** Do scoring claims validate better when the claimed team has strong scoring support?

**Finding:** Strong scoring support works. Clear/mild scoring support does not.

**Decision:** `promote_scoped`

**Implementation note:** Use only for strong scoring support. Do not boost from mild/clear scoring support.

---

## 8. `drive_conversion_scoring_support_v0`

**Question:** Do conversion/finish indicators help claims about sustaining drives and turning drives into points?

**Finding:** Not clean enough. Conversion support was weaker than plain strong scoring support.

**Decision:** `hold_research`

**Implementation note:** Keep as context only. Do not implement as product logic yet.

---

## 9. `rush_claim_support_score`

**Question:** Do rushing claims validate better when the claimed team has a strong yards-per-rush edge?

**Finding:** Strong rushing efficiency works. Clear/mild rushing support does not.

**Decision:** `promote_scoped`

**Implementation note:** Use for narrow rushing language only. Current `offensive_efficiency_support_v1` allows `yards_per_rush` to contribute only when rushing-scoped.

---

## 10. Broad `rushing_control_score`

**Question:** Does broad rushing/control context help broader game-shape claims validate?

**Finding:** Not ready. The test mostly collapsed into thin evidence.

**Decision:** `hold_research`

**Implementation note:** Do not implement until cleaner game/team-level rushing-control inputs exist.

---

# 10. How to Analyze Features

## A. Bucket distribution

Use this first to confirm the feature is wired and has usable spread.

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

## B. Metric split

Use this to learn whether the feature is carried by one metric or holds across metrics.

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

## C. Surface survival test

Use this to decide whether a feature works across product surfaces.

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

## D. Cross with `two_way_context`

Use this to learn whether offensive efficiency and two-way support combine cleanly.

```sql
SELECT
  two_way_context,
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength,
  COUNT(*) AS row_count,
  COUNTIF(validation_result = 'validated') AS validated_count,
  SAFE_DIVIDE(COUNTIF(validation_result = 'validated'), COUNT(*)) AS validation_rate
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'larger_240_level3_qa_20260517'
GROUP BY
  two_way_context,
  offensive_efficiency_support_bucket,
  offensive_efficiency_support_strength
HAVING row_count >= 20
ORDER BY validation_rate DESC, row_count DESC;
```

## E. Runtime API smoke-test checklist

Test examples:

```text
/game/20251013_BUF@ATL
/game/20251009_PHI@NYG
/game/20251208_PHI@LAC
```

Confirm:

```text
claim_language_context.feature_versions.offensive_efficiency_support exists
claim_language_context.offensive_efficiency_support_by_side.away exists
claim_language_context.offensive_efficiency_support_by_side.home exists
language_support.offensive_efficiency_support exists on relevant rows
metadata_only = true
language_boost_allowed = false
component_metrics is not present in the runtime payload
components is present as structured object
```

---

# 11. Data Quality and Interpretation Cautions

## 11.1 Missing hierarchy parents matter

Rows with missing parent hierarchy should be treated carefully:

- `missing_core_area_parent`
- `metric_has_missing_category_parent`
- `missing_category`
- `row_metadata_missing_core_area`

Best practice:

> Trust clean registry-backed paths first. Use missing-parent rows as clues, not final product logic.

## 11.2 Claim validation is not winner accuracy

Do not explain the results as:

> GameLens picked winners correctly X% of the time.

Use this instead:

> Claims in this bucket were supported by postgame data X% of the time.

## 11.3 Strong row count matters

Avoid promoting features from tiny buckets.

A reasonable first gate:

```text
row_count >= 30 per run
row_count >= 100 all selected runs
repeatability across at least 2 runs
```

## 11.4 Neutral / mixed is not always failure

Some buckets validate lower because the actual game was neutral or mixed. This is especially common in volatile areas like touchdown rate, turnovers, and red zone finish.

That does not mean the metric is useless. It means it should probably be used as restraint/context, not loud support.

---

# 12. Recommended Implementation Order

## 1. Keep `offensive_efficiency_support_v1` metadata-only

Status:

```text
Complete / keep
```

It is implemented, validated, exposed, and cleaned up.

Do not convert it into direct language boosting yet.

---

## 2. Update documentation and commit

Status:

```text
Current step
```

Include files changed:

```text
agg/gamelens_training/update_claim_training_features.py
agg/gamelens_training/build_claim_language_calibration.py
services/claim_language_features.py
services/claim_language_response.py
services/game_service.py
```

---

## 3. Continue Level 4 analysis

Next tests:

- offensive efficiency x `two_way_context`
- offensive efficiency x surface
- offensive efficiency x metric
- clean hierarchy only
- repeat across more random samples

---

## 4. Implement volatility / chaos restraint family

Warning paths:

```text
Disruption and Turnovers
Disruption and Turnovers > Turnovers
Disruption and Turnovers > Turnovers > turnover_margin_per_game
Scoring Efficiency > Scoring Production > td_rate
Scoring Efficiency > Red Zone Finish
Scoring Efficiency > Red Zone Finish > red_zone_efficiency
Pressure
Turnover Risk
```

Language use:

- soften
- caution
- block boost
- avoid “confirmed edge” wording

---

## 5. Promote `matchup_fragility_warning_v0` as language restraint

Use:

- stronger language when support is stable
- measured language when support is mixed
- soft caution when support is available but fragile

Do not use for automatic pick confidence.

---

## 6. Promote `defense_vs_dynamite_v0` and `explosive_offense_warning_v0`

Use:

- support defense when opponent offense lacks firepower
- soften defense when facing dangerous offense
- warn when claim is against explosive opponent with weak support

---

## 7. Add strong-only scoring and rushing support

Rules:

- `strong_scoring_support` only
- `strong_rush_efficiency_support` only
- no boost for clear/mild buckets

Avoid treating mild/clear support as meaningful.

---

## 8. Keep passing efficiency in scoped re-test

Use clean path only:

```text
Offensive Output > Passing Game > yards_per_pass
```

Then retest.

---

## 9. Keep Defensive Control / Scoring Suppression as review

Use as context/supporting language only.

Do not boost confidence broadly from `points_allowed_per_play` alone.

---

## 10. Do not implement drive-conversion or broad rushing-control yet

Hold these:

- `drive_conversion_scoring_support_v0`
- broad `rushing_control_score`
- true drive sustainability
- full rushing control / clock control

These need cleaner game/team-level inputs, not claim-row reconstruction.

---

# 13. Suggested Product Language Patterns

## Strong support

> This claim has clean efficiency support, so GameLens can speak more confidently about the matchup shape.

## Measured support

> The signal leans in this direction, but support is not strong enough to overstate it.

## Mixed profile

> The efficiency profile is split, so this should stay measured.

## Warning / restraint

> This area is volatile, so the claim should stay measured unless confirmed by stronger support elsewhere.

## Strength-on-strength

> Both sides have meaningful support, so this is more of a strength-on-strength matchup than a clean edge.

## Missing context

> The claim does not have enough supporting context to justify stronger language.

---

# 14. QA Checklist Before Promoting Any Feature

Before a feature graduates from metadata to language effect, confirm:

```text
[ ] Feature has enough row count
[ ] Feature has lift above baseline
[ ] Lift survives metric split
[ ] Lift survives claim_type / claim_layer split
[ ] Lift survives clean hierarchy filtering
[ ] Feature does not accidentally boost blocked metrics
[ ] Feature does not change matchup_lean
[ ] Feature does not change outcome_confidence
[ ] Feature does not change Model Trust
[ ] Feature does not change frontend visible copy unless intentionally wired
[ ] Frontend receives backend-owned language fields, not inferred rules
```

---

# 15. Current Status Snapshot

## Completed

```text
clean_hierarchy_context_v1 added to Level 3
Level 3 hierarchy metadata validated
Level 4 v0.1 two_way_context support working
offensive_efficiency_support_v1 added to Level 3
offensive_efficiency_support_v1 validated above baseline
Level 4 sees offensive_efficiency_support_v1 as metadata
/game exposes offensive_efficiency_support_v1 in claim_language_context
Runtime anchor issue fixed
component_metrics duplicate removed from runtime payload
metadata_only / language_boost_allowed guardrails preserved
```

## Not yet done

```text
No frontend UI rendering yet
No automatic stronger-language use yet
No production language rewrite yet
No Model Trust or matchup lean changes
No winner-confidence changes
No public success-rate UI
```

## Best next move

```text
Commit backend metadata exposure and documentation.
Then continue Level 4 calibration analysis before frontend promotion.
```

---

# 16. One-Sentence Summary

> **GameLens should boost language only from repeatable efficiency support, soften language around volatility and fragile finishing, and avoid turning descriptive context into winner confidence.**
