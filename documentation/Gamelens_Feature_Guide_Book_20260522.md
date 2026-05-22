# GameLens Feature Guide Book

_Last updated: 2026-05-22_

## Purpose

This guide captures the current GameLens feature-engineering discoveries, testing results, exploratory hierarchy findings, and recommended implementation order.

The main goal is not to build a forced pick machine.

The goal is:

> Help GameLens speak more truthfully about matchup claims.

That means these features should mainly support:

- claim-language calibration
- stronger or softer wording
- warning / caution metadata
- matchup-path explanation
- future `claim_strength_language_signal`
- confidence restraint when the data is fragile

They should **not** directly drive:

- automatic winner prediction
- matchup lean override
- automatic High Confidence
- Model Trust override
- betting-style pick logic

---

# 1. Current Working Thesis

The discovery work now points to a cleaner product thesis:

> **GameLens should trust efficient production more than volatile event outcomes.**

The data repeatedly favored efficient offense and broad offensive output, while warning against chaos-driven or fragile finishing signals.

## Clean support family

These are the strongest support patterns found so far:

- `Offensive Output`
- `Passing Game` inside `Offensive Output`
- `Rushing Game` inside `Offensive Output`
- `yards_per_play`
- `points_per_play`
- `yards_per_pass`
- `yards_per_rush`

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

---

# 3. Layered Discovery Results

## 3.1 Metric-level findings

### Best metric opportunities

| Metric | Interpretation |
|---|---|
| `points_per_play` | Clean scoring-efficiency support |
| `yards_per_play` | Broad snap-to-snap offensive efficiency |
| `yards_per_pass` | Passing efficiency support |
| `yards_per_rush` | Rushing efficiency support |

### Metric warnings

| Metric | Interpretation |
|---|---|
| `td_rate` | Major warning; very volatile |
| `red_zone_efficiency` | Warning; finishing is fragile |
| `turnover_margin_per_game` | Warning; turnover edge is unstable |

### Metric context only

| Metric | Interpretation |
|---|---|
| `third_down_pct` | High-volume but mostly neutral |
| `1st_down_rate` | Descriptive, not a strong driver |

---

## 3.2 Category-level findings

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
| `Scoring Production` | Warning category despite useful `points_per_play` inside it |
| `Red Zone Finish` | Suspicious / generally weak |

### Review / context categories

| Category | Read |
|---|---|
| `Passing Game` | Promising but needs clean scoping |
| `Scoring Suppression` | Positive but not fully repeat-clean |
| `Offensive Rhythm` | Mixed because it contains both useful and weak signals |
| `Drive Conversion` | High-volume neutral/context |
| `Scoring Efficiency` | Looks positive in some rows, but shape needs caution |

---

## 3.3 Core Area findings

| Core Area | Decision | Read |
|---|---|---|
| `Offensive Output` | Opportunity | Cleanest broad Core Area support |
| `Disruption and Turnovers` | Warning | Cleanest broad Core Area warning |
| `Defensive Control` | Review | Directionally useful, but not clean enough alone |
| `Scoring Efficiency` | Review / mixed | Contains both strong and dangerous signals |

Football summary:

> Offensive Output is sturdy. Disruption and Turnovers is volatile. Defensive Control needs scoping. Scoring Efficiency is split.

---

## 3.4 Three-level hierarchy finding

The hierarchy query was the most useful exploratory view because it showed where the signal actually lives:

```text
Core Area
  → Category
      → Metric
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
    third_down_pct = neutral
```

That means we should **not** say:

> Scoring Efficiency is good.

The better rule is:

> Some scoring-efficiency metrics are good, but finishing and touchdown-rate metrics are fragile.

---

# 4. Data Quality and Interpretation Cautions

## 4.1 Missing hierarchy parents matter

The hierarchy query surfaced rows with:

- `missing_core_area_parent`
- `metric_has_missing_category_parent`
- `missing_category`

These rows should not be thrown away immediately, but they should be treated carefully.

Best practice:

> Trust `data_shape_note = ok` paths first. Use missing-parent rows as clues, not final product logic.

## 4.2 Claim validation is not winner accuracy

Do not explain the results as:

> GameLens picked winners correctly X% of the time.

Use this instead:

> Claims in this bucket were supported by postgame data X% of the time.

## 4.3 Strong row count matters

Avoid promoting features from tiny buckets.

A reasonable first gate:

```text
row_count >= 30 per run
row_count >= 100 all selected runs
repeatability across at least 2 runs
```

## 4.4 Neutral / mixed is not always failure

Some buckets validate lower because the actual game was neutral or mixed. This is especially common in volatile areas like touchdown rate, turnovers, and red zone finish.

That does not mean the metric is useless. It means it should probably be used as restraint/context, not loud support.

---

# 5. Feature Candidate Guide

## Existing Level 3 foundation

### `offense_finish_score`

**Question:** Does this team have pregame support for moving the ball and finishing drives?

**Current use:** Existing Level 3 feature.

**Lesson:** Useful only when scoped correctly. Do not attach it to defensive, turnover, or pressure claims.

**Status:** Keep, but refine with hierarchy discoveries.

---

### `defensive_suppression_score`

**Question:** Does this team have pregame support for limiting opponent scoring/offensive output?

**Current use:** Existing Level 3 feature.

**Lesson:** It works best when Defensive Control exists. Metric-only fallback was dangerous.

**Status:** Keep as scoped defensive context.

---

### `two_way_context`

**Question:** Does the claimed team have both offensive finish support and defensive suppression support?

**Buckets:**

```text
supportive
available_mixed
unavailable
```

**Lesson:** The bucket is more useful than the raw `two_way_edge_score`.

**Status:** Keep and use as claim-language support, not confidence.

---

# 6. Ten Tested Feature Candidates

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

**Implementation note:** Good candidate after clean-path validation.

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

**Implementation note:** Use for narrow rushing language only.

---

## 10. Broad `rushing_control_score`

**Question:** Does broad rushing/control context help broader game-shape claims validate?

**Finding:** Not ready. The test mostly collapsed into thin evidence.

**Decision:** `hold_research`

**Implementation note:** Do not implement until cleaner game/team-level rushing-control inputs exist.

---

# 7. Recommended Implementation Order

## 1. Create a clean hierarchy QA view first

**Why first:** Before implementing more logic, verify which rows have clean Core Area → Category → Metric paths.

**Goal:** Separate clean paths from missing-parent paths.

**Output needed:**

```text
clean_hierarchy_flag
missing_core_area_parent_flag
missing_category_parent_flag
data_shape_note
```

**Likely files / areas:**

- `build_claim_training_examples.py`
- `metric_registry.py`
- BigQuery QA query or view

**Decision:** Must do before hardcoding more product behavior.

---

## 2. Add / formalize a Level 4 calibration summary table

**Why second:** The next big step is not another clever feature. It is a table that summarizes which claim types and feature contexts historically validate.

**Suggested table:**

```text
Analytics.gamelens_claim_language_calibration
```

**Suggested grain:**

```text
feature_formula_version
claim_type
claim_layer
claim_name
core_area
category
metric
hierarchy_path
feature_bucket
sample_size
validation_rate
lift_vs_baseline
repeatability_label
language_modifier
```

**Likely files / areas:**

- `agg/gamelens_training/build_claim_language_calibration.py`
- `services/claim_language_support_registry.py`
- `services/claim_language_response.py`

**Decision:** Highest architectural value.

---

## 3. Implement the clean offensive-efficiency support family

**Why third:** This was the cleanest positive discovery.

**Promote support paths:**

```text
Offensive Output
Offensive Output > Passing Game
Offensive Output > Rushing Game
Offensive Output > Offensive Rhythm > yards_per_play
Offensive Output > Passing Game > yards_per_pass
Offensive Output > Rushing Game > yards_per_rush
Scoring Efficiency > Scoring Production > points_per_play
```

**Language use:** allow stronger language when the claim is in one of these clean paths and sample support is repeat-positive.

**Do not use for:** winner confidence.

**Likely files / areas:**

- `services/claim_language_support_registry.py`
- `services/claim_language_response.py`
- possibly `claim_language_features.py`

---

## 4. Implement the volatility / chaos restraint family

**Why fourth:** This was the clearest warning discovery.

**Warning paths:**

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

**Language use:** soften, caution, or block boost.

**Example language:**

> This signal is volatile, so GameLens should keep the claim measured unless other support is strong.

**Likely files / areas:**

- `services/claim_language_support_registry.py`
- `services/claim_language_response.py`
- `model_trust_service.py` later, if miss severity copy needs it

---

## 5. Promote `matchup_fragility_warning_v0` as language restraint

**Why fifth:** This is a clean combo-language feature.

**Use:**

- stronger language when support is stable
- measured language when support is mixed
- soft caution when support is available but fragile

**Do not use for:** automatic pick confidence.

---

## 6. Promote `defense_vs_dynamite_v0` and `explosive_offense_warning_v0`

**Why sixth:** These are useful opponent-context features.

**Use:**

- support defense when opponent offense lacks firepower
- soften defense when facing dangerous offense
- warn when claim is against explosive opponent with weak support

**Likely product phrasing:**

> The defense has support, but the opposing offense has enough firepower to make the read less clean.

---

## 7. Add strong-only scoring and rushing support

**Why seventh:** These are good scoped support features, but the hierarchy already covers part of them.

**Rules:**

- `strong_scoring_support` only
- `strong_rush_efficiency_support` only
- no boost for clear/mild buckets

**Avoid:** treating mild/clear support as meaningful.

---

## 8. Keep passing efficiency in scoped re-test

**Why eighth:** The hierarchy result improved confidence in `yards_per_pass`, but the earlier feature test was not perfectly clean.

**Use:** Start with clean path only:

```text
Offensive Output > Passing Game > yards_per_pass
```

Then retest.

---

## 9. Keep Defensive Control / Scoring Suppression as review

**Why ninth:** Defensive Control is positive but not repeat-clean.

**Use:** context/supporting language only.

**Do not:** boost confidence broadly from `points_allowed_per_play` alone.

---

## 10. Do not implement drive-conversion or broad rushing-control yet

**Why last:** They are interesting but not ready.

Hold these:

- `drive_conversion_scoring_support_v0`
- broad `rushing_control_score`
- true drive sustainability
- full rushing control / clock control

These need cleaner game/team-level inputs, not claim-row reconstruction.

---

# 8. Suggested Product Language Patterns

## Strong support

> This claim has clean efficiency support, so GameLens can speak more confidently about the matchup shape.

## Soft support

> The signal leans in this direction, but support is not strong enough to overstate it.

## Warning / restraint

> This area is volatile, so the claim should stay measured unless confirmed by stronger support elsewhere.

## Strength-on-strength

> Both sides have meaningful support, so this is more of a strength-on-strength matchup than a clean edge.

## Missing context

> The claim does not have enough supporting context to justify stronger language.

---

# 9. Recommended Next SQL Work

## A. Clean-path-only hierarchy query

Re-run the hierarchy rollup using only:

```text
data_shape_note = ok
```

Goal:

> Confirm whether the same support/warning families hold after removing missing-parent paths.

## B. Claim-type/layer split

Break each hierarchy path by:

```text
claim_type
claim_layer
```

Goal:

> Learn whether a metric is good for metric highlights, category summaries, core-area comparisons, or game-profile claims.

## C. Repeat with more random samples

Suggested future runs:

```text
fresh_96_features_qa_v3
fresh_96_features_qa_v4
fresh_96_features_qa_v5
```

Goal:

> Confirm stability before productizing more logic.

## D. Clean hierarchy parent audit

Find exactly why rows have:

```text
missing_core_area_parent
missing_category_parent
```

Goal:

> Decide whether those are expected row shapes or metadata gaps to fix.

---

# 10. Final Working Roadmap

## Immediate next work

1. Clean hierarchy QA view.
2. Clean-path-only hierarchy validation.
3. Level 4 calibration summary table design/update.
4. Implement offensive-efficiency support and chaos-restraint rules as language calibration metadata.

## Medium next work

5. Promote `matchup_fragility_warning_v0`.
6. Promote `defense_vs_dynamite_v0` / `explosive_offense_warning_v0`.
7. Add strong-only scoring/rushing support.
8. Re-test passing efficiency and defensive control using clean hierarchy paths.

## Backburner

9. Drive conversion / true drive sustainability.
10. Broad rushing control.
11. Field Control / snap-count confidence.
12. Hidden lean score.
13. Model Trust miss severity language.

---

# 11. One-Sentence Summary

> **GameLens should boost language from repeatable efficiency support, soften language around volatility and fragile finishing, and avoid turning descriptive context into winner confidence.**

