# GameLens Training Note — `two_way_context` Validation and Usage Decision

_Last updated: May 15, 2026_

## Purpose

This note documents the current validation read for the Level 3 engineered feature:

`two_way_context`

The goal is to preserve the testing logic, interpretation, and implementation boundary before wiring this feature into GameLens language or calibration logic.

---

## Product Boundary

`two_way_context` is **claim-support context**.

It should help answer:

> When GameLens makes a pregame claim, does that claim have stronger supporting matchup context?

It should **not** be treated as:

- winner prediction
- spread prediction
- moneyline signal
- automatic confidence boost
- proof that the leaned team will win

This feature belongs in the GameLens product frame:

> matchup intelligence and confidence calibration

not:

> forced pick machine

---

## Feature Meaning

Current values:

| Value | Meaning |
|---|---|
| `supportive` | Both offensive-finish and defensive-suppression context support the claimed side enough to suggest stronger claim support |
| `available_mixed` | Two-way context exists, but it is mixed or not clearly supportive |
| `unavailable` | One or both required context components are unavailable |

Current formula version:

`offense_finish_v2__defensive_suppression_v3__two_way_context_v1`

---

## Anti-Leakage Boundary

This feature must remain **pregame-only**.

Level 3 feature creation should not use:

- `validation_result`
- `actual_gap`
- `actual_side`
- `actual_team`
- final score
- winner result
- postgame metrics
- postgame validation labels

Postgame validation belongs in Level 2.

Feature engineering belongs in Level 3 and must only use pregame-safe claim/context fields.

---

## Tested Runs

The current validation decision is based on three 96-game QA runs:

| Run ID | Notes |
|---|---|
| `baseline_96_stage1_v2` | Original baseline sample |
| `fresh_96_features_qa_v2` | Fresh random sample |
| `fresh_96_features_qa_v3` | Additional fresh sample collected May 15, 2026 |

The third sample had 96 games collected, 70 games not previously seen in local QA payload runs, and 26 overlapping games. This was considered fresh enough for repeatability testing.

---

## Overall Three-Run Result

Across all claim rows, `supportive` repeatedly validated better than `available_mixed` and `unavailable`.

| Run | Supportive | Available Mixed | Unavailable | Lift vs Best Other |
|---|---:|---:|---:|---:|
| `baseline_96_stage1_v2` | 52.75% | 44.76% | 45.18% | +7.57 to +7.99 pts |
| `fresh_96_features_qa_v2` | 56.16% | 47.60% | 48.78% | +7.38 to +8.56 pts |
| `fresh_96_features_qa_v3` | 64.26% | 43.77% | 45.42% | +18.84 to +20.49 pts |

Interpretation:

`two_way_context = supportive` is now a repeated positive signal for claim validation.

However, the feature should still be used as a **language-support flag**, not as direct outcome confidence.

---

## Claim Layer Result

`supportive` beat the comparison buckets for both headline and supporting claims.

### Headline Claims

| Run | Supportive | Available Mixed | Unavailable |
|---|---:|---:|---:|
| `baseline_96_stage1_v2` | 54.87% | 45.96% | 46.36% |
| `fresh_96_features_qa_v2` | 55.10% | 50.87% | 52.60% |
| `fresh_96_features_qa_v3` | 63.24% | 46.75% | 48.24% |

### Supporting Claims

| Run | Supportive | Available Mixed | Unavailable |
|---|---:|---:|---:|
| `baseline_96_stage1_v2` | 51.76% | 44.20% | 44.71% |
| `fresh_96_features_qa_v2` | 56.65% | 46.04% | 47.21% |
| `fresh_96_features_qa_v3` | 64.76% | 42.41% | 44.25% |

Interpretation:

The feature is useful for both headline and supporting language, but the strongest implementation target is still metric/category language rather than Game Profile or winner confidence.

---

## Claim Type + Layer Aggregate Result

Aggregated across the three tested runs:

| Claim Type | Claim Layer | Supportive Rate | Best Other Rate | Lift |
|---|---|---:|---:|---:|
| `category_summary` | supporting | 56.85% | 44.79% | +12.06 pts |
| `metric_highlight` | supporting | 58.75% | 46.98% | +11.77 pts |
| `metric_highlight` | headline | 58.06% | 47.20% | +10.86 pts |
| `team_comparison_metric` | supporting | 57.49% | 46.70% | +10.79 pts |
| `core_area_summary` | supporting | 56.51% | 47.06% | +9.45 pts |
| `core_area_comparison` | headline | 59.60% | 51.02% | +8.58 pts |
| `game_profile` | headline | 52.56% | 50.68% | +1.88 pts |

Implementation read:

Strongest targets:

- `category_summary + supporting`
- `metric_highlight + supporting`
- `metric_highlight + headline`
- `team_comparison_metric + supporting`

Secondary/watch:

- `core_area_summary + supporting`
- `core_area_comparison + headline`

Do not prioritize:

- `game_profile + headline`

---

## Strong Allowlist

Use `two_way_context = supportive` as a language-support flag only for the following strong candidates.

### Category Summary — Supporting

Allow firmer language when:

| Claim Type | Layer | Metric | Notes |
|---|---|---|---|
| `category_summary` | supporting | `1st_down_rate` | Strong supportive lift |
| `category_summary` | supporting | `points_allowed_per_play` | Strongest repeatable defensive suppression signal |
| `category_summary` | supporting | `yards_per_rush` | Good rushing/control candidate |
| `category_summary` | supporting | `red_zone_efficiency` | Strong enough for careful support language |

### Metric Highlight

Allow firmer language when:

| Claim Type | Layer | Metric | Notes |
|---|---|---|---|
| `metric_highlight` | supporting | `points_allowed_per_play` | Very strong signal |
| `metric_highlight` | supporting | `1st_down_rate` | Very strong signal |
| `metric_highlight` | headline | `points_allowed_per_play` | Strong signal |

### Team Comparison Metric — Supporting

Allow firmer language when:

| Claim Type | Layer | Metric | Notes |
|---|---|---|---|
| `team_comparison_metric` | supporting | `points_allowed_per_play` | Strong signal |
| `team_comparison_metric` | supporting | `red_zone_efficiency` | Strong signal |
| `team_comparison_metric` | supporting | `points_per_play` | Strong candidate only in this surface |

---

## Watch Candidates

These showed interesting signal but should not be broadly used yet.

| Claim Type | Layer | Metric | Reason |
|---|---|---|---|
| `metric_highlight` | headline | `yards_per_pass` | High validation rate but smaller row count |
| `metric_highlight` | supporting | `red_zone_efficiency` | Useful but weaker than category/team-comparison versions |
| `metric_highlight` | headline | `turnover_margin_per_game` | Interesting but turnover signal remains volatile |
| `metric_highlight` | supporting | `third_down_pct` | Some signal, but not consistent enough broadly |
| `metric_highlight` | supporting | `points_per_play` | Some signal, but better handled carefully |
| `category_summary` | supporting | `yards_per_pass` | Watch candidate only |

---

## Do Not Use Yet

Do not use `two_way_context` to strengthen language for these broadly:

- `td_rate`
- `turnover_margin_per_game` broadly
- `third_down_pct` broadly
- `yards_per_play` broadly
- `points_per_play` in `category_summary`
- Game Profile claims
- winner confidence
- model outcome confidence

Specific caution:

`third_down_pct` and `turnover_margin_per_game` can still appear as claims, but `two_way_context = supportive` should not automatically make their language stronger.

---

## Suggested Product Language Behavior

When a claim is on the strong allowlist and `two_way_context = supportive`, the API may use slightly firmer support language.

Examples:

- “This claim has stronger two-way matchup support.”
- “The supporting context backs this metric edge more cleanly.”
- “This metric is supported by both finish and suppression context.”
- “Stronger supporting context is present, but this remains a claim-level signal.”

Avoid language like:

- “This means the team should win.”
- “High confidence pick.”
- “Lock.”
- “The model is confident because of two-way context.”
- “Two-way support confirms the outcome.”

---

## Suggested Backend Implementation Direction

Do not hardcode this directly into winner scoring.

Preferred implementation:

1. Add a small helper or config allowlist for claim-level language support.
2. Check:
   - `two_way_context == "supportive"`
   - claim type/layer is allowlisted
   - metric is allowlisted for that surface
3. Add a structured field such as:

```json
{
  "language_support": {
    "two_way_context": "supportive",
    "language_boost_allowed": true,
    "reason": "supported_by_offensive_finish_and_defensive_suppression_context"
  }
}
```

4. Let frontend display or lightly emphasize the explanation.
5. Do not change model winner outcome confidence from this alone.

---

## Possible Files To Touch Later

Likely backend files:

- `agg/gamelens_training/update_claim_training_features.py`
  - keep feature calculation and formula documentation updated
- `services/game_service.py`
  - if `/game` begins surfacing claim-level language support
- `services/model_trust_service.py`
  - only if language support becomes part of model trust summaries
- future calibration/model script
  - if Level 4 turns this into a learned calibration layer

Possible future config file:

- `analytics/claim_language_support_registry.py`

or similar.

Purpose:

Create a small maintainable allowlist for:

- claim type
- claim layer
- metric
- allowed language behavior
- caution notes

---

## Current Decision

Adopt `two_way_context` as a **claim-language support feature**, not an outcome feature.

Strongest first use:

- metric/category/team-comparison explanation language
- especially `points_allowed_per_play`
- then `1st_down_rate`, `red_zone_efficiency`, and selected rushing/scoring metrics

Do not use this for:

- Game Profile language yet
- winner confidence
- high-confidence pick logic
- broad turnover language
- broad third-down language

---

## Next Recommended QA Step

Before wiring this into `/game`, consider one more focused SQL pass:

- compare allowlisted metrics only
- test whether `supportive` remains useful by season
- test whether `supportive` remains useful by early/mid/late/postseason bucket
- inspect example rows for false positives

The feature has earned a role, but the safest first production use is a small, bounded language-support flag.