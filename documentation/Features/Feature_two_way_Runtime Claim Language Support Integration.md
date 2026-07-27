---

# Addendum — Runtime Claim Language Support Integration

_Last updated: May 15, 2026_

## Summary

We moved `two_way_context` from a validated training feature into a bounded runtime API metadata layer.

The important product decision:

> `two_way_context` should support stronger claim language only when the data shows the claim is better supported.

It should **not** be used as:

- winner confidence
- pick confidence
- spread / moneyline prediction
- automatic model boost
- Game Profile override
- Model Trust override

This addendum documents how the runtime version was added, what files changed, what the API now returns, how the frontend should use it, and what guardrails should remain in place.

---

## What Was Added

### New Files

```text
analytics/claim_language_support_registry.py
services/claim_language_features.py
services/claim_language_response.py
```

### Updated File

```text
services/game_service.py
```

### Existing Documentation File

```text
documentation/two_way_context.md
```

---

## Implementation Goal

The goal was to expose `two_way_context` safely through `/game` without changing model behavior.

The API now computes side-level claim language context at runtime:

```json
"claim_language_context": {
  "available": true,
  "scope": "claim_language_support",
  "two_way_context_by_side": {
    "away": {
      "offense_finish_score": -0.7081,
      "defensive_suppression_score": -0.729,
      "two_way_edge_score": -0.729,
      "two_way_context": "available_mixed"
    },
    "home": {
      "offense_finish_score": 0.7762,
      "defensive_suppression_score": 0.7516,
      "two_way_edge_score": 0.7516,
      "two_way_context": "supportive"
    }
  }
}
```

This tells the frontend:

- which side has supportive two-way context
- whether a claim is eligible for stronger language
- whether a row should remain normal/unboosted

---

## Product Meaning of “Claim Language Support”

A **claim** is a specific pregame statement such as:

- GB has an edge in `points_allowed_per_play`
- BUF has a stronger `red_zone_efficiency` profile
- A team comparison row favors NE in `points_per_play`
- A category summary leans toward PHI in Offensive Rhythm

`claim_language_support` means:

> The system has enough supporting context to let the UI describe this specific claim with slightly firmer language.

It does **not** mean:

> The team should win.

It does **not** mean:

> The model should increase confidence.

It does **not** mean:

> This claim is guaranteed.

Good framing:

```text
This claim has stronger supporting context.
```

Bad framing:

```text
This means the team is more likely to win.
```

---

## Data-Backed Reason For Adding This

The feature was tested across multiple QA runs.

### Overall `two_way_context` Result

Across the tested runs:

| Run | Supportive | Available Mixed | Unavailable | Read |
|---|---:|---:|---:|---|
| `baseline_96_stage1_v2` | 52.75% | 44.76% | 45.18% | Supportive beat both other buckets |
| `fresh_96_features_qa_v2` | 56.16% | 47.60% | 48.78% | Supportive repeated |
| `fresh_96_features_qa_v3` | 64.26% | 43.77% | 45.42% | Supportive repeated strongly |

Interpretation:

`supportive` claims were supported by postgame validation more often than `available_mixed` or `unavailable`.

This supports using the feature as a **claim-language support flag**.

It does not support using it as a direct winner predictor.

---

## Strong Allowlist From QA

We did not use `two_way_context` everywhere.

The final implementation uses an allowlist in:

```text
analytics/claim_language_support_registry.py
```

A row can receive `language_boost_allowed = true` only when:

1. `two_way_context == "supportive"`
2. the claim type/layer/metric is allowlisted
3. the row points to the supportive side

---

## Strong Candidate Rows

The strongest rows from QA were:

| Claim Type | Layer | Metric | Supportive Rate | Best Other Rate | Lift |
|---|---|---:|---:|---:|---:|
| `metric_highlight` | supporting | `points_allowed_per_play` | 73.33% | 39.47% | +33.86 pts |
| `category_summary` | supporting | `1st_down_rate` | 67.74% | 40.30% | +27.44 pts |
| `category_summary` | supporting | `points_allowed_per_play` | 67.09% | 43.20% | +23.89 pts |
| `team_comparison_metric` | supporting | `points_allowed_per_play` | 67.09% | 43.20% | +23.89 pts |
| `category_summary` | supporting | `yards_per_rush` | 65.00% | 46.30% | +18.70 pts |
| `metric_highlight` | supporting | `1st_down_rate` | 72.22% | 54.55% | +17.67 pts |
| `metric_highlight` | headline | `points_allowed_per_play` | 61.54% | 46.55% | +14.99 pts |
| `category_summary` | supporting | `red_zone_efficiency` | 55.00% | 41.61% | +13.39 pts |
| `team_comparison_metric` | supporting | `red_zone_efficiency` | 55.00% | 41.61% | +13.39 pts |
| `team_comparison_metric` | supporting | `points_per_play` | 65.22% | 56.86% | +8.36 pts |

These became the first production-safe allowlist.

---

## Current Allowlist

The current allowlist is intentionally narrow.

### Team Comparison Metric

```text
team_comparison_metric + supporting + points_allowed_per_play
team_comparison_metric + supporting + red_zone_efficiency
team_comparison_metric + supporting + points_per_play
```

### Metric Highlight

```text
metric_highlight + supporting + points_allowed_per_play
metric_highlight + supporting + 1st_down_rate
metric_highlight + headline + points_allowed_per_play
```

### Category Summary

```text
category_summary + supporting + 1st_down_rate
category_summary + supporting + points_allowed_per_play
category_summary + supporting + yards_per_rush
category_summary + supporting + red_zone_efficiency
```

---

## Explicitly Not Allowed Yet

These should not receive stronger language broadly:

```text
third_down_pct
turnover_margin_per_game
td_rate
yards_per_play
yards_per_pass broadly
Game Profile claims
winner/outcome confidence
```

Important nuance:

Some of these metrics may still be useful as normal claims.  
They just should not receive `two_way_context` language support yet.

Example:

`third_down_pct` may still appear as a visible edge.

But if `language_boost_allowed = false`, the frontend should not add stronger wording just because the side had supportive context.

---

## Runtime Architecture

### `analytics/claim_language_support_registry.py`

Purpose:

- stores the allowlist
- exposes `build_language_support()`
- decides whether a row can receive `language_boost_allowed = true`

This file is the main safety rail.

It prevents the feature from spreading into unsupported metrics.

---

### `services/claim_language_features.py`

Purpose:

- computes runtime two-way context by side
- uses `/game` objects such as:
  - `core_area_comparison`
  - `team_comparison`

It creates:

```json
{
  "offense_finish_score": 0.7762,
  "defensive_suppression_score": 0.7516,
  "two_way_edge_score": 0.7516,
  "two_way_context": "supportive"
}
```

Important note:

The runtime helper is a practical `/game` implementation.  
The training pipeline remains the source of truth for full Level 3 feature engineering.

Future Level 4 calibration may eventually replace or refine this runtime calculation.

---

### `services/claim_language_response.py`

Purpose:

- annotates response rows with `language_support`
- does not rewrite summaries
- does not change model scoring
- does not change confidence
- does not change Model Trust

It currently annotates:

```text
team_comparison
matchup_breakdown.metric_highlights
matchup_breakdown.category_summaries
category_summaries.drivers
```

It intentionally does not annotate:

```text
context_notes
core_area_summaries
game_profile
model_trust
matchup_lean
model_outcome
```

---

### `services/game_service.py`

Purpose:

- builds the normal `/game` response
- computes `claim_language_context`
- applies response-only `language_support` annotations after model logic is complete

Important order:

```text
team_comparison built
core_area_comparison built
claim_language_context built
game_profile built
matchup_lean built
model_outcome built
model_trust built
matchup_breakdown built
response-only language annotations applied
response returned
```

This order matters because the language annotations do not feed back into:

```text
matchup_lean
model_outcome
model_trust
confidence
saved model results
```

---

## API Shape

### Top-Level `claim_language_context`

Example:

```json
"claim_language_context": {
  "available": true,
  "scope": "claim_language_support",
  "two_way_context_by_side": {
    "away": {
      "defensive_suppression_score": -0.729,
      "offense_finish_score": -0.7081,
      "two_way_context": "available_mixed",
      "two_way_edge_score": -0.729
    },
    "home": {
      "defensive_suppression_score": 0.7516,
      "offense_finish_score": 0.7762,
      "two_way_context": "supportive",
      "two_way_edge_score": 0.7516
    }
  }
}
```

This object is global context for the game.

Frontend can use it to understand:

- which side has two-way support
- whether support exists at all
- why certain rows are allowed to show stronger claim language

---

### Row-Level `language_support`

Example allowed row:

```json
"language_support": {
  "language_boost_allowed": true,
  "reason": "supported_by_offensive_finish_and_defensive_suppression_context",
  "scope": "claim_language_support",
  "two_way_context": "supportive",
  "two_way_edge_score": 0.7516
}
```

Example blocked row:

```json
"language_support": {
  "language_boost_allowed": false,
  "reason": null,
  "scope": "claim_language_support",
  "two_way_context": "supportive",
  "two_way_edge_score": 0.7516
}
```

Blocked rows can still have `two_way_context = supportive`.

That means:

> The team had supportive context, but this specific metric/claim was not allowlisted for stronger language.

This is important for the frontend.

Do not treat `two_way_context = supportive` alone as enough.

The frontend should check:

```js
row.language_support?.language_boost_allowed === true
```

not:

```js
row.language_support?.two_way_context === "supportive"
```

---

## Frontend Usage Guide

### Primary Rule

Only use claim language support when:

```js
language_support.language_boost_allowed === true
```

Do not use:

```js
two_way_context === "supportive"
```

by itself.

---

## Recommended Frontend Display

### Option A — Small Badge

Best first implementation:

```text
Supported claim
```

or:

```text
Two-way support
```

or:

```text
Stronger support
```

Recommended badge placement:

- Team Comparison row
- Metric Highlight card
- Category Summary card/driver row

Avoid making this badge larger than the actual metric edge.

This should feel like a quiet confidence note, not a betting signal.

---

### Option B — Tooltip

Recommended tooltip:

```text
This claim has extra support because the same side also shows both offensive-finish and defensive-suppression context.
```

Alternative shorter tooltip:

```text
This edge is backed by both finish and suppression context.
```

More cautious tooltip:

```text
This strengthens the claim language, but it does not change winner confidence.
```

---

### Option C — Subtext Under Metric

Example:

```text
GB shows a clear advantage in Points Allowed Per Play.
Supported by two-way matchup context.
```

or:

```text
GB shows a clear advantage in Red Zone Efficiency.
This claim has stronger supporting context.
```

Use sparingly.

The safest UI is a badge + tooltip.

---

## Recommended Frontend Logic

Pseudo-code:

```js
function getClaimSupportBadge(row) {
  const support = row.language_support;

  if (!support?.language_boost_allowed) {
    return null;
  }

  return {
    label: "Supported claim",
    tooltip:
      "This claim has extra support from both offensive-finish and defensive-suppression context.",
    scope: support.scope,
    reason: support.reason,
  };
}
```

For category summaries:

```js
function getCategorySupportBadge(summary) {
  const support = summary.language_support;

  if (!support?.language_boost_allowed) {
    return null;
  }

  return {
    label: "Supported category read",
    tooltip:
      "One or more drivers in this category have two-way matchup support.",
    supportedDriverCount: support.supported_driver_count,
  };
}
```

For driver-level rows:

```js
function getDriverSupportBadge(driver) {
  if (!driver.language_support?.language_boost_allowed) {
    return null;
  }

  return {
    label: "Supported driver",
    tooltip:
      "This driver is backed by two-way matchup context.",
  };
}
```

---

## Frontend Do / Do Not

### Do

Use this as:

```text
claim support
language support
context support
explanation strength
```

Good UI labels:

```text
Supported claim
Two-way support
Backed by context
Stronger support
```

Good tooltip wording:

```text
This claim has stronger support from both offensive-finish and defensive-suppression context.
```

---

### Do Not

Do not label this as:

```text
High confidence
Prediction boost
Win signal
Model lock
Best bet
Outcome support
```

Do not write:

```text
GB is more likely to win because two_way_context is supportive.
```

Do not write:

```text
This makes the model more confident.
```

Do not change:

```text
matchup_lean.confidence
model_trust.edge
model_trust.reasoning
model_outcome
```

based on `language_support`.

---

## Example Runtime QA

### Supportive Case

Game:

```text
20251123_MIN@GB
```

Runtime context:

```text
away_context = available_mixed
home_context = supportive
```

Boost count result:

```text
team_comparison: 3
metric_highlights: 0
category_summaries: 3
category_drivers: 3
```

This was expected because GB/home had supportive two-way context and several allowlisted rows pointed to GB.

Allowed examples included:

```text
team_comparison + points_per_play
team_comparison + points_allowed_per_play
team_comparison + red_zone_efficiency
category_summary driver + 1st_down_rate
category_summary driver + red_zone_efficiency
category_summary driver + points_allowed_per_play
```

---

### Non-Supportive Case

Game:

```text
20251020_TB@DET
```

Runtime context:

```text
away_context = available_mixed
home_context = available_mixed
```

Boost count result:

```text
Boosted rows found: 0
```

This was expected because neither side had supportive two-way context.

---

## Multi-Game Smoke Test Result

A multi-game smoke test was run against known QA games.

Expected behavior:

- games with one supportive side can show boost counts
- games with both sides `available_mixed` should show zero boosts
- no crashes
- no model confidence changes
- no Model Trust changes

Observed examples:

| Game ID | Away Context | Home Context | Boost Result |
|---|---|---|---|
| `20251207_CIN@BUF` | available_mixed | supportive | boosts appeared |
| `20251013_BUF@ATL` | available_mixed | available_mixed | 0 boosts |
| `20251009_PHI@NYG` | available_mixed | available_mixed | 0 boosts |
| `20251207_CHI@GB` | available_mixed | available_mixed | 0 boosts |
| `20251123_MIN@GB` | available_mixed | supportive | boosts appeared |
| `20260111_LAC@NE` | available_mixed | supportive | boosts appeared |
| `20260110_GB@CHI` | available_mixed | available_mixed | 0 boosts |
| `20250928_GB@DAL` | available_mixed | available_mixed | 0 boosts |
| `20241215_PIT@PHI` | available_mixed | supportive | boosts appeared |
| `20250111_LAC@HOU` | available_mixed | available_mixed | 0 boosts |
| `20251020_TB@DET` | available_mixed | available_mixed | 0 boosts |

This confirms the runtime behavior is bounded.

---

## Current Status

This feature is now safe as a metadata layer.

Current implementation:

```text
Runtime context computed
Allowlist enforced
Rows annotated
Frontend can read metadata
No summary rewrites yet
No confidence changes
No winner logic changes
```

This is the right stopping point for backend v1.

---

## Recommended Next Frontend Step

First frontend use should be visual only:

```text
small badge + tooltip
```

Do not rewrite summaries yet.

Recommended first UI locations:

1. Team Comparison rows
2. Category Summary cards
3. Category Summary driver rows
4. Metric Highlight cards only if `language_boost_allowed = true`

Recommended badge:

```text
Supported claim
```

Recommended tooltip:

```text
This claim has stronger support from both offensive-finish and defensive-suppression context. It does not change winner confidence.
```

---

## Future Backend Step

A future version may add a generated phrase such as:

```json
"language_support": {
  "language_boost_allowed": true,
  "display_label": "Supported claim",
  "tooltip": "This claim has stronger support from both offensive-finish and defensive-suppression context. It does not change winner confidence."
}
```

That would reduce frontend copy decisions and keep language centralized.

But for now, frontend can safely interpret:

```text
language_support.language_boost_allowed
language_support.reason
language_support.scope
language_support.two_way_context
language_support.two_way_edge_score
```

---

## Future Calibration Direction

The longer-term architecture should still move toward a Level 4 calibration summary.

Possible table:

```text
Analytics.gamelens_claim_calibration_summary
```

Possible grain:

```text
feature_formula_version
claim_type
claim_layer
metric
feature_bucket
sample_size
validation_rate
language_modifier
confidence_modifier
```

That would let `/game` ask:

> Historically, how reliable is this kind of claim under this feature context?

For now, we are not there yet.

The current runtime allowlist is a practical, bounded bridge.

---

## Final Decision

Keep `claim_language_support` as:

```text
explanation metadata
```

not:

```text
prediction logic
```

The feature has earned a frontend role, but only as careful claim-level support language.

Best current frontend behavior:

```text
Show a small “Supported claim” badge only when language_boost_allowed is true.
```

Do not use it to change picks, confidence, or model trust.




---

# Frontend Language Rewrite — Plain-English Claim Support

## Goal

The backend uses terms like:

```text
two_way_context
claim_language_support
offensive finish
defensive suppression
```

Those are useful for engineering, but they should not appear directly in the frontend.

A normal user does not need to know the backend math.

The frontend meaning should be:

> This specific read is stronger because other matchup signs point the same way.

---

## Recommended User-Facing Label

Use:

```text
Backed by the matchup
```

This is clearer than:

```text
Two-way support
```

because it tells the user what is happening without requiring backend knowledge.

---

## Recommended Tooltip

Use this:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

Why this works:

- simple
- football-friendly
- not misleading
- does not sound like a betting lock
- explains why the badge exists
- avoids backend terms

---

## Shorter Tooltip Option

If the UI needs something shorter:

```text
Other matchup signs agree with this read.
```

Or slightly safer:

```text
Other matchup signs agree with this read, but it does not guarantee the result.
```

---

## What The Badge Means

When a user sees:

```text
Backed by the matchup
```

it means:

> This specific edge is supported by more than just one stat.

It does **not** mean:

> This team is guaranteed to win.

It also does **not** mean:

> The model confidence should go up.

This badge belongs to a specific matchup read, not the overall pick.

---

## Real NFL Example 1 — Packers vs Vikings

Game:

```text
20251123_MIN@GB
```

The API found:

```text
GB side: supportive
MIN side: available_mixed
```

That means GB had several matchup signs pointing in the same direction.

### Team Comparison Example

```text
Points Allowed Per Play
GB edge
Backed by the matchup
```

Tooltip:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

Plain-English meaning:

> GB was not just better in points allowed per play. The broader matchup also supported GB, so this defensive read is stronger than a standalone stat.

---

### Category Example

```text
Scoring Suppression leans toward GB
Backed by the matchup
```

Tooltip:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

Plain-English meaning:

> GB’s defensive scoring read is supported by the wider matchup profile.

---

### Driver Example

```text
First Down Rate
GB advantage
Backed by the matchup
```

Plain-English meaning:

> GB’s first-down edge fits with the broader matchup picture.

---

## Real NFL Example 2 — Lions vs Buccaneers

Game:

```text
20251020_TB@DET
```

The API found:

```text
TB side: available_mixed
DET side: available_mixed
```

So the frontend should show **no “Backed by the matchup” badges**.

Even though DET won and had several strong reads, this specific feature did not find the same kind of extra support.

That is good.

It means the badge is not just a “winner badge.”

### Correct UI Behavior

```text
Red Zone Efficiency
DET edge
```

No badge.

Why?

> DET may have the edge, but this specific support layer did not qualify it for stronger language.

---

## Real NFL Example 3 — Patriots vs Chargers

Game:

```text
20260111_LAC@NE
```

The API found:

```text
NE side: supportive
LAC side: available_mixed
```

Good frontend example:

```text
Points Allowed Per Play
NE edge
Backed by the matchup
```

Tooltip:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

Plain-English meaning:

> NE’s defensive read was supported by the larger matchup picture, so the UI can describe that edge a little more confidently.

---

## Recommended Frontend Rule

Only show the badge when:

```js
row.language_support?.language_boost_allowed === true
```

Do **not** show it just because:

```js
row.language_support?.two_way_context === "supportive"
```

Reason:

A team can have supportive context, but not every metric is safe to strengthen.

Example:

```text
Turnover Margin Per Game
```

may still be too volatile.

So even if the team has supportive context, the frontend should only trust:

```text
language_boost_allowed = true
```

---

## Best Badge Text Options

### Best overall

```text
Backed by the matchup
```

### Shorter

```text
Backed up
```

### More formal

```text
Extra support
```

### Football-friendly

```text
Matches the matchup
```

### My recommendation

Use:

```text
Backed by the matchup
```

It is clear, simple, and does not overpromise.

---

## Best Tooltip Options

### Best overall

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

### Short version

```text
Other matchup signs agree with this read.
```

### Safer short version

```text
Other matchup signs agree with this read, but it does not guarantee the result.
```

### More conversational

```text
This is not just one stat standing alone. Other parts of the matchup support the same read.
```

### My recommendation

Use:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

---

## Frontend Examples

### Team Comparison Row

```text
Points Allowed Per Play
GB edge
Backed by the matchup
```

Tooltip:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```

---

### Metric Highlight Card

```text
GB shows a clear advantage in Points Allowed Per Play
Backed by the matchup
```

Tooltip:

```text
This is not just one stat standing alone. Other parts of the matchup support the same read.
```

---

### Category Summary Card

```text
Red Zone Finish leans toward GB
Backed by the matchup
```

Tooltip:

```text
One or more important stats in this category are supported by the broader matchup.
```

---

### Category Driver Row

```text
First Down Rate
GB advantage
Backed by the matchup
```

Tooltip:

```text
Other matchup signs agree with this read.
```

---

## Good Wording

Use:

```text
Backed by the matchup
```

```text
This read has extra support.
```

```text
Other matchup signs agree.
```

```text
This edge fits the broader matchup.
```

```text
This is not just one stat standing alone.
```

---

## Wording To Avoid

Avoid:

```text
Two-way support
```

Avoid:

```text
Offensive finish and defensive suppression agree
```

Avoid:

```text
Claim language support
```

Avoid:

```text
The model is more confident
```

Avoid:

```text
This confirms the pick
```

Avoid:

```text
Win signal
```

Avoid:

```text
Lock
```

Avoid:

```text
Best bet
```

---

## Simple Frontend Helper

```js
function getMatchupSupportBadge(row) {
  const support = row.language_support;

  if (!support?.language_boost_allowed) {
    return null;
  }

  return {
    label: "Backed by the matchup",
    tooltip:
      "Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.",
  };
}
```

---

## Simple Category Helper

```js
function getCategorySupportBadge(summary) {
  const support = summary.language_support;

  if (!support?.language_boost_allowed) {
    return null;
  }

  return {
    label: "Backed by the matchup",
    tooltip:
      "One or more important stats in this category are supported by the broader matchup.",
  };
}
```

---

## Final Frontend Rule

The frontend should treat this as a small explanation badge.

Use it to say:

```text
This read has extra support.
```

Do not use it to say:

```text
This team will win.
```

Recommended final UI copy:

```text
Backed by the matchup
```

Recommended tooltip:

```text
Other matchup signs point in the same direction, so this read has extra support. It still does not guarantee the result.
```