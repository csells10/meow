# GameLens Idea — Postgame Signal Validation Feedback Loop

_Last updated: May 12, 2026_

## Product Frame

GameLens should not define success only by whether it picked the winning team.

A better definition:

> GameLens is successful when it correctly identifies the important matchup shape, uses confidence responsibly, and avoids overstating uncertain games.

This means the product should evaluate whether its pregame matchup reads were useful, not just whether the final winner matched the lean.

The goal is to build a postgame feedback loop that can answer:

- Did the pregame pressure signal actually show up?
- Did the team with the projected turnover edge actually back it up?
- Did the stronger scoring-efficiency profile appear in the final game data?
- Did Core Area advantages translate into actual game-level strengths?
- Was a No Pick actually good restraint?
- Was confidence calibrated appropriately?

This supports GameLens as a matchup intelligence and confidence-calibration tool rather than a forced pick machine.

---

## Why This Matters

A simple win/loss model outcome is too narrow.

Example:

```text
Pregame read:
Pressure: Elevated
Tilt: Team B generating more pressure
```

After the game is final and box score data is loaded, GameLens can validate:

```text
Did Team B actually generate more pressure?
```

If yes, that pregame signal was useful even if Team B lost the game.

That creates a better learning loop:

```text
Pregame signal → Final game stat outcome → Validation result → Model/product learning
```

---

## Core Concept

Create a postgame validation layer that compares pregame GameLens claims against final-game stats.

This layer should validate the **shape of the matchup**, not only the final winner.

GameLens should track several forms of success:

| Success Type | Question |
|---|---|
| Winner accuracy | Did the leaned/predicted team win? |
| Signal accuracy | Did the pressure, turnover, or scoring-efficiency tilt actually show up? |
| Core Area accuracy | Did the pregame Core Area leader also lead in final-game stats? |
| Confidence calibration | Was the confidence level appropriate for how the game played out? |
| No Pick quality | Was avoiding a pick justified by the final game shape? |
| Explanation quality | Were the stated drivers actually relevant after the game? |

---

## Proposed Name

Possible names:

```text
Postgame Signal Validation
Pregame Claim Validation
GameLens Feedback Loop
Signal Confirmation Layer
Model Learning Ledger
```

Recommended working name:

```text
Postgame Signal Validation Layer
```

---

## Validation Layers

### 1. Matchup Outcome Validation

This is the current simple result:

```text
Predicted / leaned team won or lost.
```

Useful fields:

```text
game_id
predicted_team
actual_winner
result
confidence
matchup_label
profile_type
```

Possible results:

```text
correct
incorrect
no_pick
pending
```

This should remain useful, but it should not be the only success measure.

---

### 2. Game Profile Signal Validation

Validate each signal in `game_profile`.

Current examples:

```text
Pressure
Turnover Risk
Scoring Efficiency
```

Pregame example:

```json
{
  "category": "Pressure",
  "level": "Elevated",
  "tilt_team": "home",
  "tilt_text": "DEN generating more pressure"
}
```

Postgame question:

```text
Did DEN actually generate more pressure?
```

Possible validation results:

```text
confirmed
partially_confirmed
not_confirmed
contradicted
not_scorable
insufficient_data
```

Example output:

```json
{
  "category": "Pressure",
  "pregame_level": "Elevated",
  "pregame_tilt_team": "home",
  "postgame_confirming_team": "home",
  "validation_result": "confirmed",
  "summary": "DEN backed up the pregame pressure signal."
}
```

---

### 3. Core Area Validation

Validate each pregame Core Area advantage against final game stats.

Pregame Core Areas:

```text
Defensive Control
Disruption and Turnovers
Offensive Output
Scoring Efficiency
Field Control / Special Teams, later, after source repair
```

Pregame example:

```text
Disruption and Turnovers: LAC Edge
Scoring Efficiency: DEN Edge
Offensive Output: DEN Edge
Defensive Control: DEN Edge
```

Postgame validation asks:

```text
Did the same team actually lead that Core Area in final-game stat output?
```

Possible result labels:

```text
confirmed
mixed
contradicted
not_scorable
```

Important note:

Core Area validation should not require perfection. A Core Area may be partially confirmed if some major metrics supported the pregame read while others did not.

---

### 4. Team Comparison Validation

Validate the visible Team Comparison rows.

Current examples:

```text
Points per Play
Points Allowed per Play
3rd Down %
Red Zone TD %
Turnover Margin / Game
```

For each row:

```text
Pregame better side → final game better side
```

Example:

```json
{
  "metric": "turnover_margin_per_game",
  "label": "Turnover Margin / Game",
  "pregame_better": "away",
  "postgame_better": "away",
  "validation_result": "confirmed"
}
```

This is useful because it checks whether the visible UI explanations were backed up by the game result.

---

### 5. No Pick Validation

No Pick should not be judged as a failure.

It should be judged as a restraint decision.

Pregame No Pick questions:

```text
Were signals mixed?
Was the profile split?
Was the edge narrow?
Was confidence appropriately low?
```

Postgame validation questions:

```text
Did the final game look volatile, close, split, or difficult to read?
Did the pregame signals conflict with each other?
Would a forced pick have been misleading?
```

Possible labels:

```text
good_restraint
properly_avoided_chaos
too_cautious
missed_opportunity
not_enough_information
```

Example:

```json
{
  "game_id": "20260104_LAC@DEN",
  "pregame_result": "no_pick",
  "postgame_no_pick_quality": "good_restraint",
  "summary": "The model avoided a forced call in a split profile where pressure and turnover signals leaned LAC, while scoring and offensive output favored DEN."
}
```

---

## Possible Backend Object

Eventually `/game` could include a postgame section when final stats are available:

```json
"postgame_validation": {
  "available": true,
  "overall": {
    "result": "mixed_but_useful",
    "summary": "The model avoided a winner call, but correctly identified the pressure and turnover environment."
  },
  "signals": [],
  "core_areas": [],
  "team_comparison": [],
  "confidence_calibration": {},
  "no_pick_quality": {}
}
```

This should be additive and should not break existing frontend fields.

---

## Proposed BigQuery Table

Possible table:

```text
Analytics.game_signal_validation
```

Suggested grain:

```text
game_id + validation_type + category_or_metric
```

Suggested fields:

```text
season
season_phase
game_id
game_date
game_week
away_team_id
away_team_abv
home_team_id
home_team_abv

validation_type
category
core_area
metric
label

pregame_level
pregame_tilt_side
pregame_tilt_team
pregame_value_away
pregame_value_home
pregame_better
pregame_summary

postgame_value_away
postgame_value_home
postgame_better
postgame_confirming_team

validation_result
validation_strength
validation_score
validation_summary

confidence
matchup_label
profile_type
model_result
actual_winner

created_at
```

Possible `validation_type` values:

```text
game_profile_signal
core_area
team_comparison
matchup_outcome
no_pick_quality
confidence_calibration
```

---

## Proposed Validation Result Labels

Use more than binary right/wrong.

Recommended labels:

```text
confirmed
partially_confirmed
mixed
not_confirmed
contradicted
not_scorable
insufficient_data
```

For No Pick:

```text
good_restraint
properly_avoided_chaos
too_cautious
missed_opportunity
not_enough_information
```

For confidence:

```text
well_calibrated
too_loud
too_cautious
reasonable_but_missed
unclear
```

---

## Suggested Validation Logic Examples

### Pressure

Pregame source:

```text
game_profile.category = Pressure
```

Possible postgame metrics:

```text
sacks
sacks_taken
sack_yards_lost
pressure_rate, only after definition/source repair
```

Caution:

Current pressure data may need source/definition review. Avoid over-trusting pressure validation until the pressure metric is cleaned.

Possible rule:

```text
If pregame tilt_team also has better postgame pressure metric result → confirmed.
If pressure metrics are split → partially_confirmed or mixed.
If opposite team clearly leads → contradicted.
```

---

### Turnover Risk

Pregame source:

```text
turnover_margin_per_game
fumbles_lost
interceptions_thrown
defensive_interceptions
fumbles_recovered
```

Postgame metrics:

```text
turnover_margin
interceptions_thrown
fumbles_lost
defensive_interceptions
fumbles_recovered
```

Preferred user-facing validation:

```text
Did the pregame turnover-profile team win the actual turnover battle?
```

Example:

```text
Pregame: LAC better turnover profile
Postgame: LAC won turnover battle
Result: confirmed
```

---

### Scoring Efficiency

Pregame source:

```text
points_per_play
red_zone_efficiency
td_rate
third_down_pct
```

Postgame metrics:

```text
points_per_play
red_zone_efficiency
td_rate
actual_points
```

Possible validation:

```text
If pregame scoring-efficiency tilt team leads final points_per_play or core scoring-efficiency bundle → confirmed.
If scoring metrics split → mixed.
If opposite team leads clearly → contradicted.
```

---

### Core Area Advantage

Pregame source:

```text
core_area_comparison
matchup_breakdown.core_area_summaries
```

Postgame comparison:

Recalculate comparable game-level Core Area values using final box score stats.

Possible validation:

```text
Pregame Core Area leader == postgame Core Area leader → confirmed
Pregame neutral / postgame close → confirmed_neutral or not_scorable
Pregame leader != postgame leader → contradicted
```

---

## MVP Implementation Plan

### Phase 1 — Internal QA only

Do not expose this publicly yet.

Start with a local script or BigQuery query that compares:

```text
pregame /game response
final game stats
validation result
```

Output a small summary by game.

Goal:

```text
Learn whether pregame signals are useful.
```

---

### Phase 2 — Store validation rows

Create a table such as:

```text
Analytics.game_signal_validation
```

Store one row per game/signal/metric/core-area validation result.

Goal:

```text
Create a durable model learning ledger.
```

---

### Phase 3 — Build summary views

Possible views:

```text
Analytics.v_signal_validation_summary
Analytics.v_core_area_validation_summary
Analytics.v_no_pick_quality_summary
Analytics.v_confidence_calibration_summary
```

Useful outputs:

```text
Pressure signal confirmation rate
Turnover signal confirmation rate
Scoring-efficiency signal confirmation rate
Core Area confirmation rate
No Pick good-restraint rate
High-confidence too-loud rate
Low-confidence too-cautious rate
```

---

### Phase 4 — Add internal/admin endpoint

Possible endpoint:

```text
/admin/model-validation?season=2025
```

Keep it internal first.

Do not make this a public-facing accuracy scoreboard yet.

---

### Phase 5 — Add frontend postgame explanation later

Eventually, final games could show:

```text
Postgame Signal Check
Pressure read: confirmed
Turnover read: contradicted
Scoring read: confirmed
No Pick: good restraint
```

This should explain the model, not shame it.

---

## Important Product Guardrails

Do not turn this into a simple gambling-style accuracy board.

Avoid framing like:

```text
Model is 64% accurate, therefore follow the picks.
```

Prefer framing like:

```text
GameLens correctly identified the scoring-efficiency environment in 68% of reviewed games.
No Pick was useful restraint in 72% of low-confidence games.
Pressure signals were confirmed less often, suggesting that layer needs cleanup.
```

The purpose is model improvement and matchup understanding, not selling certainty.

---

## Relationship to Existing Backlog

This idea connects directly to several backlog themes:

- outcome quality labels
- postgame swing factors
- historical / stability context
- QA script upgrades
- model trust improvements
- confidence calibration
- No Pick quality
- using historical games as a testing ground

It should probably sit under:

```text
Future high-value QA / model-trust layer
```

Recommended title in backlog:

```text
Postgame Signal Validation Feedback Loop
```

---

## Relationship to League Discovery / Lens Tags

This layer can eventually support league discovery.

If postgame validation shows that certain lenses are consistently confirmed, GameLens can learn which lenses are more reliable.

Examples:

```text
Scoring-efficiency lens is usually stable.
Turnover lens is useful but volatile.
Pressure lens needs better source data.
Red-zone lens may be noisy in small samples.
No Pick is strongest when Core Areas are split and visible Team Comparison edge is low.
```

This could later feed:

- league trend dashboards
- movers and shakers
- lens heatmaps
- team identity changes
- confidence calibration summaries

---

## Open Questions

Questions to answer before implementation:

1. Which final-game metrics should validate each pregame signal?
2. Should validation use raw final stats or final-game registry-derived metrics?
3. How strict should confirmation be?
4. Should validation compare team-vs-team only, or also league context?
5. Should pressure validation wait until pressure source data is cleaned?
6. How should neutral/near-even postgame outcomes be treated?
7. Should No Pick validation require close final score, mixed stats, or both?
8. Should this start as a BigQuery view, Python builder, or API endpoint?

---

## Recommended Next Step

Start with a simple internal validation script for 5–10 historical games.

For each game, produce:

```text
game_id
model_result
matchup_label
pregame game_profile signals
postgame signal validation results
core_area validation results
short summary
```

Do not automate or frontend this until the rules feel fair.

The first goal is to learn whether the validation method itself makes sense.

