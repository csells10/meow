# GameLens v1.5+ Mission-Critical Status, Frontend Handoff, and Backlog

_Last updated: May 13, 2026_

## Current Product Frame

GameLens is best understood as:

```text
A matchup intelligence and confidence-calibration tool.
```

The product should help users understand:

- where each team has an edge
- where the matchup is close or near-even
- which areas are clean, split, mixed, or uncertain
- whether confidence language is appropriate
- why the model leaned a certain direction
- what supported the read
- what challenged the read
- what parts of the matchup were descriptive context rather than decisive evidence
- whether the pregame read held up after the game is final

GameLens should **not** behave like a forced pick machine.

The winner lean can remain part of the page, but the larger value is explaining the matchup shape and confidence level honestly.

---

# Current Section Ownership

This is now the clearest mental model for the matchup page.

```text
Matchup Lean = pregame read + pregame confidence
Game Profile = football environment signals
Team Comparison / Core Area Advantage = visible matchup evidence
Model Trust & Outcome = postgame validation
What supported or challenged the read? = diagnostic explanation
```

## Matchup Lean

Purpose:

```text
Show the pregame decision-support read.
```

This section owns:

- target team / no clear edge
- matchup shape
- profile strength
- pregame confidence
- why the read is clean, thin, mixed, or cautious
- useful caution notes

Current frontend display now includes:

```text
MATCHUP READ
Clear Lean / Medium Confidence

Profile: GB has the matchup lean, but the profile is not overwhelming.
Confidence: The profile is strong, but outcome confidence is kept measured.
```

## Game Profile

Purpose:

```text
Show football-readable environment signals.
```

Examples:

- Pressure
- Turnover Risk
- Scoring Efficiency

This section helps users understand the football shape of the game, not just the winner lean.

Important interpretation:

```text
Game Profile signals are also used downstream in Model Trust diagnostics as Game Profile Signals.
```

## Model Trust & Outcome

Purpose:

```text
Validate the pregame read after the game is final.
```

This section owns:

- Correct / Incorrect / No Pick
- Predicted vs Actual
- why the model picked or avoided the game
- whether the pregame read held up
- matchup advantage counts
- learning label
- diagnostic explanation of what supported or challenged the read

Important current rule:

```text
Model Trust & Outcome no longer shows its own confidence pill.
```

Reason:

```text
Confidence has one clear home: Matchup Lean / Matchup Read.
```

---

# Mission-Critical Objectives

These are the priorities that matter most before large new feature work.

## 1. Productionize the New Builder Chain Safely

Status:

```text
Mission critical / not yet complete
```

Current concern:

```text
The live ingestion flow in app.py may still be tied to the older aggregate process.
The new GameLens data model now depends on cleaned facts, windowed metrics, and ranking builders.
```

Current new builder chain:

```text
agg/build_metric_facts.py
→ agg/build_windowed_metrics.py
→ agg/build_metric_rankings.py
→ /game output
```

Do not rush this into `app.py` without an ingestion/idempotency review.

Questions to settle before automation:

- When stats are inserted, which season should rebuild?
- Should the build chain run only for the affected season?
- Should it run facts → windowed → rankings every time?
- How do we prevent duplicate/stale runs?
- How should failures be handled if one builder succeeds and another fails?
- Should env flags control each builder?
- Should local/dev and Cloud Run behavior differ?
- Should the build run immediately after ingestion, or as a scheduled/queued job?

Possible future env vars:

```text
ENABLE_METRIC_FACTS_BUILD=false
ENABLE_WINDOWED_METRICS_BUILD=false
ENABLE_METRIC_RANKINGS_BUILD=false
```

This is one of the most important backend items before relying on live automated updates.

---

## 2. Keep GameLens From Becoming a Pick Machine

Status:

```text
Always active product guardrail
```

GameLens should explain matchup shape.

It should not become:

```text
Team A ranks higher in three things, therefore Team A wins.
```

Continue protecting:

- neutral/even as a real result
- low confidence / no-pick restraint
- profile strength vs outcome confidence separation
- context-only metrics as descriptive, not decisive
- historical data as support, not a pick override
- injury data as context, not a pick override
- postgame validation as learning, not a profit scoreboard
- confidence as one clear concept, owned by Matchup Lean

---

## 3. Keep Frontend v1.6.x Small

Status:

```text
v1.6.0 / v1.6.1 / v1.6.2 completed
```

Completed v1.6.x frontend work:

- displayed `matchup_lean.matchup_label` through the Matchup Read block
- added Profile + Confidence chips in Matchup Lean
- displayed profile and confidence summaries
- mapped caution codes into user-facing language
- removed duplicate confidence from Model Trust & Outcome
- renamed Model Trust diagnostic labels so they do not sound like a second confidence score

Do not use frontend/Lovable work on large redesigns yet.

Avoid spending v1.6.x work on:

- full redesign
- Field Control repair
- historical modeling
- League Discovery UI
- injury context
- dynamic driver UI
- model-performance dashboard
- full matchup_breakdown cards

Those are bigger projects.

---

## 4. Keep Field Control and Snap Counts in Audit Mode

Status:

```text
Mission critical data-quality caution
```

Current state:

```text
Field Control is hidden.
Snap-count-related metrics are interesting but not trusted enough to steer confidence.
```

Do not let Field Control or snap counts drive the strongest language until source quality and interpretation are validated.

---

## 5. Build Internal QA Before Public Success-Rate UI

Status:

```text
Mission critical product discipline
```

A historical success-rate view may be useful internally, but a public accuracy board could pull GameLens toward the wrong goal.

Better first version:

```text
Internal QA / admin reporting
```

Measure:

- pick accuracy
- no-pick rate
- high/medium/low confidence calibration
- profile_strength distribution
- outcome_confidence distribution
- matchup_label distribution
- matchup_cautions
- correct but narrow outcomes
- incorrect close-variance misses
- incorrect calibration failures
- no pick / good restraint
- no pick / missed opportunity
- signal validation
- Core Area validation

The goal is not only:

```text
Was the winner correct?
```

The better question is:

```text
Did GameLens explain the matchup honestly?
```

---

## 6. Treat Lens-Tag League Discovery as Exploratory

Status:

```text
Exploratory / after plumbing is safe
```

Lens tags may become a league discovery tool, heatmap, movers-and-shakers board, or trend explorer.

Do not build the UI yet.

First:

```text
clean product-facing lens tags
build BigQuery view/query
validate whether the patterns are useful
```

---

# Work Completed on May 12, 2026

## Turnover Margin Per Game Cleanup

Status:

```text
✅ Complete
```

This item was previously listed as a backend cleanup to-do. It has now been completed and validated.

### Problem Fixed

The new windowed `/game` source exposed cumulative:

```text
turnover_margin
```

That could exaggerate differences when windows had different game counts.

The desired user-facing metric was:

```text
turnover_margin_per_game
```

### Implementation Completed

Updated files:

```text
analytics/metric_registry.py
agg/build_windowed_metrics.py
services/game_service.py
```

Changes made:

- added `turnover_margin_per_game` as a real registry metric
- added `per_game_from_sum` as an aggregation method
- calculated per-game value as `sum(turnover_margin) / games_in_window`
- kept cumulative `turnover_margin` as supporting context
- updated visible Team Comparison to use `Turnovers::turnover_margin_per_game`
- updated Game Profile Turnover Risk to use `Turnovers::turnover_margin_per_game`
- confirmed frontend displays `Turnover Margin / Game`

### Rebuild Completed for 2025

Completed chain:

```text
game_team_metric_facts
→ team_metrics_windowed
→ team_metric_rankings
```

Observed successful row counts:

```text
Analytics.game_team_metric_facts_2025: 36,532 rows
Analytics.team_metrics_windowed_2025: 138,532 rows
Analytics.team_metric_rankings_2025: 426,086 rows
```

### Validation Completed for 2025

Validated `turnover_margin_per_game` exists in all 2025 windowed metric windows:

```text
last_3_games
last_7_games
preseason_to_date
regular_plus_postseason_to_date
regular_season_to_date
```

Validated `turnover_margin_per_game` exists in 2025 rankings across all 5 window types with 32 teams.

Validated `/game` response for:

```text
20251020_TB@DET
```

Confirmed:

```json
"metric": "turnover_margin_per_game"
"label": "Turnover Margin / Game"
```

Validated frontend after Google Build using:

```text
20260104_LAC@DEN
```

Frontend displayed:

```text
Turnover Margin / Game
LAC 0.250
DEN -0.312
```

---

## Historical Rebuild With Current Registry

Status:

```text
✅ Complete
```

The current metric registry and builder chain have now been rebuilt and validated for:

```text
2023
2024
2025
```

### 2024 Rebuild Completed

Completed chain:

```text
game_team_metric_facts_2024
→ team_metrics_windowed_2024
→ team_metric_rankings_2024
```

Observed successful row counts:

```text
Analytics.game_team_metric_facts_2024: 33,060 rows
Analytics.team_metrics_windowed_2024: 132,986 rows
Analytics.team_metric_rankings_2024: 439,708 rows
```

Validated `turnover_margin_per_game` exists in available 2024 windows:

```text
last_3_games
last_7_games
regular_plus_postseason_to_date
regular_season_to_date
```

Validation result:

```text
non_null_row_count = row_count
team_count = 32 in rankings
```

Note:

```text
No preseason_to_date rows appeared for 2024, likely because the 2024 source/fact data did not include preseason games.
```

### 2023 Rebuild Completed

Completed chain:

```text
game_team_metric_facts_2023
→ team_metrics_windowed_2023
→ team_metric_rankings_2023
```

Observed successful row counts:

```text
Analytics.game_team_metric_facts_2023: 33,060 rows
Analytics.team_metrics_windowed_2023: 132,986 rows
Analytics.team_metric_rankings_2023: 431,600 rows
```

Validated `turnover_margin_per_game` exists in available 2023 windows:

```text
last_3_games
last_7_games
regular_plus_postseason_to_date
regular_season_to_date
```

Validation result:

```text
non_null_row_count = row_count
team_count = 32 in rankings
```

Note:

```text
No preseason_to_date rows appeared for 2023, likely because the 2023 source/fact data did not include preseason games.
```

---

## Current Interpretation

The turnover cleanup and historical rebuilds are complete.

The behavior is better because:

- the app still captures turnover advantage
- the app no longer overstates cumulative turnover totals
- Team Comparison, Matchup Breakdown, Game Profile, rankings, and frontend display now agree
- historical seasons now use the same metric contract as 2025
- future last_3 / last_7 recent-form work can use turnover margin per game correctly

Turnover Margin Per Game Cleanup is no longer a blocker.

---

# Work Completed on May 13, 2026

## v1.6.0 — Matchup Lean Honesty Pass

Status:

```text
✅ Complete
```

Purpose:

```text
Teach the frontend about the new /game response fields and display Matchup Lean in a more honest way.
```

Updated files:

```text
src/lib/nfl-api.ts
src/pages/Matchup.tsx
```

Fields added to frontend typing:

```text
matchup_lean.profile_strength
matchup_lean.outcome_confidence
matchup_lean.matchup_label
matchup_lean.matchup_cautions
```

Frontend behavior added:

- existing Matchup Lean card stayed in place
- new `Matchup Read` sub-block added inside the card
- profile chip rendered from `profile_strength.label`
- confidence chip rendered from `outcome_confidence.label`
- fallback can split `matchup_label` on ` / `
- legacy fallback still uses old `matchup_lean.confidence` if new fields are absent
- profile summary shown when available
- confidence summary shown when available
- caution row shown only when useful

Example display:

```text
MATCHUP READ
Clear Lean / Medium Confidence

Profile: GB has the matchup lean, but the profile is not overwhelming.
Confidence: The profile is strong, but outcome confidence is kept measured.
```

Why it matters:

```text
The page no longer relies only on raw Low / Medium / High confidence.
It now separates matchup shape from confidence calibration.
```

---

## v1.6.0 Refinement — User-Facing Cautions

Status:

```text
✅ Complete
```

Problem fixed:

```text
Raw backend caution codes looked like debug output when shown directly to users.
```

Updated file:

```text
src/pages/Matchup.tsx
```

Implemented behavior:

- changed `Outcome:` supporting label to `Confidence:` inside Matchup Read
- confidence chips now read as `Low Confidence`, `Medium Confidence`, `High Confidence` when appropriate
- caution codes are mapped into user-facing language
- all-caps `CAUTIONS` label was softened to `Why cautious`
- philosophy-style caution `strong_profile_does_not_guarantee_outcome` is hidden
- unknown caution fallback converts snake_case to readable title case

Known caution mapping:

```text
core_area_gap_is_small → Core areas are close
core_areas_are_split → Core areas are split
core_areas_do_not_fully_confirm_lean → Broader profile is mixed
supporting_context_is_mixed_or_limited → Supporting context is limited
strong_profile_does_not_guarantee_outcome → hidden
```

---

## v1.6.1 — Duplicate Confidence Cleanup

Status:

```text
✅ Complete
```

Problem fixed:

```text
Confidence appeared in both Matchup Lean and Model Trust & Outcome.
```

Example problem:

```text
Matchup Lean: Clear Lean / Medium Confidence
Model Trust & Outcome: Correct / High Confidence
```

Why that was confusing:

- Matchup Read used newer `matchup_lean.outcome_confidence.label`
- Model Trust still used legacy `matchup_lean.confidence`
- both displayed as “confidence” even though they came from different concepts

Decision:

```text
Matchup Lean owns pregame confidence.
Model Trust & Outcome owns postgame validation.
```

Updated file:

```text
src/pages/Matchup.tsx
```

Changes made:

- removed confidence pill from Model Trust & Outcome
- Model Trust now shows only result chip: Correct / Incorrect / No Pick
- removed local `tier` and `confStyle` variables inside `ModelTrustCard`
- left shared confidence helpers in place because Matchup Read legacy fallback still uses them
- left `confidence_context` rendering in place below Predicted vs Actual

Current behavior:

```text
Matchup Lean = pregame confidence source
Model Trust & Outcome = result validation source
```

---

## v1.6.2 — Model Trust Diagnostic Label Cleanup

Status:

```text
✅ Complete
```

Problem fixed:

```text
The diagnostic labels inside Model Trust sounded like a second confidence verdict.
```

Old labels:

```text
How reliable was the edge?
Metric Agreement
Signal Alignment
```

Why they were confusing:

- `Metric Agreement: Strong` sounded like overall model confidence
- it actually meant visible Team Comparison edge strength
- `Signal Alignment` came from Game Profile signals, but the label did not make that clear
- this could conflict mentally with `Matchup Read: Low Confidence`

Updated file:

```text
src/pages/Matchup.tsx
```

New labels:

```text
How reliable was the edge? → What supported or challenged the read?
Metric Agreement → Team Comparison Support
Signal Alignment → Game Profile Signals
```

New tooltip meanings:

```text
Team Comparison Support:
How lopsided the visible Team Comparison stats are. Edge strength only — not overall confidence.

Game Profile Signals:
Whether each Game Profile signal, such as Pressure, Turnover Risk, and Scoring Efficiency, tilted toward the predicted team.
```

Important interpretation:

```text
Team Comparison Support is not overall confidence.
Game Profile Signals are diagnostic context.
The diagnostic area explains what supported or challenged the read.
```

Example improved reading:

```text
Matchup Read: Strong Profile / Low Confidence
Team Comparison Support: Strong
Game Profile Signals: Mixed
```

Now reads as:

```text
Visible comparison metrics supported the lean, but Game Profile signals were mixed, so confidence stayed cautious.
```

---

# v1.5.0 Release Status

## v1.5.0 — Completed and Shipped

Status:

```text
✅ Committed
✅ Tagged
✅ Pushed
✅ Google Cloud Build passed
✅ Frontend loaded successfully
✅ API response shape stayed stable
✅ Existing frontend expectations still work
```

## v1.5.0 Release Summary

GameLens v1.5.0 completed the backend metric foundation and matchup explanation API layer.

This release:

- made the pregame-safe windowed metrics source the default `/game` metric source
- removed the need to rely on `USE_WINDOWED_METRICS_FOR_GAME=true`
- wired ranking context into the `/game` response
- added ranking-based matchup explanation summaries
- added profile strength vs outcome confidence labels
- filtered supporting/context metrics out of headline matchup drivers
- hid Field Control from Core Area output until the source layer is repaired
- preserved the existing API fields expected by the frontend

---

# Completed Backend / API Work

## 1. Product Refocus

Status:

```text
✅ Complete
```

GameLens has been reframed away from:

```text
A forced pick machine
```

and toward:

```text
A matchup intelligence and confidence-calibration tool
```

Current product language should explain:

- clean edge
- slight edge
- near even
- mixed profile
- split profile
- conflicting profile
- no clear edge
- context only
- supporting context
- insufficient data

Avoid language like:

- lock
- guaranteed
- must-pick
- winner because rank is higher
- confidence automatically High because one team leads several metrics

---

## 2. Metric Registry Foundation

Status:

```text
✅ Complete, but future audit recommended
```

Canonical file:

```text
analytics/metric_registry.py
```

The registry is now the metadata contract for:

- metric labels
- definitions
- categories
- Core Areas
- comparison direction
- formatting
- ranking usage
- signal strength
- edge language eligibility
- confidence eligibility
- data quality status
- lens tags

Important interpretation:

```text
The registry is not the scoring model.
It tells downstream code how carefully each metric may be used.
```

Registry metadata now helps prevent bad language, such as treating context-only metrics as better/worse edges.

Future audit is still recommended, but not required for the current v1.5/v1.6 path.

---

## 3. Cleaned Fact Tables

Status:

```text
✅ Complete
```

Builder:

```text
agg/build_metric_facts.py
```

Output tables:

```text
Analytics.game_team_metric_facts_2023
Analytics.game_team_metric_facts_2024
Analytics.game_team_metric_facts_2025
```

Purpose:

```text
Create cleaned game/team/metric fact rows using registry metadata instead of legacy parser category/core_area fields.
```

Important rule:

```text
Analytics.game_metrics_flat.category
Analytics.game_metrics_flat.core_area
```

are legacy/non-authoritative.

---

## 4. Windowed Metrics Tables

Status:

```text
✅ Complete
```

Builder:

```text
agg/build_windowed_metrics.py
```

Output tables:

```text
Analytics.team_metrics_windowed_2023
Analytics.team_metrics_windowed_2024
Analytics.team_metrics_windowed_2025
```

Window types:

```text
regular_season_to_date
regular_plus_postseason_to_date
last_3_games
last_7_games
preseason_to_date
```

Primary `/game` source now uses:

```text
regular_season_to_date
```

for regular-season games.

Important improvement:

```text
Windowed metrics fixed the old early-season issue where preseason data leaked into regular-season pregame profiles.
```

Derived rates are recalculated from summed ingredients, not averaged from existing rates.

Examples:

```text
points_per_play = sum(actual_points) / sum(total_plays)
red_zone_efficiency = sum(red_zone_tds) / sum(red_zone_attempts)
yards_per_play = sum(total_yards) / sum(total_plays)
turnover_margin_per_game = sum(turnover_margin) / games_in_window
```

---

## 5. Ranking Tables

Status:

```text
✅ Complete
```

Builder:

```text
agg/build_metric_rankings.py
```

Output tables:

```text
Analytics.team_metric_rankings_2023
Analytics.team_metric_rankings_2024
Analytics.team_metric_rankings_2025
```

Ranking grain:

```text
season + as_of_date + window_type + metric + team_id
```

Ranking fields now available to `/game`:

```text
league_rank
league_percentile
tier
tier_label
ranking_kind
rank_direction
rank_interpretation
data_lag_days
ranking_usage
signal_strength
edge_language_allowed
confidence_eligible
data_quality_status
lens_tags
```

Important interpretation:

```text
Edge metrics can support better/worse language.
Context metrics describe style, volume, or identity.
Context metrics should not create winner logic.
```

---

## 6. `/game` Uses Windowed Metrics by Default

Status:

```text
✅ Complete
```

Updated file:

```text
queries/game_queries.py
```

Current behavior:

```text
/game now uses Analytics.team_metrics_windowed_{season} by default.
```

The old env var is no longer needed for normal use:

```text
USE_WINDOWED_METRICS_FOR_GAME
```

Current default source:

```text
Analytics.team_metrics_windowed_{season}
```

Old source:

```text
Analytics.team_metrics_season_{season}
```

is no longer the live `/game` source.

Rollback would come from Git history, not by flipping the env var.

---

## 7. Exact Neutral and Near-Even Handling

Status:

```text
✅ Complete
```

Updated files:

```text
services/game_service.py
services/model_trust_service.py
```

Completed behavior:

- exact ties return `better: "neutral"`
- near-even ranking gaps return `comparison_strength: "near_even"`
- neutral/near-even rows do not create fake edges
- neutral rows are counted in `model_trust.matchup_advantage.neutral`
- near-even rows soften visible Team Comparison strength

Validated examples:

```text
BUF@ATL Turnover Margin: 1.0 vs 1.0 → neutral
PHI@NYG Points Allowed Per Play: tiny gap → near_even
```

---

## 8. High-Confidence Guardrail

Status:

```text
✅ Complete
```

Problem addressed:

```text
Some games had High confidence even when visible Team Comparison edge strength was low.
```

Implemented behavior:

```text
If visible Team Comparison edge_strength is low or none, High confidence is capped to Medium.
```

Validated example:

```text
20251020_TB@DET
High → Medium
reason: low_visible_team_comparison_edge
```

This keeps GameLens from sounding too loud when the visible matchup edge is narrow.

---

## 9. Field Control Hidden

Status:

```text
✅ Complete for API behavior, source repair still future
```

Current behavior:

```text
Field Control is hidden from core_area_comparison.
Field Control does not feed matchup_lean.
Field Control does not influence confidence.
```

Reason:

```text
The source/parser layer for Field Control is not trustworthy enough yet.
```

Future work:

```text
Repair backend source/extraction layer before reintroducing Field Control.
```

---

## 10. Step 3 Matchup Explanation Layer

Status:

```text
✅ Complete
```

Updated file:

```text
services/game_service.py
```

New top-level API section:

```json
"matchup_breakdown": {
  "available": true,
  "metric_highlights": [],
  "category_summaries": [],
  "core_area_summaries": [],
  "context_notes": [],
  "freshness": {},
  "summary_counts": {}
}
```

Purpose:

```text
Summarize the matchup by metric, category, and Core Area using ranking context.
```

This allows GameLens to explain:

- which metric edges matter
- which categories lean toward each team
- which Core Areas are clean, split, or close
- which metrics are only context
- how fresh the ranking data is

---

## 11. Metric Highlights

Status:

```text
✅ Complete
```

API field:

```text
matchup_breakdown.metric_highlights
```

This section includes headline-eligible metrics only.

A metric can become a headline driver only when:

```text
ranking_usage = edge
ranking_kind = edge
edge_language_allowed = true
confidence_eligible = true
signal_strength = strong
data_quality_status = good
```

This prevents noisy/supporting metrics from steering the main story.

---

## 12. Category Summaries

Status:

```text
✅ Complete
```

API field:

```text
matchup_breakdown.category_summaries
```

Examples of category summaries:

```text
Scoring Production leans toward BUF.
Offensive Rhythm leans toward DET.
Turnovers looks close to even.
```

Category summaries are built from headline-eligible metrics only.

Supporting/context metrics are excluded from category scoring.

---

## 13. Core Area Summaries

Status:

```text
✅ Complete
```

API field:

```text
matchup_breakdown.core_area_summaries
```

Examples:

```text
Scoring Efficiency leans toward BUF.
Defensive Control looks close to even.
Offensive Output leans toward NYG.
```

Important note:

```text
core_area_comparison and matchup_breakdown.core_area_summaries are related but not identical.
```

- `core_area_comparison` is broader normalized Core Area scoring.
- `matchup_breakdown.core_area_summaries` is ranking-based and headline-filtered.

Frontend should be careful not to present them as the exact same calculation.

---

## 14. Context Notes

Status:

```text
✅ Complete
```

API field:

```text
matchup_breakdown.context_notes
```

Context notes preserve useful information without letting it become a headline edge.

Examples of context/supporting metrics:

```text
Total Defensive Snaps
Pass Attempts
Total Plays
Rushing Attempts
Actual Points
Passing Yards
Rushing Yards
Red Zone Attempts
```

These now appear as:

```text
context_only
supporting_context
not_confidence_eligible
data_quality_watch
```

Instead of being treated as decisive matchup drivers.

---

## 15. Profile Strength vs Outcome Confidence

Status:

```text
✅ Complete
```

New fields inside:

```text
matchup_lean
```

Fields:

```json
"profile_strength": {},
"outcome_confidence": {},
"matchup_label": "",
"matchup_cautions": []
```

Purpose:

```text
Separate how strong the matchup profile looks from how confident the model should be about the final outcome.
```

Example:

```text
Strong Profile / Medium Outcome Confidence
```

Frontend now displays this through the Matchup Read block.

---

## 16. API Compatibility

Status:

```text
✅ Complete / validated
```

Existing frontend-safe fields remain:

```text
header
final_score
game_profile
team_comparison
core_area_comparison
matchup_lean
model_outcome
model_trust
```

New fields are additive:

```text
ranking_context
matchup_breakdown
matchup_lean.profile_strength
matchup_lean.outcome_confidence
matchup_lean.matchup_label
matchup_lean.matchup_cautions
```

Frontend smoke test passed:

```text
Page loads
No observed frontend errors
Google Cloud Build passed
API returns everything frontend expects
```

---

# Current Frontend Contract

## Matchup Lean Uses

Primary fields:

```text
matchup_lean.target_team
matchup_lean.lean_summary
matchup_lean.focus_summary
matchup_lean.core_area_context
matchup_lean.profile_strength.label
matchup_lean.profile_strength.summary
matchup_lean.outcome_confidence.label
matchup_lean.outcome_confidence.summary
matchup_lean.matchup_label
matchup_lean.matchup_cautions
```

Frontend fallback order for Matchup Read:

```text
1. profile_strength.label + outcome_confidence.label
2. split matchup_label on " / "
3. use matchup_label as one chip
4. legacy fallback to matchup_lean.confidence
```

## Model Trust & Outcome Uses

Primary fields:

```text
model_outcome.result
model_outcome.predicted_team
model_outcome.actual_winner
model_trust.reasoning
model_trust.matchup_advantage
model_trust.edge
model_trust.signal_alignment
model_trust.learning_label
matchup_lean.confidence_context
```

Important behavior:

```text
Model Trust no longer displays matchup_lean.confidence as a second confidence pill.
```

## Diagnostic Area Uses

Current title:

```text
What supported or challenged the read?
```

Rows:

```text
Team Comparison Support = model_trust.edge.strength
Game Profile Signals = model_trust.signal_alignment.summary + signal rows
```

Interpretation:

```text
Team Comparison Support explains visible metric edge strength.
Game Profile Signals explains whether football environment signals supported or challenged the picked side.
Neither row is an overall confidence verdict.
```

---

# Current Backlog

## Immediate / Next

### 1. Commit / Review v1.6.x Frontend Polish

Status:

```text
Immediate housekeeping
```

Completed work should be reviewed and committed when ready:

```text
v1.6.0 Matchup Read display
v1.6.1 duplicate confidence cleanup
v1.6.2 diagnostic label cleanup
```

Suggested commit scope:

```text
src/lib/nfl-api.ts
src/pages/Matchup.tsx
```

Suggested validation games:

```text
Clear Lean / Medium Confidence
Strong Profile / High Confidence
Thin Edge / Low Confidence
No Clear Edge / Low Confidence
Correct final result
Incorrect final result
No Pick final result
Mixed Game Profile Signals
Neutral Game Profile signal included
```

---

### 2. QA Script Upgrade

Status:

```text
Useful next quality-of-life improvement
```

Update the QA script to summarize new fields:

```text
profile_strength
outcome_confidence
matchup_label
matchup_cautions
matchup_breakdown.summary_counts
turnover_margin_per_game presence
model_trust.edge.strength
model_trust.signal_alignment.summary_label
model_trust.signal_alignment.summary
```

This will make future before/after testing easier.

---

### 3. Rerun Larger QA Batch After v1.6.x Commit

Status:

```text
Optional but useful
```

We previously tested targeted games after hard-coding the windowed source and after frontend polish.

Still useful later:

```text
Run the larger 73-game QA again after v1.6.x settles to confirm no accidental drift.
```

Add new QA summaries for:

```text
profile_strength distribution
outcome_confidence distribution
matchup_label distribution
matchup_cautions counts
headline_metric_count
context_note_count
turnover_margin_per_game presence
Team Comparison Support distribution
Game Profile Signals distribution
```

---

### 4. Builder Automation Planning

Status:

```text
Mission critical before full production confidence
```

Do not wire builders into `app.py` yet.

Reason:

```text
Need ingestion/idempotency review before automating post-stats builders.
```

Future possible env vars:

```text
ENABLE_METRIC_FACTS_BUILD=false
ENABLE_WINDOWED_METRICS_BUILD=false
ENABLE_METRIC_RANKINGS_BUILD=false
```

Recommended planning questions:

- What triggers the builder chain?
- Which season rebuilds after a completed game?
- Should rebuilds run immediately or as scheduled/queued jobs?
- How should failures be logged and recovered?
- How do we avoid duplicate/stale runs?
- How should local/dev behavior differ from Cloud Run?

---

## Future Frontend Work

### 5. Frontend Adoption of `matchup_breakdown`

Status:

```text
Future frontend pass / likely v1.7
```

Goal:

```text
Expose the Step 3 explanation layer in a readable way.
```

Possible sections:

```text
Top Metric Drivers
Category Reads
Core Area Reads
Context Notes
Freshness / As-Of Context
```

Important:

```text
Do not dump every field.
Use matchup_breakdown as a summary layer, not a wall of data.
```

---

### 6. Full Frontend Redesign

Status:

```text
Future major version
```

Recommended version:

```text
GameLens 2.0
```

A true redesign should center the page around:

```text
matchup intelligence
profile strength
outcome confidence
metric drivers
context notes
model trust
```

This is bigger than simply showing one new label.

---

## Future Backend Work

### 7. Metric Registry Audit Branch

Status:

```text
Future backend cleanup
```

Create a separate branch later:

```bash
git checkout -b registry-metadata-audit
```

Goal:

```text
Align registry metadata with service-layer behavior from v1.5/v1.6.
```

Candidates for audit:

```text
total_defensive_snaps
defensive_snap_load
opponent_total_plays
total_plays
total_drives
passing_yards
rushing_yards
actual_points
red_zone_tds
passing_tds
rushing_tds
fourth_down_pct
```

Important:

```text
Changing metric_registry.py requires rebuilding downstream tables.
```

Rebuild chain:

```text
metric_registry.py
→ game_team_metric_facts
→ team_metrics_windowed
→ team_metric_rankings
→ /game output
```

---

### 8. Field Control Source Repair

Status:

```text
Future
```

Current state:

```text
Field Control is hidden.
```

Future work:

```text
Fix source/parser/backend extraction.
Decide whether it belongs in Core Area logic or only context notes.
Re-enable only after validation.
```

---

### 9. Historical / Stability Context

Status:

```text
Future high-value layer
```

Goal:

```text
Explain whether the current team profile looks stable, recent, mixed, or unsupported.
```

Possible questions:

- Does last_3 agree with season-to-date?
- Does last_7 agree with season-to-date?
- Is this a recent shift?
- Is the profile stable?
- Is early-season confidence supported?

Historical context should not directly pick winners.

It should support or soften confidence language.

---

### 10. Injury Context

Status:

```text
Future context layer
```

Current state:

```text
A development injury API/test file exists.
Not productionized.
Not part of daily ingestion.
```

Future role:

```text
Explain roster risk, position group weakness, and context that current metrics may not capture.
```

Do not let injury context directly override picks until the layer is stable.

---

### 11. Outcome Quality Labels

Status:

```text
Future model trust layer
```

Goal:

```text
Separate whether the model was directionally useful from whether the final winner pick was simply correct/incorrect.
```

Possible labels:

```text
correct and clean
correct but narrow
incorrect close variance miss
incorrect calibration failure
no pick / good restraint
no pick / missed opportunity
```

---

### 12. Postgame Swing Factors

Status:

```text
Future postgame explanation layer
```

Possible swing factors:

```text
turnovers
explosive plays
kicking
injuries
late-game execution
defensive touchdowns
overtime variance
one-quarter scoring spikes
```

Purpose:

```text
Explain what changed the game without pretending the pregame model should have known everything.
```

---

### 13. Recent Form / Rolling-Window Layer

Status:

```text
Future supporting context
```

Available windows:

```text
last_3_games
last_7_games
```

Current rule:

```text
Do not make last_3 or last_7 the primary Matchup Lean source yet.
```

Future use:

```text
Show whether recent form supports, conflicts with, or sharpens the season-to-date profile.
```

---

### 14. Internal Historical QA / Model Performance View

Status:

```text
Future internal/admin layer
```

Purpose:

```text
Use historical seasons as a testing ground without turning GameLens into a public accuracy scoreboard.
```

Possible measures:

```text
pick accuracy
no-pick rate
confidence-tier calibration
matchup_label distribution
outcome_confidence distribution
profile_strength distribution
miss severity
no-pick restraint quality
signal validation
Core Area validation
```

Avoid:

```text
public profit-style accuracy marketing
```

---

### 15. Postgame Signal Validation Feedback Loop

Status:

```text
Future high-value QA/model-trust layer
```

Purpose:

```text
Validate whether GameLens correctly identified the shape of the game, not only whether it picked the winner.
```

Possible validation areas:

```text
Pressure
Turnover Risk
Scoring Efficiency
Defensive Control
Offensive Output
Team Comparison
No Pick restraint
Confidence calibration
```

Possible labels:

```text
confirmed
partially confirmed
contradicted
not enough data
good restraint
missed opportunity
```

This should start as an internal QA layer before becoming user-facing.

---

### 16. Lens-Tag League Discovery Tool

Status:

```text
Exploratory / future intelligence platform layer
```

Purpose:

```text
Use lens_tags to show league-wide patterns over time.
```

Potential displays:

```text
movers and shakers
league heatmap
team trend lines
rising/falling themes
weekly identity shifts
```

Important:

```text
This should remain an intelligence/discovery layer, not a betting edge board.
```

---

# Future / Exploratory

## Dynamic Matchup Advantage / Dynamic Matchup Drivers

Status:

```text
Exploratory / v2 product idea
```

Current state:

The visible Team Comparison / Matchup Advantage section currently uses a stable set of static metrics:

```text
Points per Play
Points Allowed per Play
3rd Down %
Red Zone TD %
Turnover Margin / Game
```

This is useful because it gives every game a consistent comparison baseline.

Exploratory v2 idea:

```text
Add a dynamic matchup-driver layer that selects the most meaningful metrics for each specific game, instead of always showing only the same static metrics.
```

Important caution:

Dynamic drivers should not simply mean:

```text
pick the biggest percentile gaps
```

They should select metrics that are:

- headline eligible
- confidence eligible
- strong signal
- edge-language allowed
- good data quality
- not near-even
- not repetitive across the same category/Core Area

Possible selection rules:

```text
ranking_usage = edge
ranking_kind = edge
edge_language_allowed = true
confidence_eligible = true
signal_strength = strong
data_quality_status = good
```

Possible diversity rules:

- max 2 metrics from the same Core Area
- max 1–2 metrics from the same category
- prefer multiple Core Areas when available
- exclude near-even metrics from dynamic headline drivers
- keep context-only/supporting metrics in context notes

Potential v2 structure:

```text
Core Comparison = stable/familiar baseline
Key Matchup Drivers = dynamic game-specific explanation
Context Notes = descriptive but not decisive
```

Goal:

```text
Move GameLens from showing the same five metrics every game toward showing what actually separates these two teams in this matchup.
```

This should begin as an explanation/display layer before it influences confidence or model scoring.

---

# Version Roadmap

## v1.5.0 — Complete

```text
Backend metric foundation + matchup explanation API
```

Included:

- windowed metrics default
- ranking context
- matchup_breakdown
- profile strength vs outcome confidence
- near-even handling
- confidence guardrail
- supporting/context filtering
- Field Control hidden
- Turnover Margin / Game cleanup
- 2023/2024/2025 rebuilt with current metric contract

## v1.6.0 — Complete

```text
Frontend Matchup Read display using profile strength and outcome confidence.
```

Included:

- typed new `matchup_lean` fields
- displayed Matchup Read block
- displayed Profile + Confidence chips
- displayed summary text
- mapped user-facing caution labels

## v1.6.1 — Complete

```text
Duplicate confidence cleanup.
```

Included:

- Matchup Lean owns confidence
- Model Trust no longer displays a second confidence pill

## v1.6.2 — Complete

```text
Model Trust diagnostic label clarification.
```

Included:

- `What supported or challenged the read?`
- `Team Comparison Support`
- `Game Profile Signals`
- updated tooltips

## v1.6.3 — Backburner Polish

See separate file:

```text
GameLens_Backburner_Cleanup_v1.6.3.md
```

Likely theme:

```text
Small wording polish only; not urgent.
```

## v1.7.0 — Suggested Next Larger Frontend Step

```text
Frontend displays matchup_breakdown cards.
```

## v2.0.0 — Larger Redesign

```text
Full matchup page redesign around GameLens explanation model.
```

Possible v2+ additions:

```text
dynamic matchup drivers
historical/stability context
lens-based league discovery
internal QA dashboard
postgame signal validation
```

---

# Do Not Do Yet

Do not:

- rebuild the frontend around every new field immediately
- turn rankings into automatic winner logic
- re-enable Field Control before fixing the source layer
- make last_3 or last_7 the main Matchup Lean source yet
- wire builders into `app.py` before ingestion/idempotency review
- treat historical context as a pick override
- treat injury context as a pick override
- remove QA discipline just because v1.5/v1.6 shipped
- expose a public model-success-rate board before internal QA is mature
- build League Discovery UI before lens-tag cleaning and backend validation
- show raw backend lens tags directly to users
- let Model Trust become a second confidence source

---

# Practical Frontend Note

Current stable frontend direction:

```text
Use Matchup Lean / Matchup Read as the confidence source.
Use Model Trust & Outcome as the validation source.
Use the diagnostic area to explain what supported or challenged the read.
```

Later frontend adoption:

```text
matchup_breakdown.metric_highlights
matchup_breakdown.category_summaries
matchup_breakdown.core_area_summaries
matchup_breakdown.context_notes
```

This lets the frontend adopt backend intelligence gradually without requiring a full GameLens 2.0 redesign immediately.

---

# Practical Backend Note

The current backend priority is:

```text
Make sure the new data-building system becomes the trusted production path.
```

Recommended next backend order:

```text
1. Add/upgrade internal QA summary script.
2. Design safe builder orchestration for app.py.
3. Decide how completed-game ingestion should trigger facts → windowed → rankings.
4. Only then wire builder automation into live ingestion.
5. Later, explore historical/stability context and lens-tag discovery views.
```
