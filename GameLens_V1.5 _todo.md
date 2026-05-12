# GameLens v1.5+ Mission-Critical Status, Frontend Handoff, and Backlog

_Last updated: May 12, 2026_

## Current Product Frame

GameLens is best understood as:

> A matchup intelligence and confidence-calibration tool.

It should help users understand:

- where each team has an edge
- where the matchup is close or near-even
- which areas are clean, split, mixed, or uncertain
- whether confidence language is appropriate
- why the model leaned a certain direction
- what parts of the matchup were descriptive context rather than decisive evidence

GameLens should **not** behave like a forced pick machine.

The winner lean can remain part of the page, but the larger value is explaining the matchup shape.

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
The new GameLens data model now depends on the cleaned facts, windowed metrics, and ranking builders.
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

Possible future env vars:

```text
ENABLE_METRIC_FACTS_BUILD=false
ENABLE_WINDOWED_METRICS_BUILD=false
ENABLE_METRIC_RANKINGS_BUILD=false
```

Do not wire builders into `app.py` until this is designed.

---

## 2. Rebuild and Validate Older Seasons

Status:

```text
Mission critical / next backend validation step
```

Why this matters:

```text
The metric registry changed.
turnover_margin_per_game now exists as a real windowed metric.
Older seasons should be rebuilt so historical testing uses the same metric contract as 2025.
```

Recommended rebuild chain for each season:

```bash
python -m agg.build_metric_facts --season 2023 --if-exists replace
python -m agg.build_windowed_metrics --season 2023 --if-exists replace
python -m agg.build_metric_rankings --season 2023 --if-exists replace

python -m agg.build_metric_facts --season 2024 --if-exists replace
python -m agg.build_windowed_metrics --season 2024 --if-exists replace
python -m agg.build_metric_rankings --season 2024 --if-exists replace
```

Validate that `turnover_margin_per_game` exists in:

```text
Analytics.team_metrics_windowed_2023
Analytics.team_metric_rankings_2023
Analytics.team_metrics_windowed_2024
Analytics.team_metric_rankings_2024
```

---

## 3. Keep GameLens From Becoming a Pick Machine

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

---

## 4. Keep Frontend v1.6 Small

Status:

```text
Mission critical scope control
```

The safest next frontend step is still:

```text
Display matchup_lean.matchup_label and related profile/outcome summaries.
```

Do not use limited frontend/Lovable work on:

- full redesign
- Field Control repair
- historical modeling
- League Discovery UI
- injury context
- dynamic driver UI

Those are bigger projects.

---

## 5. Keep Field Control and Snap Counts in Audit Mode

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

## 6. Build Internal QA Before Public Success-Rate UI

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
- matchup_label distribution
- matchup_cautions
- correct but narrow outcomes
- incorrect close-variance misses
- incorrect calibration failures
- no pick / good restraint
- no pick / missed opportunity

The goal is not only:

```text
Was the winner correct?
```

The better question is:

```text
Did GameLens explain the matchup honestly?
```

---

## 7. Treat Lens-Tag League Discovery as Exploratory

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
- rebuilt downstream 2025 tables
- updated visible Team Comparison to use `Turnovers::turnover_margin_per_game`
- updated Game Profile Turnover Risk to use `turnover_margin_per_game`
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

### Validation Completed

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

### Current Status

This is no longer a pending cleanup item.

Remaining related work:

```text
Rebuild 2023 and 2024 using the updated registry and builders.
```

---

# v1.5.0 Release Status

## v1.5.0 — Completed and Shipped

Status:

✅ Committed  
✅ Tagged  
✅ Pushed  
✅ Google Cloud Build passed  
✅ Frontend loaded successfully  
✅ API response shape stayed stable  
✅ Existing frontend expectations still work  

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

# Completed Work

## 1. Product Refocus

Status: ✅ Complete

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

Status: ✅ Complete, but future audit recommended

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

Future audit is still recommended, but not required for the current v1.5.0 release.

---

## 3. Cleaned Fact Tables

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete

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

## 6. `/game` Now Uses Windowed Metrics by Default

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete for API behavior, source repair still future

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

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete

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

Status: ✅ Complete

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

This is important because a team can have a strong statistical profile and still lose.

Frontend should eventually prefer `matchup_label` over displaying raw `confidence` alone.

---

## 16. API Compatibility

Status: ✅ Complete / validated

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

## 17. Turnover Margin Per Game Cleanup

Status: ✅ Complete

This was previously listed as:

```text
Definitely do / backend cleanup
```

It is now complete.

Completed behavior:

- `turnover_margin_per_game` exists as a real derived windowed metric
- it is defined in `analytics/metric_registry.py`
- it is calculated in `agg/build_windowed_metrics.py`
- it exists in `team_metrics_windowed`
- it exists in `team_metric_rankings`
- visible Team Comparison uses `Turnovers::turnover_margin_per_game`
- Game Profile Turnover Risk uses `Turnovers::turnover_margin_per_game`
- cumulative `turnover_margin` remains supporting context

Goal achieved:

```text
Visible matchup comparison uses per-game turnover margin, while cumulative turnover margin is not the default user-facing comparison metric.
```

---

# Frontend Handoff

## What frontend can use now

### Best immediate field to display

```text
matchup_lean.matchup_label
```

Examples:

```text
Strong Profile / Medium Outcome Confidence
Clear Lean / Medium Outcome Confidence
Strong Profile / High Outcome Confidence
```

This is safer and more accurate than displaying only:

```text
matchup_lean.confidence
```

### Useful supporting fields

```text
matchup_lean.profile_strength.label
matchup_lean.profile_strength.summary
matchup_lean.outcome_confidence.label
matchup_lean.outcome_confidence.summary
matchup_lean.matchup_cautions
```

### New future display section

```text
matchup_breakdown
```

Possible frontend cards:

- Metric Highlights
- Category Summary
- Core Area Summary
- Context Notes
- Freshness / As-Of Context

---

## Suggested frontend display hierarchy

Recommended eventual page structure:

1. Game Header
2. Final Score / Status
3. Matchup Lean
4. Matchup Label
5. Profile Strength
6. Outcome Confidence
7. Core Area Comparison
8. Matchup Breakdown
   - Metric Highlights
   - Category Summaries
   - Core Area Summaries
   - Context Notes
9. Model Trust / Outcome Learning

---

# Remaining Backlog

## Immediate / Next

### 1. Frontend adoption of `matchup_label`

Status:

```text
Next frontend-friendly step
```

Goal:

```text
Display the new matchup label so users see profile strength and outcome confidence separately.
```

Suggested display:

```text
BUF edge
Strong Profile / High Outcome Confidence
```

or:

```text
PHI edge
Strong Profile / Medium Outcome Confidence
```

This is likely a v1.6-level frontend update, not a full redesign.

---

### 2. Frontend adoption of `matchup_breakdown`

Status:

```text
Future frontend pass
```

Goal:

```text
Expose the new Step 3 explanation layer in a readable way.
```

Possible sections:

```text
Top Metric Drivers
Category Reads
Core Area Reads
Context Notes
```

This may be v1.7-level work.

---

### 3. Full frontend redesign

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

### 4. Rerun 73-game QA after final hard-code

Status:

```text
Optional but useful
```

We already tested targeted games after hard-coding the windowed source.

Still useful later:

```text
Run the larger 73-game QA again after v1.5.0 to confirm no accidental drift.
```

Add new QA summaries for:

```text
profile_strength distribution
outcome_confidence distribution
matchup_label distribution
matchup_cautions counts
headline_metric_count
context_note_count
```

---

### 5. Metric registry audit branch

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
Align registry metadata with the service-layer behavior from v1.5.0.
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

### 6. QA script upgrade

Status:

```text
Future quality-of-life improvement
```

Update the QA script to summarize new fields:

```text
profile_strength
outcome_confidence
matchup_label
matchup_cautions
matchup_breakdown.summary_counts
```

This will make future before/after testing easier.

---

## Future Backend Work

### 7. Field Control source repair

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

### 8. Historical / stability context

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

### 9. Injury context

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

### 10. Outcome quality labels

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

### 11. Postgame swing factors

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

### 12. Recent form / rolling-window layer

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

### 13. Builder automation in app.py

Status:

```text
Deferred / mission critical before full production confidence
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

Current interpretation:

```text
This is one of the most important backend items before relying on live automated updates.
```

---

### 14. Rebuild Older Seasons With Current Registry

Status:

```text
Mission critical / next backend validation step
```

Need to rebuild:

```text
2023
2024
```

Reason:

```text
The registry and windowed builder changed after turnover_margin_per_game was added.
Historical QA and future discovery tools should use consistent metric definitions.
```

---

### 15. Internal Historical QA / Model Performance View

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
```

Avoid:

```text
public profit-style accuracy marketing
```

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

Move GameLens from:

```text
Here are the same five metrics every game.
```

toward:

```text
Here is what actually separates these two teams in this matchup.
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

## v1.6.0 — Suggested next

```text
Frontend displays matchup_label and profile/outcome confidence summaries.
```

## v1.7.0 — Suggested after that

```text
Frontend displays matchup_breakdown cards.
```

## v2.0.0 — Larger redesign

```text
Full matchup page redesign around GameLens explanation model.
```

Possible v2+ additions:

```text
dynamic matchup drivers
historical/stability context
lens-based league discovery
internal QA dashboard
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
- remove QA discipline just because v1.5.0 shipped
- expose a public model-success-rate board before internal QA is mature
- build League Discovery UI before lens-tag cleaning and backend validation
- show raw backend lens tags directly to users

---

# Practical Frontend Note

For now, the safest frontend adoption is:

```text
Use matchup_lean.matchup_label as the display label.
```

Then later add:

```text
matchup_breakdown.metric_highlights
matchup_breakdown.category_summaries
matchup_breakdown.core_area_summaries
matchup_breakdown.context_notes
```

This lets the frontend adopt the new backend intelligence gradually without requiring a full GameLens 2.0 redesign immediately.

---

# Practical Backend Note

The current backend priority is:

```text
Make sure the new data-building system becomes the trusted production path.
```

Recommended next backend order:

```text
1. Rebuild and validate 2023.
2. Rebuild and validate 2024.
3. Add/upgrade internal QA summary script.
4. Design safe builder orchestration for app.py.
5. Only then wire builder automation into live ingestion.
```
