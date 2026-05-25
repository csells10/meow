# GameLens Admin Claim Health API — MVP Notes

_Date: 2026-05-24_  
_Project: GameLens NFL App_  
_Endpoint tested locally:_  
`/admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025`

---

## 1. What We Built

Today we created the MVP skeleton for an **admin-facing aggregate claim-health API**.

The endpoint is designed to help answer:

> How is GameLens doing at telling the correct pregame football story?

This endpoint is **not** intended to be a public scoreboard.

It is also **not** a game-by-game review tool yet.

The current MVP is aggregate-level only. It summarizes claim-validation performance across groups of claims, football areas, confidence labels, claim surfaces, and feature buckets.

The source table is:

```text
nfl-stream-406420.Analytics.gamelens_claim_training_examples

The current test run is:

run_id = full_2025_reg_post_claim_matrix_pilot
season = 2025

The endpoint currently returns:

coverage
baseline
section_metadata
sections.core_area_matrix
sections.category_matrix
sections.confidence_core_area_matrix
sections.feature_scorecard
sections.surface_matrix
2. Why This Endpoint Exists

The goal is to create an internal/admin view that shows whether GameLens is historically making useful, truthful claims.

GameLens does not only need to answer:

Did the predicted team win?

It also needs to answer:

Were the claims GameLens made before the game actually supported by postgame data?

That distinction matters.

A team can lose the game, but a specific claim may still validate.

Example:

GameLens says Team A has the better passing profile.
Team A loses the game.
But Team A does produce the better passing efficiency.
That claim can still validate.

So this endpoint is about claim quality, not just winner accuracy.

3. Current Full-Slate Pilot Status

The 2025 full-slate pilot successfully created a real dataset for admin reporting.

Current known values:

Expected 2025 regular season + postseason games: 285
Games with claim rows: 269
Games without claim rows: 16
Claim rows: 8,067
Validated claims: 3,960
Not validated claims: 3,111
Neutral or mixed claims: 996
Overall validation rate: ~49.1%

Important context:

The 16 games without claim rows were Week 1 games.
Those games were payload-covered, but produced no claim rows because pregame ranking/window context was unavailable.

This is not treated as a failure. It is treated as an algorithm coverage finding.

The future admin board should show both:

Game coverage
Claim validation performance

Those are related, but not the same thing.

4. Current API Response Shape

The MVP response shape is:

{
  "available": true,
  "scope": "aggregate_claim_health",
  "run_id": "full_2025_reg_post_claim_matrix_pilot",
  "season": "2025",
  "generated_at": "...",
  "coverage": {},
  "baseline": {},
  "section_metadata": {},
  "sections": {
    "core_area_matrix": [],
    "category_matrix": [],
    "confidence_core_area_matrix": [],
    "feature_scorecard": [],
    "surface_matrix": []
  }
}

This is intentionally graph-ready.

The frontend should not need to guess what each section means because section_metadata now describes:

title
description
chart_type
primary_metric
5. What Each API Section Means
5.1 coverage
Purpose

Shows whether the data universe is complete enough to trust.

Current fields:

expected_games
games_with_claims
games_without_claims
context_note
How to interpret

This section answers:

Did GameLens produce claim rows for the games we expected?

For the current pilot:

285 expected games
269 games with claims
16 games without claims

The 16 missing claim games are expected because Week 1 had no pregame context.

Frontend display suggestion

Use a top-level card:

Coverage
285 expected games
269 with claims
16 no-claim games

Include the context_note as a small warning/info callout.

Suggested UI style:

Card title: Coverage
Primary number: 269 / 285 games with claims
Secondary note: 16 Week 1 games had no claim rows due to missing pregame context.
5.2 baseline
Purpose

Shows the overall claim-validation baseline for the selected run.

Current fields:

claim_row_count
game_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
neutral_or_mixed_rate
How to interpret

This is the overall benchmark.

For the current pilot:

Overall validation rate: ~49.1%

Every other section should be compared against this baseline.

Example:

Offensive Output at 53.1% is above baseline.
Disruption and Turnovers at 44.3% is below baseline.
Frontend display suggestion

Use a top-level card:

Baseline Claim Validation
49.1%
8,067 claim rows
269 games with claims

Also show counts:

3,960 validated
3,111 not validated
996 neutral/mixed

Suggested visualization:

Donut chart or stacked horizontal bar:
Validated / Not Validated / Neutral-Mixed
5.3 core_area_matrix
Purpose

Shows claim validation by broad football area.

Current core areas include:

Offensive Output
Defensive Control
Scoring Efficiency
Disruption and Turnovers
missing_core_area
How to interpret

This section answers:

Which broad football areas are telling the correct pregame story most often?

Current findings from the MVP response:

Offensive Output: ~53.1%
Defensive Control: ~51.7%
Scoring Efficiency: ~47.6%
missing_core_area: ~46.8%
Disruption and Turnovers: ~44.3%

Interpretation:

Offensive Output is currently the strongest broad claim area.
Defensive Control is useful and slightly above baseline.
Scoring Efficiency is below baseline and should stay measured.
Disruption and Turnovers is the weakest broad area and should stay cautious.
Frontend display suggestion

Use a bar chart sorted by validation rate.

Recommended chart:

Bar chart
X-axis: validation rate
Y-axis: core area
Reference line: baseline validation rate

Also show neutral/mixed rate as a secondary column or tooltip.

Why:

Some areas may have okay validation but high neutral/mixed rate.
High neutral/mixed rate means the claim often lands in a muddy actual result.
5.4 category_matrix
Purpose

Shows claim validation by more specific football category within each core area.

Examples:

Passing Game
Rushing Game
Offensive Rhythm
Scoring Suppression
Turnovers
Red Zone Finish
Drive Conversion
Scoring Production
Pressure
How to interpret

This section answers:

Which categories are carrying or dragging each broad area?

Current important findings:

Passing Game is very strong: ~59.2%
Scoring Suppression is modestly useful: ~51.5%
Rushing Game is near baseline: ~49.4%
Red Zone Finish is weak: ~47.7%
Drive Conversion is weak: ~47.5%
Scoring Production is weak/noisy: ~46.2%
Turnovers is weak: ~44.1%
Pressure is weak: ~41.2%

Interpretation:

Offensive Output looks good mostly because Passing Game is strong.
Rushing Game is not obviously bad, but it is not a strong automatic claim driver yet.
Scoring Efficiency is dragged down by red zone, drive conversion, and scoring production volatility.
Turnovers and Pressure should stay contextual/cautionary.
Frontend display suggestion

Use a grouped table first, not a fancy chart.

Recommended columns:

Core Area
Category
Claim Rows
Game Count
Validation Rate
Neutral/Mixed Rate
Delta vs Baseline

Nice-to-have UI behavior:

Group rows by core_area.
Sort within each core_area by validation_rate.
Color validation_rate relative to baseline:
  above baseline = positive
  near baseline = neutral
  below baseline = caution

Do not overdo colors yet. Keep it readable.

5.5 confidence_core_area_matrix
Purpose

Shows claim validation by confidence label and core area.

Confidence labels:

High
Medium
Low
How to interpret

This section answers:

Does GameLens confidence actually line up with better claim validation?

Important current pattern:

High and Medium confidence generally validate better than Low.
Medium confidence is very strong for Offensive Output and Defensive Control.
High confidence is strong in Defensive Control and Offensive Output.
Even High confidence is weak for Disruption and Turnovers.

Examples from current response:

High + Defensive Control: ~64.6%
High + Offensive Output: ~59.9%
Medium + Offensive Output: ~59.9%
Medium + Defensive Control: ~57.1%
Low + most areas: generally lower
High + Disruption and Turnovers: ~43.5%

Interpretation:

The confidence ladder is directionally useful, but not universally reliable.
High confidence does not magically fix weak/volatile areas like Disruption and Turnovers.
Medium confidence may be a very healthy zone where GameLens is useful but not over-loud.
Frontend display suggestion

Use grouped bars or a heatmap.

Recommended visual:

Rows: Core Area
Columns: High / Medium / Low
Cell value: validation_rate

Suggested interpretation aid:

Show baseline line or baseline label.
Highlight cells above baseline.
Flag cells below baseline.

Potential title:

Confidence Calibration by Football Area
5.6 feature_scorecard
Purpose

Shows claim validation by feature bucket for offensive_efficiency_support_v1.

Buckets include:

repeat_positive_strong
repeat_positive_supportive
not_relevant
context_only
negative_caution
mixed_near_even
anchor_unavailable
caution_only
opposing_efficiency_signal
How to interpret

This section answers:

Does the new feature separate stronger offensive-efficiency claim contexts from weaker/noisier ones?

Current finding:

Yes, it does.

Current bucket ladder:

repeat_positive_strong: ~63.6%
repeat_positive_supportive: ~54.4%
not_relevant: ~49.2%
context_only: ~46.9%
negative_caution: ~46.2%
mixed_near_even: ~45.2%
anchor_unavailable: ~45.1%
caution_only: ~43.8%
opposing_efficiency_signal: ~35.1%

Interpretation:

repeat_positive_strong is a real signal.
repeat_positive_supportive is useful but should stay measured.
caution/mixed/unavailable/opposing buckets correctly underperform.

Product meaning:

This feature is a Level 4 language-calibration candidate.
It should not be used for winner prediction, Matchup Lean confidence, or Model Trust overrides.
Frontend display suggestion

Use a bar chart sorted by validation rate.

Recommended visual:

Bar chart:
Y-axis: bucket
X-axis: validation_rate
Reference line: baseline
Tooltip: claim rows, game count, neutral/mixed rate

Add a small explanatory note:

This feature is used to evaluate claim-language support, not winner prediction.
5.7 surface_matrix
Purpose

Shows claim validation by claim type and claim layer.

Surfaces include:

core_area_comparison / headline
metric_highlight / headline
core_area_summary / supporting
category_summary / supporting
team_comparison_metric / supporting
metric_highlight / supporting
game_profile / headline
How to interpret

This section answers:

Which kinds of GameLens claims are cleaner or noisier?

Current findings:

Headline core area comparisons are strongest: ~52.2%
Headline metric highlights are also above baseline: ~51.4%
Supporting claims are generally closer to or below baseline.
Game Profile headlines are weak: ~46.8%

Interpretation:

Headline claims are generally cleaner than supporting/detail claims.
Game Profile claims should stay cautious.
Supporting metric highlights may need more filtering or stronger gating.
Frontend display suggestion

Use a table first.

Recommended columns:

Claim Type
Claim Layer
Claim Rows
Game Count
Validation Rate
Neutral/Mixed Rate
Delta vs Baseline

Later this can become a small bar chart.

Suggested title:

Claim Surface Health
6. Frontend Display Concept
Admin Board MVP Layout

Recommended first-pass layout:

Admin Claim Health
  ├── Top Summary Cards
  │     ├── Coverage
  │     ├── Baseline Validation
  │     ├── Claim Rows
  │     └── Games With Claims
  │
  ├── Core Area Health
  │     └── Bar chart
  │
  ├── Category Health
  │     └── Grouped table
  │
  ├── Confidence by Core Area
  │     └── Heatmap or grouped bar chart
  │
  ├── Offensive Efficiency Feature Scorecard
  │     └── Bar chart
  │
  └── Claim Surface Health
        └── Table
Recommended UI Style

Keep this practical and boring at first.

Avoid:

Too many labels
Too many badges
Automatic “good/bad” judgments
AI action cards
Game-level clutter

Prefer:

Clear section titles
Validation rate
Row count
Game count
Neutral/mixed rate
Baseline comparison
Simple tooltips
Suggested visual rules

For each chart/table:

Always show claim_row_count.
Always show game_count.
Always show validation_rate.
Show neutral_or_mixed_rate where useful.
Compare to baseline.
Avoid over-interpreting small samples.

Possible frontend helper calculation:

delta_vs_baseline = row.validation_rate - baseline.validation_rate

Display examples:

+14.5 pts vs baseline
-5.3 pts vs baseline
near baseline
7. How This Can Refresh Daily in the Future

The current MVP live-queries the claim-training table for a selected run_id.

Future version should become automated and refreshed on a schedule.

Future daily-ish pipeline

Recommended future flow:

Completed games
→ collect /game payloads
→ build claim rows
→ validate claims
→ update Level 3 features
→ aggregate admin claim-health snapshots
→ admin board reads latest snapshot

This should run after:

games are final
postgame metric facts are available
claim rows have been generated
validation has run
Level 3 feature updates have run

This means the board is not truly live second-by-second.

It is better described as:

as of latest completed pipeline run
Recommended future table

Eventually create a snapshot table such as:

Analytics.gamelens_admin_claim_health_snapshots_v1

Possible fields:

snapshot_date
snapshot_timestamp
run_id
season
matrix_name
row_key
row_label
column_key
column_label
claim_row_count
game_count
validated_count
not_validated_count
neutral_or_mixed_count
unavailable_count
validation_rate
neutral_or_mixed_rate
created_at

Why snapshots?

The admin board can load quickly.
Historical trend over time becomes possible.
Daily refreshes can be compared.
The frontend does not need to run heavy BigQuery aggregations on demand.
Future automation model

Short-term:

Admin endpoint live-queries BigQuery using run_id.

Medium-term:

Nightly or postgame job inserts aggregate matrix snapshots.

Long-term:

Admin board defaults to latest snapshot and can compare snapshots over time.

Possible future command/script:

python -m agg.gamelens_training.build_admin_claim_health_snapshots \
  --run-id production_2025_latest \
  --season 2025 \
  --write-bigquery

Possible future schedule:

Cloud Scheduler
→ Cloud Run job
→ refresh claim-health snapshots
8. Future API Direction
Current endpoint
GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

Current behavior:

Live aggregate query from claim-training table.
Future endpoints
Latest aggregate health
GET /admin/gamelens/claim-health/latest?season=2025

Reads the latest completed snapshot.

Specific snapshot
GET /admin/gamelens/claim-health?snapshot_date=2026-05-25&season=2025

Reads a specific snapshot.

Matrix-only endpoint
GET /admin/gamelens/claim-health/matrix?matrix=feature_scorecard&season=2025

Useful if the frontend wants to lazy-load heavy sections.

Future game drilldown
GET /admin/gamelens/game-review/:game_id

This should be built later and kept separate from the aggregate board.

9. Future Expansion Ideas
9.1 Add metric_matrix

Add aggregate validation by metric.

Purpose:

See which specific metrics are useful or noisy.

Example metrics:

yards_per_pass
points_per_play
points_allowed_per_play
turnover_margin_per_game
red_zone_efficiency
td_rate
third_down_pct
yards_per_rush

This would help confirm findings like:

yards_per_pass is strong.
points_per_play is useful.
td_rate is noisy.
turnover_margin_per_game is volatile.
9.2 Add feature_surface_matrix

Add validation by:

offensive_efficiency_support_bucket
claim_type
claim_layer

Purpose:

See whether feature buckets survive across claim surfaces.

This is important because a feature may work for headline claims but not supporting claims.

9.3 Add feature_metric_matrix

Add validation by:

metric
offensive_efficiency_support_bucket

Purpose:

See whether the feature separates strong and weak buckets within specific metrics.

This is how we found that repeat-positive support helped metrics like:

yards_per_pass
yards_per_play
points_per_play
yards_per_rush
9.4 Add week/phase trends

Add aggregate views by:

season_phase
week
bucket

Possible buckets:

early_regular
mid_regular
late_regular
postseason

Purpose:

See whether GameLens changes over the season.

Important:

Do not over-interpret daily trends from sampled QA runs.
Daily/weekly trends are most useful only on full-slate data.
9.5 Add confidence trend

Track:

High / Medium / Low confidence validation rate over time
No Pick rate over time
High-confidence miss rate

Purpose:

See whether confidence calibration is improving or drifting.
9.6 Add coverage detail

Add a separate coverage endpoint or section to show:

expected games
payload-collected games
claim-producing games
validated games
feature-updated games
missing-context games

Purpose:

Do not let missing data silently disappear.
9.7 Add game-level drilldown later

Future admin game tools could include:

View one game’s claims
View validation results for one game
Trigger payload recollection
Trigger claim rebuild
Trigger validation update
Compare old/new output for one game

Important:

Do not mix this into the aggregate MVP.
Keep game review tools separate.
10. Important Interpretation Rules
Rule 1: This is claim validation, not winner accuracy

Do not describe this board as:

Model accuracy
Prediction accuracy
Betting accuracy

Better wording:

Claim health
Claim validation
Algorithm health
Historical claim matrix
Rule 2: Validation rate needs context

A high validation rate with low row count can mislead.

Always inspect:

claim_row_count
game_count
neutral_or_mixed_rate
Rule 3: Neutral/mixed is not the same as wrong

actual_neutral_or_mixed means the postgame data was too close, tied, or muddy.

It should not always be treated like failure.

Rule 4: Coverage matters

The current pilot has:

285 expected games
269 claim-producing games
16 no-claim games

The 16 no-claim games are an important admin finding.

Rule 5: Strong features should separate buckets

A useful feature should create a ladder like:

strong support > measured support > baseline > caution/mixed/unavailable

offensive_efficiency_support_v1 currently does this.

Rule 6: Do not rush frontend conclusions

This API should first help us look at the data.

It should not immediately create public badges, confidence overrides, or winner-prediction changes.

11. Current Product Finding From the MVP Data

The first full-slate admin-health response suggests:

Offensive Output is the strongest broad claim area.
Defensive Control is useful and slightly above baseline.
Scoring Efficiency is weaker and more volatile.
Disruption and Turnovers is the weakest broad area.
Passing Game is one of the strongest categories.
Turnovers and Pressure are weak/cautionary.
Headline claims are generally cleaner than supporting/detail claims.
The offensive_efficiency_support_v1 feature has a strong validation ladder and is a real Level 4 language-calibration candidate.

These are not final product rules yet.

They are early full-slate observations that should guide future review.

12. Current Implementation Files

Files added/updated for MVP:

queries/admin_claim_health_queries.py
services/admin_claim_health_service.py
routes/admin_claim_health_routes.py
app.py

Primary endpoint:

GET /admin/gamelens/claim-health?run_id=full_2025_reg_post_claim_matrix_pilot&season=2025

Current endpoint scope:

aggregate_claim_health

Current sections:

coverage
baseline
section_metadata
core_area_matrix
category_matrix
confidence_core_area_matrix
feature_scorecard
surface_matrix
13. Good Commit Summary
Add aggregate admin claim-health API for GameLens QA

Longer summary:

Added an MVP admin claim-health endpoint that exposes aggregate claim-validation performance for a selected run_id and season. The endpoint returns coverage, baseline validation, core area, category, confidence/core-area, offensive efficiency feature, and claim-surface matrices. It also includes section metadata for future frontend chart rendering. The endpoint is intentionally aggregate-level only and does not include game-ID drilldowns.
14. Next Suggested Work

Recommended next steps:

1. Commit the MVP admin API.
2. Add a simple frontend admin page that consumes this endpoint.
3. Display top summary cards first.
4. Render Core Area Health and Feature Scorecard as the first two charts.
5. Add Category Health and Surface Health tables.
6. Keep game-level drilldown for a later admin tool.
7. Later create a snapshot table and scheduled refresh process.

The next frontend page should start simple.

Do not overbuild it.

The goal is:

Let me see the data clearly.
Let me compare each section to baseline.
Let me notice where GameLens is useful, noisy, or cautious.