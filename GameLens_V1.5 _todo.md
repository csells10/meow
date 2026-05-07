# GameLens v1.5+ Status, Frontend Handoff, and Backlog

_Last updated: May 7, 2026_

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

Example:

```text
points_per_play = sum(actual_points) / sum(total_plays)
red_zone_efficiency = sum(red_zone_tds) / sum(red_zone_attempts)
yards_per_play = sum(total_yards) / sum(total_plays)
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

# Frontend Handoff

## What frontend can use now

### Best immediate field to display

```text
matchup_lean.matchup_label
```

Example:

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
Deferred
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
```

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



### Turnover Margin Per Game Cleanup

Status:

```text
Definitely do / backend cleanup

Current issue:

/game now uses the windowed metrics source by default. The old legacy query path previously created turnover_margin_per_game, but the new windowed source currently exposes cumulative turnover_margin.

That means visible matchup comparison may currently show total window turnover margin instead of average turnover margin per game.

Why this matters:

cumulative turnover margin can exaggerate differences when windows have different game counts
per-game turnover margin is easier to understand in the UI
Team Comparison, ranking context, and matchup breakdown should agree on what “Turnover Margin” means

Preferred fix:

add turnover_margin_per_game as a real derived windowed metric
define it in analytics/metric_registry.py
calculate it in the windowed metrics builder
rebuild downstream tables:
game_team_metric_facts
team_metrics_windowed
team_metric_rankings
update visible Team Comparison to use Turnovers::turnover_margin_per_game
keep cumulative turnover_margin as supporting context if still useful

Goal:

Visible matchup comparison should use per-game turnover margin, while cumulative turnover margin should not be the default user-facing comparison metric.


## Add this under “Future / Exploratory”

```markdown
### Dynamic Matchup Advantage / Dynamic Matchup Drivers

Status:

```text
Exploratory / v2 product idea

Current state:

The visible Team Comparison / Matchup Advantage section currently uses a stable set of static metrics:

Points per Play
Points Allowed per Play
3rd Down %
Red Zone TD %
Turnover Margin

This is useful because it gives every game a consistent comparison baseline.

Exploratory v2 idea:

Add a dynamic matchup-driver layer that selects the most meaningful metrics for each specific game, instead of always showing only the same static metrics.

Important caution:

Dynamic drivers should not simply mean “pick the biggest percentile gaps.”

They should select metrics that are:

headline eligible
confidence eligible
strong signal
edge-language allowed
good data quality
not near-even
not repetitive across the same category/Core Area

Possible selection rules:

ranking_usage = edge
ranking_kind = edge
edge_language_allowed = true
confidence_eligible = true
signal_strength = strong
data_quality_status = good

Possible diversity rules:

max 2 metrics from the same Core Area
max 1–2 metrics from the same category
prefer multiple Core Areas when available
exclude near-even metrics from dynamic headline drivers
keep context-only/supporting metrics in context notes

Potential v2 structure:

Core Comparison = stable/familiar baseline
Key Matchup Drivers = dynamic game-specific explanation
Context Notes = descriptive but not decisive

Goal:

Move GameLens from:

Here are the same five metrics every game.

toward:

Here is what actually separates these two teams in this matchup.

This should begin as an explanation/display layer before it influences confidence or model scoring.


## My recommendation

Put **Turnover Margin Per Game Cleanup before frontend v1.6**, because it affects the meaning of one of your core visible metrics.

