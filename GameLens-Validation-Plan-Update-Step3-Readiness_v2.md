# GameLens Local QA Plan Update — Registry, Windowed Data, and Ranking Layer Complete

## Update Purpose

This update records the backend/data-layer work completed after the original **GameLens Local QA Plan — Windowed Source + Product Refocus Validation**.

The original validation plan framed the product goal clearly:

> GameLens should explain where each team has an edge, where the matchup is even, whether the profile is clean/split/messy, and whether confidence language is appropriate.

This update documents the work completed to support that goal and explains why the project is now ready to move into:

```text
Step 3 — Summarize by Metric, Category, and Core Area
```

---

# Completed Work Summary

## Step 1 — Treat “Even” as a Real Result ✅

### Files updated

```text
services/game_service.py
services/model_trust_service.py
```

### What changed

Team Comparison now supports:

```text
better = "neutral"
```

when away/home values are exactly equal.

Previously, tied values could accidentally become a fake home or away edge.

### Validation result

Validated locally with the repeatable QA batch:

```text
20251207_CIN@BUF
20251013_BUF@ATL
20251020_TB@DET
20251009_PHI@NYG
```

The cleanest proof case was:

```text
20251013_BUF@ATL
Turnover Margin: 1.0 vs 1.0 → better: neutral
```

### What now works

Neutral/even rows now:

- appear in `team_comparison` as `better: "neutral"`
- appear in `model_trust.matchup_advantage.neutral`
- do not add to away/home signal score
- do not become fake reasoning drivers
- soften `model_trust.edge.score` through `total_visible`

### Why this matters for Step 3

Step 3 will summarize matchup strength by metric, category, and core area.

That only works if the system can honestly say:

```text
This area is even.
```

Without neutral handling, grouped summaries would overstate category/core-area edges and create fake certainty.

---

# Step 2A — Metric Registry Metadata Upgrade ✅

## File updated

```text
analytics/metric_registry.py
```

## What changed

The registry is now a richer metadata contract, not just a display-label dictionary.

It now carries:

```text
definition
ranking_usage
signal_strength
edge_language_allowed
include_in_core_area_advantage
confidence_eligible
data_quality_status
lens_tags
```

It also exposes the full metadata set through:

```python
get_metric_meta(metric)
```

including:

```text
numerator
denominator
format
decimals
notes
```

## Key design split

The registry now separates three different ideas:

| Field | Purpose |
|---|---|
| `comparison_direction` | How values compare: `higher`, `lower`, or `context` |
| `ranking_usage` | How rankings may use the metric: `edge`, `context_only`, or `exclude` |
| `signal_strength` | How loudly the metric may speak: `strong`, `supporting`, `context`, or `exclude` |

## Guardrail rules

### Edge metrics

```text
ranking_usage = edge
```

These may support better/worse language if the direction is trustworthy.

### Context metrics

```text
ranking_usage = context_only
```

These may be ranked or displayed as style, volume, opportunity, pace, or identity.

They must not create winner/edge/confidence language.

### Excluded metrics

```text
ranking_usage = exclude
```

These remain registered for audit/formula completeness but are excluded from rankings and edge logic.

Examples:

```text
pressure_rate
sacks_plus_sacks_taken
sack_to_turnover_ratio
```

## Why this matters for Step 3

Step 3 needs to summarize groups of metrics without turning every metric into a “better/worse” claim.

For example:

```text
pass_attempts
```

can help describe passing volume or game script, but it should not say one team has the better offense.

The new registry gives Step 3 the metadata needed to summarize safely.

---

# Step 2B — Fact and Windowed Tables Rebuilt ✅

## Files updated

```text
agg/build_metric_facts.py
agg/build_windowed_metrics.py
recreate_gamelens_metric_tables.py
```

## BigQuery tables rebuilt

```text
Analytics.game_team_metric_facts_2023
Analytics.game_team_metric_facts_2024
Analytics.game_team_metric_facts_2025

Analytics.team_metrics_windowed_2023
Analytics.team_metrics_windowed_2024
Analytics.team_metrics_windowed_2025
```

## Final row counts

### Fact tables

| Table | Rows |
|---|---:|
| `Analytics.game_team_metric_facts_2023` | 33,060 |
| `Analytics.game_team_metric_facts_2024` | 33,060 |
| `Analytics.game_team_metric_facts_2025` | 36,532 |

### Windowed tables

| Table | Rows |
|---|---:|
| `Analytics.team_metrics_windowed_2023` | 130,732 |
| `Analytics.team_metrics_windowed_2024` | 130,732 |
| `Analytics.team_metrics_windowed_2025` | 136,184 |

## Important schema decision

`lens_tags` is stored as:

```text
REPEATED STRING
```

This keeps the warehouse clean and frontend-friendly.

Example:

```json
"lens_tags": ["scoring-efficiency", "efficiency", "strong-signal"]
```

## Validation completed

Validated examples:

### Edge metric

```text
points_per_play
```

Expected:

```text
comparison_direction = higher
ranking_usage = edge
signal_strength = strong
edge_language_allowed = true
include_in_core_area_advantage = true
confidence_eligible = true
data_quality_status = good
```

Result: passed.

### Context metric

```text
pass_attempts
```

Expected:

```text
comparison_direction = context
ranking_usage = context_only
signal_strength = context
edge_language_allowed = false
include_in_core_area_advantage = false
confidence_eligible = false
```

Result: passed.

### Excluded metric

```text
pressure_rate
```

Expected:

```text
ranking_usage = exclude
signal_strength = exclude
data_quality_status = exclude
edge/core/confidence flags = false
```

Result: passed.

## Why this matters for Step 3

Step 3 needs to summarize at multiple levels:

```text
metric
category
core_area
```

That requires every row in the fact/windowed tables to carry trustworthy metadata.

The rebuilt tables now give Step 3 the raw material to group metrics into product-readable summaries without handcoding every grouping in service logic.

---

# Step 2C — As-Of Ranking Tables Built ✅

## File created

```text
agg/build_metric_rankings.py
```

## BigQuery tables created

```text
Analytics.team_metric_rankings_2023
Analytics.team_metric_rankings_2024
Analytics.team_metric_rankings_2025
```

## Final row counts

| Table | Rows |
|---|---:|
| `Analytics.team_metric_rankings_2023` | 423,888 |
| `Analytics.team_metric_rankings_2024` | 431,852 |
| `Analytics.team_metric_rankings_2025` | 418,088 |

## Ranking table grain

```text
season + as_of_date + window_type + metric + team_id
```

## Important date fields

| Field | Meaning |
|---|---|
| `as_of_date` | Ranking date |
| `source_data_date` | Latest team metric snapshot used |
| `data_lag_days` | Days between `as_of_date` and `source_data_date` |

## Why as-of carry-forward was added

The first ranking builder used exact `data_date` matching.

That created partial ranking groups, such as:

```text
teams_ranked = 28
```

for some late-season dates.

The updated builder now uses carry-forward logic:

```text
For each as_of_date + window_type + metric:
    For each team:
        use latest source_data_date <= as_of_date
```

This lets the table answer:

```text
As of this date, where did every available team rank?
```

instead of:

```text
Which teams happened to play or update on this exact date?
```

## Ranking validation completed

### Higher-is-better edge ranking

Metric:

```text
points_per_play
```

Validated:

```text
highest value ranks first
ranking_kind = edge
rank_direction = higher_is_better
teams_ranked = 32
```

Example top result:

```text
LAR 0.476980 → rank 1
```

Result: passed.

### Lower-is-better edge ranking

Metric:

```text
points_allowed_per_play
```

Validated:

```text
lowest value ranks first
ranking_kind = edge
rank_direction = lower_is_better
teams_ranked = 32
```

Example top result:

```text
SEA 0.262911 → rank 1
```

Result: passed.

### Context-only ranking

Metric:

```text
pass_attempts
```

Validated:

```text
comparison_direction = context
ranking_usage = context_only
ranking_kind = context
rank_direction = high_value_context
teams_ranked = 32
edge_language_allowed = false
include_in_core_area_advantage = false
confidence_eligible = false
```

Example interpretation:

```text
Context ranking only. Higher percentile means a higher raw value relative to the league, not a better team edge.
```

Result: passed.

### Tie handling

Tie method:

```text
competition_min_rank
```

Example:

```text
Two teams with the same value receive the same league_rank.
```

Result: passed.

---

# Current Ranking Fields Available

The ranking table now provides:

```text
season
as_of_date
source_data_date
data_lag_days
window_type
metric
team_id
team_abv
value

label
definition
category
core_area
comparison_direction
higher_is_better
raw_or_derived
aggregation_method
numerator
denominator
format
decimals
notes
ranking_usage
signal_strength
edge_language_allowed
include_in_core_area_advantage
confidence_eligible
data_quality_status
lens_tags

league_rank
league_percentile
tier
tier_label
teams_ranked
ranking_kind
rank_direction
rank_interpretation
rank_tie_method
created_at
```

---

# Why This Work Is Important for Step 3

## Step 3 goal

Step 3 is:

```text
Summarize by Metric, Category, and Core Area
```

The app should answer broader user questions like:

```text
Who has the better scoring profile?
Who is stronger offensively?
Who creates more disruption?
Is this a turnover-pressure game?
Is this matchup clean, split, or messy?
```

## What Step 3 can now use

Because of the completed work, Step 3 can now use:

### Metric-level ranking

Example:

```text
BUF ranks strongly in points_per_play.
```

### Category-level grouping

Example:

```text
BUF’s Scoring Production profile is strong, led by points per play and touchdown rate.
```

### Core-area grouping

Example:

```text
BUF shows a stronger Scoring Efficiency profile overall.
```

### Context-only descriptions

Example:

```text
ARI ranks very high in pass attempts, which points to high passing volume, not necessarily better offense.
```

### Staleness awareness

Example:

```text
SEA’s ranking uses a source snapshot from one day earlier.
```

### Safer confidence language

Step 3 can avoid overconfident summaries by checking:

```text
ranking_usage
signal_strength
edge_language_allowed
include_in_core_area_advantage
confidence_eligible
data_quality_status
data_lag_days
```

---

# Step 3 Design Guidance

## Do

Use rankings to explain matchup shape:

```text
BUF ranks highly in Scoring Efficiency.
ATL’s defensive profile keeps the matchup from becoming a clean BUF read.
PHI shows strong disruption indicators, but NYG has better offensive-output context.
DET’s recent scoring form is stronger than its season-long profile.
```

## Do not

Use rankings as automatic winner logic:

```text
BUF ranks higher, so BUF should win.
PHI leads more rankings, so confidence is High.
DET has better scoring rank, so the game is solved.
```

## Recommended Step 3 posture

Step 3 should produce summaries like:

```text
advantage
slight advantage
near even
mixed
context only
insufficient / stale
```

Rather than:

```text
winner
lock
guaranteed edge
must-pick
```

---

# Proposed Step 3 Validation Checks

When Step 3 is implemented, validate:

## Metric summaries

- metric rank is pulled from `team_metric_rankings_{season}`
- `comparison_direction` is respected
- `ranking_kind = context` is not treated as better/worse
- `data_lag_days` is visible or accounted for

## Category summaries

- grouped summaries use metrics from the same `category`
- context-only metrics can describe style but do not create category edges
- excluded metrics do not appear
- supporting metrics do not overpower strong metrics

## Core Area summaries

- grouped summaries use metrics from the same `core_area`
- neutral/even areas are preserved
- conflicting category signals produce mixed/cautious language
- high confidence is not created from rankings alone

## Repeatable QA games

Continue validating against:

```text
20251207_CIN@BUF
20251013_BUF@ATL
20251020_TB@DET
20251009_PHI@NYG
```

Special attention:

| Game ID | Step 3 concern |
|---|---|
| `20251207_CIN@BUF` | Should preserve clean BUF read |
| `20251013_BUF@ATL` | Should avoid overstating BUF despite scoring edge |
| `20251020_TB@DET` | Should explain DET scoring/pressure lean without hiding TB defense |
| `20251009_PHI@NYG` | Should flag split/misleading profile and avoid overconfidence |

---

# Updated Step 3 Product Sentence

GameLens can now use validated as-of rankings to explain which metrics, categories, and core areas shape a matchup.

The next job is not to make the app louder.

The next job is to make the app better at saying:

```text
This is where the edge is real.
This is where the game is even.
This is where the profile is split.
This is where the data is descriptive, not decisive.
```

