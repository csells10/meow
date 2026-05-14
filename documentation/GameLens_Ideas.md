# GameLens Ideas — Consolidated Product, Data, and Implementation Backlog

_Last consolidated: May 13, 2026_

This file replaces separate GameLens idea notes so future ideas can live in one place.

Use this document as the single running idea backlog for larger GameLens concepts that are not immediately part of the main v1.5/v1.6 execution checklist.

Important rule:

```text
Do not summarize away details.
Preserve the original idea, then add implementation notes, possible files, BigQuery shapes, QA queries, and open questions.
```

---

# How to Use This File Going Forward

## New Idea Inbox

Paste new raw ideas here first before promoting them into the structured idea catalog.

### New Idea Template

````markdown
## Idea ### — Title

Status:

Priority:

Product frame:

Why this matters:

What problem it solves:

Possible user-facing shape:

Backend/data needs:

Possible files touched:

Possible BigQuery tables/views:

Possible API fields/endpoints:

Frontend notes:

QA plan:

Risks / guardrails:

Open questions:

Decision log:
````

## Status Labels

```text
Idea / not started
Planning
Backend exploration
Internal QA only
Frontend concept
Ready for implementation
Blocked
Backburner
Complete
```

## Product Guardrails for All Ideas

GameLens should remain:

```text
A matchup intelligence and confidence-calibration platform.
```

GameLens should not become:

```text
A forced pick machine.
A gambling/profit promise board.
A black-box prediction page.
A UI that invents narratives without structured backing.
```

The winner lean can remain useful, but the larger value is explaining:

```text
where the game leans
where the matchup is close
which signals supported the read
which signals challenged the read
whether confidence language is appropriate
whether the final result validated the pregame shape
```

---

# Current Shared Technical Foundation

These ideas build on the current GameLens backend foundation.

## Existing Python / Service Files Likely Relevant

```text
analytics/metric_registry.py
agg/build_metric_facts.py
agg/build_windowed_metrics.py
agg/build_metric_rankings.py
queries/game_queries.py
services/game_service.py
services/core_area_analysis.py
services/model_trust_service.py
routes/game_routes.py
app.py
```

## Existing BigQuery Assets Likely Relevant

```text
nfl-stream-406420.League.schedule
nfl-stream-406420.Scores.scores
nfl-stream-406420.Analytics.game_metrics_flat
nfl-stream-406420.Analytics.game_team_metric_facts_{season}
nfl-stream-406420.Analytics.team_metrics_windowed_{season}
nfl-stream-406420.Analytics.team_metric_rankings_{season}
nfl-stream-406420.Analytics.game_model_outcomes
nfl-stream-406420.Analytics.game_model_trust_details
```

## Existing API Concepts Likely Relevant

```text
/game/<game_id>
header
final_score
game_profile
team_comparison
core_area_comparison
matchup_lean
model_outcome
model_trust
ranking_context
matchup_breakdown
```

## Current Ownership Model

```text
Matchup Lean = pregame read + pregame confidence
Game Profile = football environment signals
Team Comparison / Core Areas = supporting matchup evidence
Model Trust & Outcome = postgame validation and diagnostic explanation
League Discovery = future league-wide pattern exploration
Postgame Signal Validation = future internal learning ledger
```

---

# Idea Catalog Index

| Idea ID | Title | Status | First Product Shape | Primary Backend Need |
|---|---|---|---|---|
| IDEA-001 | League Discovery Tool — Lens Tags Dashboard Concept | Idea / not started | Movers & Shakers / Heatmap | Lens-tag weekly profile view/table |
| IDEA-002 | Postgame Signal Validation Feedback Loop | Idea / high-value QA concept | Internal validation script / table | Game/signal validation table |

---

# IDEA-001 — League Discovery Tool / Lens Tags Dashboard Concept

## Idea Status

```text
Status: Idea / not started
Suggested timing: After mission-critical data plumbing and internal QA are stable
Recommended first shape: Backend view + QA query before frontend
```

## Product Purpose

League Discovery would help users understand changing team identities and football themes across the league.

It should answer:

```text
What is changing across the league?
Which football themes are emerging by team, week, or window?
Where are teams rising, falling, or shifting identity?
```

## Why This Belongs in the Ideas File

This is a larger product mode, not a quick matchup-page polish task.

It should stay separate from the main matchup page until the data proves useful.

## Possible Files to Add or Modify

### Backend / Python

```text
analytics/lens_tag_registry.py                  # optional allowlist / display metadata for product-facing lens tags
agg/build_team_lens_weekly_profile.py           # optional future builder if view is not enough
queries/league_discovery_queries.py             # query helpers for lens discovery views
services/league_discovery_service.py            # service layer for future endpoint
routes/league_discovery_routes.py               # optional future API route
analytics/metric_registry.py                    # source of lens_tags and metadata guardrails
agg/build_metric_rankings.py                    # source table already carries lens_tags into rankings
```

### Frontend / Future UI

```text
src/pages/LeagueDiscovery.tsx                   # possible future page
src/components/LeagueDiscoveryHeatmap.tsx       # possible heatmap component
src/components/MoversAndShakersTable.tsx        # possible mover table
src/components/TeamLensTrendCard.tsx            # possible one-team trend component
```

Do not build frontend first. Validate the data shape in BigQuery first.

## Proposed BigQuery View / Table

Start as a view:

```text
Analytics.v_team_lens_weekly_profile
```

If stable, promote to a table:

```text
Analytics.team_lens_weekly_profile
```

Suggested grain:

```text
season
as_of_date
week
team_id
team_abv
window_type
lens_tag
```

Suggested fields:

```text
lens_score
lens_rank
lens_percentile
previous_lens_score
previous_lens_rank
previous_lens_percentile
week_over_week_change
movement_label
source_metric_count
headline_metric_count
supporting_metric_count
context_metric_count
top_metrics
created_at
```

## Possible Product-Facing Lens Tag Registry

Possible file:

```text
analytics/lens_tag_registry.py
```

Possible config shape:

```python
PRODUCT_LENS_TAGS = {
    "scoring-efficiency": {
        "display_label": "Scoring Efficiency",
        "group": "Scoring",
        "description": "How efficiently a team turns plays and drives into points.",
        "is_product_facing": True,
    },
    "turnovers": {
        "display_label": "Turnovers",
        "group": "Disruption",
        "description": "Turnover creation, avoidance, and turnover-margin profile.",
        "is_product_facing": True,
    },
    "pressure": {
        "display_label": "Pressure",
        "group": "Disruption",
        "description": "Sack and pressure-related defensive profile.",
        "is_product_facing": True,
    },
}

INTERNAL_ONLY_LENS_TAGS = {
    "strong-signal",
    "volume-sensitive",
    "supporting",
    "data-quality-watch",
    "formula-ingredient",
    "per-game",
    "cumulative",
    "small-sample",
    "context_only",
}
```

## BigQuery Starter View Skeleton

Use this as a concept, not final production SQL.

```sql
CREATE OR REPLACE VIEW `nfl-stream-406420.Analytics.v_team_lens_weekly_profile` AS
WITH product_tags AS (
  SELECT 'scoring-efficiency' AS lens_tag, 'Scoring Efficiency' AS display_label UNION ALL
  SELECT 'turnovers', 'Turnovers' UNION ALL
  SELECT 'pressure', 'Pressure' UNION ALL
  SELECT 'red-zone', 'Red Zone' UNION ALL
  SELECT 'third-down', 'Third Down' UNION ALL
  SELECT 'passing-efficiency', 'Passing Efficiency' UNION ALL
  SELECT 'rushing-efficiency', 'Rushing Efficiency' UNION ALL
  SELECT 'explosiveness', 'Explosiveness' UNION ALL
  SELECT 'pace', 'Pace' UNION ALL
  SELECT 'game-script', 'Game Script' UNION ALL
  SELECT 'offensive-style', 'Offensive Style' UNION ALL
  SELECT 'defense', 'Defense' UNION ALL
  SELECT 'control-profile', 'Control Profile' UNION ALL
  SELECT 'scoring-suppression', 'Scoring Suppression' UNION ALL
  SELECT 'drive-efficiency', 'Drive Efficiency' UNION ALL
  SELECT 'touchdown-efficiency', 'Touchdown Efficiency'
),
exploded AS (
  SELECT
    CAST(r.season AS STRING) AS season,
    r.as_of_date,
    r.window_type,
    r.team_id,
    r.team_abv,
    r.metric,
    r.label,
    r.category,
    r.core_area,
    r.league_rank,
    r.league_percentile,
    r.tier,
    r.tier_label,
    r.ranking_usage,
    r.ranking_kind,
    r.signal_strength,
    r.edge_language_allowed,
    r.confidence_eligible,
    r.data_quality_status,
    lens_tag,
    pt.display_label AS lens_display_label
  FROM `nfl-stream-406420.Analytics.team_metric_rankings_2025` r,
  UNNEST(r.lens_tags) AS lens_tag
  INNER JOIN product_tags pt
    ON lens_tag = pt.lens_tag
  WHERE r.data_quality_status != 'exclude'
    AND r.ranking_usage != 'exclude'
),
scored AS (
  SELECT
    *,
    CASE
      WHEN ranking_usage = 'edge'
       AND ranking_kind = 'edge'
       AND edge_language_allowed IS TRUE
       AND confidence_eligible IS TRUE
       AND signal_strength = 'strong'
       AND data_quality_status = 'good'
       AND league_percentile >= 90 THEN 3.0
      WHEN ranking_usage = 'edge'
       AND ranking_kind = 'edge'
       AND edge_language_allowed IS TRUE
       AND signal_strength = 'strong'
       AND data_quality_status = 'good'
       AND league_percentile >= 75 THEN 2.0
      WHEN ranking_usage = 'edge'
       AND ranking_kind = 'edge'
       AND edge_language_allowed IS TRUE
       AND data_quality_status = 'good'
       AND league_percentile >= 60 THEN 1.0
      WHEN ranking_usage = 'edge'
       AND signal_strength = 'supporting' THEN 0.5
      WHEN ranking_usage = 'context_only' THEN 0.25
      ELSE 0.0
    END AS metric_lens_score
  FROM exploded
),
lens_rollup AS (
  SELECT
    season,
    as_of_date,
    window_type,
    team_id,
    team_abv,
    lens_tag,
    ANY_VALUE(lens_display_label) AS lens_display_label,
    SUM(metric_lens_score) AS lens_score,
    COUNT(*) AS source_metric_count,
    COUNTIF(metric_lens_score >= 1.0) AS headline_metric_count,
    COUNTIF(signal_strength = 'supporting') AS supporting_metric_count,
    COUNTIF(ranking_usage = 'context_only') AS context_metric_count,
    ARRAY_AGG(
      STRUCT(metric, label, category, core_area, league_percentile, metric_lens_score)
      ORDER BY metric_lens_score DESC, league_percentile DESC
      LIMIT 5
    ) AS top_metrics
  FROM scored
  GROUP BY season, as_of_date, window_type, team_id, team_abv, lens_tag
),
ranked AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY season, as_of_date, window_type, lens_tag
      ORDER BY lens_score DESC
    ) AS lens_rank,
    PERCENT_RANK() OVER (
      PARTITION BY season, as_of_date, window_type, lens_tag
      ORDER BY lens_score ASC
    ) AS lens_percentile_raw
  FROM lens_rollup
)
SELECT
  season,
  as_of_date,
  window_type,
  team_id,
  team_abv,
  lens_tag,
  lens_display_label,
  lens_score,
  lens_rank,
  ROUND(lens_percentile_raw * 100, 1) AS lens_percentile,
  source_metric_count,
  headline_metric_count,
  supporting_metric_count,
  context_metric_count,
  top_metrics,
  CURRENT_TIMESTAMP() AS created_at
FROM ranked;
```

## QA Queries for League Discovery

### Lens Tag Coverage

```sql
SELECT
  lens_tag,
  COUNT(*) AS ranking_rows,
  COUNT(DISTINCT metric) AS metric_count,
  COUNT(DISTINCT team_id) AS team_count
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2025`,
UNNEST(lens_tags) AS lens_tag
GROUP BY lens_tag
ORDER BY ranking_rows DESC;
```

### Product-Facing Tag Coverage Only

```sql
WITH product_tags AS (
  SELECT 'scoring-efficiency' AS lens_tag UNION ALL
  SELECT 'turnovers' UNION ALL
  SELECT 'pressure' UNION ALL
  SELECT 'red-zone' UNION ALL
  SELECT 'third-down' UNION ALL
  SELECT 'passing-efficiency' UNION ALL
  SELECT 'rushing-efficiency' UNION ALL
  SELECT 'explosiveness' UNION ALL
  SELECT 'pace' UNION ALL
  SELECT 'game-script' UNION ALL
  SELECT 'offensive-style' UNION ALL
  SELECT 'defense' UNION ALL
  SELECT 'control-profile' UNION ALL
  SELECT 'scoring-suppression' UNION ALL
  SELECT 'drive-efficiency' UNION ALL
  SELECT 'touchdown-efficiency'
)
SELECT
  lens_tag,
  COUNT(*) AS rows,
  COUNT(DISTINCT metric) AS metrics,
  COUNT(DISTINCT team_id) AS teams
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2025`,
UNNEST(lens_tags) AS lens_tag
WHERE lens_tag IN (SELECT lens_tag FROM product_tags)
GROUP BY lens_tag
ORDER BY rows DESC;
```

### Movers and Shakers Prototype

```sql
WITH current_window AS (
  SELECT *
  FROM `nfl-stream-406420.Analytics.v_team_lens_weekly_profile`
  WHERE season = '2025'
    AND window_type = 'last_3_games'
),
season_window AS (
  SELECT *
  FROM `nfl-stream-406420.Analytics.v_team_lens_weekly_profile`
  WHERE season = '2025'
    AND window_type = 'regular_season_to_date'
),
joined AS (
  SELECT
    c.season,
    c.as_of_date,
    c.team_abv,
    c.lens_tag,
    c.lens_display_label,
    c.lens_percentile AS recent_percentile,
    s.lens_percentile AS season_percentile,
    ROUND(c.lens_percentile - s.lens_percentile, 1) AS percentile_change,
    c.top_metrics
  FROM current_window c
  INNER JOIN season_window s
    USING (season, as_of_date, team_id, lens_tag)
)
SELECT
  *,
  CASE
    WHEN percentile_change >= 15 THEN 'rising'
    WHEN percentile_change <= -15 THEN 'falling'
    WHEN ABS(percentile_change) < 5 THEN 'stable'
    ELSE 'moving'
  END AS movement_label
FROM joined
ORDER BY ABS(percentile_change) DESC
LIMIT 50;
```

### Heatmap Prototype

```sql
SELECT
  team_abv,
  MAX(IF(lens_tag = 'scoring-efficiency', lens_percentile, NULL)) AS scoring_efficiency,
  MAX(IF(lens_tag = 'turnovers', lens_percentile, NULL)) AS turnovers,
  MAX(IF(lens_tag = 'pressure', lens_percentile, NULL)) AS pressure,
  MAX(IF(lens_tag = 'red-zone', lens_percentile, NULL)) AS red_zone,
  MAX(IF(lens_tag = 'rushing-efficiency', lens_percentile, NULL)) AS rushing_efficiency,
  MAX(IF(lens_tag = 'pace', lens_percentile, NULL)) AS pace
FROM `nfl-stream-406420.Analytics.v_team_lens_weekly_profile`
WHERE season = '2025'
  AND window_type = 'last_3_games'
  AND as_of_date = DATE '2025-10-20'
GROUP BY team_abv
ORDER BY scoring_efficiency DESC;
```

## Python Builder Skeleton If a Table Becomes Better Than a View

```python
"""
Build Analytics.team_lens_weekly_profile from Analytics.team_metric_rankings_{season}.

This should only be used after the view-based prototype proves useful.
"""

from google.cloud import bigquery
from utils.logging_setup import setup_logging, log_event

PROJECT = "nfl-stream-406420"
SOURCE_TEMPLATE = "Analytics.team_metric_rankings_{season}"
TARGET_TEMPLATE = "Analytics.team_lens_weekly_profile_{season}"

PRODUCT_LENS_TAGS = {
    "scoring-efficiency",
    "turnovers",
    "pressure",
    "red-zone",
    "third-down",
    "passing-efficiency",
    "rushing-efficiency",
    "explosiveness",
    "pace",
    "game-script",
    "offensive-style",
    "defense",
    "control-profile",
    "scoring-suppression",
    "drive-efficiency",
    "touchdown-efficiency",
}


def build_team_lens_weekly_profile(season: str, dry_run: bool = True):
    setup_logging()
    client = bigquery.Client(project=PROJECT)

    # v1 should start with SQL/view logic. If promoted to a builder,
    # keep the scoring rules explicit and logged.
    query = """
    -- Use the v_team_lens_weekly_profile SQL as the source of truth first.
    SELECT *
    FROM `nfl-stream-406420.Analytics.v_team_lens_weekly_profile`
    WHERE season = @season
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("season", "STRING", season)]
    )

    rows = list(client.query(query, job_config=job_config).result())
    log_event("info", "team_lens_profile_rows_loaded", season=season, rows=len(rows))

    if dry_run:
        return rows[:10]

    # Write path intentionally omitted until table design is finalized.
    raise NotImplementedError("Write path should be added only after view QA.")
```

## Implementation Order for IDEA-001

```text
1. Confirm product-facing lens tag allowlist.
2. Run lens tag coverage QA against 2025 rankings.
3. Build v_team_lens_weekly_profile as a BigQuery view.
4. QA Movers & Shakers and Heatmap queries manually.
5. Decide whether to keep as view or promote to table/builder.
6. Only then design API endpoint.
7. Only then design frontend.
```

## API Concept for Later

Possible internal endpoint:

```text
GET /league-discovery?season=2025&as_of_date=2025-10-20&window_type=last_3_games
```

Possible response shape:

```json
{
  "available": true,
  "season": "2025",
  "as_of_date": "2025-10-20",
  "window_type": "last_3_games",
  "movers": [],
  "heatmap": [],
  "team_trends": [],
  "freshness": {
    "source": "Analytics.v_team_lens_weekly_profile",
    "created_at": "2026-05-13T00:00:00Z"
  }
}
```

---

## Full Preserved Source Notes for IDEA-001

The following section preserves the original detail from `GameLens_Ideas.md`.

# GameLens League Discovery Tool — Lens Tags Dashboard Concept

_Last updated: May 12, 2026_

## Product Idea

GameLens can eventually support a separate **League Discovery** mode powered by `lens_tags`.

The matchup page answers:

```text
What does this specific game look like?
```

The League Discovery tool would answer:

```text
What is changing across the league?
Which football themes are emerging by team, week, or window?
Where are teams rising, falling, or shifting identity?
```

This should stay aligned with the larger GameLens product frame:

```text
A matchup intelligence and confidence-calibration platform.
```

It should **not** become a profit tool, betting edge board, or forced-pick machine.

People may use this information for fantasy football, game previews, team research, betting research, writing, or curiosity. That is fine. The product should simply provide clear football intelligence without promising outcomes or encouraging profit-seeking behavior.

---

## Why Lens Tags Matter

`lens_tags` already describe the football story connected to each metric.

Examples:

```text
scoring-efficiency
turnovers
pressure
red-zone
third-down
passing-efficiency
rushing-efficiency
explosiveness
pace
game-script
offensive-style
defense
control-profile
```

Those tags can become a bridge between:

```text
raw metric rows
```

and:

```text
human-readable team identity / league trend summaries
```

Instead of only showing one matchup at a time, GameLens could show how themes move across teams and weeks.

---

## Core Use Cases

### 1. Movers & Shakers

Show which teams are changing most by football theme.

Examples:

```text
Biggest risers in Scoring Efficiency
Biggest fallers in Defensive Control
Teams suddenly improving in Turnover Margin / Game
Teams becoming more pass-heavy
Teams gaining rushing efficiency
Teams losing red-zone finishing strength
```

This is likely the strongest first product shape because it answers:

```text
What changed this week?
```

---

### 2. League Heatmap

A heatmap could show:

```text
Rows = teams
Columns = product-facing lens tags
Color/value = rank, percentile, strength, or movement
```

Example columns:

```text
Scoring Efficiency
Turnovers
Pressure
Red Zone
Third Down
Passing Efficiency
Rushing Efficiency
Pace
Game Script
Defensive Control
```

Possible values:

```text
league_percentile
tier
lens_score
week_over_week_change
last_3 vs season-to-date difference
```

This could become a weekly league intelligence board.

---

### 3. Team Trend Lines

For one team, show how its profile changes over time.

Examples:

```text
DET scoring-efficiency trend by week
LAC turnover profile over time
DEN defensive-control rise/fall
BUF red-zone finishing stability
NYG offensive-output recent improvement
```

This helps users see team identity changing instead of only seeing a one-game snapshot.

---

### 4. Weekly Theme Board

Show the league-wide themes that are most active in a given week.

Examples:

```text
Week 7 themes:
- scoring-efficiency spikes
- rushing-efficiency risers
- turnover volatility
- pace/style shifts
- red-zone finishers
```

This could support preview writing, fantasy research, or general league curiosity without becoming a prediction tool.

---

### 5. Future NLP Summaries

NLP should summarize structured signals, not invent them.

Good use:

```text
Structured lens data says DET is rising in scoring-efficiency, red-zone finish, and rushing-efficiency.
NLP turns that into a plain-English summary.
```

Example:

```text
DET is showing a rising scoring profile, led by red-zone efficiency and rushing efficiency.
```

Avoid:

```text
The model invents a narrative without structured backing.
```

The data should speak first. NLP should translate.

---

## Product-Facing Tags vs Internal Metadata Tags

The raw `lens_tags` include both football themes and backend interpretation tags.

That is useful internally, but raw tags should not all be shown to users.

### Product-Facing Lens Tags

These can become dashboard themes:

```text
scoring-efficiency
turnovers
pressure
red-zone
third-down
passing-efficiency
rushing-efficiency
explosiveness
pace
game-script
offensive-style
defense
control-profile
scoring-suppression
drive-efficiency
touchdown-efficiency
```

### Internal / QA Tags

These should generally stay backend-only:

```text
strong-signal
volume-sensitive
supporting
data-quality-watch
formula-ingredient
per-game
cumulative
small-sample
context_only
```

These tags answer:

```text
How should the system treat this metric?
```

They do not directly answer:

```text
What football theme is emerging?
```

---

## Possible Backend Table or View

A useful first target could be:

```text
Analytics.team_lens_weekly_profile
```

or a view:

```text
Analytics.v_team_lens_weekly_profile
```

Suggested grain:

```text
season
as_of_date
week
team_id
team_abv
window_type
lens_tag
```

Suggested fields:

```text
lens_score
lens_rank
lens_percentile
previous_lens_score
previous_lens_rank
previous_lens_percentile
week_over_week_change
movement_label
source_metric_count
headline_metric_count
supporting_metric_count
context_metric_count
top_metrics
created_at
```

Movement labels:

```text
rising
falling
stable
new signal
insufficient history
```

---

## Scoring Concept

Do not simply count tags.

A better first version would weight tag appearances based on metric usefulness.

Possible weighting:

| Source / Signal Type | Suggested Weight |
|---|---:|
| headline metric + clear advantage | 3.0 |
| headline metric + advantage | 2.0 |
| headline metric + slight advantage | 1.0 |
| supporting context | 0.5 |
| context-only note | 0.25 |
| near-even metric | 0.0 |

Possible modifier:

```text
Boost slightly by percentile gap, but do not let one giant gap dominate the whole lens.
```

Important:

```text
The score is for discovery and explanation, not winner prediction.
```

---

## Example Dashboard Sections

### League Discovery Home

```text
This Week's Movers
Top Rising Themes
Top Falling Themes
League Heatmap
Team Identity Shifts
```

### Movers & Shakers Table

| Team | Lens | Movement | Current Percentile | Previous Percentile | Summary |
|---|---|---:|---:|---:|---|
| DET | Scoring Efficiency | Rising | 94 | 78 | DET's scoring profile is strengthening. |
| LAC | Turnovers | Rising | 88 | 61 | LAC's turnover profile has improved recently. |
| DEN | Defensive Control | Stable | 83 | 81 | DEN remains strong defensively. |

### Heatmap Concept

| Team | Scoring | Turnovers | Pressure | Red Zone | Rushing | Pace |
|---|---:|---:|---:|---:|---:|---:|
| DET | High | Strong | Average | Elite | Strong | Low |
| LAC | Average | Strong | High | Average | Weak | High |
| DEN | Average | Weak | Average | Strong | Strong | Average |

---

## Matchup Page Relationship

This should not replace the matchup page.

The relationship should be:

```text
Matchup Page = game-specific explanation
League Discovery = league-wide pattern exploration
```

A future matchup page may reference lens discovery lightly:

```text
DEN has been rising in Defensive Control over the last 3 games.
LAC's turnover profile has improved recently, but its scoring profile remains mixed.
```

But the first version should stay separate.

---

## Fantasy Football Angle

This could be useful to fantasy players because it surfaces changing environments.

Examples:

```text
Teams becoming more pass-heavy
Teams showing rising pace
Teams improving in red-zone opportunity
Teams allowing more defensive exposure
Teams with rising rushing control
Teams with scoring-efficiency spikes
```

Important boundary:

```text
GameLens should show the football environment.
It should not say a user should make a specific fantasy, betting, or profit-seeking move.
```

---

## MVP Path

### Step 1 — Clean Product-Facing Tags

Create an allowlist of product-facing tags.

Possible file:

```text
analytics/lens_tag_registry.py
```

or add a simple mapping in a future builder.

Fields:

```text
lens_tag
display_label
category
is_product_facing
is_internal
description
```

---

### Step 2 — Build a Lens Weekly Profile View

Start as a BigQuery view before creating a permanent table.

Goal:

```text
Generate team/lens/week rows from ranking data and lens_tags.
```

This is safer than building UI first.

---

### Step 3 — Build Movers & Shakers Query

Start with:

```text
last_3_games vs regular_season_to_date
week-over-week movement
rank or percentile movement
```

---

### Step 4 — Build Heatmap Query

Rows:

```text
team_abv
```

Columns:

```text
product-facing lens tags
```

Values:

```text
lens_percentile or movement_label
```

---

### Step 5 — Add NLP Summaries Later

Only after the structured scores are stable.

NLP should summarize:

```text
top_lenses
movement
supporting metrics
cautions
```

It should not invent the analysis.

---

## Do Not Do Yet

Do not:

- expose raw `lens_tags` directly in a public UI
- show backend metadata tags as user-facing themes
- turn lens scores into winner prediction
- present this as betting edge or profit guidance
- build Lovable UI before the backend concept is validated
- add too many labels before proving which ones help
- replace existing matchup intelligence with league discovery

---

## Suggested Priority

This is a good exploratory idea, but it should come after mission-critical data plumbing.

Recommended order:

```text
1. Productionize the new builder chain safely.
2. Rebuild and validate older seasons with the current registry.
3. Add internal QA summaries.
4. Explore lens-tag league discovery views.
5. Build heatmap / movers queries.
6. Add frontend only after the data proves useful.
```

---

## Product North Star for This Feature

```text
League Discovery helps users understand changing NFL team identities and football themes over time.
```

It should make users better at reading the league.

It should not tell users what to pick.
It should not promise profit.
It should not force certainty.


---

# IDEA-002 — Postgame Signal Validation Feedback Loop

## Idea Status

```text
Status: Idea / high-value QA concept
Suggested timing: After current matchup API and internal QA are stable
Recommended first shape: Internal script or BigQuery QA, not public UI
```

## Product Purpose

Postgame Signal Validation would evaluate whether GameLens identified the important matchup shape, not only whether it picked the winner.

It should answer:

```text
Did the pregame signals actually show up?
Did the final game validate or contradict the pregame read?
Was a No Pick useful restraint?
Was confidence calibrated responsibly?
```

## Why This Belongs in the Ideas File

This is a major model-trust and QA layer.

It should not be rushed into public UI because a raw accuracy board could push GameLens toward the wrong product goal.

## Possible Files to Add or Modify

### Backend / Python

```text
agg/build_game_signal_validation.py             # possible validation builder
queries/postgame_validation_queries.py          # query helper for validation rows
services/postgame_validation_service.py         # validation logic and summaries
services/game_service.py                        # optional future /game postgame_validation injection
services/model_trust_service.py                 # possible future wording alignment, not first step
services/core_area_analysis.py                  # reusable core-area comparison logic
analytics/metric_registry.py                    # validation metric metadata / source mapping
routes/admin_routes.py                          # optional internal/admin endpoint
app.py                                          # only later if scheduled automation is needed
```

### Existing Tables Used as Inputs

```text
League.schedule
Scores.scores
Analytics.game_team_metric_facts_{season}
Analytics.team_metrics_windowed_{season}
Analytics.team_metric_rankings_{season}
Analytics.game_model_outcomes
Analytics.game_model_trust_details
```

### Possible New Tables / Views

```text
Analytics.game_signal_validation
Analytics.v_signal_validation_summary
Analytics.v_core_area_validation_summary
Analytics.v_no_pick_quality_summary
Analytics.v_confidence_calibration_summary
```

## Proposed BigQuery Table DDL

```sql
CREATE TABLE IF NOT EXISTS `nfl-stream-406420.Analytics.game_signal_validation` (
  season STRING,
  season_phase STRING,
  game_id STRING,
  game_date DATE,
  game_week STRING,
  away_team_id STRING,
  away_team_abv STRING,
  home_team_id STRING,
  home_team_abv STRING,

  validation_type STRING,
  category STRING,
  core_area STRING,
  metric STRING,
  label STRING,

  pregame_level STRING,
  pregame_tilt_side STRING,
  pregame_tilt_team STRING,
  pregame_value_away FLOAT64,
  pregame_value_home FLOAT64,
  pregame_better STRING,
  pregame_summary STRING,

  postgame_value_away FLOAT64,
  postgame_value_home FLOAT64,
  postgame_better STRING,
  postgame_confirming_team STRING,

  validation_result STRING,
  validation_strength STRING,
  validation_score FLOAT64,
  validation_summary STRING,

  confidence STRING,
  matchup_label STRING,
  profile_type STRING,
  model_result STRING,
  actual_winner STRING,

  created_at TIMESTAMP
)
PARTITION BY game_date
CLUSTER BY season, validation_type, validation_result;
```

## Possible Validation Result Labels

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

## Validation Mapping Concept

Create a mapping layer so validation logic is explicit.

Possible file:

```text
analytics/validation_registry.py
```

Possible Python shape:

```python
VALIDATION_REGISTRY = {
    "Pressure": {
        "validation_type": "game_profile_signal",
        "postgame_metrics": ["sacks", "sacks_taken", "sack_yards_lost"],
        "preferred_metric": "sacks",
        "caution": "pressure_rate source should be reviewed before over-trusting pressure validation",
    },
    "Turnover Risk": {
        "validation_type": "game_profile_signal",
        "postgame_metrics": [
            "turnover_margin",
            "interceptions_thrown",
            "fumbles_lost",
            "defensive_interceptions",
            "fumbles_recovered",
        ],
        "preferred_metric": "turnover_margin",
    },
    "Scoring Efficiency": {
        "validation_type": "game_profile_signal",
        "postgame_metrics": [
            "points_per_play",
            "red_zone_efficiency",
            "td_rate",
            "actual_points",
        ],
        "preferred_metric": "points_per_play",
    },
}
```

## Python Builder Skeleton

```python
"""
Build postgame signal validation rows.

First version should be internal QA only.
It should compare pregame /game claims to final game facts.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Optional

from google.cloud import bigquery

from queries.game_queries import get_game_header, get_final_score
from services.game_service import get_game_details
from utils.logging_setup import setup_logging, log_event

PROJECT = "nfl-stream-406420"
TARGET_TABLE = "nfl-stream-406420.Analytics.game_signal_validation"

FINAL_STATUSES = {"Final", "Final/OT"}


def compare_side_values(away_value, home_value, higher_is_better=True):
    if away_value is None or home_value is None:
        return "not_scorable"
    if away_value == home_value:
        return "neutral"
    if higher_is_better:
        return "away" if away_value > home_value else "home"
    return "away" if away_value < home_value else "home"


def validate_signal_direction(pregame_side: str, postgame_side: str) -> str:
    if pregame_side in {None, "neutral"} or postgame_side in {None, "neutral"}:
        return "mixed"
    if postgame_side == "not_scorable":
        return "not_scorable"
    if pregame_side == postgame_side:
        return "confirmed"
    return "contradicted"


def build_game_signal_validation_rows(game_id: str) -> List[dict]:
    details = get_game_details(game_id)
    header = details.get("header") or {}

    if header.get("game_status") not in FINAL_STATUSES:
        return []

    rows = []
    created_at = datetime.now(timezone.utc)

    # First MVP: validate game_profile rows only.
    for signal in details.get("game_profile") or []:
        category = signal.get("category")
        pregame_tilt_side = signal.get("tilt_team")

        # TODO: fetch final-game postgame metric comparison by category.
        # This should eventually use registry-derived final-game facts.
        postgame_better = "not_scorable"

        validation_result = validate_signal_direction(
            pregame_side=pregame_tilt_side,
            postgame_side=postgame_better,
        )

        rows.append({
            "season": str(header.get("season"))[:4],
            "season_phase": None,
            "game_id": game_id,
            "game_date": header.get("game_date"),
            "game_week": header.get("game_week"),
            "away_team_id": header.get("away_team", {}).get("id"),
            "away_team_abv": header.get("away_team", {}).get("abbreviation"),
            "home_team_id": header.get("home_team", {}).get("id"),
            "home_team_abv": header.get("home_team", {}).get("abbreviation"),
            "validation_type": "game_profile_signal",
            "category": category,
            "core_area": None,
            "metric": None,
            "label": category,
            "pregame_level": signal.get("level"),
            "pregame_tilt_side": pregame_tilt_side,
            "pregame_tilt_team": None,
            "pregame_value_away": None,
            "pregame_value_home": None,
            "pregame_better": pregame_tilt_side,
            "pregame_summary": signal.get("tilt_text") or signal.get("tilt"),
            "postgame_value_away": None,
            "postgame_value_home": None,
            "postgame_better": postgame_better,
            "postgame_confirming_team": None,
            "validation_result": validation_result,
            "validation_strength": None,
            "validation_score": None,
            "validation_summary": None,
            "confidence": (details.get("matchup_lean") or {}).get("confidence"),
            "matchup_label": (details.get("matchup_lean") or {}).get("matchup_label"),
            "profile_type": (details.get("matchup_lean") or {}).get("profile_type"),
            "model_result": (details.get("model_outcome") or {}).get("result"),
            "actual_winner": (details.get("model_outcome") or {}).get("actual_winner"),
            "created_at": created_at,
        })

    return rows


def run_validation_for_games(game_ids: List[str], dry_run: bool = True):
    setup_logging()
    client = bigquery.Client(project=PROJECT)
    all_rows = []

    for game_id in game_ids:
        rows = build_game_signal_validation_rows(game_id)
        all_rows.extend(rows)
        log_event("info", "game_signal_validation_built", game_id=game_id, rows=len(rows))

    if dry_run:
        return all_rows

    errors = client.insert_rows_json(TARGET_TABLE, all_rows)
    if errors:
        raise RuntimeError(errors)

    return all_rows
```

## QA Queries for Postgame Signal Validation

### Validation Counts by Type

```sql
SELECT
  validation_type,
  validation_result,
  COUNT(*) AS rows
FROM `nfl-stream-406420.Analytics.game_signal_validation`
WHERE season = '2025'
GROUP BY validation_type, validation_result
ORDER BY validation_type, rows DESC;
```

### Signal Confirmation Rate

```sql
SELECT
  category,
  COUNT(*) AS total_signals,
  COUNTIF(validation_result = 'confirmed') AS confirmed,
  COUNTIF(validation_result = 'partially_confirmed') AS partially_confirmed,
  COUNTIF(validation_result = 'contradicted') AS contradicted,
  SAFE_DIVIDE(COUNTIF(validation_result = 'confirmed'), COUNT(*)) AS confirmed_rate
FROM `nfl-stream-406420.Analytics.game_signal_validation`
WHERE season = '2025'
  AND validation_type = 'game_profile_signal'
GROUP BY category
ORDER BY confirmed_rate DESC;
```

### No Pick Quality Summary

```sql
SELECT
  validation_result,
  COUNT(*) AS games
FROM `nfl-stream-406420.Analytics.game_signal_validation`
WHERE season = '2025'
  AND validation_type = 'no_pick_quality'
GROUP BY validation_result
ORDER BY games DESC;
```

### Confidence Calibration Summary

```sql
SELECT
  confidence,
  validation_result,
  COUNT(*) AS rows
FROM `nfl-stream-406420.Analytics.game_signal_validation`
WHERE season = '2025'
  AND validation_type = 'confidence_calibration'
GROUP BY confidence, validation_result
ORDER BY confidence, rows DESC;
```

### Games Where Winner Was Wrong but Signal Was Useful

```sql
SELECT
  game_id,
  ANY_VALUE(matchup_label) AS matchup_label,
  ANY_VALUE(model_result) AS model_result,
  COUNTIF(validation_result = 'confirmed') AS confirmed_signals,
  COUNTIF(validation_result = 'contradicted') AS contradicted_signals,
  ARRAY_AGG(STRUCT(category, validation_result, validation_summary) ORDER BY category) AS signals
FROM `nfl-stream-406420.Analytics.game_signal_validation`
WHERE season = '2025'
  AND validation_type = 'game_profile_signal'
GROUP BY game_id
HAVING model_result = 'incorrect'
   AND confirmed_signals > 0
ORDER BY confirmed_signals DESC;
```

## Possible `/game` Additive API Object Later

Do not add until validation rules are stable.

```json
{
  "postgame_validation": {
    "available": true,
    "overall": {
      "result": "mixed_but_useful",
      "summary": "The model missed the winner, but correctly identified the turnover environment."
    },
    "signals": [
      {
        "category": "Turnover Risk",
        "pregame_level": "Elevated",
        "pregame_tilt_team": "LAC",
        "postgame_confirming_team": "LAC",
        "validation_result": "confirmed",
        "summary": "LAC backed up the pregame turnover signal."
      }
    ],
    "core_areas": [],
    "team_comparison": [],
    "confidence_calibration": {},
    "no_pick_quality": {}
  }
}
```

## Implementation Order for IDEA-002

```text
1. Pick 5-10 historical games from existing QA batches.
2. Build a dry-run script that validates game_profile signals only.
3. Manually review whether the validation labels feel fair.
4. Add Team Comparison validation.
5. Add Core Area validation.
6. Add No Pick quality validation.
7. Create Analytics.game_signal_validation only after dry-run logic feels fair.
8. Build summary views.
9. Consider internal/admin endpoint.
10. Only then consider frontend postgame validation UI.
```

## Suggested First Test Game IDs

Use already-reviewed QA examples so the results can be judged against known notes.

```text
20251207_MIA@NYJ
20251207_CHI@GB
20251116_CIN@PIT
20251109_ARI@SEA
20251012_DAL@CAR
20251013_BUF@ATL
20251020_TB@DET
20251009_PHI@NYG
20251222_SF@IND
20260110_GB@CHI
20260125_NE@DEN
```

## Manual QA Output Format

```text
game_id:
model_result:
matchup_label:
pregame lean:
actual winner:

Game Profile Validation:
- Pressure: confirmed / mixed / contradicted / not_scorable
- Turnover Risk: confirmed / mixed / contradicted / not_scorable
- Scoring Efficiency: confirmed / mixed / contradicted / not_scorable

Team Comparison Validation:
- Points per Play:
- Points Allowed per Play:
- 3rd Down %:
- Red Zone TD %:
- Turnover Margin / Game:

Core Area Validation:
- Defensive Control:
- Disruption and Turnovers:
- Offensive Output:
- Scoring Efficiency:

No Pick Quality:

Confidence Calibration:

Short interpretation:
```

---

## Full Preserved Source Notes for IDEA-002

The following section preserves the original detail from `GameLens_Ideas_2.md`.

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


---

# Shared Backlog Notes and Paste-Friendly Future Sections

## Backburner / Cleanup Inbox

Use this section to keep future cleanup ideas from becoming separate files.

### Possible Future Cleanup — v1.6.3 Frontend Language Polish

Status:

```text
Backburner / not urgent
```

Context:

After v1.6.2 QA, the Model Trust diagnostic labels are much clearer. A future polish pass could improve signal summary language and matchup confidence summaries.

Possible improvements:

```text
"All signals agreed" may be misleading when one Game Profile signal is neutral.
Better alternatives:
- No opposing Game Profile signals
- Directional signals agreed
```

Possible files:

```text
services/model_trust_service.py
src/pages/Matchup.tsx
```

Possible backend location:

```text
model_trust.signal_alignment.summary_label
model_trust.signal_alignment.summary_code
```

Possible frontend location:

```text
ModelTrustCard inside src/pages/Matchup.tsx
```

Potential improvement:

```text
If one or more Game Profile signals are neutral, but no signal opposes the predicted side, say:
No opposing Game Profile signals
```

Instead of:

```text
All signals agreed
```

Also consider making Matchup Lean confidence summary text more context-aware.

Example:

```text
Current generic:
Confidence: This should be treated cautiously rather than as a strong outcome read.

Possible contextual:
Confidence: Kept low because Game Profile signals were mixed.
```

Possible rule:

```text
If confidence is Low and signal_alignment.summary_code = mixed:
Use a summary that names mixed Game Profile signals.
```

Do not do this until current v1.6.2 labels have been used enough to know whether the extra specificity helps.

---

# Appendix A — Future Idea Paste Template

````markdown
# IDEA-### — Idea Title

## Idea Status

```text
Status:
Suggested timing:
Recommended first shape:
```

## Product Purpose

## Why This Belongs in the Ideas File

## Possible Files to Add or Modify

### Backend / Python

```text

```

### Frontend / Future UI

```text

```

## Proposed BigQuery Tables / Views

```text

```

## Possible API Shape

```json
{
  "available": true
}
```

## QA Queries

```sql

```

## Python Notes or Skeleton

```python

```

## Implementation Order

```text
1.
2.
3.
```

## Risks / Guardrails

## Open Questions

## Decision Log
````

---

# Appendix B — Consolidation Notes

This file intentionally keeps the full original idea content from:

```text
GameLens_Ideas.md
GameLens_Ideas_2.md
```

Additional sections were added for:

```text
possible files touched
BigQuery table/view structures
QA SQL
Python builder skeletons
API response ideas
future paste-friendly idea template
v1.6.3 cleanup inbox
```

The goal is to avoid losing ideas across multiple files while keeping future implementation work grounded and testable.
