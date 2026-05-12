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
