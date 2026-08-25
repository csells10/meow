# GameLens Matchup Lens — Product, Data, Query, and Implementation Specification

**Document status:** Proposed product-and-implementation authority; documentation only  
**Prepared:** 2026-08-25  
**Product surface:** Matchup Lens  
**Overview screen:** Matchup Dashboard  
**Repository:** `csells10/meow`  
**Production authority:** `main`  
**Learning architecture authority:** `documentation/learning_lite`  
**Intended implementation branch:** `learning-lite`, subject to an explicit checkpoint authorization  
**Current implementation state:** The analytical spine exists on `main`; the Matchup Lens demo exists as a frontend concept; the canonical Matchup Lens backend contract described here is not yet implemented.

---

## 1. Why this document exists

This document is the durable specification for the Matchup Lens component/product. It preserves the product decisions, data findings, query contracts, proposed calculations, infrastructure boundaries, navigation behavior, loading behavior, quality controls, and unresolved questions developed during the Matchup Lens exploration.

It exists so that a future implementation session does not have to reconstruct the product from screenshots, chat history, demo code, or an obsolete infrastructure plan.

This document must not be used to claim that proposed Matchup Lens behavior is already running in production. It distinguishes four evidence states:

| State | Meaning |
|---|---|
| **Existing** | Confirmed in the current `main` backend or current BigQuery analytical spine |
| **Prototype evidence** | Demonstrated by the supplied CSV extracts or Lovable prototype, but not yet canonical backend behavior |
| **Recommended v1** | The smallest approved-looking implementation path; still requires an explicit code checkpoint |
| **Deferred** | Intentionally postponed until data volume or product use justifies it |

---

## 2. Position in the GameLens reference hierarchy

Read the current GameLens material in this order:

1. `documentation/learning_lite/README.md` — checkpoint status, branch authority, and handoff record.
2. `documentation/learning_lite/GameLens_Learning_Lite_Architecture.md` — stable learning boundaries, data ownership, and non-goals.
3. `documentation/learning_lite/GameLens_Learning_Lite_Sprint.md` — checkpoint sequence, gates, safety target, and carry-forward work.
4. `documentation/learning_lite/GameLens_Learning_Lite_Salvage_Matrix.md` — selective reuse decisions for the archived development prototype.
5. **This document** — Matchup Lens product, data, query, API, navigation, infrastructure, and QA authority.
6. `documentation/GameLens_Product_Ideas.md` — broader idea backlog, including League Discovery and Postgame Signal Validation.
7. Archived `documentation/live` packet work and the Backend August Readiness Plan — historical evidence only.

If this document conflicts with the Learning Lite README about checkpoint status or authorization, the README wins. If it conflicts with the Learning Lite Architecture about persistent data or ownership, the Architecture wins. If a future accepted Matchup Lens checkpoint records a changed behavior, append a dated correction here rather than silently rewriting historical evidence.

---

## 3. Executive product decision

Matchup Lens should become a **game-first, navigational analytical dashboard** that answers:

> What separates these teams, where do their profiles collide, and what evidence makes that interpretation credible before kickoff?

The first screen is not an encyclopedia of every available visualization. It is a compact decision and exploration surface that:

1. starts from a real scheduled matchup;
2. presents three or four understandable headline insights;
3. explains what the user should notice;
4. lets the user choose one analytical path;
5. preserves matchup, phase, window, date, and selected-lens context while navigating;
6. keeps detailed evidence below the fold or behind an intentional drill-down.

The first implementation should reuse the existing `/game` data-loading path and ranking rows. It should not create a new BigQuery table, materialized view, scheduler, coordinator, cache platform, or prediction system.

---

## 4. Product philosophy and user promise

Matchup Lens belongs inside the GameLens frame:

> A matchup intelligence and confidence-calibration platform.

It is useful for game previewing, team research, fantasy research, writing, curiosity, and profile-based betting research. It is not:

- a forced-pick machine;
- a win-probability claim unless a separately governed model produces one;
- a profit promise;
- a black-box recommendation surface;
- a place where a language model invents a football story without structured evidence.

The user comes to Matchup Lens to understand:

- the largest supported difference between the teams;
- the most comparable area of the matchup;
- each team’s strongest league-relative profile;
- where one team’s behavior meets an opposing counter-profile;
- which metrics built the interpretation;
- how current and complete the underlying evidence is;
- eventually, how the profiles have changed over time.

Every summary must preserve the phrase-level boundary:

> Profile signal, not a prediction.

---

## 5. Scope

### 5.1 Recommended v1 scope

- Game selector based on scheduled games.
- Compact matchup context header.
- Automatically selected season-aware comparison window.
- Headline insight ticker/carousel.
- Game Brief observations.
- Six-lens Constellation comparison.
- One-lens evidence drill-down.
- Supported profile collisions, beginning with Turnover Watch.
- Reverse tracing from a product lens, Lens Tag, or supporting metric.
- Previous meaningful meeting between the two teams.
- Data readiness, method, sample, freshness, and limitation metadata.
- Desktop and mobile loading, empty, error, and stale-data states.
- Deep-linkable navigation that preserves matchup and selected context.

### 5.2 Explicit v1 non-goals

- No new warehouse product table for lens scores.
- No new ETL pipeline.
- No separate Matchup Lens scheduler.
- No winner prediction or automated betting recommendation.
- No K-means, clustering, or NLP requirement.
- No automatic language-generation service.
- No arbitrary user-defined lens builder.
- No user-configurable metric weights.
- No event-frequency table masquerading as matchup evidence.
- No dashboard filled with every possible graph.
- No automatic Last 3/Last 7 control until those choices can be explained cleanly.
- No Momentum Shift claims with only two or three preseason games.

---

## 6. Naming and terminology

| Term | User-facing meaning | Technical meaning |
|---|---|---|
| Matchup Lens | The overall product area | Game-first analytical experience built from rankings and Lens Tags |
| Matchup Dashboard | The compact entry/overview screen | Navigational dashboard for one selected game |
| Lens | A stable football question such as Explosiveness | A versioned grouping of canonical metrics |
| Lens Score | League-relative profile summary from 0 to 100 | Aggregate of eligible directional metric percentiles under a versioned formula |
| Lens Tag | A football theme or interpretation tag | Repeated string metadata owned by `metric_registry.py` |
| Supporting metric | Evidence explaining a lens | A metric shown for interpretation; it may or may not be score-eligible |
| Constellation | Overlaid six-axis profile comparison | Radar chart with identical 0–100 axes |
| Collision | One team behavior meeting an opponent counter-profile | Comparison between two explicitly mapped metric groups |
| Reverse trace | Explore how lens, tag, and metric connect | Deterministic graph derived from registry and lens membership |
| Momentum Shift | Change in profile across comparable windows | Time-series or window-difference feature, deferred until sufficient history |

Do not use **weighted percentile** in the interface unless the backend implements and versions explicit weights. The recommended label is **Lens Score**.

Required tooltip/copy:

> A 0–100 league-relative summary of the metrics assigned to this lens. It describes profile strength, not win probability.

Internal shorthand such as `strong · w2`, `strong-w0`, raw weight codes, or registry flags must not appear in the normal user interface. Translate them to:

- Primary signal
- Supporting signal
- Context only
- Reduced influence: volume-sensitive
- Excluded from the score

---

## 7. Confirmed current analytical foundation

### 7.1 Existing ingestion and serving path

Current `main` provides:

```text
Cloud Scheduler / manual request
    -> Schedule ingestion
    -> Stats ingestion
    -> Scores ingestion
    -> accepted Stats game gate
    -> Facts builder
    -> Windowed Metrics builder
    -> Rankings builder
    -> authenticated Flask /game response
```

The GameLens metric conductor runs these stages in order:

```text
facts -> windowed_metrics -> rankings
```

It runs only after at least one Stats game is accepted. The conductor stops subsequent stages when an earlier stage fails and returns a structured stage summary. Matchup Lens must reuse this pipeline; it must not introduce a second aggregation path.

### 7.2 Existing authenticated API surfaces

- `GET /games?date=YYYY-MM-DD` returns scheduled games for a date.
- `GET /game/<game_id>` returns the current structured matchup response.
- Both routes require Firebase bearer authentication and an active allowed user.
- The existing `/game` response includes header, final score, Game Profile, Team Comparison, Core Area Comparison, Matchup Lean, Model Outcome, Model Trust, ranking context, and matchup breakdown.

### 7.3 Existing analytical tables

| Asset | Existing purpose | Matchup Lens use |
|---|---|---|
| `League.schedule` | Game identity, teams, date, time, status, season, week, phase | Game selector, phase/window choice, matchup identity, prior meeting |
| `Scores.scores` | Quarter and final score rows | Previous-meeting result and completed-game display |
| `Analytics.game_metrics_flat` | Parsed per-game/team metric source | Indirect source only; Matchup Lens should not query it directly |
| `Analytics.game_team_metric_facts_{season}` | Clean completed-game fact grain | Source for windows; future evidence and time-series drill-down |
| `Analytics.team_metrics_windowed_{season}` | Team metric snapshots by phase/window/date | Current metric values and future Momentum Shift |
| `Analytics.team_metric_rankings_{season}` | League-wide as-of ranks and percentiles | Primary Matchup Lens source |
| `Analytics.game_model_outcomes` | Existing outcome record | Not needed for pregame Matchup Lens v1 |
| `Analytics.game_model_trust_details` | Existing trust/diagnostic record | Not needed for pregame Matchup Lens v1 |

---

## 8. Table contracts and grain

### 8.1 `game_team_metric_facts_{season}`

**Existing grain:**

```text
season + game_id + team_id + metric
```

Important fields:

- season, game ID, game date, kickoff chronology;
- game week and canonical `season_type`;
- normalized `season_phase`;
- phase week and global ordering key;
- preseason/regular/postseason flags;
- team ID, abbreviation, home/away type;
- canonical metric and numeric value;
- label, definition, category, Core Area;
- comparison direction and aggregation behavior;
- ranking usage, signal strength, edge-language permission;
- Core Area and confidence eligibility;
- data-quality status;
- repeated `lens_tags`;
- creation timestamp.

Only final or final/overtime schedule games are included. Registry metadata replaces legacy parser categories and Core Areas. Unknown metrics are excluded. Unsupported season types fail validation.

### 8.2 `team_metrics_windowed_{season}`

**Existing grain:**

```text
season + team_id + data_date + window_type + metric
```

Important fields:

- games in window;
- window start/end date;
- latest included game and chronological order;
- metric value;
- complete metric-registry metadata;
- repeated Lens Tags;
- creation timestamp.

Supported window types:

```text
preseason_to_date
regular_season_to_date
regular_plus_postseason_to_date
last_3_games
last_7_games
```

Derived rates are recalculated from summed numerator/denominator ingredients rather than averaging already-derived rates. The output is rounded to six decimal places.

### 8.3 `team_metric_rankings_{season}`

**Existing grain:**

```text
season + as_of_date + window_type + metric + team_id
```

Important fields:

- `as_of_date` — ranking comparison date;
- `source_data_date` — actual team snapshot used;
- `data_lag_days` — freshness difference;
- window, team, metric, raw value;
- all registry metadata and repeated Lens Tags;
- league rank, league percentile, tier, teams ranked;
- ranking kind, direction, interpretation, and tie method.

Rankings use carry-forward logic: for every as-of date, window, and metric, each team contributes its latest available source row on or before the as-of date. This avoids ranking only teams that happened to play on the same date.

Excluded metrics and data-quality-excluded metrics are not ranked. Edge metrics are direction-normalized. Context-only metrics are ranked by high raw value but must not be described as better or worse.

Current rank calculation:

```text
league_rank = competition/min rank
league_percentile = ((teams_ranked - league_rank) / (teams_ranked - 1)) * 100
```

For one available team, percentile is 100. Ties share the minimum rank and therefore the same percentile.

---

## 9. 2026 discovery evidence and data-quality findings

### 9.1 Table inventory supplied during discovery

| Table | Rows | Teams | Metrics | First date | Latest date |
|---|---:|---:|---:|---|---|
| Facts | 4,132 | 32 | 69 | 2026-08-06 | 2026-08-22 |
| Windowed | 4,672 | 32 | 73 | 2026-08-06 | 2026-08-22 |
| Rankings | 10,141 | 32 | 67 | 2026-08-06 | 2026-08-22 |

### 9.2 Prototype extract findings

`gamelens_complete_lens_metrics_2026.csv` contains:

- 1,452 rows;
- one as-of date: 2026-08-23;
- all 32 teams;
- 46 selected metrics;
- 44 metrics with all 32 teams;
- `fourth_down_pct` with 31 teams;
- `total_defensive_snaps` with 13 teams;
- two or three games in the selected preseason-to-date window;
- data lags from zero to three days;
- 91 rows at lag 0, 904 at lag 1, 273 at lag 2, and 184 at lag 3.

`gamelens_lens_inventory_2026.csv` contains:

- 55 distinct Lens Tags;
- 54 tags covering all 32 teams;
- `snap-volume` covering 13 teams because its contributing snap metrics are incomplete;
- 22 tags connected to only one or two metrics;
- an average of 3.64 metrics per tag.

`gamelens_latest_team_coverage_2026.csv` contains all 32 teams. Teams have 60–67 ranked metrics and 41–46 metrics classified as eligible by the discovery query.

`gamelens_team_lens_profiles_2026.csv` contains exactly 100 rows, 17 teams, and six product lenses. Because it ends at a round 100 rows and is not league-complete, it should be treated as a bounded discovery extract rather than evidence that the other teams lacked profiles. Ninety-nine rows show complete prototype coverage; one Drive Control row shows 7 of 8 metrics.

### 9.3 What the data proves

- The three-table analytical spine is populated across all teams.
- The ranking table is sufficient to drive a game-level Lens experience.
- Lens Tags have broad coverage and can support deterministic reverse tracing.
- Direction-normalized league percentiles produce understandable user copy.
- Freshness varies by team because schedules differ; `as_of_date` must not be presented as though every team played that date.
- Rare-event and zero-heavy metrics frequently tie at the top. They must not dominate a lens score or headline just because their percentile is 100.
- Preseason samples of two or three games are useful for product plumbing and visual proof, not stable team identity claims.

### 9.4 Reproducible prototype checks

The 2026-08-23 extract reproduces the prototype Explosiveness values:

```text
LAR = mean(First Down Rate 96.8,
           Yards Per Play 90.3,
           Yards Per Rush 87.1,
           Yards Per Pass 64.5)
    = 84.675 -> 84.7

CLE = mean(6.5, 19.4, 35.5, 41.9)
    = 25.825 -> 25.8
```

It also reproduces the Turnover Watch profiles:

```text
NE takeaways = mean(Defensive Interceptions 87.1,
                    Fumbles Recovered 100.0)
             = 93.55 -> 93.5

CLE ball security = mean(Turnovers Committed 6.5,
                         Interceptions Thrown 0.0,
                         Fumbles Lost 58.1)
                  = 21.53 -> 21.5
```

Those exact matches validate the direction-normalized percentile approach. They do not prove every prototype lens mapping or weight. The SQL that created the six-lens profile extract is not currently a canonical repository artifact, so formula parity must be captured before production promotion.

---

## 10. Season and window behavior

### 10.1 Canonical phase field

`League.schedule.seasonType` is authoritative. `gameWeek` is descriptive and is used only for the postseason boundary and compatibility fallbacks.

Recommended v1 automatic selection:

| Scheduled game phase | Window |
|---|---|
| Preseason | `preseason_to_date` |
| Regular Season | `regular_season_to_date` |
| Wild Card | `regular_season_to_date` |
| Divisional Round or later postseason | `regular_plus_postseason_to_date` |

Current `main` implements and tests this behavior.

### 10.2 Pregame date boundary

For a target game:

```text
metric data_date < target game_date
ranking as_of_date < target game_date
```

This prevents the target game from entering its own pregame comparison.

### 10.3 User-facing phase control

Recommended v1: display the automatically selected basis but do not ask a first-time user to choose it.

Example:

```text
Regular season to date · as of Sep 24 · NE 3 games / CLE 3 games
```

Recommended later advanced control:

- Regular Season
- Regular + Postseason
- Last 3 Games
- Last 7 Games

Preseason must never silently mix with regular-season or postseason identity. A future advanced override must preserve the selected value in the URL and label every visual with the resulting basis.

---

## 11. Product information architecture

### 11.1 Entry model

The primary entry is a real game, not two arbitrary teams.

```text
Games
  -> select scheduled game
  -> Matchup Dashboard
  -> choose analytical path
  -> evidence or collision drill-down
  -> return to same matchup and selection
```

An arbitrary team-pair comparison can remain an internal/demo capability, but the production product should lead with scheduled matchups because that gives the analysis purpose, phase, date, and possession context.

### 11.2 Recommended routes

```text
/matchup-lens/:game_id
/matchup-lens/:game_id/constellation
/matchup-lens/:game_id/lens/:lens_slug
/matchup-lens/:game_id/collisions
/matchup-lens/:game_id/collision/:collision_slug
/matchup-lens/:game_id/history
```

Recommended query state:

```text
?lens=turnover-balance
&trace=interceptions-thrown
&trace_view=list|network|packed
&window=automatic|regular_season_to_date|last_3_games|last_7_games
```

The canonical game ID belongs in the path. Optional filters belong in the query string. Back navigation must restore the prior lens, trace, scroll region, and comparison mode.

### 11.3 Sticky context header

After the full game selector scrolls out of view, show a compact sticky bar containing:

- away abbreviation and home abbreviation;
- full matchup in accessible text;
- phase/window basis;
- as-of date;
- current destination, such as `Viewing: Scoring Finish`;
- Back to Overview;
- Change matchup.

The sticky bar must not duplicate the full selector controls or consume excessive mobile height.

---

## 12. Matchup Dashboard layout

### 12.1 Desktop above the fold

The first viewport should contain only:

1. Matchup Dashboard title and one-sentence purpose.
2. Game selector with date, phase, game count, and as-of context.
3. Headline insight ticker.
4. “Start here — what to notice” with three observations.
5. “Choose what to explore” navigation cards.

The user should not see a full radar chart, a full evidence table, multiple graphs, and a collision board before making a choice.

### 12.2 Desktop below the fold

Optional preview modules may appear below the initial navigation:

- compact Constellation preview;
- top profile gaps;
- previous meeting;
- data readiness and method.

Each preview must have one clear action. Avoid putting the entire drill-down inside the overview.

### 12.3 Mobile above the fold

The mobile first viewport should include:

1. compact matchup identity;
2. one headline insight at a time;
3. two or three “what to notice” rows;
4. vertically stacked exploration actions.

Do not place the full Constellation above the fold on a narrow screen. Use a small preview or defer it until the user chooses Compare the Teams.

---

## 13. Headline insight ticker

### 13.1 Purpose

The ticker answers “What should I notice first?” without showing every calculation.

Eligible insight types:

1. Biggest Edge / Largest Separation
2. Closest Lens
3. Turnover Watch
4. Supported Profile Collision
5. Strongest Team Identity
6. Previous Meeting, when available and relevant

Do not fill the ticker with arbitrary metrics merely to create motion.

### 13.2 Rotation behavior

- Automatically advance every six to eight seconds.
- Use a restrained cross-fade or short horizontal transition.
- Preserve a fixed content height to prevent layout shift.
- Pause on hover, keyboard focus, or pointer interaction.
- Provide previous/next controls and position dots.
- Reset the timer after manual navigation.
- Honor `prefers-reduced-motion`; under reduced motion, do not auto-animate.
- Do not repeatedly announce automatic changes through an assertive live region.

### 13.3 Card contract

Each item contains:

- eyebrow label;
- one plain-English headline;
- one evidence line;
- leader or direction when appropriate;
- `Why this appears` tooltip or action;
- one context-preserving exploration action.

Example:

```text
TURNOVER WATCH
NE’s takeaway profile meets a CLE offense with weaker ball-security standing.
NE takeaways: 93.5 / 100 · CLE ball security: 21.5 / 100
Profile signal, not a prediction.
[Inspect the evidence]
```

---

## 14. Game Brief

### 14.1 Purpose

Game Brief is the clearest plain-English pregame entry point. It should summarize:

- where Team A is strongest;
- where Team B is strongest;
- the widest supported profile collision;
- optionally the most comparable lens;
- the data basis.

### 14.2 Observation contract

Every observation must include:

- a human sentence;
- a plain-English explanation of the lens/profile;
- rank and percentile context;
- an `Explore` action;
- an info tooltip if the concept is not self-evident.

Bad:

```text
NE is strongest in Turnover Balance.
```

Better:

```text
New England’s clearest strength is managing the turnover battle.
Turnover Balance · 3rd of 32
[What this means] [Explore]
```

### 14.3 Data readiness

Keep detailed readiness collapsed by default. Expose:

- phase/window;
- as-of date;
- games per team;
- source dates and maximum lag;
- lens coverage;
- whether the sample is preseason or early-season;
- “not a forecast” language;
- the score method/version.

---

## 15. Constellation

### 15.1 Product role

Constellation is the primary visual comparison experience. It answers:

> Where do the teams’ six profiles overlap, and where do they separate?

### 15.2 Radar-chart contract

- Exactly six stable axes in the same order for every matchup.
- Identical 0–100 scale on every axis.
- Rings at 25, 50, 75, and 100.
- Direct legend labels for both teams.
- Different marker shapes in addition to color.
- Moderate translucent fills and clear strokes.
- No 3-D styling.
- No axis-specific scale changes.
- Overlay is default; side-by-side is optional.
- A selected axis receives a strong visual state and keyboard focus.

### 15.3 Axis interaction

The chart labels and the six summary cards must both be explicitly interactive:

- pointer cursor;
- visible hover treatment;
- focus ring;
- `Explore` label or chevron;
- tooltip: `Open the metrics behind this lens`;
- keyboard Enter/Space activation.

Clicking an axis must update the selected lens in the URL and either scroll to a visible evidence region or navigate to the lens route. It must never change content below the fold without an immediate, visible confirmation.

### 15.4 Summary-card contract

Each compact lens card contains:

- lens label;
- Team A Lens Score;
- Team B Lens Score;
- gap;
- leader or `near even`;
- rank context for each team where space permits;
- Explore action.

On desktop, use a two-row grid or a horizontally scrollable strip. On mobile, use horizontal scrolling with snap points rather than squeezing six dense cards into a tiny grid.

---

## 16. Lens evidence detail

### 16.1 Header

The detail view begins with:

- `Why [team] leads in [lens]` or `[lens] is nearly even`;
- one sentence defining the lens;
- one sentence stating the gap;
- Team A and Team B score cards;
- rank, percentile, tier, sample, and freshness.

### 16.2 Supporting evidence

Show three evidence cards initially. Each card contains:

- metric label;
- role: Primary, Supporting, Context, or Excluded;
- Team A bar and readable context;
- Team B bar and readable context;
- `Better than X% of teams · Yth of N`;
- direction-aware definition;
- trace action.

Provide `View all evidence (N)` instead of rendering every metric by default.

### 16.3 Method disclosure

Collapsed sections:

- Signals used
- How this score is built
- Data readiness and method

The plain-English score method must be visible before raw configuration fields.

---

## 17. Matchup Collision

### 17.1 Product role

Collision is not a general gap chart. It answers:

> When one team has the ball, how does one behavior profile meet the opponent’s counter-profile?

That possession direction must always be stated.

### 17.2 Recommended initial collision families

#### A. Ball Security vs Takeaways

```text
ball_security = mean(
    percentile(turnovers),
    percentile(interceptions_thrown),
    percentile(fumbles_lost)
)

takeaways = mean(
    percentile(defensive_interceptions),
    percentile(fumbles_recovered)
)
```

All source percentiles are already direction-normalized, so a higher number means a stronger league-relative profile.

#### B. Protection vs Disruption

```text
protection = mean(
    percentile(sacks_taken),
    percentile(sack_yards_lost)
)

disruption = percentile(sacks)
```

The copy must clarify that sacks are currently the available disruption measure; `pressure_rate` is excluded because its present formula mixes pressure created and pressure allowed.

#### C. Scoring Finish vs Scoring Resistance

Candidate only after formula QA:

```text
scoring_finish = versioned scoring-finish lens score
scoring_resistance = mean(
    percentile(points_allowed_per_play),
    percentile(points_allowed),
    percentile(yards_allowed)
)
```

### 17.3 Collision selection

For each offense/defense direction:

1. calculate only supported collision families;
2. require minimum metric coverage;
3. compute absolute gap;
4. reject rare-event-only comparisons;
5. select the widest supported collision;
6. present the result as a profile matchup, never a forecast.

### 17.4 Collision UI

Use two profile cards and a sentence—not an abstract physics visualization.

```text
With CLE holding the ball, its ball-security profile meets NE’s takeaways profile.

CLE Ball Security 21.5 / 100
NE Takeaways 93.5 / 100

[Open collision detail]
```

The collision detail shows the source metrics, roles, ranks, coverage, method, and possession direction.

---

## 18. Reverse tracing Lens Tags and metrics

### 18.1 User question

Reverse trace answers:

> Why is this metric or tag connected to this lens, and where else does it participate?

### 18.2 Deterministic data model

Node types:

- product lens;
- product-facing Lens Tag;
- internal/interpretation tag;
- metric.

Edge types:

- `lens_includes_metric`;
- `metric_has_tag`;
- `tag_supports_lens`;
- `metric_is_primary_for_lens`;
- `metric_is_supporting_for_lens`;
- `metric_excluded_from_score`.

The graph is derived from the versioned product-lens registry and `metric_registry.py`. It does not require NLP or clustering.

### 18.3 Interaction model

- Clicking a Lens Tag or metric opens a right-side drawer on desktop.
- On mobile it opens a full-screen sheet.
- The selected trace is represented in the URL.
- Closing returns to the exact prior context.
- Every connected node is selectable so exploration can continue without returning to the overview.
- Breadcrumbs show `Matchup > Lens > Tag/Metric`.

### 18.4 Trace views

#### List — default

Most understandable and accessible. Show:

- where the item lands in this matchup;
- connected tags/signals;
- parent lenses;
- metric role and influence;
- both teams’ rank/percentile;
- plain-English definition and guardrail.

#### Network — optional analytical view

Use a fixed layered layout:

```text
parent lenses -> tags/signals -> selected metric
```

Avoid free-floating force motion by default. Node color represents type; shape also distinguishes type. Edge hover/focus reveals the relationship label.

#### Packed groups — optional inventory view

Use circle packing to show groups of connected metrics. Circle size means **number of connected metrics**, not importance, confidence, or performance. State that rule directly beside the visual.

### 18.5 What not to do

- Do not make the network the default.
- Do not use unlabeled circles.
- Do not imply that a large packed circle is a stronger signal.
- Do not expose raw internal tags without explaining whether they are product-facing or QA metadata.

---

## 19. Previous meeting

### 19.1 Purpose

Show the most recent meaningful completed meeting between the selected franchises to ground the upcoming game in recognizable history.

### 19.2 Rules

- Match either home/away orientation.
- Require game date before the selected target game.
- Require `Final` or `Final/OT`.
- Include Regular Season or Postseason only by default.
- Exclude preseason.
- Return one result ordered by most recent kickoff/date.
- Clearly label the historical season and date.
- Do not imply that the prior result predicts the selected game.

### 19.3 UI

```text
LAST MEETING
Sep 22, 2024 · Regular Season
CLE 17 — NE 24
New England won by 7.
[View game]
```

If the result is unavailable, omit the card or show `No prior completed meeting found` without blocking the dashboard.

---

## 20. Momentum Shift

### 20.1 Status

Deferred until comparable history exists. The concept is retained because it becomes valuable around Weeks 10–12.

### 20.2 Future question

> How has each team’s profile changed from season-to-date to recent form, and is that movement durable?

### 20.3 Candidate comparisons

- Last 3 vs Regular Season to Date
- Last 7 vs Regular Season to Date
- Current week vs prior comparable week
- Rolling lens percentile over time

### 20.4 Minimum evidence

- At least three games for Last 3.
- At least seven games for Last 7.
- Comparable phase and metric definitions.
- Visible source dates and sample counts.
- `Insufficient history` rather than a flat or fabricated line.

### 20.5 Future visual

Prefer a small multiple line chart or slope comparison with exact week/date labels. Do not use animation as a substitute for readable time axes.

---

## 21. Removed, rejected, or secondary experiences

| Experience | Decision | Reason |
|---|---|---|
| Event Pulse | Remove | Rare-event leaderboards did not answer the matchup question and often produced ties/100th-percentile noise |
| Advantage Map / Matchup Map | Remove from primary experience | Visually abstract and less understandable than a sorted gap list or Constellation |
| Team Fingerprint | Secondary/deferred | Useful for independent team identity, but Constellation is more efficient for a matchup |
| Lens Galaxy | Remove from primary experience | Visually interesting but weak task utility; relationships belong in reverse trace |
| Lens Portrait | Reject | Visual looked decorative/crop-circle-like and did not improve interpretation |
| Tree map | Reject | Weak fit for comparison and relationship tracing |
| Network graph | Keep inside reverse trace | Useful for relationships, not primary matchup comparison |
| Circle packing | Keep inside reverse trace | Useful for grouped inventory when size semantics are explicit |
| Momentum Shift | Keep but defer | High future value, insufficient early-season data |

---

## 22. Six-lens product registry

### 22.1 Confirmed prototype lenses

```text
Explosiveness
Drive Control
Scoring Finish
Defensive Resistance
Disruption & Protection
Turnover Balance
```

Prototype expected metric counts were:

| Lens | Expected metrics in discovery extract |
|---|---:|
| Explosiveness | 4 |
| Drive Control | 8 |
| Scoring Finish | 7 |
| Defensive Resistance | 6 |
| Disruption & Protection | 3 |
| Turnover Balance | 8 |

### 22.2 Recommended versioned registry

The six-lens mapping must become one canonical backend configuration before implementation. It does not currently exist as a repository authority.

Recommended shape:

```python
PRODUCT_LENSES = {
    "explosiveness": {
        "label": "Explosiveness",
        "description": "How efficiently the offense creates yards and first downs.",
        "primary_metrics": [
            "1st_down_rate",
            "yards_per_play",
            "yards_per_pass",
            "yards_per_rush",
        ],
        "supporting_metrics": [],
    },
    "drive-control": {
        "label": "Drive Control",
        "description": "How consistently the offense sustains and converts drives.",
        "primary_metrics": [
            "1st_down_rate",
            "third_down_pct",
            "fourth_down_pct",
        ],
        "supporting_metrics": [
            "first_downs",
            "passing_first_downs",
            "rushing_first_downs",
            "third_down_conversions",
            "fourth_down_conversions",
        ],
    },
    "scoring-finish": {
        "label": "Scoring Finish",
        "description": "How efficiently the offense turns plays and scoring chances into touchdowns and points.",
        "primary_metrics": [
            "points_per_play",
            "td_rate",
            "red_zone_efficiency",
        ],
        "supporting_metrics": [
            "red_zone_tds",
            "passing_tds",
            "rushing_tds",
            "passing_tds_rushing_tds_sum",
        ],
    },
    "defensive-resistance": {
        "label": "Defensive Resistance",
        "description": "How effectively the defense limits yards and points.",
        "primary_metrics": [
            "points_allowed_per_play",
            "points_allowed",
            "yards_allowed",
        ],
        "supporting_metrics": [
            "defensive_tds",
            "defensive_two_point_returns",
            "safeties",
        ],
    },
    "disruption-protection": {
        "label": "Disruption & Protection",
        "description": "How the team creates sacks while avoiding sack losses on offense.",
        "primary_metrics": [
            "sacks",
            "sacks_taken",
            "sack_yards_lost",
        ],
        "supporting_metrics": [],
    },
    "turnover-balance": {
        "label": "Turnover Balance",
        "description": "How the team protects the ball, creates takeaways, and manages net turnover position.",
        "primary_metrics": [
            "turnover_margin_per_game",
            "turnovers",
            "interceptions_thrown",
            "fumbles_lost",
            "defensive_interceptions",
            "fumbles_recovered",
        ],
        "supporting_metrics": [
            "turnover_margin",
            "defensive_tds",
        ],
    },
}
```

### 22.3 Formula warning

The registry above is the recommended v1 semantic split between scoring metrics and explanatory metrics. It is not proof of the exact discovery SQL. Before coding:

1. recover or reconstruct the SQL that produced `gamelens_team_lens_profiles_2026.csv`;
2. compare every prototype score against the registry output;
3. record intentional differences;
4. choose and version the production formula;
5. preserve the chosen mapping in code and tests.

Do not silently hardcode prototype numbers in the frontend.

---

## 23. Calculation contracts

### 23.1 Metric eligibility

For v1 score calculation, a metric must:

- exist in the lens’s `primary_metrics` list;
- have both a league percentile and rank;
- have `ranking_usage = 'edge'`;
- have `data_quality_status != 'exclude'`;
- have a valid directional interpretation;
- belong to the selected window and pregame as-of date.

Supporting/context metrics may be displayed but do not affect the v1 Lens Score unless the versioned registry explicitly says otherwise.

### 23.2 Lens Score

Recommended v1:

```text
lens_score = arithmetic mean of available eligible primary metric percentiles
```

Do not call this weighted. Do not apply hidden `strong = 2` or `supporting = 1` weights in v1.

### 23.3 Coverage

```text
observed_primary_metrics = count of valid eligible primary metrics
expected_primary_metrics = count in registry
coverage_pct = observed / expected * 100
```

Recommended display policy:

- 100%: normal display;
- 75–99%: display with `Partial evidence`;
- below 75%: do not use the lens in a headline, largest separation, closest lens, or collision;
- zero: unavailable.

### 23.4 Gap and leader

```text
gap = abs(away_lens_score - home_lens_score)
leader = away | home | near_even | unavailable
```

Recommended near-even threshold:

```text
gap <= 3.5 percentile points
```

This mirrors the current Team Comparison guardrail. Version the threshold rather than scattering it through frontend code.

### 23.5 Largest separation and closest lens

Use only lenses meeting the minimum coverage for both teams.

```text
largest_separation = valid lens with maximum gap
closest_lens = valid lens with minimum gap
```

If all valid gaps are near even, say `Profiles are closely matched across the available lenses` rather than manufacturing a dramatic edge.

### 23.6 Strongest lens

For each team:

```text
strongest_lens = valid lens with maximum Lens Score
```

Show league rank and tier. Do not infer that a team will win because it has one high lens.

### 23.7 Metric display order

Within one lens:

1. primary metrics by absolute percentile gap descending;
2. supporting metrics by relevance, then gap;
3. context-only metrics last;
4. excluded metrics hidden by default and visible only in technical trace.

---

## 24. Query strategy — quickest implementation

### 24.1 Request flow

```text
Selected game
    -> load header once
    -> select window from season_type/game_week
    -> load all ranking rows for both teams once
    -> derive Matchup Lens payload in Python
    -> optionally load previous meeting once
    -> return one authenticated game response
```

The existing `get_team_rankings_for_game` already loads the full metric rows for the two teams. `game_service.py` retains those rows in `away_rankings` and `home_rankings` before building the lightweight public `ranking_context`. Therefore Matchup Lens should use those in-memory dictionaries. It does not need another metrics or rankings query.

### 24.2 Query count guidance

Recommended upcoming-game path:

1. Header query.
2. Windowed metrics query, retained for existing `/game` behavior.
3. Rankings query, reused for Matchup Lens.
4. Optional previous-meeting query.

Skip the final-score query when `game_status` is not `Final` or `Final/OT`. The current service queries final scores before checking status; that optimization is useful but should be a bounded change with tests.

Avoid repeated header queries by allowing existing helpers to accept a previously loaded header. This is a performance cleanup, not a requirement to launch the component.

### 24.3 Header query contract

Use the existing parameterized schedule query. Required fields:

```sql
SELECT
  s.gameID,
  s.gameDate,
  s.gameTime,
  s.gameStatus,
  s.season,
  s.gameWeek,
  s.seasonType,
  s.teamIDAway,
  s.away,
  away_logo.logoURL AS away_logo,
  s.teamIDHome,
  s.home,
  home_logo.logoURL AS home_logo,
  s.espnLink
FROM `nfl-stream-406420.League.schedule` s
LEFT JOIN `nfl-stream-406420.Teams.team_logos` away_logo
  ON s.teamIDAway = away_logo.teamID
LEFT JOIN `nfl-stream-406420.Teams.team_logos` home_logo
  ON s.teamIDHome = home_logo.teamID
WHERE s.gameID = @game_id
LIMIT 1;
```

### 24.4 Pregame rankings query contract

Use the existing query, which selects the latest ranking date strictly before the game date:

```sql
SELECT
  r.season,
  r.as_of_date,
  r.source_data_date,
  r.data_lag_days,
  r.window_type,
  r.team_id,
  r.team_abv,
  r.metric,
  r.value,
  r.label,
  r.definition,
  r.category,
  r.core_area,
  r.comparison_direction,
  r.higher_is_better,
  r.raw_or_derived,
  r.aggregation_method,
  r.numerator,
  r.denominator,
  r.format,
  r.decimals,
  r.notes,
  r.ranking_usage,
  r.signal_strength,
  r.edge_language_allowed,
  r.include_in_core_area_advantage,
  r.confidence_eligible,
  r.data_quality_status,
  r.lens_tags,
  r.league_rank,
  r.league_percentile,
  r.tier,
  r.tier_label,
  r.teams_ranked,
  r.ranking_kind,
  r.rank_direction,
  r.rank_interpretation,
  r.rank_tie_method
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026` r
WHERE r.as_of_date = (
  SELECT MAX(as_of_date)
  FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
  WHERE as_of_date < @game_date
    AND window_type = @window_type
)
  AND r.window_type = @window_type
  AND r.team_id IN UNNEST(@team_ids)
ORDER BY r.core_area, r.category, r.metric, r.team_id;
```

Production code must build the season-specific table through `runtime_config`, not string-concatenate an unvalidated user season.

### 24.5 Previous-meeting query

Recommended direct query:

```sql
WITH prior_game AS (
  SELECT
    s.gameID,
    DATE(s.gameDate) AS game_date,
    s.season,
    s.seasonType,
    s.gameWeek,
    CAST(s.teamIDAway AS STRING) AS away_team_id,
    s.away AS away_team_abv,
    CAST(s.teamIDHome AS STRING) AS home_team_id,
    s.home AS home_team_abv
  FROM `nfl-stream-406420.League.schedule` s
  WHERE DATE(s.gameDate) < @target_game_date
    AND s.gameStatus IN ('Final', 'Final/OT')
    AND LOWER(TRIM(s.seasonType)) IN ('regular season', 'postseason')
    AND (
      (CAST(s.teamIDAway AS STRING) = @team_a_id
       AND CAST(s.teamIDHome AS STRING) = @team_b_id)
      OR
      (CAST(s.teamIDAway AS STRING) = @team_b_id
       AND CAST(s.teamIDHome AS STRING) = @team_a_id)
    )
  ORDER BY DATE(s.gameDate) DESC, s.gameID DESC
  LIMIT 1
)
SELECT
  p.*,
  MAX(SAFE_CAST(sc.awayPts AS INT64)) AS away_points,
  MAX(SAFE_CAST(sc.homePts AS INT64)) AS home_points
FROM prior_game p
LEFT JOIN `nfl-stream-406420.Scores.scores` sc
  ON p.gameID = sc.gameID
GROUP BY
  p.gameID, p.game_date, p.season, p.seasonType, p.gameWeek,
  p.away_team_id, p.away_team_abv, p.home_team_id, p.home_team_abv;
```

Parameters:

```text
@target_game_date DATE
@team_a_id STRING
@team_b_id STRING
```

### 24.6 Lens coverage QA query

```sql
SELECT
  r.as_of_date,
  r.window_type,
  r.team_id,
  r.team_abv,
  COUNT(*) AS ranked_metric_count,
  COUNTIF(r.ranking_usage = 'edge') AS edge_metric_count,
  COUNTIF(r.signal_strength = 'strong') AS strong_metric_count,
  COUNTIF(r.data_quality_status = 'good') AS good_metric_count,
  COUNT(DISTINCT lens_tag) AS distinct_lens_tag_count
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026` r
LEFT JOIN UNNEST(r.lens_tags) AS lens_tag
WHERE r.as_of_date = @as_of_date
  AND r.window_type = @window_type
GROUP BY r.as_of_date, r.window_type, r.team_id, r.team_abv
ORDER BY r.team_abv;
```

### 24.7 No-future-evidence QA

```sql
SELECT *
FROM `nfl-stream-406420.Analytics.team_metric_rankings_2026`
WHERE source_data_date > as_of_date
   OR data_lag_days < 0;
```

Expected result: zero rows.

---

## 25. Service and code ownership

### 25.1 Recommended new files

```text
analytics/product_lens_registry.py
services/matchup_lens_service.py
tests/services/test_matchup_lens_service.py
tests/test_product_lens_registry.py
```

Optional query addition:

```text
queries/game_queries.py              # add get_previous_meeting(header)
tests/queries/test_game_queries.py    # test orientation, phase, and date predicates
```

### 25.2 Recommended existing modifications

```text
services/game_service.py
    -> build Matchup Lens from already loaded ranking dictionaries
    -> add additive matchup_lens response section
    -> optionally skip final score query for non-final games

queries/game_queries.py
    -> accept an already loaded header where practical
    -> add previous-meeting query

routes/game_routes.py
    -> no new route required for v1
```

### 25.3 Ownership boundaries

- Metric meaning remains in `metric_registry.py`.
- Product-lens membership remains in `product_lens_registry.py`.
- BigQuery reads remain in `game_queries.py`.
- Lens/collision calculations remain in `matchup_lens_service.py`.
- Response composition remains in `game_service.py`.
- Frontend renders the structured response; it does not calculate scores.
- Learning Lite snapshot logic preserves the complete pregame response when that checkpoint is implemented.

---

## 26. Recommended API response

Add one optional top-level section to the existing `/game/<game_id>` response:

```json
{
  "matchup_lens": {
    "available": true,
    "reason": null,
    "version": "matchup-lens-v1",
    "context": {
      "game_id": "...",
      "season": "2026",
      "season_type": "Regular Season",
      "game_week": "Week 4",
      "window_type": "regular_season_to_date",
      "as_of_date": "2026-09-24",
      "away_games_in_window": 3,
      "home_games_in_window": 3,
      "source_data_dates": ["2026-09-21", "2026-09-22"],
      "max_data_lag_days": 3
    },
    "headline_insights": [],
    "observations": [],
    "lenses": [],
    "collisions": [],
    "previous_meeting": null,
    "data_readiness": {},
    "method": {
      "score_name": "Lens Score",
      "formula_version": "lens-score-v1",
      "summary": "Average league standing of eligible primary metrics.",
      "near_even_threshold": 3.5,
      "prediction": false
    }
  }
}
```

### 26.1 Lens object

```json
{
  "slug": "explosiveness",
  "label": "Explosiveness",
  "description": "How efficiently the offense creates yards and first downs.",
  "tags": [
    {"slug": "explosiveness", "label": "Explosiveness", "product_facing": true},
    {"slug": "passing-efficiency", "label": "Passing efficiency", "product_facing": true}
  ],
  "away": {
    "score": 84.7,
    "rank": 4,
    "teams_ranked": 32,
    "tier": "strong",
    "observed_primary_metrics": 4,
    "expected_primary_metrics": 4,
    "coverage_pct": 100.0
  },
  "home": {
    "score": 25.8,
    "rank": 25,
    "teams_ranked": 32,
    "tier": "weak",
    "observed_primary_metrics": 4,
    "expected_primary_metrics": 4,
    "coverage_pct": 100.0
  },
  "gap": 58.9,
  "leader": "away",
  "near_even": false,
  "metrics": []
}
```

### 26.2 Metric evidence object

```json
{
  "metric": "yards_per_play",
  "label": "Yards Per Play",
  "definition": "Total offensive yards divided by total plays.",
  "role": "primary",
  "score_eligible": true,
  "comparison_direction": "higher",
  "signal_strength": "strong",
  "ranking_usage": "edge",
  "data_quality_status": "good",
  "lens_tags": ["offensive-efficiency", "explosiveness", "strong-signal"],
  "away": {
    "value": 6.1,
    "league_percentile": 90.3,
    "league_rank": 4,
    "teams_ranked": 32,
    "source_data_date": "2026-08-22",
    "data_lag_days": 1
  },
  "home": {
    "value": 4.4,
    "league_percentile": 19.4,
    "league_rank": 26,
    "teams_ranked": 32,
    "source_data_date": "2026-08-22",
    "data_lag_days": 1
  }
}
```

### 26.3 Unavailable response

```json
{
  "available": false,
  "reason": "no_pregame_rankings",
  "context": {
    "window_type": "regular_season_to_date",
    "as_of_date": null
  },
  "headline_insights": [],
  "observations": [],
  "lenses": [],
  "collisions": [],
  "previous_meeting": null,
  "data_readiness": {
    "message": "Current-season rankings are not available before this game."
  }
}
```

Unavailable is a successful, honest product state. Do not fall back to preseason or prior-season rankings while labeling them as current regular-season evidence.

---

## 27. Loading, error, empty, and stale states

### 27.1 Initial loading

- Preserve the layout with skeletons matching the final card heights.
- Show the selected matchup immediately when the game list already supplied it.
- Skeleton the headline ticker, observation rows, and exploration cards.
- Do not animate the radar chart before data exists.
- Announce `Loading matchup analysis` once to assistive technology.

### 27.2 Matchup changes

- Keep the selector interactive.
- Display `Updating matchup` in the sticky context bar.
- Cancel or ignore stale requests when the user changes games quickly.
- Do not allow an older response to overwrite a newer selection.
- Preserve old content only if it is visually marked stale; otherwise use skeletons.

### 27.3 Partial availability

The dashboard may render when:

- previous meeting is unavailable;
- one non-headline lens has partial coverage;
- one metric is unavailable for one team;
- Momentum Shift is unavailable.

It should fail the full Matchup Lens section only when the header or all eligible ranking evidence is unavailable.

### 27.4 Stale evidence

If `max_data_lag_days` exceeds the chosen product threshold, show:

```text
Some team metrics are based on an earlier game date because the teams last played on different days.
```

Do not call ordinary schedule-driven carry-forward stale without context.

### 27.5 API failure

Show one compact retry surface. Keep the game selector and existing page navigation usable. Do not replace the entire application shell with a blank error.

---

## 28. Visual and interaction standards

### 28.1 Brand continuity

- Reuse the established GameLens dark background, card surfaces, border tones, font family, spacing scale, and radii.
- Reuse the current team comparison colors consistently across every chart.
- Do not introduce a separate Matchup Lens palette.
- Use semantic red/green only for genuine warning/success states, not team identity.
- Pair color with labels, shapes, and position.

### 28.2 Typography

- Use the product’s existing interface typeface.
- Use tabular numerals for scores, ranks, dates, and metric values.
- Avoid tiny uppercase metadata when it becomes unreadable on mobile.
- Keep chart labels large enough to read without zooming.

### 28.3 Tooltips and affordance

Provide tooltips for:

- Lens Score;
- each product lens;
- each Lens Tag;
- rank/percentile language;
- `Why this appears`;
- metric role/influence;
- source freshness.

Tooltips must also be available through focus/tap, not hover only.

### 28.4 Mobile touch targets

- Minimum 44-by-44 CSS pixels for interactive targets.
- Do not rely on tiny chart-axis text as the only activation target.
- Use full-width rows or cards for lens navigation.
- Reverse-trace drawer becomes a full-screen sheet.

---

## 29. Performance and cost boundaries

- Reuse the existing rankings query result within the request.
- Do not run one query per lens or per Lens Tag.
- Do not run one query per metric card.
- Batch both teams in the same query.
- Keep BigQuery parameters typed.
- Use `runtime_config` to resolve project and dataset names.
- Add a request-level timing log for header, metrics, rankings, previous meeting, and payload build.
- Treat previous meeting as non-blocking and optionally lazy-load it if latency is material.
- Consider short response caching only after measuring repeated game requests. Cache is not a v1 prerequisite.
- Do not precompute a physical lens table until request latency or repeated analytical use proves it is needed.

---

## 30. Security and privacy

- Preserve Firebase authentication and active-user authorization.
- Do not expose BigQuery credentials or internal project configuration to the frontend.
- Validate game IDs and all optional enum filters.
- Never use a user-supplied season as an unchecked table suffix.
- Keep technical registry notes that reveal internal QA details behind the technical trace, not in default user copy.
- Do not log authorization headers or complete sensitive user records.

---

## 31. Observability

Recommended structured events:

```text
matchup_lens_build_started
matchup_lens_build_completed
matchup_lens_unavailable
matchup_lens_partial_coverage
matchup_lens_previous_meeting_unavailable
matchup_lens_formula_version
```

Useful fields:

- game ID, season, phase, window;
- as-of date;
- away/home metric counts;
- lens coverage by lens;
- selected headline IDs;
- collision IDs;
- max lag;
- query/build duration;
- formula and registry version;
- unavailable reason.

Do not add an operational BigQuery table for these events initially. Use existing structured logs.

---

## 32. QA plan

### 32.1 Registry tests

- Six unique stable lens slugs.
- Every primary/supporting metric exists in `metric_registry.py`.
- No metric is both primary and supporting within one lens.
- Every lens has a non-empty definition.
- Every lens has a formula/version identifier.
- Excluded metrics cannot become score-eligible.
- Product-facing tags are separated from internal tags.

### 32.2 Calculation tests

- Exact mean and rounding.
- Lower-is-better metrics use direction-normalized ranking percentiles.
- Missing metrics reduce coverage without changing the denominator.
- Below-threshold coverage cannot produce a headline.
- Near-even gap behaves at, below, and above 3.5.
- Largest/closest lens ignore unavailable lenses.
- Exact ties return neutral.
- Rare-event supporting metrics do not alter a score.
- NE/CLE Turnover Watch reproduces 93.5 vs 21.5 from the discovery extract.
- LAR/CLE Explosiveness reproduces 84.7 vs 25.8 from the discovery extract.

### 32.3 Query tests

- Phase selection is owned by `seasonType`.
- Wild Card uses regular-season-to-date.
- Divisional and later uses regular-plus-postseason-to-date.
- Rankings are strictly before game date.
- Both teams load in one query.
- Previous meeting works in either home/away orientation.
- Previous meeting excludes preseason and future/current game.
- Scheduled games do not require a final-score query.
- Missing previous score does not fail Matchup Lens.

### 32.4 API tests

- Existing `/game` keys remain unchanged.
- Additive `matchup_lens` section serializes Lens Tags as JSON arrays.
- Unavailable context returns structured reason, not HTTP 500.
- No Pick, missing ranking, and partial lens states remain honest.
- Response contains formula and registry versions.

### 32.5 Frontend tests

- Game change refreshes every dependent section.
- No stale request race.
- Ticker auto-rotation, manual control, pause, and reduced-motion behavior.
- Constellation axes and cards are keyboard-operable.
- A lens click produces an immediate visible state change.
- Deep links restore matchup, lens, trace, and view.
- Mobile drawer/sheet can be closed and returns focus correctly.
- Loading skeleton does not cause major layout shift.
- All bars and charts are understandable without color alone.

### 32.6 Manual acceptance questions

A friend who did not build GameLens should be able to answer within one minute:

1. Which two teams are being compared?
2. What data period is being used?
3. What is the biggest difference?
4. What does one selected lens mean?
5. Which metrics support it?
6. What does one collision say—and what does it not say?
7. How do they return to the overview or change the matchup?

---

## 33. Minimal implementation sequence

This sequence is intentionally smaller than a new product platform.

### M1 — Canonicalize the formula

- Recover/rebuild the discovery lens mapping.
- Create `product_lens_registry.py`.
- Record primary vs supporting membership.
- Reconcile prototype values.
- Add unit tests.
- No API or frontend change.

### M2 — Build the pure service

- Add `matchup_lens_service.py`.
- Accept header and already-loaded ranking dictionaries.
- Produce lenses, gaps, strongest lenses, headlines, collisions, and readiness.
- No BigQuery write and no new table.

### M3 — Add previous meeting

- Add one read-only parameterized query.
- Make failure non-blocking.
- Test both orientations and phase exclusion.

### M4 — Add the response section

- Add `matchup_lens` to `/game` after ranking rows are loaded.
- Preserve all current response behavior.
- Skip non-final score query only if bounded tests prove parity.

### M5 — Connect the dashboard

- Replace static demo fixtures with the authenticated response.
- Implement loading, empty, stale, and error states.
- Preserve brand tokens.
- Add routes and sticky context.
- Keep the first screen compact.

### M6 — Reverse trace

- Derive trace graph from the response and registry.
- Ship List first.
- Add Network and Packed Groups only after the List interaction is stable.

### M7 — Time series later

- Add Momentum Shift only when data thresholds are met.
- Prefer queries/views before a physical table.

---

## 34. Relationship to Learning Lite

Matchup Lens and Learning Lite are related but not interchangeable.

- Matchup Lens is a product interpretation layer over the existing ranking spine.
- Learning Lite is the evidence-preservation and learning architecture.
- Matchup Lens must not reintroduce the archived six-table operational platform.
- A future canonical Pregame Snapshot should preserve the complete `/game` response, including an additive Matchup Lens section when present.
- Claim Extraction may later extract supported Matchup Lens statements from that frozen response, but the Matchup Lens build does not require Claim Extraction to launch.
- Matchup Lens implementation must not be smuggled into LL-2 unless the checkpoint is explicitly re-scoped and documented. LL-2’s current requirement is to preserve existing `/game` behavior while establishing the safe pregame boundary.

Recommended sequencing: complete or explicitly re-plan the relevant Learning Lite checkpoint, then authorize Matchup Lens as a bounded product checkpoint using the pure, no-new-table architecture above.

---

## 35. Decisions locked by this document

1. The product is game-first.
2. The entry page is a navigational dashboard, not a wall of charts.
3. Constellation is the primary six-lens comparison.
4. Lens Score is not win probability.
5. `weighted percentile` is not user-facing v1 terminology.
6. Event Pulse is removed.
7. Matchup/Advantage Map, Lens Galaxy, Lens Portrait, and tree map are not primary experiences.
8. Network and circle packing belong inside reverse trace.
9. Momentum Shift is retained but evidence-gated.
10. `seasonType` owns automatic window selection.
11. Rankings must be strictly pregame.
12. The existing ranking query is the main Matchup Lens source.
13. Lens calculations occur in the backend, not the frontend.
14. V1 adds no BigQuery table, view, scheduler, or coordinator.
15. Previous meeting is the only new read required by the recommended v1.
16. Missing or insufficient evidence is displayed honestly.
17. Every lens formula and registry change is versioned.
18. Prototype CSVs are evidence, not production formula authority.

---

## 36. Open decisions requiring explicit resolution

1. Recover the exact SQL that produced the six-lens discovery profile.
2. Decide whether supporting metrics contribute to any v1 Lens Score or only explain it.
3. Approve the canonical product-facing Lens Tag allowlist and display metadata.
4. Confirm whether the additive Matchup Lens response belongs in `/game` immediately or behind an `include` option.
5. Approve the minimum headline/collision coverage threshold.
6. Decide the data-lag threshold for a visible freshness warning.
7. Decide whether previous meeting loads with the main response or lazily.
8. Decide the exact Matchup Lens checkpoint relative to LL-2 through LL-4.
9. Approve frontend route names and whether the overview remains a tab inside the existing Lovable product.
10. Define when Last 3, Last 7, and Momentum Shift become user-selectable.

These decisions must not be answered implicitly through frontend constants.

---

## 37. Source inventory used for this specification

### Current repository evidence

- `app.py`
- `analytics/metric_registry.py`
- `agg/build_metric_facts.py`
- `agg/build_windowed_metrics.py`
- `agg/build_metric_rankings.py`
- `services/gamelens_metric_pipeline_conductor.py`
- `queries/game_queries.py`
- `services/game_service.py`
- `services/core_area_analysis.py`
- `services/model_trust_service.py`
- `routes/game_routes.py`
- `routes/games.py`
- `services/get_games_by_date.py`
- `tests/test_game_window_selection.py`

### Current Learning Lite authority

- `documentation/learning_lite/README.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Architecture.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Sprint.md`
- `documentation/learning_lite/GameLens_Learning_Lite_Salvage_Matrix.md`

### Product and discovery evidence

- `documentation/GameLens_Product_Ideas.md`
- `gamelens_complete_lens_metrics_2026.csv`
- `gamelens_latest_team_coverage_2026.csv`
- `gamelens_lens_inventory_2026.csv`
- `gamelens_team_lens_profiles_2026.csv`
- supplied Matchup Lens screenshots and iterative product feedback

### Historical-only evidence

- GameLens Backend August Readiness Plan
- preserved `dev` packet suite at commit `26287205f420f569d81ccfcb28a8e8e0656fc24b`

---

## 38. Change log

### 2026-08-25 — Initial Matchup Lens authority

- Consolidated product, data, query, infrastructure, navigation, and QA decisions.
- Positioned Matchup Lens beneath the Learning Lite checkpoint/architecture authority.
- Selected the existing ranking query plus a pure Python rollup as the smallest backend path.
- Defined season-aware automatic windows and strict pregame date boundaries.
- Preserved Constellation, Game Brief, collisions, reverse trace, previous meeting, and future Momentum Shift.
- Removed Event Pulse and demoted/rejected low-utility visualization experiments.
- Recorded the exact 2026 discovery coverage and reproducible Explosiveness/Turnover Watch examples.
- Explicitly recorded that the six-lens production formula still requires canonicalization and prototype-parity QA.
- Added proposed API, loading, navigation, accessibility, performance, security, observability, and implementation contracts.

