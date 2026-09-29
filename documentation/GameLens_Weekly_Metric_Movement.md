# GameLens idea: weekly metric movement

**Status:** Research note, 2026-09-29. No production calculation, ETL, API, or UI change.
**Backlog:** [IDEA-003](./GameLens_Product_Ideas.md#idea-003--opponent-adjusted-team-strength-and-week-to-week-form), with overlap in [IDEA-001 League Discovery](./GameLens_Product_Ideas.md#idea-001--league-discovery-tool--lens-tags-dashboard-concept).

## The idea in one sentence

Show **what changed in a team's football profile since last week**, even when its win/loss result points the other way.

For example, a team could lose but improve substantially in third-down efficiency or pass protection. That might be useful context for its next matchup. A "power ranking up 15" needs a precise definition: 15 places in an overall team ranking, 15 places in one metric, or 15 percentile points are three different claims. The current `team_metric_rankings_{season}` ranks individual metrics; it does **not** provide an overall team power ranking.

## What we could do with it

1. **Team movers:** "Despite the loss, Chicago improved from 24th to 9th in [metric]; its measured value moved from X to Y." Show the opposing context and sample size. These numbers are illustrative.
2. **Matchup trajectory:** "Both teams look close on season-to-date values, but Chicago's last-three-game pass protection is improving while Philadelphia's pressure rate is declining." Let the user see the two trends and their sources before drawing a conclusion.
3. **Postgame review:** Ask whether an improving pregame metric was genuinely relevant to the final game, using the saved pregame read and later facts. Label an unavailable pregame snapshot as unavailable.
4. **Early research signal:** Across historical games, check whether improvement adds information when the GameLens lean is thin or Low confidence. Start as a diagnostic; do not add points to the lean from one example.
5. **Opponent context:** Later, compare the movement against the strength of recent opponents. An improvement against strong competition may mean something different from the same movement against weak competition. This requires a separately defined, point-in-time opponent-strength source.

A clear user-facing shape could be a small **"Since last week"** section: 2–3 meaningful gains or declines, each with value, league rank/percentile, window, source date, and an explanation of whether the team actually played. A league-wide "movers" view could come later.

## What the current pipeline appears to support

The checked `learning-lite` source is `agg/build_metric_rankings.py` and `queries/game_queries.py`:

- Ranking grain is **season + as_of_date + window_type + metric + team_id**. Each as-of date carries forward the latest team value available on or before that date. Rows include `source_data_date` and `data_lag_days`.
- The builder creates a set of as-of dates from source game dates across the season/window, then writes the **full recomputed season table** by default with `if_exists="replace"` / `WRITE_TRUNCATE`. This retains many historical as-of *dates* in the current table; it does not make previous builds immutable.
- The `/game` ranking query takes the latest `as_of_date` **strictly before the game's date**. That is a sound pregame boundary for the existing ranking read.
- The existing IDEA-001 already proposes a weekly lens-tag profile with `previous_lens_rank` and `week_over_week_change`, but that view is a **proposal**, not a shipped calculation. This note begins at the simpler, directly inspectable metric level.
- The source code does not prove the live Cloud Scheduler cadence, whether current 2026 table rows cover every intended week, or whether late corrections have changed older as-of results. Verify those before making claims about actual available history.

## Smallest useful read-only proof

Pick one team and 2–3 safely ranked metrics over several completed regular-season weeks. For each NFL week, choose one common **post-week cutoff** (after the last game in that week); select the latest ranking `as_of_date <= cutoff`. Compare it with the prior week's common cutoff for the **same season, phase, window, and metric**.

Calculate and display:

| Field | Meaning |
|---|---|
| Prior/current metric value | Did the team's own measured performance move? |
| Prior/current rank and percentile | Did its league-relative position move? |
| Rank improvement | Prior rank minus current rank; positive means a move toward No. 1. |
| Source dates and lag | Did this team actually add a new game, or was its value carried forward? |
| Games in window and teams ranked | Is the comparison stable enough to interpret? |
| Prior result/opponent | Context for "lost but improved," not a substitute for the metric evidence. |

Review the underlying games before writing a headline. A rank can rise because *other teams* fell, without the team's own value improving. Season-to-date windows smooth movement; last-three-game windows react faster but can jump when an old game drops out. A metric's good/bad direction, data-quality flags, phase, and available team count must be respected. Display an unavailable week honestly; do not use postgame data from the matchup being previewed.

If the comparison is useful, turn the query into a view and evaluate a small "Since last week" surface. Only after comparing many games should we test whether momentum helps explain or calibrate Matchup Lean. A separate overall power-rating model remains an optional research path, not a prerequisite for this proof.
