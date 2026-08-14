# GameLens Tank01 Team-Metric Production Hotfix Runbook

- **Document status:** Local implementation and validation complete on the uncommitted hotfix branch; review is in progress; no merge, build, deployment, promotion, or production backfill has started
- **Created:** 2026-08-14
- **Owner and final decision maker:** Christian / GameLens product stewardship
- **Repository:** `csells10/meow`
- **Production release branch:** `main`
- **Long-lived development branch:** `dev`
- **Proposed temporary branch:** `codex/tank01-team-metric-hotfix` created from a freshly synchronized `main`
- **Production project:** `nfl-stream-406420`
- **Production service:** `nfl-games-app-main` in `us-central1`
- **Production Scheduler:** `Get-NFL-Schedule`
- **Source endpoint:** Tank01 NFL through RapidAPI, `GET /getNFLBoxScore`
- **Target completion:** Before the first 2026 regular-season game on 2026-09-09
- **Companion production-release evidence:** [go_plan.md](./go_plan.md)
- **Concurrent learning work:** [GameLens Learning Orchestration Product Sprint](./GameLens_Learning_Orchestration_Product_Sprint.md)

---

## 1. Authority, purpose, and execution boundary

This document is the authoritative plan for one bounded production data-capture hotfix. It exists because the production Stats parser currently receives legitimate Tank01 team statistics that are either:

1. parsed into `Analytics.game_metrics_flat` but excluded downstream because they are not registered;
2. already parsed and registered but may be silently recorded as zero when the source field is absent; or
3. present in Tank01's team-level response but not parsed at all.

This hotfix is separate from the Packet 3 learning sprint. It must not pull unfinished Level 1-4 learning work from `dev` into production.

This document describes expected work and release gates. It does **not** itself authorize:

- source-code changes;
- branch creation or switching;
- API requests that consume Tank01 quota;
- writes to dev or production Google Cloud resources;
- merging or pushing `main`;
- deploying, promoting, retrying, backfilling, or repairing production; or
- changing the production Scheduler.

Christian remains the final decision maker. The detailed procedure is operator guidance, not a requirement for him to approve every read-only check. His future approval is required at four practical boundaries: start implementation, merge/build the production candidate, promote the candidate, and perform the production backfill.

---

## 2. Executive decision

The agreed release shape is:

```text
Read-only scope and baseline
-> create one branch from main
-> complete the bounded metric changes
-> local tests and historical isolated validation
-> merge into main
-> build a no-traffic production candidate
-> validate the candidate without production writes
-> deliberately promote the candidate
-> merge the production fix forward into dev
-> confirm the production revision and traffic
-> observe the first naturally eligible scheduled Stats run
```

The production fix must not sit only on `dev` until September. If it did, production would continue using the old parser and registry during preseason and possibly into the regular season.

The release uses a short-lived branch from `main` because `main` is the production release branch. After production promotion, updated `main` is merged forward into `dev` immediately so there is no long-running side-by-side implementation.

### 2.1 Lean execution model

This is a small, bounded data-capture release for a hobby product with fewer than ten users. The detailed sections below preserve technical facts and rollback information, but the practical path is seven outcomes:

1. Refresh the branch and production baseline, then create one short-lived branch from current `main`.
2. Implement only the approved parser, registry, snap-presence, and focused test changes.
3. Validate the supplied historical response plus one retained 2026 preseason response locally or in memory.
4. Review the focused diff, merge it into `main`, and build the 0%-traffic candidate.
5. Validate the candidate read-only, obtain Christian's promotion approval, and move traffic to it.
6. Merge released `main` forward into `dev`, confirm the production revision, and observe the next naturally eligible run.
7. Observe naturally eligible responses for valid snap counts, including historical regular-season responses, and decide later whether a delayed refresh or bounded backfill is worthwhile.

The release does **not** require a nonzero historical example for every rare event, a new core-area taxonomy, a new BigQuery schema, or 13 separate approvals from Christian.

---

## 3. Current repository and release baseline

The following values were observed on 2026-08-14 and must be refreshed before execution:

| Item | Observed baseline |
|---|---|
| Local active branch | `dev` |
| Local working tree | Clean and synchronized with `origin/dev` |
| `dev` head | `83b7cb2` - Packet 3 documentation checkpoint |
| `main` head | `77f4a06` - production release line |
| Branch divergence | `dev` is 32 commits ahead; `main` has no commits absent from `dev` |
| Aggregate difference | 27 files, approximately 7,470 insertions and 204 deletions |
| Open GitHub pull requests | None observed |
| Production trigger | Enabled for `^main$` according to current live documentation |
| Dev trigger | Recorded as disabled in current live documentation |

The current `main` to `dev` difference does **not** include changes to the principal hotfix files:

- `api_calls/api_utils/parse_nfl_stats.py`;
- `api_calls/api_utils/validate_nfl_boxscore.py`;
- `analytics/metric_registry.py`;
- `agg/build_metric_facts.py`; or
- `cloudbuild.yaml`.

That makes a main-based hotfix unusually clean: the relevant production files currently match between `main` and `dev`, even though the learning branch is substantially ahead elsewhere.

Do not rely on this snapshot during execution. Re-run the comparisons before creating the branch and again before merging.

---

## 4. Source API contract

Production Stats calls:

```text
Host: tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com
Method: GET
Path: /getNFLBoxScore
Required query: gameID=<Tank01 game ID>
Production options:
  playByPlay=false
  fantasyPoints=false
```

The loader validates the completed box score, uploads a raw-response backup to the configured GCS bucket, parses team metrics, inserts long-form metric rows into BigQuery, and marks the game loaded.

The production response must be evaluated at the team-game grain:

```text
one final game
x two identified teams
x zero or more long-form metric rows per team
```

Player statistics, individual player snap counts, scoring-play records, and play-by-play events are different grains and are outside this hotfix.

### 4.1 Historical example already inspected

The supplied Tank01 example is for:

```text
gameID: 20241020_CAR@WSH
status: Completed
score: Washington 40, Carolina 7
```

It provides useful real evidence:

| Evidence | Carolina | Washington | Reconciliation |
|---|---:|---:|---|
| `turnovers` | 2 | 0 | Equals `interceptionsThrown + fumblesLost` for both teams |
| `interceptionsThrown` | 2 | 0 | Supports turnovers-as-giveaways interpretation |
| `fumblesLost` | 0 | 0 | Supports turnovers-as-giveaways interpretation |
| `DST.defTD` | 0 | 1 | Washington recorded an interception-return touchdown |
| `teamStats.defensiveOrSpecialTeamsTds` | 0 | 1 | Same as `DST.defTD` in this game, but broader by name |
| `penalties` | `6-59` | `8-55` | Count-yards composite |
| Offensive snaps | 43 | 69 | Mirrors opponent defensive snaps |
| Defensive snaps | 69 | 43 | Mirrors opponent offensive snaps |
| Special-teams snaps | 26 | 26 | Explicitly present |

This example proves presence and shape. It does not alone prove every provider convention, especially how Tank01 represents absent optional fields or attributes rare nonzero events.

### 4.2 Current-shape confirmation

Before implementation, make one authorized read-only request using the exact production query shape against a completed current-season game. Keep the response in memory. Compare:

- top-level status, identity, date, and score fields;
- `body.teamStats.home` and `.away` keys;
- `body.DST.home` and `.away` keys;
- `teamStats.*.snapCounts`;
- explicit zero versus absent optional fields; and
- any new team-level fields not represented in this plan.

One current request confirms schema shape. It is not required to contain every rare nonzero event.

---

## 5. Metric inventory and intended treatment

### 5.1 Group A - parsed today but excluded because unregistered

These eight metric names caused the production warning `unregistered_source_metrics_excluded`. Their rows are already present in `Analytics.game_metrics_flat` for processed games.

| Existing metric key | Tank01 source | Meaning to register | Direction | Initial product use |
|---|---|---|---|---|
| `blocked_fg` | `teamStats.blockedFG` | Field goals blocked by the team | Higher is better | Supporting edge evidence; not confidence-driving |
| `blocked_punt` | `teamStats.blockedPunt` | Punts blocked by the team | Higher is better | Supporting edge evidence; not confidence-driving |
| `blocked_xp` | `teamStats.blockedXP` | Extra points blocked by the team | Higher is better | Supporting edge evidence; not confidence-driving |
| `defensive_tds` | `DST.defTD` | Defensive touchdowns | Higher is better | Supporting and volatile; not confidence-driving |
| `penalty_count` | First number in `teamStats.penalties` | Accepted penalties committed | Lower is better | Supporting discipline evidence |
| `penalty_yards` | Second number in `teamStats.penalties` | Accepted penalty yards charged | Lower is better | Supporting discipline evidence |
| `safeties` | `DST.safeties` after merge precedence | Safeties produced by the defense | Higher is better | Supporting and rare; defensive, not special-teams-only |
| `turnovers` | `teamStats.turnovers` | Turnovers committed / giveaways | Lower is better | Supporting ball-security evidence |

Implementation principles:

- Preserve the existing internal key `turnovers` for compatibility, but label and define it as **Turnovers Committed** or **Giveaways**.
- Do not classify `turnovers` as a defensive takeaway metric.
- Reconcile `turnovers` against `interceptions_thrown + fumbles_lost` when both components are present.
- Let registry metadata override the parser's legacy category and core-area labels in downstream Facts.
- Do not let rare volume counts automatically drive Core Area Advantage or confidence.
- Add all registered keys to `EXPECTED_METRICS`; the registry validates exact coverage at import time.

### 5.2 Group B - snaps already parsed and registered

The current parser and registry already include:

- `total_offensive_snaps`;
- `total_defensive_snaps`;
- `total_special_teams_snaps`;
- `total_snaps`;
- `offensive_snap_load`;
- `defensive_snap_load`; and
- `special_teams_snap_pct`.

Their current registry posture is already complete and should be preserved rather than duplicated:

| Existing snap metric | Direction / ranking use | Data quality | Current `lens_tags` |
|---|---|---|---|
| `total_offensive_snaps` | Context / context-only | Good | `snap-volume`, `offensive-opportunity`, `pace` |
| `offensive_snap_load` | Context / context-only | Good | `snap-share`, `offensive-opportunity`, `pace` |
| `total_defensive_snaps` | Context / context-only | Good | `defensive-workload`, `fatigue`, `snap-volume` |
| `defensive_snap_load` | Context / context-only | Watch | `defensive-workload`, `fatigue`, `snap-share`, `data-quality-watch` |
| `total_special_teams_snaps` | Context / context-only | Good | `special-teams`, `snap-volume`, `usage-context` |
| `special_teams_snap_pct` | Context / context-only | Good | `special-teams`, `snap-share`, `usage-context` |
| `total_snaps` | Context / context-only | Good | `snap-volume`, `formula-ingredient`, `pace` |

Do **not** add duplicate snap metrics. First trace one completed game through:

```text
Tank01 response
-> parse_game_stats output
-> Analytics.game_metrics_flat
-> Analytics.game_team_metric_facts_2026
-> Analytics.team_metrics_windowed_2026
-> Analytics.team_metric_rankings_2026
-> any API or UI selection filter
```

The known parser risk is that absent `snapCounts` currently becomes numeric zero through `.get(..., 0)`. For a completed NFL game, missing offensive or defensive snap data is not equivalent to a real zero.

Target behavior:

- if a snap field is present and numeric, capture it;
- if the entire `snapCounts` group is absent, omit all dependent snap rows;
- if only one snap field is absent, omit that field and any derived metric that requires it;
- never manufacture a snap percentage from incomplete components;
- do not warn merely because `snapCounts` is absent during the observation period; and
- preserve explicit numeric zero when the provider actually sends zero.

### 5.3 Group C - present in Tank01 but not parsed today

The inspected team-level example includes six legitimate fields that the current parser ignores:

| Tank01 field | Approved internal key | Meaning | Missing-field posture | Initial product use |
|---|---|---|---|---|
| `passingFirstDowns` | `passing_first_downs` | First downs gained through passing | Omit silently if absent | Context/supporting offensive production |
| `rushingFirstDowns` | `rushing_first_downs` | First downs gained through rushing | Omit silently if absent | Context/supporting offensive production |
| `firstDownsFromPenalties` | `first_downs_from_penalties` | First downs awarded through opponent penalties | Omit silently if absent | Context; do not treat as self-created offense |
| `twoPointConversions` | `two_point_conversions` | Successful offensive two-point conversions | Field-specific zero/absence rule after source confirmation | Rare supporting scoring event |
| `defensiveTwoPointConversionReturns` | `defensive_two_point_returns` | Defensive returns producing two points | Field-specific zero/absence rule after source confirmation | Rare context/supporting event |
| `defensiveOrSpecialTeamsTds` | `defensive_or_special_teams_tds` | Combined defensive and special-teams touchdowns | Omit silently if absent | Context only until overlap is handled explicitly |

Required consistency check:

```text
passing_first_downs
+ rushing_first_downs
+ first_downs_from_penalties
= total first_downs
```

Allow for provider-defined exceptions, but record any real mismatch during historical validation.

Do not double-count `defensive_or_special_teams_tds` alongside `defensive_tds`. The combined field may be captured for completeness, but it should initially remain context-only and excluded from confidence or automatic Core Area Advantage.

### 5.4 Explicitly out of scope

This hotfix does not add:

- player-level statistics;
- player-level offensive, defensive, or special-teams snap participation;
- play-by-play-derived metrics;
- scoring-play parsing;
- field-goal, punt, kick-return, or player fantasy-stat expansions beyond the approved team-level list;
- new BigQuery columns;
- new metric tables;
- Level 1-4 learning behavior;
- `/game` claim-language changes; or
- frontend presentation changes.

If the current Tank01 call reveals more fields, add them to an inventory appendix and decide separately. Do not enlarge implementation merely because a field exists.

### 5.5 Complete `metric_registry.py` contract

Adding a key to `METRIC_REGISTRY` is not enough. The registry factory returns 20 required metadata fields, and import-time validation requires exact agreement between `METRIC_REGISTRY` and `EXPECTED_METRICS`.

The exact Python field is `lens_tags` - plural, represented as a list of strings. There is no singular `lens_tag` field. Although the current validator only requires `lens_tags` to be a list, this release adopts the stronger requirement that every new metric have a deliberate, non-empty tag list.

#### 5.5.1 Required metadata checklist

| Required field | Requirement for this release |
|---|---|
| `label` | Plain-language display name that does not overstate source semantics |
| `definition` | Exact event or count represented, including which team committed or produced it |
| `category` | Existing category where practical; a clear new subcategory only when needed |
| `core_area` | Exactly one of the five allowed registry core areas |
| `comparison_direction` | Exactly `higher`, `lower`, or `context` |
| `higher_is_better` | Derived by `_metric`: `True`, `False`, or `None` from comparison direction |
| `raw_or_derived` | `raw` for all 14 approved additions |
| `aggregation_method` | `sum` for all 14 approved additions |
| `numerator` | `None`; none of these additions is a derived ratio or per-game metric |
| `denominator` | `None`; none of these additions is a derived ratio |
| `format` | `integer` |
| `decimals` | `0` |
| `notes` | Interpretation, volatility, overlap, and safe-use guidance |
| `ranking_usage` | Explicitly `edge` or `context_only`; do not rely on factory defaults for this release |
| `signal_strength` | Explicitly `supporting` or `context` |
| `edge_language_allowed` | Explicit Boolean consistent with ranking use and direction |
| `include_in_core_area_advantage` | Explicitly `False` for all 14 initial registrations |
| `confidence_eligible` | Explicitly `False` for all 14 initial registrations |
| `data_quality_status` | `good` after source confirmation, except the overlapping combined touchdown field starts as `watch` |
| `lens_tags` | Non-empty list of unique lowercase strings, using kebab-case for multiword tags and existing vocabulary where possible |

Every approved key must also be added to `EXPECTED_METRICS`. The registry must import successfully, and the exact-coverage validator must report neither missing nor extra keys.

#### 5.5.2 Shared storage and aggregation metadata

All 14 approved metrics share these exact values:

| Field | Value |
|---|---|
| `raw_or_derived` | `raw` |
| `aggregation_method` | `sum` |
| `numerator` | `None` |
| `denominator` | `None` |
| `format` | `integer` |
| `decimals` | `0` |

This means the windowed pipeline sums source event counts. It does not create a new ratio, normalize by games, or require a new BigQuery column. Any future per-game or rate metric must be proposed as a separate derived registry entry with its own formula metadata and validation.

#### 5.5.3 Approved semantic metadata - original eight

These values were approved on 2026-08-14. They are documented but not yet implemented.

| Key | `label` | `definition` | `category` | `core_area` | Direction / `higher_is_better` |
|---|---|---|---|---|---|
| `blocked_fg` | Blocked Field Goals | Opponent field-goal attempts blocked by the team. | Special Teams Disruption | Field Control (Special Teams) | `higher` / `True` |
| `blocked_punt` | Blocked Punts | Opponent punts blocked by the team. | Special Teams Disruption | Field Control (Special Teams) | `higher` / `True` |
| `blocked_xp` | Blocked Extra Points | Opponent extra-point attempts blocked by the team. | Special Teams Disruption | Field Control (Special Teams) | `higher` / `True` |
| `defensive_tds` | Defensive Touchdowns | Touchdowns credited to the team defense by Tank01 `DST.defTD`. | Defensive Scoring | Disruption and Turnovers | `higher` / `True` |
| `penalty_count` | Accepted Penalties Committed | Team penalties reported in the count component of Tank01's count-yards `penalties` field. | Team Discipline | Offensive Output | `lower` / `False` |
| `penalty_yards` | Accepted Penalty Yards | Penalty yards charged to the team in the yards component of Tank01's count-yards `penalties` field. | Team Discipline | Offensive Output | `lower` / `False` |
| `safeties` | Defensive Safeties | Safeties credited to the team defense by Tank01 `DST.safeties`. | Defensive Scoring | Disruption and Turnovers | `higher` / `True` |
| `turnovers` | Turnovers Committed | Offensive giveaways committed by the team, expected to reconcile to interceptions thrown plus fumbles lost when all components are present. | Turnovers | Disruption and Turnovers | `lower` / `False` |

`penalty_count` and `penalty_yards` are team-wide discipline measures, but the registry currently requires one of five core areas and has no Team Discipline core area. `Offensive Output` is the approved placement because penalties commonly suppress drive output. Both metrics remain excluded from Core Area Advantage and confidence. This hotfix does not create a sixth core area.

#### 5.5.4 Approved semantic metadata - six newly parsed fields

| Key | `label` | `definition` | `category` | `core_area` | Direction / `higher_is_better` |
|---|---|---|---|---|---|
| `passing_first_downs` | Passing First Downs | First downs gained by the team through passing plays. | Offensive Rhythm | Offensive Output | `higher` / `True` |
| `rushing_first_downs` | Rushing First Downs | First downs gained by the team through rushing plays. | Offensive Rhythm | Offensive Output | `higher` / `True` |
| `first_downs_from_penalties` | First Downs From Penalties | First downs awarded to the team because of opponent penalties. | Offensive Rhythm | Offensive Output | `context` / `None` |
| `two_point_conversions` | Two-Point Conversions | Successful offensive two-point conversions completed by the team. | Scoring Production | Scoring Efficiency | `higher` / `True` |
| `defensive_two_point_returns` | Defensive Two-Point Conversion Returns | Defensive returns of an opponent conversion attempt that produced two points for the team. | Defensive Scoring | Disruption and Turnovers | `higher` / `True` |
| `defensive_or_special_teams_tds` | Defensive or Special Teams Touchdowns | Combined touchdowns credited by Tank01 to the team defense or special teams. | Non-Offensive Scoring | Disruption and Turnovers | `higher` / `True` |

The combined touchdown field uses `Disruption and Turnovers` only as a required registry placement. Because the source combines two phases and can overlap `defensive_tds`, it starts context-only, cannot claim an edge, and cannot contribute to Core Area Advantage or confidence.

#### 5.5.5 Approved ranking and safety controls

All control values are explicit even where `_metric` could currently derive the same default. This prevents a future default change from silently changing product behavior.

| Key | `ranking_usage` | `signal_strength` | `edge_language_allowed` | `include_in_core_area_advantage` | `confidence_eligible` | `data_quality_status` |
|---|---|---|---:|---:|---:|---|
| `blocked_fg` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `blocked_punt` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `blocked_xp` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `defensive_tds` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `penalty_count` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `penalty_yards` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `safeties` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `turnovers` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `passing_first_downs` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `rushing_first_downs` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `first_downs_from_penalties` | `context_only` | `context` | `False` | `False` | `False` | `good` |
| `two_point_conversions` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `defensive_two_point_returns` | `edge` | `supporting` | `True` | `False` | `False` | `good` |
| `defensive_or_special_teams_tds` | `context_only` | `context` | `False` | `False` | `False` | `watch` |

Here, `edge_language_allowed=True` means the metric may support restrained higher/lower wording after enough prior-game evidence exists. It does not make the metric a primary signal. Every new count begins with `include_in_core_area_advantage=False` and `confidence_eligible=False`, so widening capture cannot by itself alter the overall Core Area winner or confidence score.

#### 5.5.6 Approved `lens_tags` and `notes`

Tags are functional metadata for grouping and future lens selection, not decorative keywords. Use the approved tag spellings and notes text below. Do not add aliases that express the same concept with different punctuation or wording.

| Key | Approved `lens_tags` | Approved `notes` text |
|---|---|---|
| `blocked_fg` | `special-teams`, `blocked-kicks`, `field-goal-defense`, `disruption`, `rare-event`, `volatility` | Special-teams disruption count; higher is useful, but rare and volatile, so it must not drive a core-area winner or confidence by itself. |
| `blocked_punt` | `special-teams`, `blocked-kicks`, `punt-pressure`, `field-position`, `disruption`, `rare-event`, `volatility` | Punt-pressure and field-position disruption; rare and attempt-sensitive. |
| `blocked_xp` | `special-teams`, `blocked-kicks`, `extra-point-defense`, `disruption`, `rare-event`, `volatility` | Extra-point disruption; a rare scoring event that should speak only as supporting evidence. |
| `defensive_tds` | `defense`, `defensive-scoring`, `takeaways`, `swing-play`, `rare-event`, `volatility` | Defensive scoring production; impactful but volatile and partly dependent on return opportunity and game state. |
| `penalty_count` | `penalties`, `discipline`, `drive-killers`, `team-wide`, `supporting` | Team-wide discipline count parsed from the Tank01 composite; lower is generally better, but penalty type and situation are not represented. |
| `penalty_yards` | `penalties`, `discipline`, `field-position`, `drive-killers`, `team-wide`, `supporting` | Team-wide penalty-yard cost; lower is generally better, but declined/offsetting detail and play context are not represented. |
| `safeties` | `defense`, `defensive-scoring`, `safeties`, `field-position`, `rare-event`, `volatility` | Defensive scoring and field-position disruption; rare enough to remain supporting rather than confidence-driving. |
| `turnovers` | `turnovers`, `giveaways`, `ball-security`, `risk`, `supporting` | This is turnovers committed, not takeaways. Reconcile to interceptions thrown plus fumbles lost when all components exist. |
| `passing_first_downs` | `drive-sustainability`, `passing-production`, `offensive-output`, `volume-sensitive` | Passing contribution to first-down production; higher is useful but volume-sensitive and overlaps total first downs. |
| `rushing_first_downs` | `drive-sustainability`, `rushing-production`, `offensive-output`, `volume-sensitive` | Rushing contribution to first-down production; higher is useful but volume-sensitive and overlaps total first downs. |
| `first_downs_from_penalties` | `drive-sustainability`, `penalties`, `opponent-penalties`, `offensive-context` | Context for how first downs were awarded; do not describe it as offense-created production or use it for edge language. |
| `two_point_conversions` | `scoring`, `two-point-conversions`, `situational`, `rare-event`, `volatility` | Successful two-point scoring; higher is useful, but attempts are sparse and strongly situation-dependent. |
| `defensive_two_point_returns` | `defense`, `defensive-scoring`, `two-point-return`, `swing-play`, `rare-event`, `volatility` | Rare defensive scoring event; supporting evidence only and not confidence-driving. |
| `defensive_or_special_teams_tds` | `defense`, `special-teams`, `non-offensive-scoring`, `combined-source-metric`, `overlap-risk`, `context` | Combined cross-phase source metric. Preserve for context, never add to `defensive_tds` without de-duplication, and do not use for edge or confidence. |

#### 5.5.7 Registry acceptance checks

Before Gate 3 is complete, tests must prove:

1. every approved key appears exactly once in both `EXPECTED_METRICS` and `METRIC_REGISTRY`;
2. every entry returns all 20 `REQUIRED_FIELDS`;
3. all labels, definitions, and notes are non-empty;
4. every core area and enum value is valid;
5. `higher_is_better` matches comparison direction;
6. every `lens_tags` value is a non-empty list of unique, non-blank strings;
7. each tag is lowercase and uses kebab-case for multiword values consistently with existing registry vocabulary;
8. context-only metrics have `signal_strength="context"` and all three product-influence flags set to `False`;
9. supporting edge metrics have an explicit higher/lower direction but remain excluded from Core Area Advantage and confidence; and
10. no new metric introduces numerator/denominator dependencies or a derived formula accidentally.

---

## 6. Missing, zero, malformed, and unknown field policy

### 6.1 Core rule

```text
Missing is not automatically zero.
Zero is not automatically missing.
An optional missing field is not automatically unhealthy.
```

The parser must use field-specific behavior rather than applying one global default.

### 6.2 Field classes

| Class | Examples | If absent | If malformed | Logging |
|---|---|---|---|---|
| Required payload contract | Body, final status, game ID/date, scores, two team identities | Reject game | Reject game | One game-level rejection/error |
| Meaningful completed-game evidence | Total yards, total plays, first downs, passing/rushing yards | Reject if the payload lacks all meaningful evidence | Reject affected payload | One game-level rejection/error |
| Provider-enriched group | `snapCounts` on a final game | Continue without the group and omit snap rows | Reject malformed supplied snap values | No warning for absence during the observation period |
| Optional descriptive field | First-down breakdowns | Omit metric row | Omit affected metric or reject malformed supplied value according to locked policy | No warning for absence |
| Optional sparse event count | Blocks, safeties, two-point events | Omit the metric row; do not infer zero | Supplied nonnumeric value is malformed | No warning for legitimate absence; no warning for explicit zero |
| Unknown new team field | A future Tank01 addition | Ignore safely | N/A | Debug/inventory only, not a production warning per field |

### 6.3 Warning budget

The release must not replace one useful warning with dozens of optional-field warnings.

Expected behavior:

- zero warnings for each individually absent optional statistic;
- zero warnings for explicit successful zero values;
- zero warnings merely because `snapCounts` is absent;
- one rejection event for a demonstrably unusable completed-game payload;
- zero `unregistered_source_metrics_excluded` warnings for the approved registered metric set after release; and
- no raw log dump containing the entire Tank01 response.

Snap availability is an observation item, not a production warning condition. A later refresh design can add one bounded coverage summary if natural regular-season evidence shows that the operational timing matters.

### 6.4 Reconciliation is stronger than extra warnings

Prefer stable cross-field checks over field-presence noise:

- `turnovers == interceptionsThrown + fumblesLost` when all three exist;
- first-down components reconcile to total first downs when all components exist;
- home offensive snaps equal away defensive snaps when all four values exist;
- away offensive snaps equal home defensive snaps when all four values exist;
- two distinct identified teams are present; and
- final scores are numeric.

Reconciliation mismatches should be summarized once per game. They should not emit one warning for every constituent field.

---

## 7. Data storage and downstream consequences

### 7.1 No BigQuery schema expansion is expected

GameLens stores metrics in long form. New metrics create new rows with an existing `metric` key and `value`; they do not require a new physical column per metric.

Registry metadata supplies the label, definition, category, core area, comparison direction, format, aggregation method, ranking usage, quality status, and lens tags downstream.

A schema change is necessary only if this release invents a new metadata attribute not supported by current tables. That is out of scope.

### 7.2 Original eight metrics

The original eight already exist in `Analytics.game_metrics_flat`. The Facts builder currently logs and excludes them because they are not in `METRIC_REGISTRY`.

Once registered, a full season-to-date rebuild can admit their earlier source rows:

```text
Analytics.game_metrics_flat
-> Analytics.game_team_metric_facts_2026
-> Analytics.team_metrics_windowed_2026
-> Analytics.team_metric_rankings_2026
```

The Facts builder filters by season and replaces the season Facts table when run with the normal replace behavior. The downstream builders likewise regenerate the season-consistent windows and rankings.

### 7.3 Six newly parsed fields

The six Group C fields were never written into `game_metrics_flat`. Deploying the parser only captures them for games processed after promotion.

Historical recovery options, in preferred order:

1. Reparse retained raw `nfl_boxscore_<gameID>.json` objects from GCS.
2. If a retained object is missing or unusable, make an explicitly approved historical Tank01 request by `gameID`.
3. If neither is available, document the game/field gap honestly.

The normal retry-safe Stats loader will not automatically add these metrics to games already considered complete. Its reconciliation returns early when both expected teams already have rows. Therefore, a historical backfill requires a deliberate selected-game reparse and replacement strategy; simply rerunning the daily loader is not a backfill.

The retained 2026 Stats object inspected during local validation stores the full Tank01 response inside one backup envelope: `{"body": <Tank01 response>}`. A backfill reader must unwrap that outer backup envelope exactly once before calling `validate_nfl_boxscore` or `parse_game_stats`. Passing the stored wrapper directly to the live-response validator correctly fails because the outer object does not carry the game's final-status fields.

Production historical backfill is a separate approval boundary from code promotion.

### 7.4 Rollback behavior of new long-form rows

If the new production revision is rolled back after ingesting rows:

- previously stored raw-response backups remain available;
- old registry code will exclude newly unrecognized metric keys rather than require a schema rollback;
- existing known metric rows remain compatible; and
- any correction or deletion of demonstrably wrong rows is a separate data-repair decision.

Do not automatically delete new metric rows merely because application traffic rolls back.

---

## 8. Historical validation strategy

A future live game is not required to prove parser correctness.

Use three real completed payload shapes as deterministic source evidence:

1. the supplied `20241020_CAR@WSH` response, which covers turnovers through interceptions, a defensive touchdown, penalties, first-down components, two-point fields, and nonzero snaps; and
2. one retained completed 2026 preseason response, which confirms that a valid completed payload may omit snaps entirely; and
3. the fresh historical `20250907_CAR@JAX` response, which proves that Tank01 currently supplies valid regular-season team and player snap counts.

That is sufficient historical evidence for this release. A nonzero blocked kick, safety, defensive two-point return, or two-point conversion is helpful if it occurs naturally, but is not a release gate. Focused test fixtures cover explicit zero, absent optional fields, malformed supplied values, and missing snap components without consuming API quota or searching the full historical catalog.

### 8.1 Local validation evidence - 2026-08-14

| Payload | Validation | Parsed result | Important reconciliation |
|---|---|---|---|
| Supplied `20241020_CAR@WSH` response | Accepted | 144 total rows; all 14 approved metrics for each team | Both first-down component sums equal total first downs; emitted metric set has zero unregistered names |
| Retained `gs://xtra_point/nfl_boxscore_20260806_CAR@ARI.json` | Accepted after unwrapping the known backup envelope | 130 total rows; all 14 approved metrics for each team | Source contains no `snapCounts`; parser emits zero snap rows instead of fabricated zeros |
| Fresh Tank01 `20250907_CAR@JAX` response | Accepted source shape | Snap regression fixture uses CAR `64/66/22` and JAX `66/64/22` offensive/defensive/special-teams counts | The retained response originally lacked snaps and BigQuery contains fabricated zero rows from the old parser; the current endpoint now supplies valid historical counts |
| Fresh Tank01 `20260806_CAR@ARI` response | Accepted source shape | No snap rows | The same preseason game still contains no team- or player-level snap key on 2026-08-14, so absence is not treated as failure |

The Facts metadata transformation was also exercised in memory with all 14 approved keys: 14 rows entered, 14 rows remained, all `lens_tags` values remained lists, penalties resolved to `Offensive Output`, the combined touchdown field resolved to `context_only`, and no unregistered-metric warning was emitted.

### 8.2 Test matrix

| Case | Expected outcome |
|---|---|
| Complete real historical payload | Two teams parsed; approved metric values match source |
| Explicit optional zero | Metric zero retained without warning |
| Optional field absent | Game remains accepted; field-specific omit/zero rule applied; no per-field warning |
| Entire `snapCounts` absent | Game remains accepted if otherwise valid; no snap rows and no fabricated zeroes |
| One snap component absent | Missing component and dependent ratios omitted; available independent values handled by policy |
| Malformed supplied optional value | No fabricated zero; one bounded diagnostic or rejection according to class |
| Turnovers plus components | Reconciliation passes or one summarized mismatch is visible |
| First-down breakdown plus total | Reconciliation passes or one summarized mismatch is visible |
| Duplicate execution against isolated data | No duplicate team-game-metric rows |
| Registry exact coverage | Every approved key exists once in both `EXPECTED_METRICS` and `METRIC_REGISTRY` |
| Registry metadata contract | All 20 required fields resolve to the approved values |
| Lens-tag contract | Every new `lens_tags` list is non-empty, unique, normalized, and matches the approved worksheet |
| Product-influence controls | All 14 additions remain excluded from Core Area Advantage and confidence |

### 8.3 Isolated BigQuery load evidence - 2026-08-14

The committed branch was validated against the fresh historical
`20250907_CAR@JAX` response using two isolated `Analytics_dev` tables cloned
from the existing dev source and Facts schemas:

- `qa_tank01_game_metrics_flat_c6f78e2`, eight fields;
- `qa_tank01_metric_facts_c6f78e2`, 39 fields with `lens_tags` as
  `REPEATED STRING`.

Both tables expire automatically on 2026-08-15 at approximately 20:19 UTC.
The test kept the Tank01 response in memory and did not call
`save_raw_response`, write GCS, touch production, replay a production game, or
modify the canonical dev tables.

| Check | Source QA | Facts QA |
|---|---:|---:|
| Rows | 144 | 144 |
| Distinct metrics | 72 | 72 |
| Teams | 2 | 2 |
| Approved metric rows | 28 | 28 |
| Approved metric names | 14 | 14 |
| Snap rows | 14 | 14 |
| Fabricated zero snap rows | 0 | 0 |
| Duplicate game/team/metric rows | 0 | 0 |
| Required-field issues | 0 | 0 |
| Missing/empty `lens_tags` | N/A | 0 |
| Source-to-Facts value mismatches | N/A | 0 |

The first one-off Facts adapter attempt included the source-only `data_date`
field. BigQuery correctly rejected every row and left the Facts QA table empty.
The adapter was corrected to enforce the exact existing 39-field Facts schema,
after which all 144 rows loaded. This was a QA harness correction, not an
application-code change.

---

## 9. Expected implementation surface

The exact diff is chosen only after the branch is created and current `main` is reread. The likely files are:

| File | Expected purpose |
|---|---|
| `api_calls/api_utils/parse_nfl_stats.py` | Field mappings, optional-presence handling, snap behavior, reconciliation support |
| `api_calls/api_utils/validate_nfl_boxscore.py` | Validate newly supplied numeric fields without requiring optional absence |
| `analytics/metric_registry.py` | Register approved keys and complete required metadata; update `EXPECTED_METRICS` |
| `tests/api_calls/test_parse_nfl_stats.py` | New direct parser contract tests using realistic payloads |
| `tests/api_calls/test_validate_nfl_boxscore.py` | Optional-field, malformed-field, and snap validation cases |
| `tests/api_calls/test_api_call_nfl_stats.py` | Loader/reconciliation behavior only if the parser contract changes its integration assumptions |
| This runbook and live index | Record final decisions, evidence, and release status |

No change is expected in `agg/build_metric_facts.py` unless implementation evidence proves the current registry filtering or season rebuild behavior cannot support the approved metrics.

No change is expected in `cloudbuild.yaml`; it already builds a no-traffic production candidate tagged `packet4-candidate`.

---

## 10. Detailed operator procedure

These gates preserve sequencing, stop conditions, and rollback evidence. They are not 13 separate checkboxes for Christian. Use the four practical approval boundaries in Section 2.1; routine read-only checks and tests within an approved phase do not need individual approvals.

### Gate 0 - Review and lock the plan

**Goal:** Agree on scope and expected behavior before changing code.

Decisions recorded on 2026-08-14:

- approve the original eight registrations and the six Group C internal keys;
- approve the complete Section 5.5 metadata worksheet, including `lens_tags`;
- retain the existing five core areas and place `penalty_count` and `penalty_yards` in `Offensive Output`, category `Team Discipline`, with Core Area Advantage and confidence disabled;
- preserve an explicit numeric zero, but omit an absent optional sparse field without warning rather than proving every Tank01 omission convention;
- capture `defensive_or_special_teams_tds` now as context-only with overlap protection;
- include both two-point fields in the first release;
- validate against the supplied historical response plus one retained 2026 preseason response; no exhaustive rare-event hunt is required;
- defer any delayed refresh or backfill until natural or historical valid snap-count evidence is reviewed; no missing-snap warning or production replay is part of this release; and
- approve the short-lived main-based branch and implementation approach; Christian lifted the no-code hold on 2026-08-14.

**Exit evidence:** Scope approved and local implementation authorized.
**Production impact:** None.

### Gate 1 - Read-only baseline and source inventory

**Goal:** Refresh every assumption against current GitHub, Google Cloud, BigQuery, and Tank01 state.

Read-only checks:

1. Confirm local `main`, local `dev`, `origin/main`, and `origin/dev` heads.
2. Confirm the working tree is clean.
3. Confirm no relevant hotfix file changed unexpectedly on `dev`.
4. Confirm open PR and branch state.
5. Describe the production Cloud Build trigger and verify it matches `^main$` only.
6. Confirm the dev trigger posture.
7. Describe `nfl-games-app-main`; record current serving revision, traffic, configuration, and rollback revision.
8. Describe `Get-NFL-Schedule`; record schedule, timezone, URI, retry posture, and deadline.
9. Query current-season source/Facts/window/ranking tables read-only for snap metric presence and values.
10. Make one explicitly approved production-shaped Tank01 read and compare its field inventory.
11. Confirm retained raw-response object naming and availability for selected historical games.

**Stop if:** the working tree is dirty, branch heads are unexpected, the production trigger can match non-main branches, traffic is already split, rollback identity is unclear, or the source schema materially differs from this plan.

**Exit evidence:** Timestamped baseline table recorded in this document.
**Production impact:** None; Tank01 request consumes only its authorized API quota.

### Gate 2 - Create the one main-based branch

**Goal:** Isolate the production fix from the growing learning branch.

Expected Git shape:

```text
origin/main
  `-- codex/tank01-team-metric-hotfix

origin/dev remains unchanged during implementation
```

Procedure:

1. Synchronize `main` with `origin/main` using fast-forward-only behavior.
2. Confirm the exact starting SHA.
3. Create `codex/tank01-team-metric-hotfix` from that SHA.
4. Confirm no `dev`-only Packet 2/3 files appear in the branch diff.

**Stop if:** `main` cannot be fast-forwarded cleanly, local changes would follow the branch unexpectedly, or the branch contains learning work.

**Exit evidence:** Branch name and starting SHA.
**Production impact:** None; branch creation and local commits do not deploy.

### Gate 3 - Complete the bounded metric changes

**Goal:** Implement only the approved parser, registry, validation, and test behavior.

Implementation requirements:

- no global missing-field-to-zero policy;
- no warning per missing optional statistic;
- no duplicate snap metrics;
- no BigQuery schema columns;
- no Levels 1-4 changes;
- no `/game` presentation changes;
- each approved key appears in both `EXPECTED_METRICS` and `METRIC_REGISTRY`;
- every approved registry entry resolves all 20 required metadata fields to the approved values;
- every new `lens_tags` list is non-empty, unique, normalized, and covered by tests;
- all 14 new registrations explicitly set ranking and product-influence controls rather than relying on defaults;
- registry validation passes at import;
- exact approved source-to-metric mappings are documented in tests; and
- the raw-response backup remains in place before parsing.

**Stop if:** implementation requires a table schema migration, broad pipeline rewrite, player-level ingestion, source API provider change, or unrelated production behavior.

**Exit evidence:** Focused code diff and test diff.
**Production impact:** None until a cloud validation or push is separately authorized.

### Gate 4 - Local tests

**Goal:** Prove deterministic behavior without waiting for a future game.

Required checks:

1. New direct parser tests pass.
2. Box-score validator tests pass.
3. Stats loader tests pass.
4. Registry imports and validates.
5. Metric builder/conductor tests pass when affected.
6. Full relevant regression suite passes.
7. Historical real-payload values match expected parsed rows.
8. Optional omissions generate no warning storm.
9. Missing snaps do not become fabricated zeroes.

**Stop if:** any test requires weakening an existing production safety invariant merely to pass.

**Exit evidence:** Exact commands, passed/failed/skipped counts, and test timestamp.
**Production impact:** None.

### Gate 5 - Isolated historical validation

**Goal:** Prove the real data path against non-production destinations before merging to `main`.

Default to local or in-memory validation. Use the supplied historical response and one retained 2026 preseason response through the parser and registry-backed builders as far as the test harness supports:

```text
raw response
-> parser
-> in-memory long-form metric rows
-> registry metadata and builder tests
```

Required evidence:

- exactly two expected teams;
- expected Group A and approved Group C metric rows;
- snap counts match source and opponent mirrors;
- registry metadata is attached correctly;
- no duplicate team-game-metric rows;
- source and parsed counts reconcile;
- optional absence behavior matches Gate 0 decisions; and
- no Google Cloud data is written.

Writing the rehearsal to dev datasets is optional, not a default requirement. Do it only if local tests cannot establish the downstream behavior, and obtain approval before that external write.

**Stop if:** a test target resolves to production, source counts are inconsistent, or local validation cannot establish the expected registry behavior.

**Exit evidence:** Two payload identifiers, focused test results, and source-to-parsed count summary.
**Production impact:** None.

### Gate 6 - Final pre-merge review

**Goal:** Confirm the branch contains only the intended production hotfix.

Review:

- diff against current `origin/main`;
- commits and test evidence;
- main/dev divergence since Gate 2;
- production trigger branch filter;
- no-traffic build behavior;
- current serving revision and rollback anchor;
- Scheduler timing relative to the intended promotion window; and
- whether any data backfill is being incorrectly bundled with code promotion.

**Stop if:** `main` moved in relevant files, the candidate would receive traffic automatically, the branch includes learning work, or Christian has not approved the production merge.

**Exit evidence:** Explicit GO/NO-GO decision.
**Production impact:** None.

### Gate 7 - Merge into main and build the no-traffic candidate

**Goal:** Create a production revision without changing normal traffic.

Expected behavior from `cloudbuild.yaml`:

```text
main push
-> build production image
-> deploy nfl-games-app-main
-> --no-traffic
-> tag packet4-candidate
```

Required evidence:

- merged production commit SHA;
- successful Cloud Build ID;
- candidate revision name;
- candidate image digest/commit association;
- candidate ready condition;
- candidate normal traffic remains 0%; and
- previous serving revision remains 100%.

**Stop if:** the build deploys traffic automatically, the old serving revision changes unexpectedly, candidate startup fails, or configuration differs materially.

**Exit evidence:** Candidate deployment record.
**Production impact:** A new production revision exists with production identity and data access, but normal traffic remains on the old revision.

### Gate 8 - Candidate validation

**Goal:** Validate deployability and safe read contracts without invoking production ingestion.

Allowed candidate checks are read-only at the application and data level. Follow the established `go_plan.md` candidate boundary.

Validate:

- revision health and startup logs;
- expected commit and runtime configuration;
- memory, timeout, identity, environment, and dataset/bucket targets;
- `/health`;
- safe CORS/auth behavior;
- `/games`; and
- an upcoming/non-final `/game/{gameID}` if needed and safe.

Forbidden during candidate validation:

- `POST /` because it can run production ingestion and metric builders;
- a final-game `/game/{gameID}` when it can persist model outcome/trust data;
- production backfill;
- Scheduler retargeting; and
- any manual production retry.

Candidate validation does not re-prove the write path. The write path was proven through local tests and isolated historical dev validation.

**Stop if:** unexpected 5xx, import/registry validation failure, permission error, wrong identity/configuration, data mutation, or revision mismatch occurs.

**Exit evidence:** Candidate API/log checklist and explicit GO/NO-GO.
**Production impact:** None beyond read-only candidate requests.

### Gate 9 - Deliberate promotion

**Goal:** Move normal production traffic to the validated candidate.

Before promotion:

1. Reconfirm candidate revision and commit.
2. Reconfirm current serving and rollback revisions.
3. Reconfirm candidate and serving logs are healthy.
4. Reconfirm Scheduler is not in an active execution window.
5. Record whether the established release procedure requires a temporary Scheduler pause.
6. Obtain Christian's explicit promotion approval.

Promote the candidate deliberately to 100% normal traffic. Do not use a low-percentage split that could unpredictably route Scheduler traffic.

After promotion:

- verify intended revision owns 100%;
- verify previous serving revision owns 0% and remains available as rollback;
- run only safe smoke checks;
- inspect immediate startup/request errors; and
- restore/confirm Scheduler state exactly as approved.

**Stop and roll back if:** traffic points to the wrong revision, smoke checks fail, errors spike, permissions/configuration are wrong, or service health is uncertain.

**Exit evidence:** Promotion timestamp, traffic table, smoke results, and rollback anchor.
**Production impact:** New parser and registry become active for normal requests and the next Scheduler run.

### Gate 10 - Merge the production fix forward into dev

**Goal:** Eliminate side-by-side maintenance immediately after production promotion.

Preferred history operation:

```text
main contains the released hotfix commit
-> merge updated main into dev
-> dev contains the identical production fix plus ongoing learning work
```

Do not independently reimplement the patch on `dev`. Prefer merging the released `main` history forward so future `dev` to `main` work recognizes the same production commit.

Required checks:

- relevant files match the production fix;
- Packet 2/3 learning files remain intact;
- tests still pass on merged `dev`;
- no production deployment is triggered by the `dev` push; and
- temporary hotfix branch is deleted only after both branches are confirmed.

**Exit evidence:** Dev merge commit, synchronized remote state, and branch cleanup record.
**Production impact:** None if the dev trigger remains disabled and branch filtering is correct.

### Gate 11 - Production revision confirmation

**Goal:** Confirm release state, not merely merge state.

Read-only checks:

- expected revision owns 100% traffic;
- service health is normal;
- no unexpected errors, OOMs, timeouts, permission failures, or quota failures;
- Scheduler still targets the normal service URL with the intended schedule/timezone/deadline;
- rollback revision remains known; and
- the hotfix commit is present in both `main` and `dev`.

**Exit evidence:** Production release receipt.
**Production impact:** None.

### Gate 12 - First naturally eligible production run

**Goal:** Observe the live Stats write path without manufacturing a production event.

A scheduled no-game or zero-accepted-Stats run can prove service health but cannot prove new metric capture. Treat it as healthy, not as final ingestion evidence.

On the first scheduled run with one or more eligible completed games, verify:

- Scheduler attempt started and finished;
- Cloud Run request returned 2xx on the expected revision;
- Stats accepted the expected games;
- Group A metrics no longer cause `unregistered_source_metrics_excluded`;
- approved Group C metrics appear when supplied by Tank01;
- optional absent fields do not create warning noise;
- snap rows reflect source presence and do not contain fabricated zeros;
- Facts, Windowed Metrics, and Rankings complete;
- counts reconcile across teams and stages; and
- total duration is reasonable.

**Exit evidence:** Daily production health report plus metric-specific reconciliation.
**Production impact:** Normal scheduled ingestion only.

---

## 11. Deferred refresh and backfill decision

Code promotion and historical data backfill are separate decisions.

### 11.1 Original eight

No source recollection should be necessary. Their existing `game_metrics_flat` rows can enter downstream tables on the next authorized full season rebuild after the registry is active.

### 11.2 Newly parsed fields

Already completed games require an explicit reparse because the normal loader considers two-team games complete and skips reinsertion. That is a future option, not part of this release.

Observation-first decision gate:

1. Observe naturally eligible 2026 responses and retain the distinction between absent and explicit zero.
2. Count any valid snap values that arrive, including values from a historical regular-season response.
3. Decide whether the observed timing and coverage justify a delayed refresh or bounded backfill.
4. If Christian later approves production writes, rehearse one selected-game replacement locally or in dev before touching production.
5. Only then define the affected game set and downstream Facts -> Windowed Metrics -> Rankings rebuild.

Until that later decision, do not replay production, backfill preseason games, add a retry schedule, or treat missing preseason snaps as unhealthy.

---

## 12. Rollback plan

### 12.1 Candidate fails before promotion

- Leave normal traffic on the existing revision.
- Do not promote.
- Inspect and correct the branch.
- A failed 0%-traffic candidate requires no user-traffic rollback.

### 12.2 Candidate fails after promotion

- Move 100% traffic back to the recorded rollback revision.
- Confirm health on the restored revision.
- Confirm Scheduler state and target.
- Preserve logs and revision evidence.
- Decide separately whether to revert the `main` commit and build a corrected candidate.
- Never use destructive Git reset as release rollback.

### 12.3 Data was written before rollback

- Do not automatically delete metric rows.
- Determine whether the rows are correct but temporarily unsupported, or actually wrong.
- Old code can safely exclude unregistered keys from Facts.
- Use retained raw responses to reconcile source truth.
- Require a separately approved targeted repair for demonstrably wrong data.

---

## 13. Stop conditions

Stop the release immediately if any of the following occurs:

- source semantics remain unresolved for a proposed production metric;
- current Tank01 schema differs materially from the inspected response;
- optional absence cannot be distinguished from real zero for a metric intended to rank teams;
- parser tests require fabricated behavior inconsistent with real payloads;
- relevant `main` and `dev` files diverge unexpectedly;
- branch diff includes Packet 2/3 learning work;
- registry import validation fails;
- isolated dev writes resolve to production resources;
- candidate receives normal traffic before approval;
- candidate validation mutates production data;
- production rollback revision is unknown;
- Scheduler is active or could overlap the promotion window unexpectedly;
- production request returns non-2xx;
- OOM, timeout, permission, quota, or upstream API failures appear; or
- counts no longer reconcile at team-game-metric grain.

Stopping is not failure. It preserves the existing working production revision while evidence is clarified.

---

## 14. Definition of done

The detailed sections explain how to establish these outcomes. Completion requires only this concise checklist:

- [ ] The approved 14 metrics, full registry metadata, and `lens_tags` are implemented without a schema change or learning/frontend changes.
- [ ] Optional absent fields are omitted quietly, explicit zeros are preserved, and missing snaps do not become fabricated zeros.
- [ ] Focused tests and the real-payload historical validation pass with reconciled counts.
- [ ] The focused branch is reviewed, merged into `main`, and produces a healthy 0%-traffic candidate.
- [ ] Christian approves promotion; the candidate receives 100% traffic and the prior revision remains the rollback anchor.
- [ ] Released `main` is merged forward into `dev`, the temporary branch is cleaned up, and the production revision is confirmed.
- [ ] The first naturally eligible run succeeds and the original unregistered-metric warning disappears.
- [ ] Snap availability is observed without warning noise; any delayed-refresh or backfill decision remains a separate future approval.

---

## 15. Lightweight evidence ledger

Record one concise receipt per practical outcome. Detailed command transcripts and raw log dumps are unnecessary.

| Outcome | Date/time ET | Commit/revision/run | Concise evidence | Decision |
|---|---|---|---|---|
| Plan | 2026-08-14 | N/A | Scope and registry worksheet approved; no-code hold lifted | Approved |
| Branch, implementation, and tests | 2026-08-14 | Uncommitted branch from `77f4a06` | 26 focused tests passed; supplied 2024, retained/fresh 2026 preseason, and retained/fresh 2025 regular-season shapes reconciled; in-memory Facts check passed | Local implementation complete; review pending |
| Main and candidate | Pending | Pending | Merge SHA, build ID, healthy 0%-traffic revision | Pending |
| Promotion | Pending | Pending | 100% traffic, smoke result, rollback revision | Pending |
| Dev forward merge and eligible run | Pending | Pending | Dev merge, serving revision, scheduled-run reconciliation | Pending |
| Snap refresh/backfill | Deferred | Historical 2025 counts observed; 2026 preseason still absent | Future coverage/timing evidence | Observe before deciding |

---

## 16. Decision record and execution status

The prior nine-question decision list is resolved:

1. Group C keys are approved.
2. Section 5.5 metadata and `lens_tags` are approved.
3. Penalty count and yards remain in the existing `Offensive Output` core area under `Team Discipline`; no sixth pillar is added.
4. No exhaustive Tank01 zero-omission study is required. Preserve explicit zero; omit an absent optional field without warning.
5. Capture combined defensive/special-teams touchdowns now as context-only and prevent double counting with `defensive_tds`.
6. Include both two-point conversion fields now.
7. Observe snap availability first; no delayed refresh or preseason backfill is included in this release.
8. Historical validation includes a real 2025 regular-season response with valid snap counts plus retained 2026 preseason absence; focused fixtures cover rare absence and malformed cases.
9. The main-based branch and implementation approach are approved. `codex/tank01-team-metric-hotfix` was created from synchronized `main` at `77f4a06`.

Local code and tests are complete but deliberately uncommitted and unstaged. The next decision boundary is review of the full diff. Do not merge into `main`, build the production candidate, deploy, promote traffic, or backfill production without Christian's next explicit approval.
