# GameLens Learning Lite Findings Log

**Created:** 2026-08-26  
**Repository:** `csells10/meow`  
**Authority branch:** `learning-lite`  
**Purpose:** Preserve review findings, observed data gaps, decisions, and resolution evidence without rewriting completed checkpoint history.

This log is append-only. A finding may originate in Learning Lite review without belonging to the implementation scope of the checkpoint that exposed it. Logging a finding does not authorize code, schema, data, deployment, or production changes.

## Status values

- **Open:** observed and not resolved or waived.
- **In review:** bounded diagnosis or correction has been explicitly authorized.
- **Resolved:** correction and required evidence are documented.
- **Waived:** the owner explicitly accepted the stated risk for a defined scope and duration.
- **Superseded:** replaced by a later finding or decision, with a link to that record.

## Finding template

### LL-FIND-### — Short name

**Date discovered:**  
**Status:**  
**Severity:**  
**Discovered during:**  
**Affected surface:**  

**Observation**

**Evidence**

**Product impact**

**Scope and ownership**

**Decision**

**Resolution criteria**

**Data and production effects**

---

## Findings

### LL-FIND-001 — Missing snap totals ranked as real zeroes

**Date discovered:** 2026-08-26  
**Status:** Waived for the controlled LL-3 development-fixture proof only
**Severity:** Medium; bounded away from Matchup Lean and confidence, but exposed in ranking, tier, context, and possible edge language  
**Discovered during:** Local read-only LL-2 acceptance review  
**Affected surface:** 2026 preseason Facts, Windowed Metrics, Rankings, and any `/game` explanation that consumes the affected ranking rows

**Observation**

Four snap-total metrics use inconsistent missing-value semantics:

- `total_defensive_snaps`;
- `total_offensive_snaps`;
- `total_snaps`;
- `total_special_teams_snaps`.

Some teams have no Fact and a `NULL` Windowed value, so Rankings correctly omit them. Other teams have a Fact value of `0.0`, which Windowed Metrics preserve and Rankings treat as real evidence. Zero total snaps are not credible for a completed NFL game.

**Evidence**

Read-only review of the real scheduled fixture `20260827_PIT@BUF` found:

- the latest eligible `preseason_to_date` ranking date was 2026-08-23;
- BUF had 63 ranked metrics with source data through 2026-08-22;
- PIT had 67 ranked metrics with source data through 2026-08-21;
- the only asymmetric metrics were the four snap totals above;
- BUF had zero Fact rows for those metrics, two Windowed rows per metric with `NULL` values, and no ranking rows;
- PIT had one Fact row per metric, two Windowed rows per metric with `0.0` values, and ranking rows.

The current 2026-08-23 preseason ranking snapshot showed, for each of the four metrics:

- 13 ranked teams;
- 13 zero-valued teams;
- zero positive-valued teams;
- minimum and maximum values both `0.0`.

For PIT, `total_defensive_snaps = 0.0` was ranked first of 13 with tier label `Elite`. Its metadata reported:

- `ranking_usage = edge`;
- `signal_strength = supporting`;
- `edge_language_allowed = true`;
- `include_in_core_area_advantage = false`;
- `confidence_eligible = false`;
- `data_quality_status = good`.

The other three affected metrics were `context_only`, but still received misleading rank/tier descriptions.

**Product impact**

- Matchup Lean and confidence are protected because the affected rows cannot contribute to core-area advantage or confidence.
- `total_defensive_snaps` can still support misleading directional edge language.
- All four metrics can expose misleading rank, percentile, tier, or contextual descriptions.
- A future immutable pregame snapshot would correctly preserve the product response but could permanently preserve this misleading source evidence.

**Scope and ownership**

This is a pre-existing source/data-quality issue. LL-2 did not create or modify the affected metrics, tables, builders, registry policy, route, or frontend. LL-2 contract mechanics passed and are accepted separately.

The exact correction owner must be chosen before implementation. Likely seams include source normalization, Facts validation, Windowed missing-value handling, Rankings eligibility, or registry safety metadata. Do not patch multiple layers without identifying the first trustworthy ownership boundary.

**Decision**

- Accept LL-2 as the side-effect-free pregame safety boundary.
- Track this issue outside LL-2 implementation scope.
- Do not infer or implement a correction from this finding alone.
- Resolve or explicitly waive the finding for a defined proof before LL-3 persists a real-data pregame payload.

**Resolution criteria**

A separately authorized correction or waiver must document:

1. the authoritative meaning of missing, zero, and positive snap totals;
2. the layer responsible for normalizing invalid values;
3. proof that zero/missing snap totals cannot receive a rank, percentile, tier, or edge treatment as real evidence;
4. proof that valid positive snap totals still flow correctly;
5. focused tests and a read-only or development-only data reconciliation;
6. whether affected 2026 tables require a rebuild;
7. unchanged Matchup Lean, confidence, claim language, auth, CORS, routes, frontend, and production behavior unless separately authorized.

**Data and production effects**

Discovery used local tests and read-only BigQuery `SELECT` queries. No table or row was created, altered, or written. No snapshot, claim, outcome, receipt, deployment, schedule, trigger, traffic, route, or production behavior changed.

### LL-FIND-001 status update — 2026-08-26

**Previous status:** Open
**Current status:** Waived for the controlled LL-3 development-fixture proof only
**Approved by:** Christian

**Waiver scope**

- permits one controlled LL-3 proof against the existing development-only `nfl-stream-406420.GameLens_dev.pregame_snapshots` table;
- permits no production snapshot, deployment, scheduling, trigger, traffic, or later-checkpoint use;
- does not treat the snap-total semantics as corrected;
- expires at the LL-3 exit gate, where this finding returns to Open unless separately resolved or waived again.

**Read-only target verification**

- the existing table contained seven rows and seven distinct `capture_id` values;
- no duplicate `capture_id` group was observed;
- the table matched the historical 22-field snapshot contract;
- `captured_at` is the partitioning column;
- clustering is `game_id`, then `season_type`;
- no table or row was created, replaced, altered, deleted, or written during verification.

**Decision**

LL-3 must reuse and verify the existing table. It must not create, replace, migrate, truncate, or delete it. The seven historical rows remain immutable evidence.
