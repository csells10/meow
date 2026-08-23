# GameLens Learning Lite Architecture

**Document status:** Living planning baseline; implementation has not started
**Created:** 2026-08-21
**Owner:** GameLens product stewardship
**Repository:** `csells10/meow`
**Production authority:** `main`
**Historical implementation source:** `dev` at `26287205f420f569d81ccfcb28a8e8e0656fc24b` and its `documentation/live` packet suite

---

## 1. Decision

GameLens will pursue a smaller learning architecture named **Learning Lite**.

Learning Lite starts with what already works on `main`, selectively reuses proven safety boundaries from the archived `dev` work, and postpones operational infrastructure that has not earned its maintenance cost.

This is not a rejection of the earlier packet work. That work established useful contracts, tests, failure modes, and proof. The architectural change is to preserve those lessons without promoting the entire six-table development platform into the long-term product.

---

## 2. Why the direction changed

The live `/game` product already contains more of the intended intelligence than the packet plan initially appeared to assume:

- Game Profile;
- Team Comparison;
- Core Area Advantage;
- Matchup Lean;
- ranking context;
- matchup breakdown;
- Model Outcome and Model Trust for completed games;
- response-level claim-language support, including the support surfaced by the frontend as “Fits matchup.”

The calculation owners for Claim Extraction, Claim Grading, Feature Enrichment, and Language Calibration also already exist on `main`.

The archived `dev` branch is therefore best understood as an operationalization prototype around existing intelligence. Its most important new product datum is the immutable pregame snapshot. Much of the remaining work is storage coordination, receipts, run visibility, QA, and documentation.

For a hobby-scale product, the maintenance cost of that full operational shell is not presently justified.

---

## 3. Product objective

Learning Lite should allow GameLens to answer, over time:

- What did GameLens know before kickoff?
- What did it actually say?
- Which parts of the matchup read held up?
- Which pregame conditions made particular claims more or less trustworthy?
- How should GameLens calibrate its explanatory language?
- Which team identities and league-wide football themes are changing?
- Is there enough clean evidence for a future algorithm without forcing one prematurely?

The objective is evidence preservation and calibrated learning, not infrastructure completion.

---

## 4. Non-goals

Learning Lite does not initially require:

- a prediction platform;
- automatic betting recommendations;
- an always-on workflow engine;
- a second ETL pipeline;
- a six-table run ledger;
- a permanent coordinator for every learning stage;
- a new Admin frontend;
- automatic Language Calibration decisions;
- a public League Discovery dashboard;
- synthetic Week 1 certainty;
- a new table for every analytical idea.

---

## 5. Working terminology

The existing Level names remain valid code and research terminology, but descriptive names should be used in planning and handoffs.

| Descriptive name | Existing name | Single responsibility |
|---|---|---|
| Pregame Snapshot | Capture contract / Level 0 context | Freeze the pregame product response and its evidence lineage |
| Claim Extraction | Level 1 | Extract one row per meaningful pregame claim |
| Claim Grading | Level 2 | Compare a frozen claim with accepted completed-game evidence |
| Feature Enrichment | Level 3 | Attach features calculated only from information available before kickoff |
| Language Calibration | Level 4 | Compare historical validation groups and recommend language treatment |

### Ownership rule for new ideas

New ideas belong with the layer that owns their meaning:

| New idea | Owner |
|---|---|
| New metric, category, Core Area, or lens tag | Metric registry and metric builders |
| New user-facing matchup statement | `/game` product logic and Claim Extraction |
| New definition of confirmed, mixed, contradicted, or unavailable | Claim Grading |
| New pregame signal or feature | Feature Enrichment |
| Evidence that an established claim/feature deserves different wording | Language Calibration |
| Movers, heatmaps, team identity shifts, and league themes | League Discovery |
| Future winner or other predictive algorithm | A separately defined model layer |

Language Calibration is the editor, not a catch-all.

---

## 6. Persistent data model

### 6.1 Existing analytical spine — keep

#### A. Schedule and game identity

Keep the existing schedule as the authority for game ID, teams, kickoff, season, week, phase, and status.

#### B. Completed-game Facts

Keep `Analytics.game_team_metric_facts_{season}` as the normalized record of what occurred for each team in each completed game.

#### C. Team windows

Keep `Analytics.team_metrics_windowed_{season}` for season, phase, rolling, and other approved pregame-safe windows.

#### D. Team rankings

Keep `Analytics.team_metric_rankings_{season}` for league rank, percentile, tier, window, `as_of_date`, source date, and lag.

#### E. Metric registry and lens tags

Keep the metric registry as the source of labels, categories, Core Areas, comparison direction, quality status, signal strength, ranking usage, and `lens_tags`.

#### F. Existing game outcomes

Reuse the existing Analytics outcome and trust records. Do not introduce a competing long-term outcome ledger merely because the development packet used one.

#### G. Historical claim evidence

Preserve the existing claim-training table, Claim Health queries, historical payload runs, and calibration outputs.

### 6.2 Immutable pregame snapshot — one justified new durable record

#### A. Grain

One canonical pregame snapshot per eligible game within a stable season/phase/ruleset cohort.

#### B. Timing

Capture after an accepted or known-healthy metric state is available and before scheduled kickoff.

#### C. Payload

Store the complete pregame `/game` response rather than reconstructing only selected fields.

#### D. Required lineage

Retain at minimum:

- `capture_id`;
- `learning_run_id` or equivalent stable cohort identifier;
- `game_id`;
- environment;
- season, season type, and week;
- scheduled kickoff;
- `captured_at`;
- capture status;
- canonical payload SHA-256;
- full response payload;
- evidence context;
- ranking availability and reason;
- metric source date;
- ranking `as_of_date`;
- metric pipeline run identifier when available;
- model version;
- ruleset version;
- lens tags used by the response.

#### E. Immutability

The first valid canonical capture wins. An identical retry may be a no-op. A conflicting retry must be rejected or quarantined rather than silently replacing history.

#### F. Honest absence

Missing rankings, unavailable context, No Pick, and zero extracted claims are valid states. The snapshot preserves what was known without manufacturing current-season evidence.

#### G. Future compatibility

Because the full response is stored, additive `/game` sections can be preserved without immediately redesigning the snapshot table. Structured fields should be promoted only when repeated analytical use justifies them.

### 6.3 Claim-training record — reuse and extend

#### A. Grain

One row per meaningful claim extracted from a canonical pregame snapshot.

#### B. Existing surfaces

Continue extracting supported claims from Game Profile, Core Area comparison, Core Area summaries, category summaries, metric highlights, and Team Comparison.

#### C. Snapshot lineage

New regular-season claim rows should retain `capture_id`, source payload hash, extraction version, source section, and source field path.

#### D. Pregame features

Retain values, gaps, ranks, percentiles, windows, context, confidence, hierarchy, and versioned feature outputs that were available before kickoff.

#### E. Postgame targets

Claim Grading may populate actual evidence and validation outcomes only after final score and accepted Facts are available.

#### F. Anti-leakage boundary

Feature Enrichment may run after final for workflow convenience, but its inputs must remain an explicit pregame-only projection. Validation results, final score, actual winner, and evaluated-game postgame metrics must not become features.

#### G. Extensibility

A new claim surface should normally become a new `claim_type` or versioned extraction rule, not a new table. A new feature should normally become a versioned feature output, not a rewrite of historical source data.

#### H. Zero-claim games

Zero claims are an observable result. They do not invalidate a good snapshot and must not be converted into fake rows solely to satisfy coverage counts.

### 6.4 Views and analytical products — derive later

#### A. Views organize; they do not create evidence

A BigQuery view is a reusable query over canonical data. It can be added, changed, or removed without duplicating the source evidence.

#### B. League Discovery

The planned `Analytics.v_team_lens_weekly_profile` can be derived from existing ranking rows and `lens_tags`.

Potential outputs include:

- lens score, rank, and percentile;
- prior-window or prior-week values;
- movement direction and magnitude;
- contributing metrics;
- team identity shifts;
- movers and fallers;
- heatmaps and weekly themes.

The view and frontend do not need to exist for the underlying data to remain available.

#### C. Postgame Signal Validation

A future `v_game_signal_validation` or equivalent query can join snapshots, claims, outcomes, and completed Facts to summarize Game Profile, Core Area, Team Comparison, No Pick, and confidence behavior.

#### D. Materialization rule

Start with a query or view. Create a physical table or builder only when performance, stable history, or repeated downstream use proves that a view is insufficient.

#### E. Public product rule

Internal analytical availability does not require an immediate API endpoint or frontend surface.

### 6.5 Language-calibration findings — version, review, then promote

#### A. Input

Language Calibration reads accumulated graded and enriched claim rows across a meaningful sample.

#### B. Question

It asks whether a defined claim surface under defined pregame conditions historically deserves stronger, measured, cautious, blocked, or insufficient-evidence treatment.

#### C. Output

Retain versioned summaries and recommendations containing the evaluated claim surface, feature/context bucket, row count, validation rate, comparison baseline, recommendation, and formula/ruleset version.

#### D. Additive flexibility

If Feature Enrichment later introduces a useful signal, Language Calibration may evaluate that signal without changing Facts, snapshots, or earlier claim rows. A new calibration result is a new versioned output, not an overwrite of the evidence.

#### E. Manual promotion

Calibration outputs remain advisory until a reviewed rule is deliberately promoted into the small runtime registry.

#### F. Runtime isolation

A new calibration run must not automatically change Matchup Lean, outcome confidence, Model Trust, or frontend language.

---

## 7. League Discovery data guarantee

Learning Lite must preserve the ability to build League Discovery later even if no League Discovery deliverable is scheduled.

The required source fields already belong to the ranking pipeline:

- season;
- `as_of_date`;
- source data date;
- team ID and abbreviation;
- window type;
- metric and label;
- category and Core Area;
- metric value;
- rank and percentile;
- tier;
- ranking interpretation;
- data-quality and usage metadata;
- repeated `lens_tags`.

Learning Lite must not remove, flatten away, or stop persisting these fields.

Before declaring League Discovery data-ready for a season, QA should confirm:

1. ranking rows exist across the intended dates and windows;
2. product-facing lens tags have useful coverage;
3. internal-only tags can be filtered;
4. source dates and lags are visible;
5. week-over-week or window-over-window comparisons can be reproduced;
6. insufficient-history states remain explicit.

Week 1 may contain little or no current-season identity evidence. That is expected and should be communicated through existing unavailable/insufficient-context language.

---

## 8. Algorithm runway without premature modeling

Learning Lite should make a future algorithm possible without pretending that an algorithm is already justified.

The future analytical contract is:

```text
Stable observations
    + immutable pregame response
    + normalized claim examples
    + postgame validation targets
    + versioned pregame features
    = reproducible research dataset
```

An experimental algorithm may read that dataset and produce separate versioned scores or recommendations. It must not overwrite snapshots, facts, rankings, or prior labels.

Any eventual model requires:

- a clearly stated target;
- an explicit training and evaluation split;
- no evaluated-game postgame leakage;
- sample-size reporting;
- comparison with simple baselines;
- calibration review;
- a documented abstain/insufficient-data state;
- deliberate promotion into product behavior.

The system should collect trustworthy evidence first. Modeling remains an option, not a deadline.

---

## 9. Main, learning-lite, dev, and salvage posture

### 9.1 Keep from main

- production ingestion and metric conductor;
- metric registry, Facts, windows, and rankings;
- `/game`, current product sections, and language annotations;
- existing Level 1–4 calculation owners;
- current claim and outcome analytics;
- Claim Health;
- Product Ideas and product philosophy.

### 9.2 Selectively reuse from dev

- the side-effect-free evidence loader and pregame builder boundary;
- deterministic capture identity and payload hashing;
- postgame-field rejection;
- canonical snapshot decision logic;
- the snapshot schema and immutable reconciliation behavior;
- the Claim Extraction adapter that reuses the existing extractor;
- capture-lineage claim fields;
- the explicit Feature Enrichment pregame-only projection;
- frozen-capture game grading;
- focused tests for immutability, parity, retry, identity, and leakage safety.

Reuse may mean porting a small function or test rather than copying a full service unchanged.

### 9.3 Preserve on `dev` as historical evidence

- the preserved `dev` head at `26287205f420f569d81ccfcb28a8e8e0656fc24b`;
- Packet 1–6 documents;
- development dataset recreation instructions;
- development table schemas and selected proof rows;
- QA outputs and known game-level proofs;
- Admin/run-visibility prototype and Lovable handoff.

### 9.4 Do not port initially

- the permanent Packet 4 coordinator;
- Packet 4 receipt storage;
- stage run and per-game operational tables;
- duplicate development outcome storage;
- the run-visibility service and route;
- Packet 5 frontend integration;
- Packet 6–8 orchestration scope;
- setup and backfill machinery used only by the superseded six-table path.

The `dev` history and exact commit permalink preserve the prototype evidence. Learning Lite work belongs only on `learning-lite`; `dev` should not receive new Learning Lite implementation.

---

## 10. Week 1 operating boundary

### Must be available

- existing `/game` behavior remains healthy;
- an eligible game can be captured before kickoff;
- the stored payload is pregame-safe and hash-verifiable;
- an identical retry is safe;
- Claim Extraction reads the canonical snapshot;
- zero claims and unavailable rankings remain honest;
- operators have a manual fallback and a simple verification read;
- production can disable the learning addition without disabling the existing data load or `/game`.

### Helpful but not required

- automatic invocation after the morning load;
- a League Discovery source-data QA query;
- a small internal snapshot/claim coverage summary.

### Explicitly deferred

- automatic postgame coordination;
- automatic Language Calibration;
- public League Discovery;
- run-visibility UI;
- permanent multi-table operational receipts;
- a future predictive algorithm.

---

## 11. Operational evidence rule

Persist football evidence and reusable learning results. Use normal structured logs and command summaries for routine execution evidence.

A new operational table is justified only if repeated real operation demonstrates a question that cannot be answered reliably from canonical data and logs.

An operational inconvenience is not automatically a new warehouse requirement.

---

## 12. Architecture change rule

This is a living plan. A change is allowed when it:

1. preserves immutable historical evidence;
2. names the owning layer;
3. states whether it changes persistent data;
4. states whether it changes `/game` or frontend behavior;
5. remains backward-compatible or provides a recovery path;
6. includes a bounded validation plan;
7. updates the Learning Lite README and Sprint checkpoint.

---

## 13. Source authority

This architecture builds on:

- `documentation/GameLens_Product_Ideas.md`;
- `documentation/GameLens_Philosophy_Statement.md`;
- `documentation/GameLens_Claim_Training_and_Level4_Roadmap.md`;
- `documentation/GameLens_Level4_Summary.md`;
- `documentation/GameLens_Admin_API.md`;
- the current production code on `main`;
- the current packet implementation and evidence on `dev`;
- the historical packet suite under `documentation/live`.

Where an older packet plan conflicts with this document about what should be built next, Learning Lite governs future scope. The older document remains the authority for what that packet proved at its historical checkpoint.
