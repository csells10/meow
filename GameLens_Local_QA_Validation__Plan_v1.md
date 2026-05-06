# GameLens Local QA Plan — Windowed Source + Product Refocus Validation

## Purpose

This file is a local validation guide for testing how backend/product changes affect the `/game/<game_id>` API response.

The goal is **not** to force the model to pick winners correctly.

The goal is to verify that GameLens becomes better at explaining:

- where each team has an edge
- where the matchup is even
- whether the profile is clean, split, or messy
- whether confidence language is appropriate
- whether the API response remains stable as backend logic changes

This QA batch should be rerun after meaningful backend changes, especially changes to:

- Team Comparison
- Core Area Comparison
- Matchup Lean
- Model Trust
- future rankings
- future lens views
- future historical context

---

## Current Tested QA Batch

Use these same four games as the repeatable local regression batch:

```text
20251207_CIN@BUF
20251013_BUF@ATL
20251020_TB@DET
20251009_PHI@NYG
```

These games are useful because they cover different behavior types:

| Game ID | Why it matters |
|---|---|
| `20251207_CIN@BUF` | Clean high-confidence correct edge |
| `20251013_BUF@ATL` | No Pick baseline that became a windowed-source miss |
| `20251020_TB@DET` | No Pick baseline that became a correct windowed-source lean |
| `20251009_PHI@NYG` | Low-confidence miss that became an overconfident windowed-source miss |

---

## Baseline Results Already Observed

| Game ID | Flag False Result | Flag True Result | QA Takeaway |
|---|---|---|---|
| `20251207_CIN@BUF` | BUF edge / High / Correct | BUF edge / High / Correct | Clean pass |
| `20251013_BUF@ATL` | No Pick / Low | BUF edge / Medium / Incorrect | Windowed source became more aggressive |
| `20251020_TB@DET` | No Pick / Low | DET edge / High / Correct | Windowed source sharpened correctly |
| `20251009_PHI@NYG` | PHI edge / Low / Incorrect | PHI edge / High / Incorrect | Biggest confidence warning |

Important read:

```text
The windowed source works technically, but it can make the model more aggressive.
The next changes should improve honesty, not just accuracy.
```

---

## Local Flag Setup

The source switch is controlled by the environment variable:

```text
USE_WINDOWED_METRICS_FOR_GAME
```

### Git Bash

Flag off:

```bash
export USE_WINDOWED_METRICS_FOR_GAME=false
python app.py
```

Flag on:

```bash
export USE_WINDOWED_METRICS_FOR_GAME=true
python app.py
```

### PowerShell

Flag off:

```powershell
$env:USE_WINDOWED_METRICS_FOR_GAME="false"
python app.py
```

Flag on:

```powershell
$env:USE_WINDOWED_METRICS_FOR_GAME="true"
python app.py
```

### Important

Restart the local Flask server after changing the flag.

If the env var is read at import/startup time, changing it while the server is already running may not affect the response.

---

## Local Endpoint

Local API route:

```text
http://127.0.0.1:8080/game/<game_id>
```

Examples:

```text
http://127.0.0.1:8080/game/20251207_CIN@BUF
http://127.0.0.1:8080/game/20251013_BUF@ATL
http://127.0.0.1:8080/game/20251020_TB@DET
http://127.0.0.1:8080/game/20251009_PHI@NYG
```

Note:

The `/game/<game_id>` route is protected by Firebase auth. If testing directly with `curl` or Postman, include a valid Firebase Bearer token. If testing through the frontend, the frontend should already attach the token.

---

## Recommended Local Output Folder

Create a local QA folder so before/after responses can be compared:

```text
qa/local_windowed_source_validation/
```

Suggested filenames:

```text
qa/local_windowed_source_validation/20251207_CIN@BUF_flag_false_baseline.json
qa/local_windowed_source_validation/20251207_CIN@BUF_flag_true_baseline.json
qa/local_windowed_source_validation/20251207_CIN@BUF_after_change.json

qa/local_windowed_source_validation/20251013_BUF@ATL_flag_false_baseline.json
qa/local_windowed_source_validation/20251013_BUF@ATL_flag_true_baseline.json
qa/local_windowed_source_validation/20251013_BUF@ATL_after_change.json

qa/local_windowed_source_validation/20251020_TB@DET_flag_false_baseline.json
qa/local_windowed_source_validation/20251020_TB@DET_flag_true_baseline.json
qa/local_windowed_source_validation/20251020_TB@DET_after_change.json

qa/local_windowed_source_validation/20251009_PHI@NYG_flag_false_baseline.json
qa/local_windowed_source_validation/20251009_PHI@NYG_flag_true_baseline.json
qa/local_windowed_source_validation/20251009_PHI@NYG_after_change.json
```

---

## API Sections to Validate

Each response should keep these top-level sections:

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

### Basic response-shape checks

For every game, confirm:

- `header.game_id` matches the requested game ID
- `header.away_team.abbreviation` and `header.home_team.abbreviation` are populated
- `team_comparison` returns expected visible comparison rows
- `core_area_comparison` returns useful Core Area rows
- `game_profile` returns Pressure, Turnover Risk, and Scoring Efficiency where available
- `matchup_lean` contains target, confidence, profile type, signal score, and Core Area context
- `model_trust` contains reasoning, matchup advantage, edge, signal alignment, and learning label
- final games return `model_outcome`

---

## Validation Philosophy

Do not validate only by asking:

```text
Did the model pick the winner?
```

Validate by asking:

```text
Did the API explain the matchup more honestly?
Did it avoid fake certainty?
Did it preserve useful strong reads?
Did it treat even areas as useful information?
Did confidence language match the evidence?
```

A result can be useful even if the winner prediction is wrong.

A result is risky if the API becomes overconfident from thin, split, or misleading evidence.

---

# Planned Change Validation

## 1. Treat “Even” as a Real Result

### Likely files

```text
services/game_service.py
services/core_area_analysis.py
services/model_trust_service.py
```

### What to change

Equal values should produce:

```text
better = neutral
```

instead of defaulting to `home` or `away`.

### What to validate

In `team_comparison`:

- tied values return `better: neutral`
- neutral rows do not add to away/home matchup score
- neutral rows do not appear as fake reasoning drivers

In `model_trust.matchup_advantage`:

- neutral rows should not increase away/home counts
- edge strength should soften when fake wins are removed

In `matchup_lean.signal_score`:

- neutral metrics should not add scoring weight

### Good example

If values are tied:

```json
{
  "label": "Turnover Margin",
  "away": 1.0,
  "home": 1.0,
  "better": "neutral"
}
```

User-facing interpretation:

```text
Turnover Margin is even. Neither team gets a clear edge here.
```

### Strong area

This is a clear quick win.

### Weak area

Frontend may need to display `neutral` gracefully if it currently expects only `away` or `home`.

---

## 2. Build As-Of Rankings From Windowed Metrics

### Likely files

```text
agg/build_metric_rankings.py        # new proposed builder
analytics/metric_registry.py
queries/game_queries.py
services/game_service.py
```

### Purpose

Answer:

```text
As of this game date, where does each team rank league-wide?
```

Use windowed data so the app can rank:

```text
regular_season_to_date
last_3_games
last_7_games
regular_plus_postseason_to_date
```

### What to validate later

For each game:

- rankings use only data available before the game
- ranking date aligns with the pregame-safe cutoff
- ranks can be generated for all registered metrics
- rank direction respects `comparison_direction` from `analytics/metric_registry.py`
- `higher` metrics rank higher values better
- `lower` metrics rank lower values better
- `context` metrics are handled carefully or marked as context-only

### Example API concept

```json
{
  "metric": "points_per_play",
  "team": "BUF",
  "window_type": "regular_season_to_date",
  "as_of_date": "2025-10-13",
  "value": 0.472,
  "league_rank": 2,
  "percentile": 94,
  "tier": "top_tier"
}
```

### Example user-facing sentences

```text
BUF ranks near the top of the league in Scoring Efficiency.
DET has improved recently in Red Zone Finish over its last 3 games.
NYG remains weak in Offensive Output, even though its recent form has improved.
PHI has a strong pressure profile, but the broader matchup is less clean.
```

### Strong area

This avoids hand-built metric thresholds and gives users context.

### Weak area

This can overwhelm users if exposed too directly. Backend can rank everything, but UI/API summaries should surface only the useful story.

---

## 3. Summarize by Metric, Category, and Core Area

### Likely files

```text
agg/build_metric_rankings.py        # new proposed builder
analytics/metric_registry.py
services/core_area_analysis.py
services/game_service.py
queries/game_queries.py
```

### Purpose

Users are usually asking broader questions:

```text
Who has the better scoring profile?
Who is stronger offensively?
Who creates more disruption?
Is this a turnover-pressure game?
```

So GameLens should summarize at multiple levels:

```text
metric
category
core_area
```

### Example hierarchy

```text
Metric: red_zone_efficiency
Category: Red Zone Finish
Core Area: Scoring Efficiency
```

### Example user-facing summaries

```text
BUF ranks top-tier in Scoring Efficiency, led by strong points per play and red zone finishing.
DET’s recent form shows a stronger scoring profile than its full-season average.
PHI is strong in pressure and turnovers, but NYG grades better in Offensive Output.
ATL does not dominate one single metric, but its defensive and turnover profile keeps the matchup competitive.
```

### Strong area

This makes the app easier to read and closer to how users think about football.

### Weak area

Grouped summaries can become over-declarative if they turn into another hidden scoring system. The goal is to summarize, not overstate.

---

## 4. Use Rankings to Explain, Not Force Decisions

### Likely files

```text
services/game_service.py
services/core_area_analysis.py
services/model_trust_service.py
queries/game_queries.py
```

### Purpose

Rankings should explain the matchup shape.

They should not automatically decide:

```text
winner
confidence
bet/action
```

### Good examples

```text
PHI leads the pressure metrics, but NYG grades better in Offensive Output. This creates a split matchup profile rather than a clean overall edge.
```

```text
BUF has the stronger scoring profile, but ATL’s defensive indicators keep the matchup from becoming a clean BUF read.
```

```text
DET shows stronger finishing ability, while TB holds up better defensively. That points to a mixed profile rather than a full-game edge.
```

```text
CIN and BUF are close defensively, but BUF separates more clearly in scoring and offensive efficiency.
```

### Bad examples

```text
PHI ranks higher, so PHI should win.
BUF leads three categories, so confidence is automatically High.
DET has the better scoring profile, so the game is solved.
```

### Strong area

This supports the product direction: explain the game, do not force the winner.

### Weak area

Language must stay careful. Rankings can make a matchup sound more certain than it really is.

---

## 5. Keep Field Control and Snap Counts in Observation Mode

### Likely files

```text
analytics/metric_registry.py
services/core_area_analysis.py
services/game_service.py
```

### Current posture

- Keep these metrics.
- Do not delete them.
- Do not prioritize fixing them yet.
- Do not let them drive major confidence decisions yet.

### Validation idea

If Field Control appears in `core_area_comparison`, confirm it is not creating misleading High Confidence behavior until the metric coverage and meaning are clearer.

### Simple rule

```text
Field Control can be observed, but it should not be the steering wheel yet.
```

---

## 6. Keep Historical Context Light

### Likely files

```text
queries/game_queries.py
services/game_service.py
services/model_trust_service.py
```

### Purpose

Historical data should not predict the winner.

For now, it should answer a lighter question:

```text
Does this profile look familiar, recent, or mixed?
```

### Important language definitions

To avoid vague wording:

```text
recent = last 3 games
medium recent = last 7 games
season profile = regular season to date
historical profile = prior season or prior available seasons, if used later
```

### Good examples

```text
BUF’s current scoring strength matches its season-to-date profile.
NYG’s offensive improvement is mostly recent, so treat it with some caution.
DET’s scoring profile has improved over the last 3 games compared with its full-season view.
PHI’s pressure profile looks strong, but the overall matchup still has conflict.
CAR’s recent offensive output is better than its season-long ranking, suggesting a possible shift rather than a settled profile.
```

### Bad examples

```text
BUF was good last year, so BUF should be good now.
NYG historically struggles, so ignore recent improvement.
Historical data says this team should win.
```

### Strong area

Historical data can help explain stability or change.

### Weak area

This is still fuzzy and should not become a heavy historical model until its role is clearer.

---

## 7. Use Lens Views Carefully

### Likely files

```text
services/game_service.py
services/model_trust_service.py
routes/game_routes.py
queries/game_queries.py
```

### Purpose

Lens views help users understand different parts of the game without forcing everything into a winner prediction.

Initial lens candidates:

```text
Overall Matchup
Scoring
Pressure
Turnovers / INT Risk
Rushing / Control
```

Possible later lenses:

```text
Passing Volume
Volatility
```

### Example lens output

For a messy game:

```text
Overall Matchup: split
Scoring: BUF advantage
Pressure: slight BUF advantage
Turnovers / INT Risk: mixed
Rushing / Control: unclear
```

For a cleaner game:

```text
Overall Matchup: BUF advantage
Scoring: BUF advantage
Pressure: BUF advantage
Turnovers / INT Risk: neutral
Rushing / Control: unclear
```

For an interception-focused use case:

```text
Overall Matchup: mixed
Pressure: PHI advantage
Turnovers / INT Risk: elevated against NYG
Passing Volume: uncertain
Confidence: moderate, because pressure supports the read but broader game script is unclear
```

### K-means / clustering thought

Clustering may eventually help discover common game profiles from the data instead of manually inventing labels.

Examples of possible discovered clusters:

```text
scoring-heavy mismatch
defensive-control matchup
turnover-pressure game
split-profile game
recent-form riser
low-signal matchup
```

This should stay exploratory for now.

### Strong area

Lens views fit the product direction well, especially for event environments like interception risk.

### Weak area

This can drift into label overload. Start simple and avoid adding labels no one asked for.

---

# Final Example Reads

## PHI vs NYG

Bad read:

```text
PHI is High Confidence because PHI leads pressure, turnovers, and scoring.
```

Better GameLens read:

```text
PHI shows stronger pressure and turnover indicators, but NYG grades better in Offensive Output. This creates a split matchup profile rather than a clean overall edge.
```

Lens-style summary:

```text
Pressure: PHI advantage
Turnovers / INT Risk: PHI advantage
Offensive Output: NYG advantage
Overall Matchup: split
Confidence: cautious
```

---

## BUF vs ATL

Bad read:

```text
BUF leads scoring, so BUF should win.
```

Better GameLens read:

```text
BUF has the stronger scoring profile, but ATL’s defensive and turnover indicators keep the matchup from becoming a clean BUF read.
```

Lens-style summary:

```text
Scoring: BUF advantage
Pressure: slight BUF advantage
Turnovers / INT Risk: mixed
Overall Matchup: cautious
Confidence: limited by split signals
```

---

## TB vs DET

Better GameLens read:

```text
DET shows stronger scoring and finishing ability, while TB holds up better defensively. The game leans toward DET in scoring-driven areas, but the defensive profile keeps the matchup from being one-dimensional.
```

Lens-style summary:

```text
Scoring: DET advantage
Pressure: DET advantage
Defensive Control: TB advantage
Overall Matchup: DET lean
Confidence: stronger if scoring and pressure signals align
```

---

## CIN vs BUF

Better GameLens read:

```text
BUF separates more clearly in scoring efficiency and offensive output, while the defensive comparison is closer. This creates a cleaner BUF matchup profile than games where the strengths are split.
```

Lens-style summary:

```text
Scoring: BUF advantage
Offensive Output: BUF advantage
Defensive Control: closer
Overall Matchup: BUF advantage
Confidence: stronger because multiple useful areas point the same direction
```

---

# Final Product Sentence

GameLens helps users read NFL matchups by showing where each team has an edge, where the game is even, and which areas of the matchup are most worth paying attention to.

The app should make users smarter about the game, not simply tell them who to pick.
