# GameLens Product Refocus — Matchup Intelligence, Not a Pick Machine

## Product North Star

GameLens helps users read NFL matchups more intelligently.

The goal is not to force a winner prediction. The goal is to show where a game leans, where the matchup is even, and which areas of the game are most worth paying attention to.

GameLens should help answer questions like:

- Which team has the stronger overall profile?
- Where is this matchup even?
- Which team is stronger in scoring, pressure, turnovers, or offensive output?
- Is a team improving recently, or has it been strong all season?
- Which parts of the matchup could create useful event signals, such as interception risk?
- Is the profile clean, split, or uncertain?

The winner lean can remain part of the page, but it should not be the entire product.

---

## 1. Treat “Even” as a Real Result

### Decision

GameLens should treat `neutral` or `even` as a valid matchup result.

If two teams are tied or nearly indistinguishable in a comparison, the app should not force the edge to home or away.

### Why this matters

An even result is useful. It tells the user that this part of the matchup does not create separation.

This helps prevent fake certainty.

### Strong area

This is a clear logic improvement and should be implemented first.

### Weak area

Very little downside. The only follow-up is making sure frontend language treats “even” as useful information rather than empty information.

### Likely files

- `services/game_service.py`
- `services/core_area_analysis.py`
- `services/model_trust_service.py`

### Example

If the data says:

- BUF turnover_margin = `1.0`
- ATL turnover_margin = `1.0`

The result should be:

- `better = neutral`

User-facing language:

- `Turnover Margin: even`
- `Neither team has a clear turnover-margin edge here.`

Bad behavior to avoid:

- `better = home`
- `better = away`

Equal values should not create artificial matchup advantage.

---

## 2. Build As-Of Rankings From Windowed Metrics

### Decision

GameLens should build daily/as-of rankings from the windowed metrics table.

The core question is:

> As of this game date, where does each team rank league-wide?

The rankings should come from the windowed metrics source so the app can support:

- season-to-date ranking
- last 3 games ranking
- last 7 games ranking
- postseason-inclusive ranking when needed

### Why this matters

Raw values are useful, but users usually need context.

A user may not know whether `0.472 points_per_play` is good, average, or elite. Ranking the team against the league gives that number meaning.

### Strong area

This is a strong product direction because it avoids hand-built football thresholds.

Instead of manually deciding what “good” means for every metric, the app can say where each team stands compared to the rest of the league.

### Weak area

The risk is overwhelming the user with too many rankings.

Backend can rank all 58 metrics, but frontend should only surface the clearest and most useful summaries.

### Likely files

- `agg/build_metric_rankings.py` — new proposed builder
- `analytics/metric_registry.py`
- `queries/game_queries.py`
- `services/game_service.py`

### Example user-facing summaries

- `BUF ranks near the top of the league in Scoring Efficiency.`
- `DET has improved recently in Red Zone Finish over its last 3 games.`
- `NYG remains weak in Offensive Output, even though its recent form has improved.`
- `PHI has a strong pressure profile, but the broader matchup is less clean.`
- `CIN and BUF are close defensively, but BUF separates more clearly in scoring efficiency.`

### Example backend concept

For one metric, the backend might eventually know:

- metric: `points_per_play`
- team: `BUF`
- window_type: `regular_season_to_date`
- as_of_date: `2025-10-13`
- value: `0.472`
- league_rank: `2`
- percentile: `94`
- tier: `top_tier`

The user does not need to see every field. The app can turn that context into a clean sentence.

---

## 3. Summarize by Metric, Category, and Core Area

### Decision

GameLens should not only compare individual metrics.

It should also support grouped summaries at the category and Core Area level.

This is important because users are often asking broader football questions, not narrow metric questions.

### Questions users are really asking

Users are more likely to ask:

- Who has the better scoring profile?
- Who is stronger offensively?
- Who creates more disruption?
- Is one team better at finishing drives?
- Is this a turnover-pressure game?
- Is either team clearly stronger in a key area?

They are less likely to ask:

- What is the exact percentile gap for `points_per_play`?
- What is the exact rank difference for `red_zone_efficiency`?

### Strong area

This makes the product more readable and more useful.

It turns many metrics into a game story.

### Weak area

The grouping logic needs care.

If category/Core Area summaries are too simplistic, they can become another fake scoring system. The goal is to summarize, not over-declare.

### Likely files

- `agg/build_metric_rankings.py` — new proposed builder
- `analytics/metric_registry.py`
- `services/core_area_analysis.py`
- `services/game_service.py`
- `queries/game_queries.py`

### Example hierarchy

- Metric: `red_zone_efficiency`
- Category: `Red Zone Finish`
- Core Area: `Scoring Efficiency`

### Example user-facing summaries

- `BUF ranks top-tier in Scoring Efficiency, led by strong points per play and red zone finishing.`
- `DET’s recent form shows a stronger scoring profile than its full-season average.`
- `PHI is strong in pressure and turnovers, but NYG grades better in Offensive Output.`
- `ATL does not dominate one single metric, but its defensive and turnover profile keeps the matchup competitive.`
- `TB has a better defensive profile, while DET separates more clearly in scoring and finishing drives.`

---

## 4. Use Rankings to Explain the Matchup, Not Force Decisions

### Decision

Rankings should help explain the matchup.

They should not automatically decide the winner or force confidence.

### Why this matters

The app should avoid becoming:

> Team A ranks higher in three things, therefore Team A wins.

That is too rigid.

Instead, rankings should help describe the shape of the matchup.

### Strong area

This supports the product goal perfectly.

It lets GameLens explain why a game looks clean, split, or messy.

### Weak area

The language needs to stay careful.

Rankings can make something sound more certain than it really is if the app overstates them.

### Likely files

- `services/game_service.py`
- `services/core_area_analysis.py`
- `services/model_trust_service.py`
- `queries/game_queries.py`

### Good examples

- `PHI leads the pressure metrics, but NYG grades better in Offensive Output. This creates a split matchup profile rather than a clean overall edge.`

- `BUF has the stronger scoring profile, but ATL’s defensive indicators keep the matchup from becoming a clean BUF read.`

- `DET shows stronger finishing ability, while TB holds up better defensively. That points to a mixed profile rather than a full-game edge.`

- `CIN and BUF are close defensively, but BUF separates more clearly in scoring and offensive efficiency.`

- `The matchup is not even overall, but the advantage is concentrated in one area rather than spread across the full profile.`

### Bad examples to avoid

- `PHI ranks higher, so PHI should win.`
- `BUF leads three categories, so confidence is automatically High.`
- `DET has the better scoring profile, so the game is solved.`

The product should explain, not declare.

---

## 5. Keep Field Control and Snap Counts in Observation Mode

### Decision

Field Control and snap-count-related metrics should stay available, but they should not drive major confidence decisions yet.

### Why this matters

These metrics may become useful later.

For now, they should not be deleted, but they should also not create High Confidence or heavily influence the broader matchup read until coverage and meaning are clearer.

### Likely files

- `analytics/metric_registry.py`
- `services/core_area_analysis.py`
- `services/game_service.py`

### Current posture

- Keep the metrics.
- Do not over-prioritize fixing them right now.
- Do not let them drive the strongest confidence language.
- Revisit later if the data becomes more complete or clearly useful.

### Simple rule

Field Control can be observed, but it should not be the steering wheel yet.

---

## 6. Keep Historical Context Simple Until Its Role Is Clearer

### Decision

Historical data has value, but GameLens should not build a heavy historical model yet.

Historical data should not predict the winner.

The simpler question is:

> Does this current team profile look familiar, new, or mixed?

### Why this matters

NFL history is useful, but it is not as stable as sales history.

Teams change because of:

- coaching changes
- coordinator changes
- quarterback changes
- injuries
- roster turnover
- schedule strength
- game script
- late-season identity shifts

So historical data should be used carefully.

### Strong area

Historical context can help users understand whether a current signal appears stable or recent.

### Weak area

This is still the least-defined part of the plan.

The app needs clear language so it does not sound overly analytic or make claims it cannot support.

### Likely files

- `queries/game_queries.py`
- `services/game_service.py`
- `services/model_trust_service.py`

### Better historical questions

- Does this current strength look familiar?
- Is this weakness continuing from earlier data?
- Is this improvement recent?
- Is this profile mixed between season-to-date and recent form?
- Is this too early or too noisy to say much?

### Example language

Good:

- `BUF’s current scoring strength matches its season-to-date profile.`
- `NYG’s offensive improvement is mostly recent, so treat it with some caution.`
- `DET’s scoring profile has improved over the last 3 games compared with its full-season view.`
- `PHI’s pressure profile looks strong, but the overall matchup still has conflict.`
- `CAR’s recent offensive output is better than its season-long ranking, suggesting a possible shift rather than a settled profile.`

Be clear about words like “recent.”

For example:

- `recent` could mean `last 3 games`
- `medium recent` could mean `last 7 games`
- `season profile` could mean `regular season to date`

Bad:

- `BUF was good last year, so BUF should be good now.`
- `NYG historically struggles, so ignore recent improvement.`
- `Historical data says this team should win.`

Historical context should support understanding, not force conclusions.

---

## 7. Use Lens Views Carefully

### Decision

Lens views are useful, but GameLens should avoid creating too many labels too soon.

The lens idea is strong because different users care about different parts of the game.

One user may care about interceptions. Another may care about rushing yards. Another may care about scoring. The same game can lean differently across different areas.

### Why this matters

Winner prediction compresses the whole game into one answer.

Lens views let GameLens say:

- the overall matchup is split
- scoring favors one team
- pressure favors another team
- turnover risk is elevated
- rushing/control is unclear

That is much more useful.

### Strong area

This fits the real product direction.

GameLens can become a tool for understanding event environments, not just picking winners.

### Weak area

This can easily drift into label overload.

Users already have to understand metrics, categories, Core Areas, matchup lean, and confidence. Adding many new lens names could make the app feel busier instead of smarter.

### Likely files

- `services/game_service.py`
- `services/model_trust_service.py`
- `routes/game_routes.py`
- `queries/game_queries.py`

### Lens view should start simple

Initial lens candidates:

- Overall Matchup
- Scoring
- Pressure
- Turnovers / INT Risk
- Rushing / Control

Possible later lenses:

- Passing Volume
- Volatility

Do not add too many until the product proves they are useful.

### Possible user-facing lens example

For a messy game:

- Overall Matchup: split
- Scoring: BUF advantage
- Pressure: slight BUF advantage
- Turnovers / INT Risk: mixed
- Rushing / Control: unclear

For a cleaner game:

- Overall Matchup: BUF advantage
- Scoring: BUF advantage
- Pressure: BUF advantage
- Turnovers / INT Risk: neutral
- Rushing / Control: unclear

For an interception-focused use case:

- Overall Matchup: mixed
- Pressure: PHI advantage
- Turnovers / INT Risk: elevated against NYG
- Passing Volume: uncertain
- Confidence: moderate, because pressure supports the read but broader game script is unclear

### K-means / clustering thought

Lens views may eventually benefit from clustering.

A clustering approach could help identify common game profiles without manually inventing every label.

For example, games might naturally cluster into patterns like:

- scoring-heavy mismatch
- defensive-control matchup
- turnover-pressure game
- split-profile game
- recent-form riser
- low-signal matchup

This should stay exploratory for now.

Do not build the first version around clustering.

The first version should keep the lens view simple and readable. Later, clustering may help discover better groupings from the data.

---

## Final Example: How GameLens Should Read a Game

### Example 1: PHI vs NYG

A bad product read would be:

- `PHI is High Confidence because PHI leads pressure, turnovers, and scoring.`

A better GameLens read would be:

- `PHI shows stronger pressure and turnover indicators, but NYG grades better in Offensive Output. This creates a split matchup profile rather than a clean overall edge.`
- `Pressure Lens: PHI advantage`
- `Turnovers / INT Risk: PHI advantage`
- `Offensive Output: NYG advantage`
- `Overall Matchup: split`
- `Confidence: cautious`

This helps the user understand the game without pretending the winner is solved.

### Example 2: BUF vs ATL

A bad product read would be:

- `BUF leads scoring, so BUF should win.`

A better GameLens read would be:

- `BUF has the stronger scoring profile, but ATL’s defensive and turnover indicators keep the matchup from becoming a clean BUF read.`
- `Scoring Lens: BUF advantage`
- `Pressure Lens: slight BUF advantage`
- `Turnovers / INT Risk: mixed`
- `Overall Matchup: cautious`
- `Confidence: limited by split signals`

### Example 3: TB vs DET

A better GameLens read might be:

- `DET shows stronger scoring and finishing ability, while TB holds up better defensively. The game leans toward DET in scoring-driven areas, but the defensive profile keeps the matchup from being one-dimensional.`
- `Scoring Lens: DET advantage`
- `Pressure Lens: DET advantage`
- `Defensive Control: TB advantage`
- `Overall Matchup: DET lean`
- `Confidence: stronger if scoring and pressure signals align`

### Example 4: CIN vs BUF

A better GameLens read might be:

- `BUF separates more clearly in scoring efficiency and offensive output, while the defensive comparison is closer. This creates a cleaner BUF matchup profile than games where the strengths are split.`
- `Scoring Lens: BUF advantage`
- `Offensive Output: BUF advantage`
- `Defensive Control: closer`
- `Overall Matchup: BUF advantage`
- `Confidence: stronger because multiple useful areas point the same direction`

---

## Practical Next Steps

### Step 1 — Ready Now

Fix tie handling.

Files:

- `services/game_service.py`
- `services/core_area_analysis.py`
- `services/model_trust_service.py`

Goal:

- Equal values become `neutral`.
- Neutral is treated as useful information.
- Neutral results do not create fake team advantage.

### Step 2 — Design Before Coding

Design as-of ranking tables.

Likely files:

- `agg/build_metric_rankings.py` — new
- `analytics/metric_registry.py`
- `queries/game_queries.py`
- `services/game_service.py`

Questions to settle:

- What is the ranking table grain?
- How should rankings work for `regular_season_to_date`, `last_3_games`, and `last_7_games`?
- How should category and Core Area rankings be summarized?
- Which ranking fields belong in the API response?
- Which ranking fields should stay backend-only?

### Step 3 — Keep Historical Context Light

Likely files:

- `queries/game_queries.py`
- `services/game_service.py`
- `services/model_trust_service.py`

Question to settle:

- What is the simplest useful historical sentence?

Better target:

- `Does this profile look familiar, recent, or mixed?`

Avoid:

- `Does history predict this winner?`

### Step 4 — Revisit Lens Views

Likely files:

- `services/game_service.py`
- `services/model_trust_service.py`
- `routes/game_routes.py`
- `queries/game_queries.py`

Question to settle:

- Which lens views help the user without adding label overload?

Start simple:

- Overall Matchup
- Scoring
- Pressure
- Turnovers / INT Risk
- Rushing / Control

### Step 5 — Rerun the Four-Game QA Batch Later

Use the same games:

- `20251207_CIN@BUF`
- `20251013_BUF@ATL`
- `20251020_TB@DET`
- `20251009_PHI@NYG`

Judge by:

- Did explanations get clearer?
- Did neutral/even work correctly?
- Did the app avoid fake certainty?
- Did ranking context make the game easier to understand?
- Did lens view help the user read the matchup?
- Did the app avoid drifting back into forced winner prediction?

---

## Final Product Sentence

GameLens helps users read NFL matchups by showing where each team has an edge, where the game is even, and which areas of the matchup are most worth paying attention to.

The app should make users smarter about the game, not simply tell them who to pick.


