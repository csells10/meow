# GameLens QA Findings — 73-Game Before/After Flag Comparison

## Purpose

This QA pass compared the `/game/<game_id>` API output with the windowed metrics feature flag off versus on:

- `USE_WINDOWED_METRICS_FOR_GAME=false` → before snapshot
- `USE_WINDOWED_METRICS_FOR_GAME=true` → after snapshot

The goal was not only to see whether picks improved, but to understand whether the new windowed/ranking setup makes the API more honest, more selective, or too aggressive.

This analysis used a 73-game 2025 sample across early season, midseason, late season, and postseason games.

---

## Executive Summary

The new setup is directionally encouraging, but confidence calibration still needs work.

The `flag=true` version made fewer picks, produced more No Picks, reduced incorrect picks, and slightly improved overall pick accuracy. That is a good sign because it means the new windowed/ranking setup did **not** simply make the model louder.

However, `High` confidence remains overstated. In the 73-game sample, `Medium` confidence performed better than `High`, which means the model is better at identifying matchup shape than it is at deciding when that shape deserves strong outcome confidence.

The next improvement should not be a full model rewrite. The next improvement should be guardrails that prevent fragile profiles from being described too confidently.

---

## Before/After Scoreboard

| Measure | Flag False | Flag True | Change |
|---|---:|---:|---:|
| Games tested | 73 | 73 | — |
| Picks made | 49 | 42 | Fewer picks |
| No Picks | 24 | 31 | More restraint |
| Correct picks | 27 | 24 | Down 3 |
| Incorrect picks | 22 | 18 | Down 4 |
| Pick accuracy | 55.1% | 57.1% | Slightly better |
| High confidence picks | 18 | 16 | Slightly fewer |
| High confidence misses | 8 | 7 | Slightly fewer |

### Interpretation

The `flag=true` version is more selective. It avoided more bad picks, but also backed away from some good picks. This is acceptable for a first pass because the project direction is to explain matchup shape honestly rather than force picks.

---

## Result Transition Matrix

| Before → After | Count | Read |
|---|---:|---|
| Correct → Correct | 22 | Stable wins preserved |
| Correct → No Pick | 5 | Cost of added restraint |
| Incorrect → Incorrect | 16 | Still-missed profiles |
| Incorrect → No Pick | 5 | Good restraint improvement |
| Incorrect → Correct | 1 | Direct improvement |
| No Pick → No Pick | 21 | Stable restraint preserved |
| No Pick → Correct | 1 | Useful new pick |
| No Pick → Incorrect | 2 | New bad aggression |

### Key takeaway

The most important risk category is:

- `No Pick → Incorrect`

Those are the games where the new source created a pick that probably should have stayed cautious.

---

## Confidence Findings

After `flag=true`:

| Confidence | Picks | Correct | Incorrect | Accuracy |
|---|---:|---:|---:|---:|
| High | 16 | 9 | 7 | 56.3% |
| Medium | 8 | 6 | 2 | 75.0% |
| Low picks only | 18 | 9 | 9 | 50.0% |

### Interpretation

`Medium` confidence outperforming `High` is the clearest sign that `High` is not calibrated yet.

The word `High` currently means something closer to:

> The pregame matchup profile looks strongly separated.

It does **not** yet reliably mean:

> The model has highly reliable outcome confidence.

This distinction matters for product language.

---

## Confidence Transitions

There were 16 confidence changes:

- 8 upgrades
- 8 downgrades

Upgrade results:

| Upgrade Result | Count |
|---|---:|
| Correct | 3 |
| Incorrect | 5 |

Downgrade results:

| Downgrade Result | Count |
|---|---:|
| Correct | 3 |
| Incorrect | 3 |
| No Pick | 2 |

### Low → High transitions

Low → High happened 5 times:

| Game | After Result | After Pick | Actual Winner |
|---|---|---|---|
| `20250921_CIN@MIN` | Incorrect | CIN | MIN |
| `20250928_LAC@NYG` | Incorrect | LAC | NYG |
| `20251012_DET@KC` | Incorrect | DET | KC |
| `20251201_NYG@NE` | Correct | NE | NE |
| `20251221_NE@BAL` | Correct | NE | NE |

Low → High was 2 correct and 3 incorrect.

### Recommendation

Do not let a previously cautious profile jump directly to `High` unless multiple trusted layers agree cleanly.

---

## High Confidence Misses

After `flag=true`, there were 7 High-confidence misses:

| Game | Pick | Actual Winner | Edge Strength | Signal Gap | Core Split | Max Lag Days |
|---|---|---|---|---:|---|---:|
| `20250921_CIN@MIN` | CIN | MIN | strong | 7 | 3-0 | 4 |
| `20250928_LAC@NYG` | LAC | NYG | strong | 10 | 4-0 | 4 |
| `20251012_DET@KC` | DET | KC | low | 7 | 2-1 | 4 |
| `20251102_CAR@GB` | GB | CAR | strong | 10 | 1-3 | 4 |
| `20251127_CIN@BAL` | BAL | CIN | low | 6 | 0-5 | 1 |
| `20251225_DET@MIN` | DET | MIN | strong | 5 | 3-1 | 1 |
| `20260104_KC@LV` | KC | LV | strong | 7 | 4-0 | 9 |

### Key read

Most High-confidence misses were not stale-data problems. The issue was usually that the model saw a strongly separated statistical profile and treated that as High outcome confidence.

That is the calibration problem.

---

## Strong Finding: High Confidence + Low Visible Edge Is Dangerous

Two after-flag games had:

- `after_confidence = High`
- `after_edge_strength = low`

Both were incorrect:

| Game | Pick | Actual Winner |
|---|---|---|
| `20251012_DET@KC` | DET | KC |
| `20251127_CIN@BAL` | BAL | CIN |

### Guardrail candidate

If `edge_strength == low`, do not allow `High` confidence.

This is a clean guardrail because it does not require football-specific assumptions. It simply prevents the API from saying the broader result is High confidence when the visible Team Comparison layer is weak.

---

## Field Control Finding

When Field Control appeared in the after/core-area leaders:

| Field Control Present? | Picks | Correct | Incorrect | Accuracy |
|---|---:|---:|---:|---:|
| No | 32 | 20 | 12 | 62.5% |
| Yes | 10 | 4 | 6 | 40.0% |

### Interpretation

This does not mean Field Control is useless. It means it should remain observation-only until we better understand its predictive value and stability.

### Guardrail candidate

Field Control / Special Teams can be displayed, but it should not help create:

- `confirmed_edge`
- `High` confidence
- confidence upgrades

Field Control should stay visible as context, not steering logic.

---

## Lag / Freshness Finding

The earlier concern was whether `data_lag_days` should automatically soften confidence. The 73-game sample suggests it should **not**.

Only two games had `after_ranking_max_data_lag_days >= 14`:

| Game | Result | Confidence | Max Lag Days |
|---|---|---|---:|
| `20251103_ARI@DAL` | No Pick | Low | 14 |
| `20251110_PHI@GB` | Correct | Low | 14 |

High-confidence misses generally had normal lag values: mostly 1–4 days, with one 9-day case.

### Updated guardrail

`data_lag_days` should be treated as context, not an automatic penalty.

Use it to explain freshness:

> One team has an older available profile. This may reflect a bye week or schedule gap, so it is shown as context rather than treated as a weakness.

Do not automatically lower confidence because of lag alone.

---

## Ranking Availability

Ranking context was available for 69 of 73 games.

The 4 missing games were early Week 1 cases before a useful regular-season-to-date profile existed:

- `20250904_DAL@PHI`
- `20250907_BAL@BUF`
- `20250907_DET@GB`
- `20250907_MIA@IND`

This is expected and not a concern.

---

## Profile Type Findings

After `flag=true`:

| Profile Type | Correct | Incorrect | No Pick | Pick Accuracy |
|---|---:|---:|---:|---:|
| confirmed_edge | 18 | 10 | 0 | 64.3% |
| split_profile | 2 | 5 | 0 | 28.6% |
| coin_flip_profile | 3 | 2 | 0 | 60.0% |
| conflicting_profile | 1 | 1 | 0 | 50.0% |
| no_clear_edge | 0 | 0 | 31 | — |

### Interpretation

`confirmed_edge` is useful, but it is not enough by itself to justify `High` confidence.

`split_profile` is especially weak and should continue to use cautious language.

---

## Updated Guardrail Candidates

These are implementation candidates. They should not be treated as permanent model law until retested.

### 1. Keep exact neutral handling

Already implemented and working.

Equal metric values should remain neutral and should not create fake separation.

---

### 2. Add near-even handling

Exact ties are fixed, but tiny differences can still create hard edges.

Possible first-pass rule:

- If ranking percentile gap is very small, mark the row as `near_even`
- Near-even rows can display, but should not become strong reasoning drivers
- Near-even rows should not help create `High` confidence

The exact threshold needs tuning, but `percentile_gap <= 3.5` is a reasonable starting candidate.

---

### 3. Cap High confidence when visible edge strength is low

If `edge_strength == low`, do not allow `High` confidence.

Suggested behavior:

- High → Medium
- Add confidence note: broader profile was supportive, but visible comparison separation was limited

This directly targets two High-confidence misses from the sample.

---

### 4. Keep Field Control observation-only

Field Control can remain visible, but should not help create or upgrade confidence.

Suggested behavior:

- Exclude Field Control from confidence-driving Core Area confirmation
- Keep it available in context/summary output
- Do not let Field Control turn a mixed profile into a confirmed edge

---

### 5. Treat freshness/lag as context, not penalty

Do not automatically downgrade confidence for `data_lag_days >= 14`.

Only use lag as a caution when combined with other issues:

- limited ranking coverage
- missing recent form
- close or mixed profile
- model leaning heavily on old ranking rows
- large freshness mismatch between teams

Future useful fields:

- `days_since_last_game`
- `rest_advantage_days`
- `games_in_window`

These would help separate true rest/bye context from missing or stale data.

---

### 6. Treat Low → High upgrades as suspicious

Low → High upgrades were more often wrong than right in this sample.

Suggested behavior:

- Require stronger multi-layer confirmation before allowing Low → High
- Consider allowing Low → Medium more freely than Low → High
- Add an audit flag when confidence jumps by two tiers

---

### 7. Separate profile strength from outcome confidence

The model often identifies strong matchup shape correctly, but that does not always mean the outcome confidence should be High.

Recommended product distinction:

- `profile_strength`: how separated the matchup profile looks
- `confidence`: how strongly the model should stand behind the outcome lean

Possible language:

- Strong Profile / Medium Confidence
- Mixed Profile / Low Confidence
- Clear Statistical Edge / Caution on Outcome Confidence

This may be better than forcing every strong profile into `High` confidence.

---

## Recommended Implementation Order

1. Keep exact neutral handling as-is.
2. Add near-even detection and expose it in API output.
3. Add `edge_strength == low` cap so low visible edge cannot be High confidence.
4. Keep Field Control observation-only for confidence.
5. Add freshness/coverage context fields, but do not apply automatic lag penalties.
6. Add an audit flag for two-tier confidence jumps.
7. Introduce `profile_strength` separately from `confidence`.

---

## Suggested Code Touchpoints

Likely files:

- `services/game_service.py`
- `services/core_area_analysis.py`
- `services/model_trust_service.py`
- `queries/game_queries.py` only if additional ranking fields are needed

Avoid changing the ranking builder until there is evidence that the ranking table itself is wrong. Current evidence suggests the table is mostly doing its job.

---

## Current Product Direction

The ranking/windowed setup should help GameLens explain matchup shape, not force stronger picks.

The next work should make the API less over-declarative, not more complicated.

A good near-term goal:

> The API should be able to say, “This team has the stronger statistical profile, but confidence remains limited because the edge is narrow, mixed, near-even, or supported by less-trusted context.”

That fits the direction of GameLens as a matchup explanation and confidence-calibration tool rather than a hand-coded pick machine.
