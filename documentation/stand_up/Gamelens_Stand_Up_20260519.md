# START HERE — Current GameLens Status

As of 2026-05-19, GameLens Level 4 v0.2 claim-strength calibration is complete through backend/GBQ/registry validation.

Completed:
- Added Level 3 `claim_strength_context_v1`
- Wrote claim-strength fields to `gamelens_claim_training_examples`
- Updated Level 4 calibration worker
- Wrote `level4_v0_2_claim_strength_language_calibration` to `gamelens_claim_language_calibration`
- Updated `services/claim_language_support_registry.py`
- Moved `yards_per_pass` and `yards_per_rush` from blocked to watch/measured support
- Kept `red_zone_efficiency`, `td_rate`, `turnover_margin_per_game`, and `yards_per_play` blocked
- Smoke-tested `/game` responses for `20251009_PHI@NYG` and `20251013_BUF@ATL`

Important: this does not change frontend copy, matchup lean, outcome confidence, Model Trust, or winner logic.

Next work:
Update `services/claim_language_response.py` so `/game` exposes:
- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

Do not rewrite summaries yet.


Level 4 v0.1 Handoff Summary — Claim Language Calibration
Milestone accomplished

We completed the first backend milestone for:

Level 4 v0.1 — Claim Language Calibration

This adds a guarded response-only metadata layer that can mark certain claims as having stronger historical/pre-game support without changing the model’s pick, confidence, Model Trust, or matchup lean logic.

The key idea:

two_way_context can support claim language,
but it does not predict winners and does not raise confidence by itself.
What was added

We added/validated the Level 4 claim-language support flow:

services/claim_language_support_registry.py
services/claim_language_response.py
services/claim_language_features.py
services/game_service.py integration

The registry currently supports a conservative v0.1 allowlist:

Strong allowlist
points_allowed_per_play
1st_down_rate
Watch-level allowlist
points_per_play
Conditional but disabled
third_down_pct
Blocked from automatic stronger language
red_zone_efficiency
turnover_margin_per_game
td_rate
yards_per_play
yards_per_pass
yards_per_rush
Rule behavior validated

The rule is now:

supportive two_way_context
+ allowlisted metric
+ allowlisted claim surface
= language_boost_allowed true

Everything else stays unboosted:

available_mixed = no boost
unavailable = no boost
blocked metric = no boost
conditional disabled metric = no boost
Smoke tests completed
Game	Purpose	Result
20231015_SF@CLE	Positive supportive case	Passed
20250113_MIN@LAR	Supportive but bad model miss	Passed
20251208_PHI@LAC	Available-mixed no-boost case	Passed
20240114_GB@DAL	High-confidence miss / no-boost case	Passed
20230907_DET@KC	Week 1 unavailable-data case	Passed

The MIN/LAR test confirmed that language support can exist even in a bad model miss without changing the outcome logic: MIN had supportive context, but the game still stayed logged as an incorrect model outcome.

The PHI/LAC and GB/DAL tests confirmed available_mixed correctly prevents boosts even when normal metrics like points_per_play or points_allowed_per_play appear favorable.

The DET/KC Week 1 test confirmed unavailable ranking/core-area context safely produces no language support and keeps the game as No Pick / Low confidence.

Current status
Backend Level 4 v0.1 smoke testing: complete
Functional behavior: passing
Frontend adoption: not started
Production wording: still needs care
Still required
Next step	Why
Add/confirm frontend handling for language_support	Decide whether to show a small “Supported read” affordance or keep hidden for now
Keep copy cautious	It must not imply prediction certainty
Run one broader regression batch later	Make sure no unexpected payload shape issues appear across many games
Document frontend rules	Which sections may display support metadata and which should ignore it
Eventually build Level 4 v0.2	Future feature runner / broader calibration system, but not yet

| Area                                                      | Status              |
| --------------------------------------------------------- | ------------------- |
| `two_way_context` claim-language use                      | ✅ Done for v0.1     |
| Backend smoke testing                                     | ✅ Done              |
| Frontend display decision                                 | ❌ Not done          |
| Broader regression batch                                  | Optional / not done |
| More Level 4 features                                     | ❌ Not started       |
| General feature runner architecture                       | ❌ Not built         |
| Future features like Trap Door, Drive Killer, Empty Yards | ❌ Not started       |


| Step                             |                Status | Why it matters                                             |
| -------------------------------- | --------------------: | ---------------------------------------------------------- |
| **1. two_way_context rules**     |                ✅ Done | First proven calibration signal                            |
| **2. API metadata annotation**   |                ✅ Done | `/game` can now carry `language_support` safely            |
| **3. Smoke tests**               |                ✅ Done | Behavior is correct in known cases                         |
| **4. Backend commit/checkpoint** |               ⏭️ Next | Lock the working state                                     |
| **5. Level 4 documentation**     |               ⏭️ Next | Prevent chat-history brain fog                             |
| **6. Broader regression batch**  |           Recommended | Make sure this does not break odd payload shapes           |
| **7. Decide Level 4 v0.2 scope** | After docs/regression | Pick next feature: Trap Door, Drive Killer, or Empty Yards |
| **8. Frontend display**          |                 Later | Only after backend rules are stable                        |

| Step  | Action                                                  | Why                                                  |
| ----- | ------------------------------------------------------- | ---------------------------------------------------- |
| **1** | Commit current working backend state                    | Protect the smoke-tested version                     |
| **2** | Create `build_claim_language_calibration.py`            | Makes Level 4 runnable after Level 3                 |
| **3** | Start with dry-run CSV/JSON only                        | Safer than writing BigQuery immediately              |
| **4** | Compare generated output to current hardcoded allowlist | Confirms the job reproduces what we manually learned |
| **5** | Then decide BigQuery table                              | Only after the dry run looks right                   |

We proved Level 4 rules work.
Now we need to package Level 4 as a repeatable worker.

| Item                                                                                | Why                                          |
| ----------------------------------------------------------------------------------- | -------------------------------------------- |
| Level 4 documentation                                                               | So this does not get lost in chat history    |
| Decide if `/game` should eventually read from `gamelens_claim_language_calibration` | Right now runtime still uses the registry    |
| Review the one `future_allowlist_candidate` later                                   | Do not enable yet                            |
| Plan Level 4 v0.2 features                                                          | Trap Door / Drive Killer / Empty Yards, etc. |
| Frontend display                                                                    | Later, not now                               |



----FEATURE WORK AFTER FINISHING UP LEVEL 4 DOCUMENTATION (GAMELENS_LEVEL4_SUMMARY.MD)

| Football/Product Name       | Code Name                  | Why you care                                           |
| --------------------------- | -------------------------- | ------------------------------------------------------ |
| **Is the edge real?**       | `claim_strength_score`     | Stops weak edges from sounding strong.                 |
| **Can they finish drives?** | `offense_finish_score`     | Separates empty yards from useful offense.             |
| **Is the profile messy?**   | `opposing_signal_pressure` | Prevents overconfidence when signals fight each other. |


The clean football story
1. Real offensive edges look promising

The strongest signals were:

Area	Validation
Passing Game real-edge headline metric	67.9%
Rushing Game real-edge headline metric	66.1%
Passing Game real-edge category summary	64.9%
Rushing Game real-edge category summary	61.2%

That’s a pretty clear product lesson:

When GameLens sees a real separation in passing or rushing production, those claims are much more likely to hold up.

This is the first “speak stronger here” bucket.

2. Defensive suppression also looks useful

Defensive Control / Scoring Suppression came in strong:

Area	Validation
Defensive Control core-area summary, real edge	59.2%
Scoring Suppression category summary, real edge	59.2%
Scoring Suppression usable edge	62.5%

Football translation:

“Can this team limit the other team’s scoring efficiency?” looks more trustworthy than broad defensive language.

That supports your existing direction: defensive_suppression is worth keeping and sharpening.

3. Turnovers are still danger-zone language

Turnovers stayed weak:

Area	Validation
Turnovers category summary, real edge	43.9%
Turnovers category summary, usable edge	36.2%
Disruption/Turnovers core-area summary, usable edge	36.2%
Turnover Risk game profile	41.0%

Product rule:

Turnovers can be shown, but they should not earn confident language by themselves.

Good wording:

“Turnover volatility is present.”

Bad wording:

“This team has a reliable turnover edge.”

4. Red zone / scoring production is weirdly not clean

This one is important.

Even with real edges:

Area	Validation
Red Zone Finish category summary	45.5%
Red Zone Finish headline metric	46.2%
Scoring Production category summary	46.0%
Scoring Production headline metric	43.9%

Football translation:

Scoring stats may be noisy unless paired with broader offensive movement or drive conversion.

So I would not let “Scoring Production” or “Red Zone Finish” alone drive stronger language yet.

The product takeaway

GameLens should not treat all “real edges” equally.

Better rule
Football area	Product behavior
Passing Game real edge	Can speak stronger
Rushing Game real edge	Can speak stronger
Scoring Suppression / Defensive Control	Can speak measured-stronger
Drive Conversion	Worth watching
Red Zone Finish	Caution
Scoring Production	Caution
Turnovers	Volatility only
Pressure / Turnover Risk game profile	Do not boost yet
My recommendation

For the first real feature, I’d define:

claim_strength_context

Not just:

claim_strength_score

Because the football area matters.

Plain-English version:

“A real edge only deserves stronger language when the football area has proven it can hold up.”

That’s much smarter than “big gap = strong claim.”

Next move

I’d now test a simple allow/caution grouping:

Stronger-language candidates
Passing Game
Rushing Game
Scoring Suppression
Defensive Control
Drive Conversion
Caution/blocked from stronger language
Turnovers
Red Zone Finish
Scoring Production
Pressure
Turnover Risk
Disruption and Turnovers

That keeps the next step clean and product-safe.

Level 3: claim_strength_context_v1 ✅
Level 4: claim_strength_language_calibration v0.2 ✅
GBQ sanity check ✅

Level 3 claim_strength_context_v1 ✅
Level 4 claim-strength calibration v0.2 ✅
Runtime registry v0.2 ✅
GBQ Level 4 replacement write ✅

Level 3 feature added ✅
Level 3 written to GBQ ✅
Level 4 calibration updated ✅
Registry updated ✅
Level 4 replacement written to GBQ ✅
GBQ sanity check passed ✅

wrap up at the end of the day:

Markdown summary for your notes
# GameLens Level 4 Claim-Strength Language Calibration Update

## Date
2026-05-19

## Summary

Today we added the first version of a claim-strength language feature for GameLens.

The goal was not to improve winner prediction directly. The goal was to help GameLens decide when a matchup claim deserves stronger, measured, softened, or cautious language.

In plain football terms:

> Is this edge real enough, and in a football area we trust enough, to let GameLens speak more clearly?

This work adds a safer language layer between raw matchup metrics and user-facing interpretation.

---

## What changed

### 1. Added Level 3 claim-strength feature logic

Updated:

```text
agg/gamelens_training/update_claim_training_features.py

Added the new Level 3 formula version:

claim_strength_context_v1

New fields added/populated:

claim_strength_score
claim_strength_bucket
claim_strength_context
claim_strength_language_signal
claim_strength_notes

The feature groups claims into signals such as:

boost_candidate
measured
normal
soften
caution_only
no_boost

Football meaning:

Signal	Meaning
boost_candidate	Real edge in a trusted football area
measured	Useful, but not loud enough for strong language
soften	Thin or near-even edge; calm the wording down
caution_only	Volatile/noisy football area; do not speak strongly
no_boost	Missing or unusable gap context
normal	No special language treatment

Level 3 was written to GBQ successfully.

2. Updated Level 4 claim-language calibration

Updated:

agg/gamelens_training/build_claim_language_calibration.py

Level 4 now reads and summarizes the new claim-strength fields.

New calibration output fields include:

claim_strength_calibration_recommendation
claim_strength_language_modifier
avg_claim_strength_score
min_claim_strength_score
max_claim_strength_score

The Level 4 run wrote:

440 rows

to:

nfl-stream-406420.Analytics.gamelens_claim_language_calibration

Calibration run:

larger_240_level3_qa_20260517__level4_v0_2

Calibration version:

level4_v0_2_claim_strength_language_calibration

Overall validation baseline remained:

49.23%
3. Updated runtime language-support registry

Updated:

services/claim_language_support_registry.py

The registry now treats passing/rushing efficiency as measured/watch candidates instead of blocked metrics.

Strong allowlist remains
1st_down_rate
points_allowed_per_play
Watch/measured allowlist now includes
points_per_play
yards_per_pass
yards_per_rush
Still blocked from automatic stronger language
red_zone_efficiency
td_rate
turnover_margin_per_game
yards_per_play

Important product decision:

yards_per_pass and yards_per_rush may receive measured support language, but not strong language yet.

4. Re-ran Level 4 after registry update

The updated Level 4 recommendation distribution was confirmed in GBQ:

Recommendation	Rows
keep_blocked	164
no_boost_context_not_supportive	154
review_conditional_candidate	48
keep_v0_1_strong_allow	28
insufficient_sample	21
keep_v0_1_watch_allow	18
future_allowlist_candidate	5
keep_normal_language	2

This confirmed that the registry update was applied correctly.

5. Local /game smoke test

Tested local game responses for:

20251009_PHI@NYG
20251013_BUF@ATL

The important result:

yards_per_pass
yards_per_rush

are no longer treated as blocked metrics.

However, they are still correctly gated by two_way_context.

In the tested games, the context was available_mixed, so the API did not boost language. This is correct behavior.

Blocked metrics also stayed blocked:

red_zone_efficiency
td_rate
turnover_margin_per_game
yards_per_play

This means the change did not accidentally make the language louder everywhere.

Current status

Completed:

Level 3 claim-strength feature added
Level 3 written to GBQ
Level 4 calibration updated
Registry updated
Level 4 replacement written to GBQ
GBQ sanity checks passed
/game smoke tests showed safe behavior

Not changed:

Frontend
User-facing summary copy
Matchup lean
Outcome confidence
Model Trust winner logic
Prediction logic
Product takeaway

This update gives GameLens a more careful football-language gate.

Instead of treating every edge the same, GameLens can now distinguish between:

real trusted edges
measured edges
thin edges
caution-only areas
blocked/noisy metrics

This is especially important because rushing and passing efficiency showed promise in calibration, while turnover, red zone, and TD-rate style claims remain too volatile for automatic stronger language.

