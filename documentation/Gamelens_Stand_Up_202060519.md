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