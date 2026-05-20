## Definition of Done

Today’s goal is complete when `/game` responses expose claim-strength metadata inside `language_support`:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This should be metadata-only.

Do not change:

- frontend display
- user-facing summaries
- matchup lean
- outcome confidence
- Model Trust
- winner/prediction logic

## Validation Games

Use the same smoke-test games first:

- `20251009_PHI@NYG`
- `20251013_BUF@ATL`

Expected behavior:

- `yards_per_pass` and `yards_per_rush` should remain unblocked but gated
- `red_zone_efficiency`, `td_rate`, `turnover_margin_per_game`, and `yards_per_play` should remain blocked
- claim-strength fields should no longer be `null` where the data exists
- no wording should become louder yet

## Stop Point

If the metadata appears correctly and existing guardrails still work, stop and commit before touching frontend or copy generation.


--starting working--


# GameLens Daily Notes — Claim Strength Metadata Exposure

## Work Completed

Today I updated the `/game` response claim-language metadata layer so `language_support` now exposes the new Level 4 v0.2 claim-strength fields:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This was added as response-only metadata.

## Files Updated

- `services/claim_language_support_registry.py`
- `services/claim_language_response.py`

## What Changed

### `claim_language_support_registry.py`

Updated `build_language_support()` so the payload can include:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This keeps the registry compatible with the Level 3 / Level 4 claim-strength calibration work.

### `claim_language_response.py`

Added runtime claim-strength metadata helpers that classify response rows using percentile gap and football-area grouping.

Updated language support annotation for:

- Team Comparison rows
- Matchup Breakdown metric highlights
- Matchup Breakdown category summaries and drivers

Core Area summaries and context notes were intentionally left untouched for now.

## Validation Completed

Compiled successfully:

```bash
python -m py_compile services/claim_language_response.py services/claim_language_support_registry.py

Smoke-tested local /game responses:

20251009_PHI@NYG
20251013_BUF@ATL

Confirmed:

New claim-strength fields appear in language_support
yards_per_pass and yards_per_rush show measured/trusted metadata where appropriate
Boosting remains gated by two_way_context
Blocked metrics stay blocked:
red_zone_efficiency
td_rate
turnover_margin_per_game
yards_per_play
No frontend wording, matchup lean, Model Trust, outcome confidence, or winner logic changed
Notes

One local startup error occurred:

ModuleNotFoundError: No module named 'flask_cors'

This appeared to be a virtual environment issue, not a code bug. After switching into the correct venv and restarting app.py, the API worked and the new metadata appeared correctly.

Current Status

Backend metadata exposure for Level 4 v0.2 claim-strength fields is complete and smoke-tested.

Next step: commit the two changed service files.

--api has been patched--

What the new API feature does

GameLens already says things like:

“PHI shows a clear advantage in Red Zone Efficiency.”

The new feature answers the follow-up question:

How loudly should GameLens be allowed to say that?

That is the key. This does not change the pick. It does not change confidence. It does not change Model Trust. It gives each claim a little “language permission slip.”

Your response now has this inside language_support:

{
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "caution_area_edge",
  "claim_strength_language_signal": "caution_only",
  "language_boost_allowed": false,
  "reason": "two_way_context_not_supportive"
}

That means:

“The edge is numerically real, but this is a volatile/noisy area or the broader support is not strong enough, so don’t let the UI oversell it.”

That is a big deal. That is GameLens getting smarter about tone, not just data.

The main fields
claim_strength_bucket

This is the size of the edge based mostly on percentile gap.

Think of it like:

Bucket	Meaning
near_even	Basically too close
thin_edge	Small edge, soften language
usable_edge	There is some separation
real_edge	Big separation
missing	Not enough gap data

Example from PHI/NYG: PHI had a real_edge in Red Zone Efficiency because the percentile gap was huge, but that alone does not mean we should speak strongly.

claim_strength_context

This is the football meaning of that edge.

Examples:

Context	Meaning
trusted_real_edge	Big edge in a more trusted football area
trusted_measured_edge	Useful edge, but measured
watch_area_edge	Interesting, but still watch-list quality
caution_area_edge	Big edge, but volatile/noisy area
thin_gap	Skinny edge
near_even_gap	Too close
unclassified_edge	Gap exists, but row lacks enough category/core area context

This is why Red Zone Efficiency and Turnover Margin / Game can have real_edge but still get caution_area_edge. They are real signals, but volatile. Your system is saying: “Yes, this matters — but don’t act like it’s ironclad.” 🧯

claim_strength_language_signal

This is the product-facing language hint.

Signal	How to use it
boost_candidate	Candidate for stronger wording, only if other gates agree
measured	Useful support, but don’t hype it
caution_only	Treat as volatile/swing-factor language
soften	Use softer wording
normal	No special treatment
no_boost	Missing/insufficient support

In the PHI/NYG response, a lot of PHI’s flashy advantages are caution_only, especially Red Zone Efficiency, Points Per Play, Turnover Margin, and TD Rate. That’s valuable because the model liked PHI, but NYG won 34–17. The new metadata helps explain why some pregame claims should have sounded less certain, even when the raw edge looked big.

The most important field: language_boost_allowed

This is the final “can we talk louder?” switch.

For PHI/NYG, it is basically saying:

"language_boost_allowed": false

Why? Because both sides had:

"two_way_context": "available_mixed"

not:

"supportive"

Your own feature work showed two_way_context = supportive was the repeatable lift signal. It should be treated as claim-language support, not winner confidence. Supportive two-way context validated better than mixed/unavailable by roughly 7–8 points in prior testing, but it should not become an automatic pick-confidence boost.

So the API is doing the right thing:

“The stat edge exists, but the broader claim support is not strong enough to boost the language.”

That’s mature. That’s not a toy model anymore.

How we use it in the product
1. Better frontend copy later

Instead of blindly showing:

“PHI shows a clear advantage in Turnover Margin Per Game.”

The UI could eventually show:

“PHI has the turnover edge, but this is a volatile swing factor.”

Because that row says:

"claim_strength_context": "caution_area_edge",
"claim_strength_language_signal": "caution_only",
"support_level": "blocked"

That’s exactly the kind of nuance GameLens needs.

2. Better “Why?” tooltips

For a metric like Yards Per Rush, the PHI/NYG response says:

"claim_strength_bucket": "usable_edge",
"claim_strength_context": "trusted_measured_edge",
"claim_strength_language_signal": "measured"

But it also says:

"language_boost_allowed": false
"reason": "two_way_context_not_supportive"

So a tooltip could say:

“This is a useful rushing-efficiency signal, but broader two-way support was mixed, so GameLens keeps the language measured.”

That is excellent user education. It explains the model without overpromising.

3. Cleaner internal QA

This feature lets you filter claims after the fact:

Which boost_candidate claims actually validated?
Which caution_only claims caused model overconfidence?
Which soften claims should have suppressed summary language?
Which trusted_measured_edge claims are earning promotion later?

That connects directly to your claim-learning system: the goal is not just “did we pick the winner?” It is “did the pregame claim hold up in postgame data?”

PHI vs NYG in plain English

Here is what GameLens is now able to say internally:

PHI had several real pregame edges. Red Zone Efficiency, Points Per Play, TD Rate, Turnover Margin, and Third Down % all showed meaningful separation.

But the new claim-language metadata says:

“Careful. Many of those are caution/watch/blocked areas, and the broader two-way context is only available_mixed, not supportive.”

That is exactly the lesson from this game. PHI looked good in a lot of pregame signals, but the model should not necessarily have sounded as strong as the raw edges looked. NYG had an Offensive Output/Rushing Game path showing up too, especially Yards Per Rush and First Down Rate, but those also stayed measured because the two-way context was mixed.

My recommended use rule

For now, use this hierarchy:

1. language_boost_allowed
   If true, the claim may be eligible for stronger/supportive wording.

2. claim_strength_language_signal
   Determines tone:
   - boost_candidate = maybe stronger later
   - measured = useful but careful
   - caution_only = volatile/swing factor
   - soften = reduce confidence in wording

3. rule_status / reason
   Explains why the system blocked or allowed the language.

4. two_way_context
   Final safety gate. supportive matters. available_mixed means don’t boost.
The win

You just gave GameLens a new brain layer:

It can now separate “this edge exists” from “we should speak confidently about this edge.”

That is the difference between a stats page and an intelligence product. 🔥


--finished API integration--

Today’s Definition of Done

You wanted to expose the Level 4 v0.2 claim-strength metadata inside /game without changing product behavior.

That happened.

Completed
Added claim_strength_bucket
Added claim_strength_context
Added claim_strength_language_signal
Exposed them inside language_support
Added them to:
Team Comparison rows
Metric Highlights
Category Summary drivers
Category Summary aggregate support
Confirmed blocked metrics stayed blocked
Confirmed two_way_context still gates language boosts
Confirmed no changes to:
winner logic
matchup_lean
model_trust
outcome confidence
frontend copy
Ran compile successfully
Smoke-tested:
20251009_PHI@NYG
20251013_BUF@ATL
Created notes + deep-dive documentation

Finalized it into the API then ran the 'devil's advocate' test to collect responses of the API and asked CHATGPT to poke holes and find areas that didn't make sense and would make it difficult for a user to understand.

## Team Comparison Metadata Alignment Cleanup

Completed the first cleanup item from the Devil’s Advocate API smoke test.

The issue was that `team_comparison` rows were missing `category` and `core_area`, which caused claim-strength metadata to fall back to `unclassified_edge`. This made Team Comparison less aligned with Metric Highlights and Category Summaries, even when they were describing the same metric.

Updated `services/game_service.py` so visible Team Comparison rows now include `category` and `core_area`.

Also updated `agg/gamelens_training/build_claim_training_examples.py` so future claim-training rows preserve the same Team Comparison metadata instead of dropping it.

Validation completed:
- `services/game_service.py` compiled successfully
- `agg/gamelens_training/build_claim_training_examples.py` compiled successfully
- Local `/game` spot checks confirmed Team Comparison rows now include category/core_area
- Re-ran the same 30-game API smoke sample
- `team_comparison_unclassified_claim_strength` warning dropped from 30 to 0

Result: Team Comparison now speaks the same metadata language as Metric Highlights, Category Summaries, and claim-language support.