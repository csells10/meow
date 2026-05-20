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