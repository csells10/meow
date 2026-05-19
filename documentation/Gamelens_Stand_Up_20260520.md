## Tomorrow’s work blurb

```markdown
## Tomorrow's Work

Next step is to update:

```text
services/claim_language_response.py

Goal:

Expose the new claim-strength metadata in /game responses:

claim_strength_bucket
claim_strength_context
claim_strength_language_signal

This should be metadata-only. Do not rewrite summaries, do not touch frontend copy, and do not change matchup lean or outcome confidence yet.

The current /game smoke tests showed that the registry update is working, but these claim-strength fields are still returning as null. Tomorrow’s goal is to wire those fields through cleanly so the API can show whether a claim is a real edge, thin edge, caution edge, or no-boost case.

## Tomorrow's Work

Update `services/claim_language_response.py` so `/game` responses expose the new claim-strength metadata:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`

This should remain metadata-only. Do not rewrite summaries, do not touch frontend copy, and do not change matchup lean, outcome confidence, Model Trust, or winner logic.