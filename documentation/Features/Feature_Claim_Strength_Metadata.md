# GameLens Claim-Strength Metadata Deep Dive

**Date:** 2026-05-20  
**Feature:** Claim-strength metadata exposed in `/game` response `language_support` objects  
**Status:** Backend metadata exposure complete and smoke-tested  
**Primary files updated:**

- `services/claim_language_support_registry.py`
- `services/claim_language_response.py`

---

## 1. Plain-English Summary

This feature gives GameLens a **language governor**.

Before this update, GameLens could identify matchup advantages and describe them, such as:

> PHI shows a clear advantage in Red Zone Efficiency.

That was useful, but it did not fully answer an important product question:

> Should GameLens be allowed to speak strongly about this edge?

The new claim-strength metadata helps separate:

1. **The edge exists**
2. **The edge is meaningful**
3. **The edge is in a trusted or volatile football area**
4. **The broader context supports speaking more strongly**
5. **The UI/API should or should not boost the language**

That distinction matters because a metric can have a big numerical edge but still be volatile, noisy, or unsupported by broader matchup context.

---

## 2. What We Added to the API

The `/game` response now exposes three new fields inside each supported row’s `language_support` object:

```json
{
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "caution_area_edge",
  "claim_strength_language_signal": "caution_only"
}
```

These fields now appear on:

- `team_comparison[*].language_support`
- `matchup_breakdown.metric_highlights[*].language_support`
- `matchup_breakdown.category_summaries[*].drivers[*].language_support`
- `matchup_breakdown.category_summaries[*].language_support` as an aggregate/summary-level metadata object

We intentionally did **not** annotate these yet:

- `matchup_breakdown.core_area_summaries`
- `matchup_breakdown.context_notes`

Those can be evaluated later.

---

## 3. What This Feature Is Not

This feature does **not**:

- Pick winners
- Change `matchup_lean`
- Change `outcome_confidence`
- Change `model_trust`
- Change final prediction logic
- Rewrite frontend copy yet
- Automatically make a claim stronger
- Override blocked metrics

It only attaches structured metadata so later product/UI layers can decide how to talk about each claim.

---

## 4. Why This Matters

GameLens is becoming more than a stats page.

A basic stats app can say:

> Team A has a better number than Team B.

A smarter matchup product should say:

> Team A has a better number, but this edge is volatile, thin, blocked, or not fully supported.

This new feature lets GameLens distinguish between:

| Situation | Old API Behavior | New API Meaning |
|---|---|---|
| Big gap in a trusted area | Shows advantage | May become measured or boost candidate |
| Big gap in volatile area | Shows advantage | Mark as caution-only |
| Small gap | Shows advantage/lean | Mark as thin or soften |
| Near-even gap | Might still appear directional elsewhere | Mark as near-even/soften |
| Blocked metric | Could still look flashy | Explicitly blocked from stronger language |
| Mixed broader profile | Still shows raw edge | Prevents language boost |

This gives the app room to become more honest, careful, and educational.

---

## 5. The Main Fields

### 5.1 `claim_strength_bucket`

This describes the size of the pregame edge, usually from percentile gap.

| Bucket | Meaning | Product Interpretation |
|---|---|---|
| `missing` | No percentile-gap evidence | Do not boost |
| `near_even` | Gap is too small | Soften or avoid edge language |
| `thin_edge` | Small edge | Use softer wording |
| `usable_edge` | Meaningful but not huge | Measured language |
| `real_edge` | Large separation | Candidate for stronger language, depending on context |

Example:

```json
{
  "metric": "red_zone_efficiency",
  "percentile_gap": 100.0,
  "claim_strength_bucket": "real_edge"
}
```

This means the edge is numerically large.

But that does **not** automatically mean GameLens should speak strongly.

---

### 5.2 `claim_strength_context`

This describes the football/product meaning of the edge.

| Context | Meaning |
|---|---|
| `missing_gap_context` | No gap data exists |
| `near_even_gap` | Too close to speak strongly |
| `thin_gap` | Small edge; soften language |
| `trusted_real_edge` | Large edge in a more trusted area |
| `trusted_measured_edge` | Useful edge in a trusted area, but not enough for strongest wording |
| `watch_area_edge` | Interesting, but still should be measured |
| `caution_area_edge` | Large edge in a volatile/noisy area |
| `unclassified_edge` | Edge exists, but row lacks enough category/core-area context to classify cleanly |

Examples:

```json
{
  "metric": "yards_per_rush",
  "claim_strength_bucket": "usable_edge",
  "claim_strength_context": "trusted_measured_edge"
}
```

This says:

> Rushing efficiency is a useful/trusted area, but this edge should stay measured.

Another example:

```json
{
  "metric": "turnover_margin_per_game",
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "caution_area_edge"
}
```

This says:

> The turnover edge is real numerically, but turnover-related claims are volatile and should not be hyped.

---

### 5.3 `claim_strength_language_signal`

This is the product-language hint.

| Signal | Meaning | Possible Future UI Behavior |
|---|---|---|
| `no_boost` | Missing or insufficient data | Do not enhance |
| `soften` | Thin/near-even edge | Use cautious wording |
| `normal` | No special treatment | Keep normal wording |
| `measured` | Useful but not strong | Use measured support language |
| `caution_only` | Volatile or noisy | Call it a swing factor/caution |
| `boost_candidate` | Candidate for stronger wording | Only boost if other gates agree |

The most important point:

> `boost_candidate` does not mean “boost now.”  
> It means “this claim could be eligible if the other support gates agree.”

---

## 6. The Final Gate: `language_boost_allowed`

The new fields are descriptive, but the existing final gate still matters:

```json
{
  "language_boost_allowed": false
}
```

This remains the safest product control.

A claim can have:

```json
{
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "trusted_real_edge",
  "claim_strength_language_signal": "boost_candidate"
}
```

and still not receive boosted language if:

```json
{
  "two_way_context": "available_mixed"
}
```

That is intentional.

The current design keeps `two_way_context = supportive` as the key gate for allowing stronger claim-language support.

---

## 7. How This Works With `two_way_context`

The response also includes:

```json
"claim_language_context": {
  "two_way_context_by_side": {
    "away": {
      "offense_finish_score": 0.0836,
      "defensive_suppression_score": 0.2782,
      "two_way_context": "available_mixed",
      "two_way_edge_score": 0.0836
    }
  }
}
```

The claim-strength feature asks:

> Is this specific claim strong enough and in the right football area?

The two-way context asks:

> Does the claimed side have broader support from both offensive finish and defensive suppression?

Both matter.

### Practical rule

Use this order:

1. Check `language_boost_allowed`
2. If false, do not enhance language
3. Check `claim_strength_language_signal` for tone guidance
4. Check `rule_status` and `reason` to explain why
5. Check `two_way_context` to see whether broader support was strong enough

---

## 8. PHI vs NYG Example

Game: `20251009_PHI@NYG`  
Final: NYG 34, PHI 17

GameLens leaned PHI pregame, but NYG won. This is exactly the kind of game where language calibration matters.

### 8.1 Big PHI edges existed

Examples from the response:

- Red Zone Efficiency: PHI huge edge
- Points Per Play: PHI clear edge
- TD Rate: PHI clear edge
- Turnover Margin / Game: PHI clear edge
- Third Down %: PHI edge

Old behavior could make those sound very confident.

New behavior says:

> Yes, the edges exist, but many are caution/watch/blocked signals and broader two-way support was mixed.

---

### 8.2 Red Zone Efficiency

Example:

```json
{
  "metric": "red_zone_efficiency",
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "caution_area_edge",
  "claim_strength_language_signal": "caution_only",
  "support_level": "blocked",
  "language_boost_allowed": false,
  "reason": "red_zone_efficiency is blocked from automatic stronger language."
}
```

Plain English:

> PHI had a real red-zone edge, but red-zone efficiency is too volatile to automatically strengthen the language.

Potential future UI wording:

> PHI had a major red-zone edge, but this is treated as a volatile finishing signal rather than a locked-in advantage.

---

### 8.3 Turnover Margin / Game

Example:

```json
{
  "metric": "turnover_margin_per_game",
  "claim_strength_bucket": "real_edge",
  "claim_strength_context": "caution_area_edge",
  "claim_strength_language_signal": "caution_only",
  "support_level": "blocked",
  "language_boost_allowed": false
}
```

Plain English:

> The turnover profile strongly favored PHI, but turnover edges are volatile and should be framed as swing factors.

Potential future UI wording:

> PHI had the turnover-profile edge, but GameLens treats this as a volatile swing factor.

---

### 8.4 Yards Per Rush

Example:

```json
{
  "metric": "yards_per_rush",
  "claim_strength_bucket": "usable_edge",
  "claim_strength_context": "trusted_measured_edge",
  "claim_strength_language_signal": "measured",
  "language_boost_allowed": false,
  "reason": "two_way_context_not_supportive"
}
```

Plain English:

> NYG had a useful rushing-efficiency edge, but the broader support was mixed, so GameLens should keep the language measured.

Potential future UI wording:

> NYG showed a meaningful rushing-efficiency edge, but the broader claim support was mixed.

---

### 8.5 Points Allowed Per Play

Example:

```json
{
  "metric": "points_allowed_per_play",
  "claim_strength_bucket": "near_even",
  "claim_strength_context": "near_even_gap",
  "claim_strength_language_signal": "soften",
  "language_boost_allowed": false
}
```

Plain English:

> The defensive scoring suppression gap was too close to speak strongly.

Potential future UI wording:

> Defensive scoring suppression was effectively close, so this should not be treated as a major edge.

---

## 9. How Frontend Could Use This Later

### 9.1 Tooltips

Each row could show a small “Why?” tooltip based on `language_support`.

Example logic:

```ts
if (language_support.claim_strength_language_signal === "caution_only") {
  return "This edge exists, but GameLens treats this area as volatile.";
}

if (language_support.claim_strength_language_signal === "soften") {
  return "The gap is thin or near-even, so GameLens keeps the language cautious.";
}

if (language_support.claim_strength_language_signal === "measured") {
  return "This is useful support, but not strong enough to overstate.";
}

if (language_support.claim_strength_language_signal === "boost_candidate") {
  return "This is a candidate for stronger language, but only if other gates agree.";
}
```

---

### 9.2 Badges

Possible future badges:

| Signal | Badge |
|---|---|
| `boost_candidate` | Backed by matchup candidate |
| `measured` | Measured support |
| `caution_only` | Volatile signal |
| `soften` | Thin edge |
| `normal` | Standard read |
| `no_boost` | Insufficient support |

Important:

Do not show “Backed by matchup” just because `boost_candidate` exists.

Use:

```ts
language_boost_allowed === true
```

as the actual permission to show stronger support language.

---

### 9.3 Summary Copy

Future copy could become more nuanced.

Instead of:

> PHI shows a clear advantage in Red Zone Efficiency.

Use:

> PHI shows a major red-zone edge, but GameLens treats this as a volatile finishing signal.

Instead of:

> NYG shows a clear advantage in Yards Per Rush.

Use:

> NYG has a meaningful rushing-efficiency edge, though broader support is mixed.

Instead of:

> Points Allowed Per Play leans PHI.

Use:

> Points Allowed Per Play is close enough that GameLens softens the read.

---

## 10. How Backend QA Can Use This

This feature creates better QA slices.

Useful questions:

- Which `boost_candidate` claims actually validated?
- Which `caution_only` claims caused model overconfidence?
- Which `soften` claims should have suppressed summary language?
- Which `trusted_measured_edge` claims are earning promotion later?

### By signal

```sql
SELECT
  claim_strength_language_signal,
  COUNT(*) AS rows,
  AVG(CASE WHEN validated_flag THEN 1 ELSE 0 END) AS validation_rate
FROM Analytics.gamelens_claim_training_examples
GROUP BY claim_strength_language_signal
ORDER BY rows DESC;
```

### By context

```sql
SELECT
  claim_strength_context,
  COUNT(*) AS rows,
  AVG(CASE WHEN validated_flag THEN 1 ELSE 0 END) AS validation_rate
FROM Analytics.gamelens_claim_training_examples
GROUP BY claim_strength_context
ORDER BY rows DESC;
```

### By metric

```sql
SELECT
  metric,
  claim_strength_language_signal,
  COUNT(*) AS rows,
  AVG(CASE WHEN validated_flag THEN 1 ELSE 0 END) AS validation_rate
FROM Analytics.gamelens_claim_training_examples
WHERE metric IS NOT NULL
GROUP BY metric, claim_strength_language_signal
ORDER BY rows DESC;
```

---

## 11. Recommended Product Use Rules

### Rule 1: Never use claim strength as winner confidence

Bad:

> `boost_candidate` means the pick is stronger.

Good:

> `boost_candidate` means the claim’s language might deserve stronger wording if gates agree.

---

### Rule 2: `language_boost_allowed` is the permission gate

Only enhance language when:

```ts
language_support.language_boost_allowed === true
```

Everything else should be treated as explanation metadata.

---

### Rule 3: Use `claim_strength_language_signal` for tone

Recommended tone mapping:

| Signal | Tone |
|---|---|
| `boost_candidate` | Stronger candidate, but still gated |
| `measured` | Balanced/qualified |
| `caution_only` | Volatile/swing-factor wording |
| `soften` | Light/cautious wording |
| `normal` | Standard wording |
| `no_boost` | No special wording |

---

### Rule 4: Blocked metrics stay blocked

Even if the gap is huge, these should not receive automatic stronger language:

- `red_zone_efficiency`
- `td_rate`
- `turnover_margin_per_game`
- `yards_per_play`

Example:

```json
{
  "metric": "red_zone_efficiency",
  "claim_strength_bucket": "real_edge",
  "language_boost_allowed": false,
  "support_level": "blocked"
}
```

Interpretation:

> Big edge, but blocked from stronger language.

---

### Rule 5: Treat `available_mixed` as a brake

If:

```json
{
  "two_way_context": "available_mixed"
}
```

then the API should not boost language, even when the claim-strength signal looks interesting.

That protects against overconfidence.

---

## 12. Current Known Limitation

### Team Comparison rows may show `unclassified_edge`

Some `team_comparison` rows do not carry `category` or `core_area`, so claim-strength context may show:

```json
"claim_strength_context": "unclassified_edge"
```

This is acceptable for now.

The row still gets:

- `claim_strength_bucket`
- `claim_strength_language_signal`
- `two_way_context`
- `rule_status`
- `reason`

Future improvement:

- enrich Team Comparison rows with `category` and `core_area`
- then the runtime classifier can identify trusted/watch/caution areas more accurately

---

## 13. Current Known Guardrail

Core Area summaries are not annotated yet.

Current code intentionally leaves out:

- `core_area_summaries`
- `context_notes`

Reason:

> We should first validate the metadata on Team Comparison, Metric Highlights, and Category Summaries before adding more surfaces.

This was the right restraint.

---

## 14. Suggested Frontend Adoption Plan

### Phase 1: No UI changes yet

Use this phase to inspect and QA payloads.

Goals:

- Confirm fields appear consistently
- Confirm blocked metrics stay blocked
- Confirm `available_mixed` blocks boosts
- Confirm `soften`, `measured`, and `caution_only` make sense

---

### Phase 2: Developer-only display

Add temporary debug display or console visibility for:

- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`
- `language_boost_allowed`
- `reason`

This helps QA without changing user-facing product copy.

---

### Phase 3: Tooltips

Add safe tooltips first.

Examples:

- `caution_only`: “This edge exists, but GameLens treats this area as volatile.”
- `soften`: “The gap is thin or near-even, so language stays cautious.”
- `measured`: “This is useful support, but not strong enough for stronger wording.”
- `boost_candidate`: “This is a candidate for stronger language, but only if other gates agree.”

---

### Phase 4: Badges

Only add badges after tooltip behavior feels right.

Potential badges:

- Measured Support
- Volatile Signal
- Thin Edge
- Backed by Matchup

Do not show “Backed by Matchup” unless `language_boost_allowed` is true.

---

### Phase 5: Rewrite summaries

This should come last.

Only after enough QA, allow backend/frontend copy to use the metadata to rewrite summary language.

---

## 15. Why PHI vs NYG Is a Great Test Case

This game is a valuable calibration example because:

- GameLens had many PHI-leaning pregame signals
- PHI had several big statistical edges
- NYG won clearly
- The new metadata shows many PHI advantages were caution/watch/blocked
- Broader two-way context was only `available_mixed`
- The model outcome was incorrect, but the metadata helps explain where language should have been more careful

This is the exact reason the feature exists.

The goal is not to say:

> Never trust PHI-style edges.

The goal is to say:

> Some edges are real but should be talked about carefully.

---

## 16. One-Sentence Definition

Claim-strength metadata lets GameLens separate **“this matchup edge exists”** from **“we should speak confidently about this matchup edge.”**

That is the feature.

---

## 17. Current Status

Completed:

- Added claim-strength fields to `build_language_support()`
- Added runtime claim-strength classification helpers
- Annotated Team Comparison rows
- Annotated Metric Highlights
- Annotated Category Summary drivers and aggregate summaries
- Preserved two-way gating
- Preserved blocked-metric behavior
- Preserved matchup lean, outcome confidence, Model Trust, frontend copy, and winner logic
- Smoke-tested with:
  - `20251009_PHI@NYG`
  - `20251013_BUF@ATL`

Not done yet:

- Annotating Core Area summaries
- Annotating context notes
- Frontend badges
- Frontend tooltips
- Summary copy rewriting
- Using claim strength for any prediction or confidence logic

---

## 18. Recommended Next Step

The next safe step is not to rewrite copy yet.

Recommended next step:

> Add a developer/QA-only frontend display or tooltip prototype that shows `language_support.reason`, `claim_strength_language_signal`, and `language_boost_allowed`.

This will let the product inspect the new intelligence layer without changing the user-facing story too early.
