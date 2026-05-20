# GameLens Devil’s Advocate API Smoke Test Findings

**Purpose:** Review the current `/game` API response as a product/API contract after adding claim-strength metadata.  
**Main question:** Does the response make logical sense across a broader sample, or is it too noisy/conflicting?  
**Sample run:** 30 finalized games, balanced across 2023, 2024, and 2025.  
**Seed:** `20260520`  
**Result:** 30 successful payloads, 0 failed payloads.

---

## 1. Executive Verdict

The `/game` API response is **working as an internal intelligence payload**, but it needs a cleanup pass before frontend badges, public tooltips, or rewritten user-facing language.

The new claim-strength metadata is not the problem. It is doing its job.

The main issue is that the API now has several rich reasoning layers that can appear to disagree unless their roles are clearly separated.

Current layers include:

- `team_comparison`
- `core_area_comparison`
- `matchup_breakdown.metric_highlights`
- `matchup_breakdown.category_summaries`
- `matchup_breakdown.core_area_summaries`
- `language_support`
- `claim_strength_bucket`
- `claim_strength_context`
- `claim_strength_language_signal`
- `two_way_context`
- `matchup_lean`
- `model_trust`
- `outcome_confidence`

The system is becoming powerful, but the product contract needs clearer boundaries so each section “speaks the same language.”

---

## 2. Run Summary

From the 30-game API sample:

| Item | Result |
|---|---:|
| Selected games | 30 |
| Successful payloads | 30 |
| Failed payloads | 0 |
| 2023 games | 10 |
| 2024 games | 10 |
| 2025 games | 10 |
| Correct model results | 14 |
| Incorrect model results | 10 |
| No Pick results | 6 |
| Average API response time | 12.151 seconds |

This was a valid sample for a first devil’s advocate pass.

---

## 3. Warning Summary

The smoke test found these warning counts:

| Warning | Count | Severity | Meaning |
|---|---:|---|---|
| `category_aggregate_missing_no_boost_reason` | 218 | Low-Medium | Category aggregate rows often say no boost but give no reason |
| `team_comparison_unclassified_claim_strength` | 30 | Medium-High | Every game had Team Comparison rows missing classification context |
| `core_area_layer_leader_mismatch` | 32 | Medium-High | Broad Core Area layer and headline Core Area layer sometimes disagree |
| `confidence_field_mismatch` | 10 | High | `confidence` and `outcome_confidence` disagree in some games |
| `model_trust_tooltip_even_area_mismatch` | 6 | Medium | Tooltip wording does not always match actual visible metric counts |

No critical warning category appeared in the run summary.

That matters because the scary failure modes did **not** appear:

- blocked metrics were not boosted
- non-supportive `two_way_context` did not appear to allow boosts
- the API run itself completed cleanly

---

## 4. Key Finding 1 — Team Comparison Needs `category` and `core_area`

### Problem

Every game triggered:

```text
team_comparison_unclassified_claim_strength
```

This means Team Comparison rows often produce:

```json
"claim_strength_context": "unclassified_edge"
```

because they do not currently carry enough context to classify the metric.

### Why this matters

A metric can be classified correctly in one section and vaguely in another.

Example pattern:

| Metric | Team Comparison | Metric Highlights / Category Summary |
|---|---|---|
| `red_zone_efficiency` | `unclassified_edge` | `caution_area_edge` |
| `turnover_margin_per_game` | `unclassified_edge` | `caution_area_edge` |
| `points_per_play` | `unclassified_edge` | often `caution_area_edge` |
| `third_down_pct` | `unclassified_edge` | `watch_area_edge` |

This can make the frontend feel inconsistent.

### Product problem

The user might see:

> Red Zone Efficiency is a clear edge.

But another section says:

> Red Zone Efficiency is caution-only / blocked from stronger language.

Both can technically be true, but the product language needs to align.

### Recommended fix

Update:

```text
services/game_service.py
```

Function:

```text
build_team_comparison()
```

Add `category` and `core_area` to each Team Comparison row.

Example desired row shape:

```python
comparison.append({
    "label": label,
    "metric": metric_name,
    "category": category,
    "core_area": core_area,
    "away": fmt(away_val),
    "home": fmt(home_val),
    "better": comparison_result["better"],
    ...
})
```

### Expected improvement

After this fix:

- Team Comparison claim-strength context should stop being `unclassified_edge`
- Team Comparison should classify volatile metrics consistently
- Frontend tooltips will be safer later
- API sections will speak more consistently

### Priority

**Priority 1 — do this next.**

---

## 5. Key Finding 2 — Confidence Naming Is Confusing

### Problem

Some games showed a mismatch like:

```json
"confidence": "High"
```

while also showing:

```json
"outcome_confidence": {
  "label": "Medium"
}
```

The 30-game run found this in 10 games.

### Why this happens

The API now separates:

- raw signal confidence
- matchup/profile strength
- outcome confidence

That separation is good.

The problem is the old field name:

```text
matchup_lean.confidence
```

It sounds like the final user-facing confidence, but the newer and safer field is:

```text
matchup_lean.outcome_confidence.label
```

### Product risk

A frontend developer might show:

> High Confidence

when the intended product message is:

> Medium Outcome Confidence

That is a real product mismatch.

### Recommended fix

Do not immediately change behavior. First clarify naming.

Possible future structure:

```json
"raw_signal_confidence": "High",
"outcome_confidence": {
  "code": "medium",
  "label": "Medium"
}
```

Or at minimum, frontend should prefer:

```text
matchup_lean.outcome_confidence.label
```

over:

```text
matchup_lean.confidence
```

### Priority

**Priority 2 or 3**, depending on how soon the frontend uses confidence labels.

---

## 6. Key Finding 3 — Core Area Layers Need Clearer Names

### Problem

The run found 32 cases of:

```text
core_area_layer_leader_mismatch
```

This means:

```text
core_area_comparison
```

and:

```text
matchup_breakdown.core_area_summaries
```

sometimes point to different leaders.

### Why this may not be a bug

These two sections likely answer different questions.

| Section | Likely job |
|---|---|
| `core_area_comparison` | Broad Core Area score using many metrics |
| `matchup_breakdown.core_area_summaries` | Headline-eligible ranking drivers inside that Core Area |

So one can say:

> Broad Defensive Control favors Team A.

while the other says:

> Headline-ranking Defensive Control drivers are near-even.

That is potentially useful nuance.

### Product risk

Both sections currently sound like “Core Area.” That can feel contradictory.

### Recommended naming

Use clearer product labels:

| Current section | Better label |
|---|---|
| `core_area_comparison` | **Broad Core Area Score** |
| `matchup_breakdown.core_area_summaries` | **Headline Core Area Drivers** |

### Recommended rule

Do not change the calculations yet.

First clarify the section role and naming.

### Priority

**Priority 3.**

---

## 7. Key Finding 4 — Category Aggregate Reasons Are Too Quiet

### Problem

Many category aggregate objects show:

```json
"language_boost_allowed": false,
"supported_driver_count": 0,
"reason": null
```

The run found 218 warnings for this.

### Why this is not dangerous

This is not a model safety problem.

It does not cause boosts.

It only makes QA harder because the aggregate row gives no reason even when all drivers failed to qualify.

### Recommended fix

Update:

```text
services/claim_language_response.py
```

When `supported_driver_count` is `0`, provide a default reason.

Possible reason:

```json
"reason": "no_category_drivers_allowed_language_boost"
```

or:

```json
"reason": "category_drivers_not_supported_or_not_two_way_supportive"
```

### Priority

**Low-Medium.**

This is easy, but not as important as Team Comparison alignment.

---

## 8. Key Finding 5 — Model Trust Tooltip Wording Needs Cleanup

### Problem

The run found 6 cases where the tooltip referred to “several even areas,” but the actual visible neutral count was low.

Example concept:

```json
"matchup_advantage": {
  "away": 4,
  "home": 0,
  "neutral": 1
}
```

But tooltip says:

> Some Team Comparison metrics lean one way, but several even areas keep the edge from looking clean.

### Why this matters

This is not a logic bug, but the wording undersells or misstates the visible metric count.

### Recommended fix

Update tooltip logic in:

```text
services/model_trust_service.py
```

Possible wording:

> Most visible Team Comparison metrics favored the same side, though one near-even area keeps this from being treated as overwhelming.

### Priority

**Low-Medium.**

Good polish item, but not first.

---

## 9. Claim Strength + Two-Way Context: How They Work Together

This is the most important design understanding.

Claim strength and two-way context are not duplicates.

They answer different questions.

---

### 9.1 Claim Strength

Claim strength asks:

> Is this specific claim worth talking about, and how should it sound?

It looks at the row/claim.

It uses fields like:

```json
"claim_strength_bucket": "real_edge",
"claim_strength_context": "caution_area_edge",
"claim_strength_language_signal": "caution_only"
```

Meaning:

> This claim has a real numerical edge, but it is in a caution area, so do not hype it.

---

### 9.2 Two-Way Context

Two-way context asks:

> Does the team have broader support around the claim?

It is side/team-level context.

It uses:

```json
"two_way_context": "available_mixed"
```

or eventually:

```json
"two_way_context": "supportive"
```

Meaning:

> The broader support is either mixed or supportive.

---

### 9.3 Synergy Rule

Use the two systems together.

| Claim Strength | Two-Way Context | Product Meaning |
|---|---|---|
| `boost_candidate` | `supportive` | Candidate for stronger language |
| `boost_candidate` | `available_mixed` | Interesting, but do not boost |
| `measured` | `supportive` | Useful support, still measured |
| `measured` | `available_mixed` | Keep normal/cautious |
| `caution_only` | any | Treat as volatile |
| `soften` | any | Use soft language |
| `blocked` | any | Do not boost |
| `near_even` / `thin_edge` | any | Do not talk loudly |

The key concept:

> Claim strength tells you whether the row is interesting.  
> Two-way context tells you whether the broader team context supports stronger language.

---

## 10. Recommended API Consumer Hierarchy

When using `language_support`, follow this order:

### Step 1 — Check `language_boost_allowed`

If false, do not boost language.

```json
"language_boost_allowed": false
```

This is the safety gate.

---

### Step 2 — Check `claim_strength_language_signal`

Use this for tone:

| Signal | Tone |
|---|---|
| `boost_candidate` | Candidate for stronger wording, only if gates allow |
| `measured` | Useful but balanced |
| `caution_only` | Volatile or swing-factor wording |
| `soften` | Thin/near-even/cautious |
| `normal` | Standard wording |
| `no_boost` | No special wording |

---

### Step 3 — Check `rule_status`

This explains the hard rule.

Examples:

| Rule Status | Meaning |
|---|---|
| `allowed` | The row passed claim-language support rules |
| `blocked_metric` | Metric is blocked from stronger language |
| `conditional_disabled_metric` | Metric is interesting but disabled for boosting |
| `context_not_supportive` | Two-way context did not support stronger language |
| `no_rule` | No matching rule |

---

### Step 4 — Check `reason`

This is the best tooltip source for early frontend/debug use.

Example:

```json
"reason": "red_zone_efficiency is blocked from automatic stronger language."
```

---

## 11. Recommended Step-by-Step Work Plan

## Step 1 — Fix Team Comparison metadata alignment

**Goal:** Add `category` and `core_area` to Team Comparison rows.

**File:**

```text
services/game_service.py
```

**Function:**

```text
build_team_comparison()
```

**Why first:**

This warning appeared in 30/30 games.

**Definition of Done:**

- Team Comparison rows include `category`
- Team Comparison rows include `core_area`
- `team_comparison_unclassified_claim_strength` warnings drop sharply or disappear
- Team Comparison and Metric Highlights classify shared metrics consistently

---

## Step 2 — Re-run the same 30-game sample

Use the same seed:

```bash
python qa/collect_gamelens_api_smoke_payloads.py --sample-size 30 --seed 20260520
```

**Why same seed:**

This gives a cleaner before/after comparison.

**Compare:**

- `run_summary.json`
- `alignment_warnings.csv`
- `payload_summary.csv`

Expected result:

- `team_comparison_unclassified_claim_strength` should drop from 30
- no new critical warnings should appear
- blocked metrics should still not boost
- non-supportive two-way context should still not boost

---

## Step 3 — Add category aggregate no-boost reason

**File:**

```text
services/claim_language_response.py
```

**Goal:**

Replace aggregate `reason: null` when no category drivers are boost-eligible.

Possible default reason:

```text
no_category_drivers_allowed_language_boost
```

or:

```text
category_drivers_not_supported_or_not_two_way_supportive
```

**Definition of Done:**

- `category_aggregate_missing_no_boost_reason` warning count drops sharply
- QA rows are easier to interpret

---

## Step 4 — Clarify confidence naming

**Goal:**

Prevent frontend/product confusion between:

```text
matchup_lean.confidence
```

and:

```text
matchup_lean.outcome_confidence.label
```

**Options:**

1. Rename old field to `raw_signal_confidence`
2. Add new field and keep old for compatibility
3. Update frontend to use `outcome_confidence.label`
4. Add documentation explaining that `confidence` is legacy/raw signal confidence

**Definition of Done:**

- Frontend does not show misleading “High Confidence” when outcome confidence is Medium
- API consumers know which field is user-facing

---

## Step 5 — Clarify Core Area layer names

**Goal:**

Make the two Core Area layers feel complementary instead of contradictory.

Recommended labels:

| Current | Better label |
|---|---|
| `core_area_comparison` | Broad Core Area Score |
| `matchup_breakdown.core_area_summaries` | Headline Core Area Drivers |

**Definition of Done:**

- Core Area mismatches are understood as layer differences
- Frontend copy explains the difference
- No immediate calculation rewrite unless mismatch remains confusing after labeling

---

## Step 6 — Fix Model Trust tooltip wording

**File:**

```text
services/model_trust_service.py
```

**Goal:**

Make tooltip wording match actual neutral/even counts.

**Definition of Done:**

- No tooltip says “several even areas” when neutral count is low
- Tooltip wording reflects actual `matchup_advantage` counts

---

## 12. What Not To Do Yet

Do not start frontend badges yet.

Avoid adding user-facing labels like:

- Backed by Matchup
- Volatile Signal
- Measured Support
- Thin Edge

until the API sections are better aligned.

Also avoid rewriting summary copy yet.

The better move is:

> Clean the API contract first, then expose it.

---

## 13. Recommended Next Action

The next best action is:

> Add `category` and `core_area` to Team Comparison rows, then rerun the same 30-game sample.

This is the smallest fix with the largest clarity payoff.

---

## 14. One-Sentence Summary

The devil’s advocate run showed that the new claim-strength metadata works, but the API now needs alignment cleanup so Team Comparison, Core Area layers, confidence labels, and claim-language support all tell the same story without confusing the frontend.
