
## Main goal

Tomorrow is not a big-build day.

The goal is to analyze the Admin Calibration + Claim Health dashboard we created and decide what deserves polish, what should be trimmed back, and what should wait.

Because Lovable credits are low, avoid large implementation requests. Use Lovable only for small, targeted fixes after we know exactly what is wrong.

## Current status

The backend and frontend are now talking to each other.

The Admin API is returning the expanded dashboard contract:

- `scope: admin_calibration_claim_health`
- `tabs`
- `default_tab`
- `formula_notes`
- `season_phase_groups`
- `section_metadata`
- populated new sections
- preserved legacy sections

The dashboard now has these tabs:

- Overview
- Game Calibration
- Core Area Alignment
- Pillar Health
- Feature Health
- Technical Debug

The Calibration Over Time chart is now rendering live data.

Lovable also fixed:
- chart spelling from “Calibrtion” to “Calibration”
- overlapping Overall Claim Validation / Selected Segment Validation behavior

## Tomorrow’s mindset

Do not ask: “What else can we add?”

Ask:

1. Does this page help me understand GameLens faster?
2. Which sections actually answer useful questions?
3. Which sections are noisy, redundant, or too technical?
4. Which pieces should be admin headline sections?
5. Which pieces should be hidden, collapsed, or moved lower?
6. What is the smallest next fix that improves clarity?

## Priority order

### 1. Visual scan first

Open the Admin page and go tab by tab.

For each tab, write one sentence:

- What is this tab trying to tell me?
- Is it obvious within 5 seconds?
- Is anything confusing, too dense, or visually broken?

Tabs to review:

- Overview
- Game Calibration
- Core Area Alignment
- Pillar Health
- Feature Health
- Technical Debug

Do not fix anything yet. Just observe.

## Tab-by-tab review notes

### Overview

Purpose:

This should answer:

“Is GameLens getting healthier over time?”

Check:

- Does the page open to Overview by default?
- Are coverage and baseline cards clear?
- Does the line chart explain enough?
- Are claim validation and game pick accuracy visually distinct?
- Does Week 1 missing data make sense?
- Is the page too crowded or just right?

Possible decisions:

- Keep as-is
- Add a short note under the chart
- Move some cards lower
- Add clearer labels for claim validation vs pick accuracy

### Game Calibration

Purpose:

This should answer:

“When GameLens gave a game-level read, did it align with the final result?”

Check:

- Can I quickly compare Low / Medium / High confidence?
- Are No Pick games clearly separated?
- Does “No Clear Edge” avoid looking like a failure?
- Are close misses and severe misses visible enough?

Important interpretation:

No Pick should not be treated as wrong. If a row has only no-picks, the UI should say that plainly.

Possible decisions:

- Keep matrix
- Add tooltip / explanation
- Make No Pick visually clearer
- Add small summary cards later

### Core Area Alignment

Purpose:

This should answer:

“When Matchup Lean said one thing, did Core Areas confirm it, split, or push back?”

Check:

- Is `confirmed_edge` easy to understand?
- Is `split_profile` easy to understand?
- Is `conflicting_profile` easy to understand?
- Are `avg_core_gap` and `avg_signal_gap` useful or too technical?
- Does this tab feel important enough?

Possible decisions:

- Add plain-English labels later
  - confirmed_edge → Confirmed Edge
  - split_profile → Split Profile
  - conflicting_profile → Conflicting Profile
  - coin_flip_profile → Coin-Flip Profile
  - no_clear_edge → No Clear Edge
- Keep raw codes for now if credits are low

### Pillar Health

Purpose:

This should answer:

“Which football areas are producing truthful claims?”

Check:

- Are Core Area and Category grouped clearly?
- Can I tell which areas are strongest?
- Can I tell which areas are weak?
- Does `missing_core_area` create confusion?
- Is the weekly table useful or overwhelming?

Possible decisions:

- Keep Pillar Health Matrix as primary
- Collapse Weekly Health by default if too dense
- Rename missing hierarchy labels later
- Add sorting later

### Feature Health

Purpose:

This should answer:

“Which engineered features are earning trust?”

Check:

- Are Football Calibration Features visually separated from Data Quality / Metadata Features?
- Is `offensive_efficiency_support_v1` understandable?
- Is `two_way_context` understandable?
- Are `offense_finish_score` and `defensive_suppression_score` too technical without helper text?
- Are low-sample warnings visible?

Possible decisions:

- Keep grouped table
- Add descriptions per feature family later
- Collapse legacy feature scorecard by default
- Move technical feature details lower

### Technical Debug

Purpose:

This should be useful for QA, not the headline dashboard.

Check:

- Is Surface Matrix only here?
- Does it feel clearly lower priority?
- Is it still useful for regression/debugging?

Possible decisions:

- Keep as-is
- Rename visible title to “Technical Claim Surface Debug”
- Collapse by default later

## Data interpretation checklist

Look for these patterns tomorrow.

### Game-level calibration

Questions:

- Is Medium Confidence meaningfully better than Low?
- Is High Confidence actually strong enough?
- Are Strong Profile games behaving better than Clear Lean games?
- Are No Clear Edge games mostly no-picks?
- Are severe misses concentrated in one confidence/profile bucket?

### Claim health

Questions:

- Which Core Area has the best validation rate?
- Which Core Area has the weakest validation rate?
- Are some categories surprisingly strong?
- Are some categories repeatedly weak?
- Is neutral/mixed high in specific areas?

### Feature health

Questions:

- Which feature buckets have positive lift?
- Which feature buckets have negative lift?
- Are low-sample buckets being over-interpreted?
- Are metadata features being visually separated from football features?
- Does offensive efficiency still look useful?
- Does two_way_context still deserve attention?

## What not to do tomorrow

Do not spend Lovable credits on:

- New backend sections
- Game drilldown
- Major redesign
- New filters
- New chart libraries
- New colors/fonts
- Sidebar/header changes
- Big refactors

Avoid asking Lovable to “make it better” generally.

Only ask for small fixes like:

- Rename this title
- Collapse this section
- Fix this label
- Improve this tooltip
- Hide duplicate line
- Show null as —
- Move this section lower
- Add one explanatory sentence

## Small Lovable prompts if needed

### Prompt 1 — tiny label cleanup

Use this if raw labels are confusing.

```text
Please make a tiny frontend-only label cleanup pass on the Admin Calibration dashboard.

Do not change backend, routing, auth, layout, fonts, colors, or data contracts.

Only add display-label helpers for known raw values:
confirmed_edge -> Confirmed Edge
split_profile -> Split Profile
conflicting_profile -> Conflicting Profile
coin_flip_profile -> Coin-Flip Profile
no_clear_edge -> No Clear Edge
missing_core_area -> Missing Core Area
missing_category -> Missing Category

Keep raw values available in tooltips if useful.
Build must pass.
Prompt 2 — collapse noisy legacy sections

Use this if the page feels too crowded.

Please make a small frontend-only polish pass.

Do not change backend, routing, auth, fonts, colors, or API fields.

Collapse older compatibility sections by default:
- Category Health
- Offensive Efficiency Feature Scorecard
- legacy Core Area / Confidence matrices if they appear below the newer matrix

Keep them accessible, but visually lower priority.

Do not remove sections.
Build must pass.
Prompt 3 — improve no-pick clarity

Use this if Game Calibration makes no-pick rows look bad.

Please improve no-pick clarity in the Game-Level Calibration matrix only.

Do not change backend or API fields.

If correct_count + incorrect_count === 0 and no_pick_count > 0:
- Display “No directional picks”
- Do not show 0% correct as the main value
- Keep no_pick_rate visible

No layout redesign. Build must pass.
Suggested tomorrow workflow
Open dashboard.
Take screenshots of each tab.
Write quick notes:
keep
confusing
trim/collapse
needs tiny fix
Do not touch code yet.
Pick one tiny Lovable fix only if needed.
Save observations for later backend/product work.
Success criteria for tomorrow

Tomorrow is successful if I can answer:

Which tab is most useful?
Which tab is least useful?
Which section should be the dashboard headline?
Which section should be collapsed or demoted?
Which labels need plain-English cleanup?
Which feature signals deserve more analysis later?
Whether the current dashboard is good enough to keep as the admin foundation
Likely next product direction

The dashboard should become less about “show every table” and more about a clear admin story:

Overall health over time
Game-level calibration
Core Area alignment
Pillar claim truth
Feature trust
Technical debug

That order still feels right.

Tomorrow should focus on whether the page actually communicates that story.


My honest advice for tomorrow: **do screenshots + notes first, Lovable second**. With low credits, your best move is to become the product reviewer, not the builder. 🧠