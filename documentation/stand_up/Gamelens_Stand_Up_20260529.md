Tomorrow Plan — Core Area Durability Confidence Calibration
Current State

Today we completed the first full audit cycle for the new confidence-calibration idea:

core_area_durability_context_v0

This feature is currently audit-only.

It does not change:

/game API behavior
winner pick
matchup lean
profile_type
Model Trust
frontend copy
production confidence labels
BigQuery runtime tables

The purpose is to test whether High Confidence should be harder to earn when the model has a loud signal but weaker Core Area durability.

The core idea:

Same pick.
Same lean.
More honest confidence label.
What Was Tested

We tested an audit-only simulation:

If current confidence = High
and core_gap < 0.45
then simulated confidence = Medium

Otherwise keep current confidence unchanged.

This is not a production rule yet.

The test was run against existing game-level confidence audit CSVs for:

2023
2024
2025

using the new script:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

New output files:

core_durability_confidence_simulation_games.csv
core_durability_confidence_ladder_summary.csv
core_durability_confidence_action_summary.csv
Main Finding

The simulation behaved well across all three seasons.

Season-by-season impact
Season	Current High	Simulated High	Lift	Current Medium	Simulated Medium	Medium Change
2023	64.29%	72.73%	+8.44	62.77%	62.16%	-0.61
2024	68.33%	79.17%	+10.84	67.42%	65.60%	-1.82
2025	57.14%	75.00%	+17.86	64.52%	61.76%	-2.76
Pooled 2023–2025 impact
Label	Current games	Current correct %	Simulated games	Simulated correct %
High	110	65.14%	47	76.60%
Medium	276	64.86%	339	63.31%
Low	421	53.99%	421	53.99%

High-specific pooled result:

Current High:
110 games
65.14% correct
57.82% claim validation
4 severe misses
26 close misses

Simulated High:
47 games
76.60% correct
62.52% claim validation
1 severe miss
9 close misses
Plain-English interpretation

The simulation made High Confidence:

smaller
cleaner
more accurate
better validated
less severe-miss prone

Medium absorbed the softened High games without collapsing.

Low stayed unchanged.

This directly addresses the original Admin-page concern:

High Confidence was not clearly behaving better than Medium Confidence.
Important Guardrails

This is not ready for production.

Do not yet wire into:

game_service.py
model_trust_service.py
/game API
frontend
Admin UI
runtime confidence labels

Also do not treat this as:

core_gap < 0.45 = bad pick

The correct interpretation is:

core_gap < 0.45 may mean the lean is still usable,
but it may not deserve High Confidence.

The threshold is currently a research threshold, not a football law.

Why This Is Not Kitchen Soup

The current simulation is intentionally narrow:

Only High → Medium softening is tested.
Medium → Low is not tested.
Medium → High is not tested.
Low is untouched.
Winner picks are untouched.
Profile type is untouched.

That keeps the feature focused on the original problem:

High Confidence should earn the High label.

Recommended Next Steps
Step 1 — Commit the audit-only script

Commit the new file:

agg/gamelens_training/build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim.py

Suggested commit message:

Add core durability confidence simulation audit

Longer message:

Add v0.1.3 confidence calibration audit script for Core Area durability simulation.

This introduces an audit-only confidence simulation that softens current High Confidence games to Medium when core_gap is below the durability review threshold. The simulation preserves winner picks, matchup lean, profile type, runtime behavior, BigQuery state, and frontend output.

New outputs include simulated confidence game assignments, current vs simulated confidence ladder summaries, and calibration action summaries. This is intended for review-only confidence calibration testing and does not introduce production confidence rules.
Step 2 — Run v0.1.3 from BigQuery source

CSV mode passed. Next we should confirm the script works directly from BigQuery rows.

python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2023_reg_post_claim_matrix_pilot \
  --season 2023 \
  --dry-run
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2024_reg_post_claim_matrix_pilot \
  --season 2024 \
  --dry-run
python -m agg.gamelens_training.build_confidence_calibration_audit_v0_1_3_core_durability_confidence_sim \
  --run-id full_2025_reg_post_claim_matrix_pilot \
  --season 2025 \
  --dry-run

Goal:

Confirm the new audit script works as a real audit worker,
not only as a CSV post-processor.
Step 3 — Human-review softened games

Review games where:

calibration_action_v0 = soften_high_to_medium

Use this to inspect:

import pandas as pd
from pathlib import Path

runs = {
    "2023": "test_2023_core_durability_sim",
    "2024": "test_2024_core_durability_sim",
    "2025": "test_2025_core_durability_sim",
}

base = Path("qa/gamelens_confidence_calibration_audit_v0_1_3_runs")
frames = []

for season, run_id in runs.items():
    p = base / run_id / "core_durability_confidence_simulation_games.csv"
    df = pd.read_csv(p)
    df["season"] = season
    frames.append(df)

out = pd.concat(frames, ignore_index=True)
softened = out[out["calibration_action_v0"] == "soften_high_to_medium"].copy()

cols = [
    "season",
    "game_id",
    "model_result_normalized",
    "final_margin_abs",
    "confidence_group",
    "simulated_confidence_group_v0",
    "core_gap",
    "core_area_durability_band",
    "signal_gap",
    "profile_strength_label",
    "profile_type",
    "actual_claim_validation_rate",
    "audit_failure_tags",
    "calibration_reason_v0",
]

print(
    softened[cols]
    .sort_values(["season", "model_result_normalized", "core_gap"])
    .to_string(index=False)
)

Questions to answer:

Were softened incorrect games truly overconfident?
Were softened correct games still acceptable as Medium?
Were any softened games obvious Highs that the rule unfairly punished?
Step 4 — Human-review retained High misses

Review games that stayed High but were still incorrect:

retained_high_misses = out[
    (out["simulated_confidence_group_v0"] == "High") &
    (out["model_result_normalized"] == "incorrect")
]

Questions:

Were these mostly close misses?
Were any severe misses?
Did claim validation collapse?
Do these suggest a future second guardrail?

Important:

Do not add a second guardrail yet.
Document first.
Step 5 — Consider Admin-only visibility later

If human review still supports the feature, the next safe product step is an Admin/debug section:

High Confidence Durability Review

Suggested groups:

Current High
Simulated Retained High
Softened High → Medium

Suggested columns:

games
graded games
correct %
claim validation %
severe misses
close misses
avg core_gap
avg signal_gap
avg team_comp_edge_score

No frontend user-facing change yet.

Stopping Point Recommendation

Tomorrow should not start with production implementation.

Tomorrow should start with:

1. Commit audit-only script.
2. Verify BigQuery-source mode.
3. Review softened games.
4. Review retained High misses.
5. Decide whether this deserves Admin-only visibility.

The working conclusion:

core_area_durability_context_v0 looks promising as an audit-supported confidence calibration feature.

It improves High Confidence quality across 2023–2025 while keeping Medium usable and Low unchanged.

But it should stay audit-only until human review confirms the softened and retained groups make football/product sense.
One-Line Handoff
We found a promising audit-only confidence calibration feature: High Confidence games with insufficient Core Area durability can be softened to Medium without changing picks. Across 2023–2025, simulated High improved from 65.14% to 76.60%, claim validation improved, severe misses dropped, Medium stayed usable, and Low was unchanged. Next step is BigQuery-source verification plus human review of softened and retained High games before any production/API/frontend work.

Starting work on steps for today:

Human review found that softened High games were not uniformly bad. Correct softened games often won comfortably and had strong postgame claim validation, while incorrect softened games tended to have lower claim validation and smaller margins. This supports using Core Area Durability as a High-confidence softening lens rather than a rejection rule. Retained High misses were mostly close, but BUF@BAL 2024 and DAL@ARI 2023 remain important exceptions for future retained-High miss analysis.

Looking at this analysis a few different ways.

The best extra viewpoints

I’d look at five views:

1. Softened correct games — “Are we being too harsh?”

These are games that would move from High to Medium but were correct. We need to know whether they were obvious Highs, or still reasonable as Medium.

2. Softened incorrect games — “Did the feature catch overconfidence?”

These are the games the feature is supposed to help with.

3. Retained High misses — “What did this feature fail to catch?”

This prevents us from over-celebrating. The BUF@BAL 2024 type of game matters.

4. Margin buckets — “Are we reducing bad misses or just moving close variance?”

A good calibration feature should reduce severe/material pain, not just reshuffle close games.

5. Core-gap threshold sensitivity — “Is 0.45 reasonable, or too cute?”

We do not want fake precision. We should compare 0.40, 0.45, and 0.50 again from the combined file.

Deeper review confirmed that core_area_durability_context_v0 is a useful High-confidence calibration lens, not a rejection rule. The 0.45 threshold remains the best audit balance: it improves retained High accuracy to 76.60% while keeping 47 retained High games. Higher thresholds become too restrictive, while lower thresholds leave High too noisy. Softened correct games often remained strong wins, proving the feature should soften to Medium, not Low. Retained High misses show that Core Area durability does not catch every failure mode, especially games with very strong core_gap but weak postgame claim validation.

Starting to wire in these new updates into admin then /game
So the corrected plan is
Step 1: Admin calibrated preview
Step 2: Verify preview numbers
Step 3: Add shared calibration helper
Step 4: Wire /game using helper
Step 5: Recollect payloads into new run_id
Step 6: Confirm /game and Admin match on new run_id

Yes — this passed the exact backend sanity check we needed. ✅

What your output confirms

For 2025, current Admin sections still show:

Low: 154
Medium: 93
High: 22

The new calibrated preview sections show:

Low: 154
Medium: 103
High: 12

And both calibrated tables agree:

calibrated_game_level_calibration: Low 154 / Medium 103 / High 12
calibrated_core_area_alignment_matrix: Low 154 / Medium 103 / High 12

That means the Admin preview is matching the audit expectation:

10 High games moved to Medium
High dropped from 22 → 12
Medium increased from 93 → 103
Low stayed unchanged

That is exactly what we wanted. 🎯

Important note

This confirms the target is 12 High games for 2025, not 13, at least for this run/configuration.

So our QA target should be:

2025 calibrated Admin preview High = 12

Later, if /game is updated and we recollect payloads into a new run_id, that new run should also show:

/game collected payloads High = 12
Admin claim-health High = 12
BigQuery distinct game High = 12

assuming the same rule and same 2025 game set.

Next backend check

Before touching UI, I’d print the exact changed rows from the two calibrated sections:

python - <<'PY'
import json
from pathlib import Path

p = Path("claim_health_2025_with_calibrated_preview.json")
data = json.loads(p.read_text(encoding="utf-8"))
sections = data["sections"]

def print_rows(section_name, key_fields):
    print(f"\n=== {section_name} ===")
    for row in sections[section_name]:
        bits = [f"{k}={row.get(k)}" for k in key_fields]
        bits += [
            f"games={row.get('game_count')}",
            f"graded={row.get('graded_game_count')}",
            f"correct={row.get('correct_count')}",
            f"incorrect={row.get('incorrect_count')}",
            f"correct_rate={row.get('correct_rate')}",
            f"avg_margin={row.get('avg_final_margin_abs')}",
        ]
        if "avg_core_gap" in row:
            bits.append(f"avg_core_gap={row.get('avg_core_gap')}")
        if "avg_signal_gap" in row:
            bits.append(f"avg_signal_gap={row.get('avg_signal_gap')}")
        print(" | ".join(bits))

print_rows(
    "game_level_calibration",
    ["profile_strength_label", "outcome_confidence_label"],
)

print_rows(
    "calibrated_game_level_calibration",
    ["profile_strength_label", "outcome_confidence_label"],
)

print_rows(
    "core_area_alignment_matrix",
    ["profile_type", "outcome_confidence_label"],
)

print_rows(
    "calibrated_core_area_alignment_matrix",
    ["profile_type", "outcome_confidence_label"],
)
PY

We want to confirm the two specific visual changes:

Strong Profile / High: 22 → 12
Strong Profile / Medium: 21 → 31

confirmed_edge / High: 22 → 12
confirmed_edge / Medium: 93 → 103
Then we’re ready for a tiny frontend ask

____________________________#_#_#_#_#_#_#_#_#_#_#_#____


Note for today’s work — Admin calibrated confidence preview

Before committing, backend QA passed for the new Admin-only calibrated confidence preview sections.

Added preview sections:

calibrated_game_level_calibration
calibrated_core_area_alignment_matrix

These do not replace the current Admin sections. They sit beside the existing tables and show what the Admin view would look like if core_area_durability_context_v0 were applied as a confidence softener.

Current rule being previewed:

If outcome_confidence_label = High
and core_gap < 0.45
then preview as Medium
else keep the original confidence label.

Important boundaries:

No /game change yet.
No production confidence change yet.
No BigQuery write.
No existing Admin sections replaced.
Current baseline remains intact.
2025 backend check

The calibrated preview matches the audit expectation:

Current:
Low 154
Medium 93
High 22

Calibrated preview:
Low 154
Medium 103
High 12

The key visual change is exactly what we expected:

Strong Profile / High:
22 games, 57.1% correct
→ 12 games, 75.0% correct

Strong Profile / Medium:
21 games, 80.9% correct
→ 31 games, 66.7% correct

And for Core Area Alignment:

confirmed_edge / High:
22 games, 57.1% correct
→ 12 games, 75.0% correct

confirmed_edge / Medium:
93 games, 64.5% correct
→ 103 games, 61.8% correct

This confirms the preview behaves as intended: High becomes smaller and cleaner, Medium absorbs the softened games, and Low stays unchanged.

Next step after commit/build

Wait for Google build to pass, then use Lovable only for a small frontend patch:

Display calibrated_game_level_calibration and calibrated_core_area_alignment_matrix if present.
Use the same existing matrix/table rendering.
Do not create a new tab or chart type.
Place each calibrated section directly after its current/original section.

Longer-term QA requirement remains:

If /game eventually applies this calibration, then newly collected /game payloads, rebuilt claim-training rows, and Admin Claim Health 




_______________________________________



Copy/paste this into the next chat

We are working in the GameLens NFL App project. The current task is to move carefully from an Admin-only calibrated confidence preview toward eventually wiring the same confidence calibration into /game, but only after preserving a clear QA contract.

The feature is:

core_area_durability_context_v0

Current rule being tested:

If outcome confidence = High
and core_gap < 0.45
then calibrated confidence = Medium
else keep original confidence

Important: this does not change winner pick, matchup lean direction, profile type, model result, Model Trust, or any underlying metric. It only calibrates confidence loudness.

1. Current milestone status

We have successfully completed:

Audit CSV proof ✅
BigQuery-source audit proof ✅
Admin backend preview ✅
Google build/deploy ✅
Authenticated deployed API check ✅
Admin UI visible benchmark ✅
/game behavior untouched ✅

The deployed Admin Claim Health response now contains two new preview sections:

calibrated_game_level_calibration
calibrated_core_area_alignment_matrix

These are Admin-only what-if sections. They do not replace the original sections.

The existing/current sections still exist:

game_level_calibration
core_area_alignment_matrix

The Admin response shape already supports section_metadata, sections, and tabs; the added calibrated sections follow that same structure. The current response has top-level fields like available, baseline, coverage, section_metadata, sections, and tabs.

2. Why this feature exists

The original issue was:

High Confidence was not clearly outperforming Medium Confidence.

In the 2025 Admin view before calibration, the table showed an awkward pattern:

Strong Profile / High:
22 games, 57.1% correct

Strong Profile / Medium:
21 games, 80.9% correct

And in Core Area Alignment:

confirmed_edge / High:
22 games, 57.1% correct

confirmed_edge / Medium:
93 games, 64.5% correct

That made High Confidence look too loose.

The feature fixes confidence calibration by making High harder to earn when the broader Core Area foundation is not durable enough.

3. Evidence from audit testing

The audit tested the rule across 2023, 2024, and 2025.

Pooled 2023–2025 result
Current High:
110 games
65.14% correct

Simulated High:
47 games
76.60% correct

Medium absorbed the softened games without collapsing:

Current Medium:
276 games
64.86% correct

Simulated Medium:
339 games
63.31% correct

Low stayed unchanged:

Low:
421 games
53.99% correct

The deeper threshold test showed core_gap < 0.45 was the best current audit balance: at 0.45, retained High was 47 games at 76.60% correct, while lower floors left High noisier and higher floors got too restrictive.

4. Most important interpretation

This feature is not a rejection rule.

It does not mean:

core_gap < 0.45 = bad pick

It means:

core_gap < 0.45 may still be a useful lean,
but may not deserve the loudest High Confidence label.

The deeper review proved that softened correct games often still had strong final margins and strong claim validation, while softened incorrect games had much weaker claim validation.

So the product interpretation is:

Same pick.
Same matchup lean.
More honest confidence label.
5. Current Admin implementation

The backend now previews calibrated confidence in Admin.

The local and deployed checks passed.

Verified 2025 counts

Current Admin sections:

Low: 154
Medium: 93
High: 22

Calibrated preview sections:

Low: 154
Medium: 103
High: 12

This exactly matches the audit expectation.

The detailed Python check showed:

Game-Level Calibration

Current:

Strong Profile / High:
22 games
21 graded
12 correct
9 incorrect
57.14% correct

Calibrated preview:

Strong Profile / High:
12 games
12 graded
9 correct
3 incorrect
75.00% correct

Current:

Strong Profile / Medium:
21 games
21 graded
17 correct
4 incorrect
80.95% correct

Calibrated preview:

Strong Profile / Medium:
31 games
30 graded
20 correct
10 incorrect
66.67% correct
Core Area Alignment

Current:

confirmed_edge / High:
22 games
21 graded
12 correct
9 incorrect
57.14% correct
avg_core_gap 0.4800
avg_signal_gap 8.7273

Calibrated preview:

confirmed_edge / High:
12 games
12 graded
9 correct
3 incorrect
75.00% correct
avg_core_gap 0.57625
avg_signal_gap 8.4167

Current:

confirmed_edge / Medium:
93 games
93 graded
60 correct
33 incorrect
64.52% correct

Calibrated preview:

confirmed_edge / Medium:
103 games
102 graded
63 correct
39 incorrect
61.76% correct

These backend results were confirmed in the uploaded Python output.

6. What was added to Admin

The patch added calibrated preview sections only.

Likely changed files:

queries/admin_claim_health_queries.py
services/admin_claim_health_service.py

New query functions likely added:

get_calibrated_game_level_calibration
get_calibrated_core_area_alignment_matrix

These mirror the existing functions:

get_game_level_calibration
get_core_area_alignment_matrix

The current non-calibrated query functions already group game-level rows by profile_strength_label × outcome_confidence_label and profile_type × outcome_confidence_label.

The calibrated functions use the same output shape but group by:

calibrated outcome_confidence_label

using this CASE logic:

CASE
  WHEN outcome_confidence_label = 'High'
   AND core_gap < 0.45
    THEN 'Medium'
  ELSE outcome_confidence_label
END

Important: This is still a preview, not production behavior.

7. Current UI status

The Admin web UI now shows the calibrated tables one above/below the current tables.

The user confirmed:

The calibrated tables are visible.
They look good.
They represent the intended benchmark.
Published to the web.

Lovable credits are low, around:

7 credits remaining

So do not spend Lovable credits on polish unless absolutely needed.

No new tab or chart type was created. The frontend reused existing table/matrix rendering, which was the correct low-credit approach.

8. The user’s key requirement before touching /game

The user needs this final QA guarantee:

If /game eventually applies this calibration, then /game, rebuilt BigQuery claim rows, and Admin Claim Health must all agree on total Low / Medium / High counts for the same run_id.

Specifically:

/game payload confidence counts
=
BigQuery claim-training distinct game confidence counts
=
Admin Claim Health confidence counts

This matters because otherwise there will be two truths:

/game says one confidence count
Admin says another confidence count

That is unacceptable.

9. Correct work order from here

The safe order is:

1. Admin calibrated preview — DONE
2. Confirm UI/API numbers match audit — DONE for 2025
3. Plan shared confidence calibration helper
4. Wire /game to use calibrated confidence
5. Recollect payloads into a NEW run_id
6. Build claim-training examples from that new payload run
7. Run validation/features/audit as needed
8. Verify /game payload counts = BigQuery counts = Admin counts

Do not mutate the old run_id:

full_2025_reg_post_claim_matrix_pilot

That is the old baseline.

Use a new run_id later, something like:

full_2025_reg_post_confidence_calibrated_v1

or:

full_2025_reg_post_core_durability_confidence_v1
10. Critical architecture recommendation

Do not duplicate the calibration rule in many places forever.

Long-term, create one shared helper that both /game and training extraction can rely on.

Suggested file:

services/confidence_calibration.py

or if project organization prefers:

services/core_area_durability_confidence.py

Suggested helper:

from __future__ import annotations

from typing import Optional, TypedDict


CORE_AREA_DURABILITY_CONFIDENCE_VERSION = "core_area_durability_context_v0"
DEFAULT_CORE_GAP_FLOOR = 0.45


class ConfidenceCalibrationResult(TypedDict):
    raw_confidence_label: Optional[str]
    calibrated_confidence_label: Optional[str]
    confidence_calibrated: bool
    calibration_reason: Optional[str]
    calibration_feature: str
    core_gap_floor: float
    production_use_allowed: bool


def apply_core_area_durability_confidence_calibration(
    *,
    confidence_label: Optional[str],
    core_gap: Optional[float],
    core_gap_floor: float = DEFAULT_CORE_GAP_FLOOR,
    production_use_allowed: bool = True,
) -> ConfidenceCalibrationResult:
    raw_label = confidence_label

    calibrated_label = raw_label
    calibrated = False
    reason = None

    if raw_label == "High" and core_gap is not None and core_gap < core_gap_floor:
        calibrated_label = "Medium"
        calibrated = True
        reason = "insufficient_core_area_durability_for_high"

    return {
        "raw_confidence_label": raw_label,
        "calibrated_confidence_label": calibrated_label,
        "confidence_calibrated": calibrated,
        "calibration_reason": reason,
        "calibration_feature": CORE_AREA_DURABILITY_CONFIDENCE_VERSION,
        "core_gap_floor": core_gap_floor,
        "production_use_allowed": production_use_allowed,
    }

Potential nuance: production_use_allowed is currently false in Admin preview. When wiring /game, this may become true for /game confidence display, but still keep raw/debug trace available internally.

11. What /game should eventually do

The final desired /game behavior is not to show a new public label.

It should simply show the calibrated confidence.

Example:

Before:

Strong Profile / Confirmed Edge / High Confidence

After:

Strong Profile / Confirmed Edge / Medium Confidence

No need to display:

soften_high_to_medium
core_area_durability_context_v0
calibration_action_v0

Those are internal/debug/admin concepts.

The user specifically pushed back on response bloat. The mature product behavior should simply update confidence, not add a bunch of new user-facing labels.

12. Minimal /game response philosophy

Public-facing /game should ideally expose:

{
  "confidence": "Medium"
}

or whatever existing field currently carries confidence.

Optionally, for QA/debug only:

{
  "raw_confidence_label": "High",
  "calibrated_confidence_label": "Medium",
  "confidence_calibrated": true,
  "calibration_reason": "insufficient_core_area_durability_for_high"
}

But avoid adding this to normal user-facing response unless needed.

Best compromise:

Use calibrated confidence for display.
Keep raw/calibrated trace only in debug/admin/training metadata if already appropriate.
13. Need to identify exactly where /game confidence is assembled

Likely files to inspect:

services/game_service.py
services/model_trust_service.py
services/claim_language_response.py
game_routes.py

Important project memory:

The user wants file names included for proposed changes and implementation notes.

Likely target is services/game_service.py, because that assembles the game response. However, do not guess blindly. Search for:

grep -R "outcome_confidence" -n .
grep -R "confidence_label" -n services routes agg | head -100
grep -R "profile_strength_label" -n services routes agg | head -100
grep -R "matchup_lean" -n services routes agg | head -100

Also search for where claim-training examples extract fields:

grep -R "outcome_confidence_label" -n agg/gamelens_training
grep -R "model_result" -n agg/gamelens_training/build_claim_training_examples.py

The next chat should inspect actual code before giving edit instructions.

14. Important distinction: Admin preview vs final truth

Admin preview currently applies SQL CASE logic against old claim rows.

That is okay for preview.

But once /game is changed, the Admin “official” sections should eventually reflect a newly collected run, not a SQL what-if over the old baseline.

Old run:

full_2025_reg_post_claim_matrix_pilot

should remain current/raw baseline.

New calibrated run later:

full_2025_reg_post_confidence_calibrated_v1

should contain calibrated confidence rows collected from /game.

Then Admin current sections should naturally show the calibrated counts because the stored claim rows were created from calibrated /game payloads.

At that point, the preview sections may be less important or can remain as a diagnostic comparison.

15. QA plan after /game is changed

This is the big contract.

Step A — collect new /game payloads

Use the existing payload collector.

Prior payload command style used:

python qa_collect_gamelens_payloads.py \
  --seasons 2025 \
  --games-per-season 400 \
  --sample-mode even \
  --run-name full_2025_reg_post_confidence_calibrated_v1 \
  --timeout-seconds 180

Need confirm exact collector options and whether it calls local backend code or deployed /game. Earlier payloads were collected locally from backend code.

If testing deployed /game, ensure collector target/source is correct. Do not assume.

Step B — count confidence labels directly from collected payloads

Need inspect actual payload JSON shape. But likely write a script like:

import json
from pathlib import Path
from collections import Counter

payload_dir = Path("qa/gamelens_payload_runs/full_2025_reg_post_confidence_calibrated_v1/payloads")

counts = Counter()
missing = []

for p in payload_dir.glob("*.json"):
    data = json.loads(p.read_text(encoding="utf-8"))

    # TODO: adjust this path to actual /game payload structure.
    # Possible examples:
    # confidence = data["matchup_lean"]["confidence"]
    # confidence = data["matchup_read"]["confidence"]
    # confidence = data["outcome_confidence_label"]
    confidence = None

    # Inspect one payload first before finalizing.

    if confidence:
        counts[confidence] += 1
    else:
        missing.append(p.name)

print(counts)
print("missing:", len(missing))

The exact path must be determined from one new payload.

Step C — build claim training examples into new run_id

Use existing Stage 1 builder:

python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run qa/gamelens_payload_runs/full_2025_reg_post_confidence_calibrated_v1 \
  --run-id full_2025_reg_post_confidence_calibrated_v1 \
  --dry-run

Then write:

python -m agg.gamelens_training.build_claim_training_examples \
  --payload-run qa/gamelens_payload_runs/full_2025_reg_post_confidence_calibrated_v1 \
  --run-id full_2025_reg_post_confidence_calibrated_v1 \
  --write-bigquery \
  --replace-run
Step D — run validation
python -m agg.gamelens_training.update_claim_training_validation \
  --run-id full_2025_reg_post_confidence_calibrated_v1 \
  --write-bigquery
Step E — run features if needed
python -m agg.gamelens_training.update_claim_training_features \
  --run-id full_2025_reg_post_confidence_calibrated_v1 \
  --write-bigquery
Step F — verify BigQuery distinct game confidence counts
SELECT
  outcome_confidence_label,
  COUNT(DISTINCT game_id) AS games
FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
WHERE run_id = 'full_2025_reg_post_confidence_calibrated_v1'
GROUP BY outcome_confidence_label
ORDER BY outcome_confidence_label;

Expected, assuming same game set and same calibration:

Low: 154
Medium: 103
High: 12

But note: this assumes claim extraction stores the calibrated confidence label.

Step G — verify Admin Claim Health for new run_id

Call:

curl -sS -H "Authorization: Bearer ${TOKEN}" \
  "https://nfl-games-app-main-362530996210.us-central1.run.app/admin/gamelens/claim-health?run_id=full_2025_reg_post_confidence_calibrated_v1&season=2025" \
  -o claim_health_2025_confidence_calibrated_v1.json

Then:

import json
from pathlib import Path

data = json.loads(Path("claim_health_2025_confidence_calibrated_v1.json").read_text())

sections = data["sections"]

def summarize(section_name):
    out = {}
    for row in sections[section_name]:
        label = row["outcome_confidence_label"]
        out[label] = out.get(label, 0) + int(row["game_count"])
    return out

print("Admin game_level_calibration:", summarize("game_level_calibration"))
print("Admin core_area_alignment_matrix:", summarize("core_area_alignment_matrix"))

Expected:

{'Low': 154, 'Medium': 103, 'High': 12}
Step H — compare all three

Final QA table should be:

Source                         Low   Medium   High
/game payloads                 154   103      12
BigQuery claim rows            154   103      12
Admin game_level_calibration   154   103      12
Admin core_area_alignment      154   103      12

That is the success condition.

16. SQL QA script for final agreement

Once new run_id exists:

WITH claim_counts AS (
  SELECT
    outcome_confidence_label AS confidence,
    COUNT(DISTINCT game_id) AS games
  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id = 'full_2025_reg_post_confidence_calibrated_v1'
  GROUP BY confidence
)

SELECT *
FROM claim_counts
ORDER BY
  CASE confidence
    WHEN 'Low' THEN 1
    WHEN 'Medium' THEN 2
    WHEN 'High' THEN 3
    ELSE 99
  END;

Optional sanity check by profile type:

WITH games AS (
  SELECT
    game_id,
    ANY_VALUE(profile_type) AS profile_type,
    ANY_VALUE(profile_strength_label) AS profile_strength_label,
    ANY_VALUE(outcome_confidence_label) AS confidence,
    LOWER(TRIM(CAST(ANY_VALUE(model_result) AS STRING))) AS model_result,
    ANY_VALUE(final_margin_abs) AS final_margin_abs,
    ANY_VALUE(core_gap) AS core_gap,
    ANY_VALUE(signal_gap) AS signal_gap
  FROM `nfl-stream-406420.Analytics.gamelens_claim_training_examples`
  WHERE run_id = 'full_2025_reg_post_confidence_calibrated_v1'
  GROUP BY game_id
)

SELECT
  profile_type,
  confidence,
  COUNT(*) AS games,
  COUNTIF(model_result IN ('correct', 'incorrect')) AS graded_games,
  COUNTIF(model_result = 'correct') AS correct_games,
  COUNTIF(model_result = 'incorrect') AS incorrect_games,
  SAFE_DIVIDE(
    COUNTIF(model_result = 'correct'),
    COUNTIF(model_result IN ('correct', 'incorrect'))
  ) AS correct_rate,
  AVG(SAFE_CAST(core_gap AS FLOAT64)) AS avg_core_gap,
  AVG(SAFE_CAST(signal_gap AS FLOAT64)) AS avg_signal_gap,
  AVG(SAFE_CAST(final_margin_abs AS FLOAT64)) AS avg_final_margin_abs
FROM games
GROUP BY profile_type, confidence
ORDER BY profile_type, confidence;

Expected for 2025 if calibrated:

confirmed_edge / High:
12 games, 75.0%

confirmed_edge / Medium:
103 games, about 61.8%
17. Potential issue: exact confidence field path in /game

Before coding, inspect one /game response.

Need know whether confidence appears in:

matchup_lean.confidence
matchup_lean.confidence.label
matchup_read.confidence
outcome_confidence_label
model_trust.outcome_confidence

Do not assume.

Search code and inspect payload.

The frontend/admin terms may not exactly match API terms.

Earlier frontend patches referenced Matchup Lean confidence paths like:

lean.user_facing_confidence?.label?.trim() || lean.confidence || null

Project memory says v1.7.13 worked on Matchup Lean user-facing confidence wiring. That suggests /game may have multiple confidence fields. The next chat must identify the canonical one.

Potential issue:

If /game only changes visible confidence but claim-training extraction pulls a different raw field,
Admin and /game counts will not match.

So the QA is essential.

18. What not to do

Do not:

- Replace current Admin sections yet
- Mutate old run_id
- Change profile_type to conflicting_profile
- Change winner prediction
- Change model_result
- Change Model Trust correctness logic
- Add a big public “core durability” label
- Spend Lovable credits on new UI polish
- Treat 0.45 as permanent football law
- Add Medium → Low rules yet
- Add extra guardrails for retained High misses today

Do:

- Use Admin preview as benchmark
- Use shared helper if possible
- Keep raw/effective confidence trace for QA
- Recollect into a new run_id
- Verify all counts agree
19. Product wording

If any UI copy is needed later, use plain wording:

Calibrated Confidence Preview

or:

Core Area Durability Confidence Preview

Safe explanation:

Shows how confidence labels would change if High Confidence required stronger Core Area durability.

Avoid:

Bad High games
Wrong High games
Rejected picks
Core Area says the team is weaker

Best product framing:

High Confidence becomes smaller and cleaner.
Medium absorbs softened games.
Low is unchanged.
Overall picks are unchanged.
20. Current benchmark for 2025

For the old run:

full_2025_reg_post_claim_matrix_pilot

Current/raw Admin:

Low: 154
Medium: 93
High: 22

Calibrated preview:

Low: 154
Medium: 103
High: 12

This is now visible in the Admin UI.

Future /game work should aim for the same 2025 distribution in the new calibrated run, assuming same games and same rule.

21. Suggested next chat opening prompt

Paste this into the next chat:

We are continuing GameLens core_area_durability_context_v0 work. Admin preview is already done and deployed. It adds calibrated_game_level_calibration and calibrated_core_area_alignment_matrix to Admin Claim Health. For 2025 old run_id full_2025_reg_post_claim_matrix_pilot, current counts are Low 154 / Medium 93 / High 22; calibrated preview counts are Low 154 / Medium 103 / High 12. UI shows these tables. /game is still untouched.

Next task: inspect /game response assembly and determine the safest way to wire the same confidence calibration into /game using a shared helper, while preserving QA traceability. Do not change BigQuery old run_id. Do not change winner picks, profile_type, matchup lean direction, or Model Trust. The final QA requirement is that after we recollect payloads into a new run_id, /game payload confidence counts, BigQuery claim-training confidence counts, and Admin Claim Health confidence counts must all match.
22. Suggested first commands in next chat

Ask the next chat to inspect code with these:

grep -R "outcome_confidence_label" -n services routes agg | head -100
grep -R "user_facing_confidence" -n services routes agg | head -100
grep -R "confidence" -n services/game_service.py services/model_trust_service.py routes game_routes.py | head -200
grep -R "profile_strength_label" -n services routes agg | head -100
grep -R "matchup_lean" -n services routes agg | head -100

Then inspect:

services/game_service.py
routes/game_routes.py or game_routes.py
agg/gamelens_training/build_claim_training_examples.py

Goal:

Find canonical confidence field in /game.
Find where claim-training extraction reads confidence.
Design a shared calibration helper.
Wire /game and extraction consistently.
23. Suggested implementation design for next phase
Phase 1 — helper only

Add:

services/confidence_calibration.py

with:

apply_core_area_durability_confidence_calibration()

Unit/smoke test the helper:

assert High + 0.44 => Medium
assert High + 0.45 => High
assert Medium + 0.20 => Medium
assert Low + 0.60 => Low
assert High + None => High

Open question: If core_gap is missing, do we keep High or soften? Current Admin SQL keeps High because core_gap < 0.45 is false when NULL. Stay consistent unless there is a reason to change.

Phase 2 — wire /game

Find where final confidence label is assigned.

Change display/effective confidence to calibrated label.

Keep raw label internally if useful.

Potential object:

"confidence_calibration": {
  "feature": "core_area_durability_context_v0",
  "raw_confidence_label": "High",
  "calibrated_confidence_label": "Medium",
  "confidence_calibrated": true,
  "reason": "insufficient_core_area_durability_for_high",
  "core_gap_floor": 0.45
}

But do not expose this in user-facing UI unless needed. It may be safe in payload for QA, but the user wants to avoid response bloat. Decide carefully.

Phase 3 — claim extraction

Ensure build_claim_training_examples.py uses the same confidence label that /game displays.

If it currently reads outcome_confidence_label from payload, make sure that field is calibrated after /game patch.

If it reads a raw nested field, update extraction to use calibrated/effective field.

Phase 4 — recollect and QA

Collect into new run_id. Build claim examples. SQL check. Admin check.

24. The most likely hidden issue

There may be several confidence representations:

raw outcome confidence
Matchup Lean confidence
user_facing_confidence
profile_strength_label
model_trust confidence
claim row outcome_confidence_label

Do not update one and forget the others.

The single most important QA question is:

Which exact field does the frontend show as confidence?
Which exact field does build_claim_training_examples store as outcome_confidence_label?

Those must align.

25. Done-state definition for /game phase

The /game phase is done only when:

1. /game displays calibrated confidence.
2. Admin current sections for a new calibrated run show matching confidence counts.
3. BigQuery distinct game confidence counts match /game payload counts.
4. Existing Admin preview still works or is clearly documented.
5. No winner picks changed.
6. No profile_type changed.
7. No Model Trust correctness logic changed.
8. A notes file documents the new run_id and QA evidence.
26. Today’s final status in one paragraph

Today we completed the Admin benchmark for core_area_durability_context_v0. The deployed Admin Claim Health response now includes calibrated preview versions of Game-Level Calibration and Matchup Lean × Core Area Alignment. The 2025 preview moves 10 current High games into Medium, changing counts from Low 154 / Medium 93 / High 22 to Low 154 / Medium 103 / High 12, matching the audit results. The UI now visibly displays the benchmark. /game remains untouched. The next phase is to wire the same confidence calibration into /game using a shared helper and then recollect payloads into a new run_id so /game, BigQuery claim-training rows, and Admin Claim Health can be verified to agree.


__________________________________________________



