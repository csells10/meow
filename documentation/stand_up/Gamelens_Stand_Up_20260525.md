Using the Admin Claim Health API notes, help me design a simple frontend admin page for GameLens.

Goal:
Create an aggregate-level admin dashboard that helps me visually understand claim-validation performance.

Use these API sections:
- coverage
- baseline
- core_area_matrix
- category_matrix
- confidence_core_area_matrix
- feature_scorecard
- surface_matrix
- section_metadata

Important:
- Do not build game-ID drilldown yet.
- Keep the dashboard aggregate-level only.
- Prioritize clarity over fancy visuals.
- Show baseline comparison wherever possible.
- Include row count and game count so I do not overread small samples.
- The first charts should probably be:
  1. Top summary cards
  2. Core Area Health bar chart
  3. Offensive Efficiency Feature Scorecard bar chart
  4. Category Health table
  5. Confidence by Core Area heatmap or grouped bar chart
  6. Claim Surface Health table

Please suggest a practical frontend MVP layout and implementation plan.