## GameLens Branding Correction Pass

Completed a focused branding polish pass after the initial logo integration exposed asset-crop and login presentation issues.

### What changed
- Replaced `gamelens-horizontal-light.png` and `gamelens-horizontal-dark.png` with tightly cropped versions so the header logo no longer renders like a tiny sticker.
- Updated `AppShell.tsx` with small responsive class changes:
  - prevented nav label wrapping
  - tightened header spacing at medium widths
  - hid the user email later/earlier appropriately for better tablet fit
- Updated `Login.tsx` to stop using the square icon asset.
- Login now uses the same theme-aware horizontal GameLens logo assets as the header.
- Added an accessibility-only `sr-only` heading for GameLens.
- Kept live text for `NFL Matchup Intelligence` and `Sign in to continue`.

### Why this mattered
The issue was not just logo design. The original header PNGs had excessive transparent padding, so even correct CSS sizing made the visible logo appear tiny. Cropping the actual PNG bounds fixed the real cause.

The login screen also felt disconnected because it used a square icon/tile instead of a product lockup. Reusing the repaired horizontal assets created a cleaner, more consistent brand experience without needing more generated logo files.

### Confirmed unchanged
- Routes
- Navigation structure
- Dashboards
- Admin pages
- API calls
- Business logic
- Auth flow
- Color tokens
- Data visualizations

### Follow-up
Old branding assets are currently unreferenced but intentionally kept as backups. Consider deleting them in a later cleanup-only commit after confirming the new branding works well in production.

## Claim Health Baseline Readability Fix

Improved baseline readability on the /admin/claim-health horizontal bar charts.

Changes:
- Moved the baseline label out of the chart plot area into a header badge.
- Kept the dashed baseline line inside the chart.
- Strengthened the dashed baseline line so it reads more clearly against the grid.
- Added custom bar label logic that nudges percentage labels away from the baseline line when they would visually collide.

Result:
- Baseline is now visible and readable.
- Bar values near the baseline are easier to scan.
- No backend math, API shape, validation rates, or chart calculations changed.