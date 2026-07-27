## Mobile Google Sign-In Fix

Login issue was narrowed to iPhone Chrome only. Desktop Chrome and iPhone Safari both worked, so this was not a full Firebase outage or production deploy failure.

Planned fix:
- Keep `signInWithPopup` for desktop.
- Use `signInWithRedirect` for mobile browsers.
- Add a separate `isSigningIn` guard to prevent double taps.
- Disable the login button immediately after first tap.
- Handle `getRedirectResult` on app load.
- Use `browserLocalPersistence` so mobile redirect sessions persist.
- Keep backend allowlist/security unchanged.

Goal: stabilize iPhone Chrome login without touching admin charts, APIs, or backend auth rules.