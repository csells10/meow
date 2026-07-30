# GameLens Mobile Auth Issue — React/Firebase Handoff Summary

## Core Problem

GameLens uses Firebase Google Sign-In for a private NFL analytics app at:

* Production: `https://gamelens.io`
* Auth project: `nfl-stream-406420`
* Original Firebase authDomain: `nfl-stream-406420.firebaseapp.com`
* New custom authDomain attempted: `auth.gamelens.io`

The persistent issue is:

**iPhone Chrome / Chrome iOS reaches the GameLens login page, starts Google sign-in, but authentication does not complete.**

The visible error after the recent fixes is:

```text
Google sign-in didn’t complete. Please tap Sign in with Google to try again.
```

This does **not** appear to be an `allowed_users` / backend access-denied problem, because the user is not getting to the point where Firebase auth completes and `/me` can return 403. The failure happens before a usable Firebase user is established.

Expected access-denied flow would be:

```text
Google auth succeeds
→ returns to GameLens
→ Firebase user exists
→ backend /me returns 403
→ Access denied screen appears
```

That is not what is happening on iPhone Chrome.

---

## Current Browser Matrix

### Working

* Desktop Chrome: works
* Desktop sign out / sign in: works
* iPhone Safari: works after latest publish
* Google app on iPhone: previously worked
* GameLens app itself loads on mobile

### Still failing

* iPhone Chrome / CriOS:

  * reaches GameLens login
  * taps “Sign in with Google”
  * redirects through the configured Firebase auth domain
  * returns to GameLens
  * shows “Google sign-in didn’t complete”
  * repeating login repeats the loop

---

## Recent Changes Tried

## 1. Branding / Logo Work

Recent UI changes included:

* Replaced padded logo assets with tightly cropped horizontal logo PNGs.
* Updated `AppShell.tsx` header spacing/nav wrapping.
* Updated `Login.tsx` to use horizontal GameLens logo instead of square icon.
* Added `sr-only` GameLens heading.
* No intended auth logic changes.

We checked whether the logo/layout could be blocking the login button.

Findings:

* Login button is not covered by the logo.
* Logo is in normal document flow above the button.
* No absolute/fixed overlay.
* No pointer-events interception.
* Button `onClick` still calls `handleGoogleSignIn`.
* `handleGoogleSignIn` calls `signInWithGoogle`.
* Logo work is unlikely to be the root cause.

---

## 2. Removed Suspicion of Lovable Preview Gate

Testing the Lovable gated preview initially confused the issue because iPhone Chrome sometimes hit a Lovable login wall before reaching GameLens.

Later production testing confirmed:

* `https://gamelens.io` loads the real GameLens login page in iPhone Chrome.
* The actual GameLens “Sign in with Google” button is clickable.
* The issue is inside the GameLens Firebase auth flow, not just the Lovable preview gate.

---

## 3. Browser-Specific Auth Strategy

AuthContext was updated to use browser buckets.

Current intended strategy:

```text
Desktop Chrome/Safari/Firefox/Edge → signInWithPopup
iPhone Safari → signInWithPopup
Google app iOS → popup-first, fallback only if needed
Android Chrome → popup-first
iPhone Chrome / CriOS → signInWithRedirect
```

Reason:

* Popup flow was unreliable/crashy on iPhone Chrome.
* Redirect flow is preferred for iPhone Chrome.
* Safari previously broke when all mobile browsers were forced through redirect, so Safari was moved back to popup-first.

Additional safety added:

* `isSigningIn` guard to prevent double-tap duplicate auth attempts.
* Button disables while signing in.
* `isReady` waits for redirect drain + first auth event.
* `pendingRedirect` flag persisted in storage.
* 10-second post-redirect watchdog surfaces a visible error instead of spinning forever.
* Access denied screen added for backend `/me` 403.
* 401 signs out and returns to login.

Result:

* iPhone Chrome no longer crashes.
* iPhone Chrome no longer spins forever.
* iPhone Chrome now returns to GameLens with controlled error.
* But Firebase auth still does not complete.

---

## 4. Google OAuth Configuration Fix

We checked Google Cloud OAuth Client.

Originally, Authorized JavaScript Origins only had:

```text
http://localhost
http://localhost:5000
https://nfl-stream-406420.firebaseapp.com
```

Added:

```text
https://gamelens.io
https://www.gamelens.io
https://preview--nfl-analytica-pro.lovable.app
```

Authorized redirect URI already had:

```text
https://nfl-stream-406420.firebaseapp.com/__/auth/handler
```

After this, iPhone Chrome still failed, but behavior improved from crash/stuck to controlled watchdog error.

---

## 5. Cleared iPhone Chrome Data

We cleared Chrome browser data/site state and retested.

Result:

* Did not fix the issue.
* iPhone Chrome still reaches GameLens login.
* Still redirects through Firebase auth.
* Still returns with “Google sign-in didn’t complete.”

Conclusion:

Likely not stale cache, old JS, stale Firebase redirect state, or stuck tab.

---

## 6. Custom Firebase Auth Domain Setup

Hypothesis:

Original flow used cross-domain auth handler:

```text
gamelens.io
→ nfl-stream-406420.firebaseapp.com
→ gamelens.io
```

This may fail on iPhone Chrome due to storage partitioning / ITP-style restrictions.

Planned fix:

```text
gamelens.io
→ auth.gamelens.io
→ gamelens.io
```

Reason:

`auth.gamelens.io` and `gamelens.io` share the same registrable domain family.

Steps completed:

* Firebase Hosting enabled for project `nfl-stream-406420`.
* `auth.gamelens.io` added as Firebase Hosting custom domain.
* Firebase Hosting shows `auth.gamelens.io` as Connected.
* `https://auth.gamelens.io/__/auth/iframe` loads without a certificate warning.
* `auth.gamelens.io` added to Firebase Authorized Domains.
* Google OAuth Authorized JavaScript Origins includes:

```text
https://auth.gamelens.io
```

* Google OAuth Authorized Redirect URIs includes:

```text
https://auth.gamelens.io/__/auth/handler
```

* Old Firebase entries were kept for rollback:

```text
https://nfl-stream-406420.firebaseapp.com
https://nfl-stream-406420.firebaseapp.com/__/auth/handler
```

Then one-line code change was made:

```ts
authDomain: "nfl-stream-406420.firebaseapp.com"
```

changed to:

```ts
authDomain: "auth.gamelens.io"
```

No AuthContext changes were made with this step.

---

## 7. Result After `auth.gamelens.io` Change

Post-deploy:

### Working

* Desktop Chrome works.
* iPhone Safari works.
* App still loads.

### iPhone Chrome behavior

* Opens `https://gamelens.io`
* Login page loads.
* Tap “Sign in with Google.”
* Redirect now goes through:

```text
auth.gamelens.io
```

instead of:

```text
nfl-stream-406420.firebaseapp.com
```

* Browser returns to GameLens.
* Error appears:

```text
Google sign-in didn’t complete. Please tap Sign in with Google to try again.
```

This means the custom auth domain is active, but iPhone Chrome still does not establish a Firebase user.

---

## Current Interpretation

The problem is now very narrow:

```text
iPhone Chrome
→ GameLens login loads
→ Sign-in button fires
→ Firebase redirect starts
→ auth.gamelens.io is used
→ browser returns to GameLens
→ getRedirectResult likely returns null
→ onAuthStateChanged likely fires with no user
→ watchdog/null-result error appears
```

This is not currently a total app load failure, logo problem, backend auth problem, or access-denied problem.

The key remaining question:

**Why does Firebase still return no usable auth result on iPhone Chrome even after switching authDomain to `auth.gamelens.io`?**

---

## Questions for the React/Firebase GPT

Please evaluate from all addressable angles:

1. Is `auth.gamelens.io` as a Firebase Hosting custom domain sufficient, or does Firebase require the auth handler to be on the exact same host as the app, e.g.:

```text
https://gamelens.io/__/auth/handler
```

2. If exact host is required, how can that work when `gamelens.io` is hosted by Lovable, not Firebase Hosting?

3. Does this require a reverse proxy or path-based rewrite for:

```text
gamelens.io/__/auth/*
```

to Firebase Hosting?

4. Could the current client code be clearing `pendingRedirect` too early?

5. Could `getRedirectResult` be called before Firebase persistence is fully initialized?

6. Could `browserLocalPersistence` or session storage behavior in iPhone Chrome still be the issue?

7. Should iPhone Chrome use `browserSessionPersistence`, `inMemoryPersistence`, or a different persistence strategy?

8. Is there any known issue where `signInWithRedirect` returns null if `getRedirectResult` runs after `onAuthStateChanged` or vice versa?

9. Is the app accidentally re-rendering/remounting AuthProvider in a way that loses redirect state?

10. Could React StrictMode double-invocation affect the redirect-drain logic in development or production?

11. Could the OAuth client / Firebase Auth provider still be misconfigured even though the new origin and redirect URI are present?

12. Should `auth.gamelens.io/__/firebase/init.json` be checked, and what should it contain?

13. Is the fact that `auth.gamelens.io/__/auth/iframe` loads enough, or should we test other reserved URLs?

14. Should the redirect URL in the address bar visibly become `auth.gamelens.io/__/auth/handler`, or can it show only `auth.gamelens.io` during the blank phase?

15. What exact instrumentation would prove whether:

    * `getRedirectResult` returns null
    * it throws
    * `onAuthStateChanged` fires with null
    * user exists briefly and is lost
    * `/me` is never called
    * `/me` is called and fails

---

## Current Recommendation

Do not keep randomly changing auth strategy.

Before further changes, add temporary diagnostic instrumentation or visible debug state to answer:

```text
browserBucket
selectedAuthStrategy
pendingRedirect flag before redirect
redirectResult status: success/null/error
first onAuthStateChanged result
currentUser after redirect
whether /me is called
/me result if called
```

The current failure is likely not random UI or routing. It is specifically the Firebase redirect result not being restored on iPhone Chrome, even after switching to the custom auth domain.

---

## Known Good Rollback

If auth breaks broadly, rollback is one line:

```ts
authDomain: "nfl-stream-406420.firebaseapp.com"
```

Republish.

Old Firebase/OAuth entries are still present for rollback.
Still debugging Chrome iPhone