# Android Mochi Client - Effort Analysis

## Executive Summary

Building an Android app that acts as a full Mochi client with frontends for all 17 apps is a **significant but tractable effort** — roughly **3–5 months for a single experienced Android developer**, or **6–10 weeks for a 2–3 person team**. The server already exposes clean JSON APIs and WebSocket channels, so the Android client would be a pure consumer — no backend changes needed.

---

## Architecture Overview

### What the Android Client Needs to Talk To

Mochi is a Go-based personal server that exposes:

- **REST/JSON endpoints** — All app actions support JSON responses via content negotiation (`Accept: application/json`)
- **WebSocket channel** (`GET /_/websocket?key=<channel>`) — Server-push for real-time updates (chat messages, notifications, etc.)
- **Static file serving** — App assets, attachments, thumbnails
- **P2P protocol** (optional) — CBOR-over-libp2p streams for federated communication

The critical insight: **every Starlark action already returns JSON when the client sends `Accept: application/json`**. The server has full content negotiation built in. An Android client doesn't need to scrape HTML — it just needs to call the same action endpoints with a JSON Accept header.

### Authentication

The server supports multiple auth methods, all workable from Android:

| Method | Android Feasibility | Notes |
|--------|-------------------|-------|
| Email magic link + JWT | Easy | Standard HTTP flow, store JWT in EncryptedSharedPreferences |
| API tokens (`mochi-*`) | Easy | Bearer token in Authorization header |
| TOTP (2FA) | Easy | Standard TOTP verification POST |
| Passkeys/WebAuthn | Medium | Requires Android Credential Manager API (API 28+) |
| Recovery codes | Easy | Simple POST |

JWT tokens are valid for 1 year and signed with per-session secrets. The flow:
1. `POST /_/auth/begin` → initiate login
2. `POST /_/auth/code` → request email code
3. `POST /_/auth/verify` → verify code → receive JWT
4. All subsequent requests: `Authorization: Bearer <jwt>`

---

## Per-App Effort Breakdown

### Tier 1: Simple Apps (1–3 days each)

| App | What It Does | Android UI | Effort |
|-----|-------------|-----------|--------|
| **Login** | Auth gateway | Login form, email code entry, TOTP entry | 2–3 days |
| **Home** | Dashboard/launcher | Grid/list of app icons, launcher activity | 1–2 days |
| **Settings** | User/system config | Standard preferences screens (AndroidX Preference) | 2–3 days |
| **Notifications** | Push alerts | Firebase + WebPush subscription, notification list | 2–3 days |
| **People** | User/contact directory | RecyclerView list, search, user detail cards | 2–3 days |
| **Apps** | App management | App list, install/uninstall actions | 1–2 days |

**Subtotal: ~10–16 days**

### Tier 2: Medium Apps (3–7 days each)

| App | What It Does | Android UI | Effort |
|-----|-------------|-----------|--------|
| **Feeds** | RSS aggregation | Feed list, article reader, pull-to-refresh | 4–5 days |
| **Wikis** | Knowledge base | Markdown renderer, page browser, editor | 5–7 days |
| **Publisher** | Content publishing | Rich text/markdown editor, publish flow | 5–7 days |
| **Projects** | Project management | Task lists, kanban-style boards, detail views | 5–7 days |
| **CRM** | Customer management | Contact forms, pipeline views, relationship graphs | 5–7 days |

**Subtotal: ~24–33 days**

### Tier 3: Complex Apps (1–2 weeks each)

| App | What It Does | Android UI | Effort |
|-----|-------------|-----------|--------|
| **Chat** | Real-time messaging | Message bubbles, WebSocket integration, media attachments, typing indicators | 7–10 days |
| **Forums** | Discussion boards | Threaded view, nested comments, pagination | 5–7 days |
| **Repositories** | Git management | File browser, diff viewer, commit history, branch management | 10–14 days |
| **Chess** | Multiplayer chess | Custom canvas/board view, move validation, real-time P2P | 7–10 days |
| **Go** | Dev tools | Code editor with syntax highlighting | 7–10 days |

**Subtotal: ~36–51 days**

### Infrastructure / Shared Components (2–3 weeks)

| Component | What It Covers | Effort |
|-----------|---------------|--------|
| **Network layer** | Retrofit/OkHttp client, JWT interceptor, error handling, retry logic | 3–4 days |
| **Auth module** | Login flow, token storage, session refresh, biometric unlock | 3–4 days |
| **WebSocket manager** | Connection lifecycle, reconnection, channel multiplexing | 3–4 days |
| **Entity framework** | Entity CRUD, fingerprint display, privacy indicators | 2–3 days |
| **Attachment handling** | Upload/download, image loading (Coil/Glide), thumbnails | 2–3 days |
| **Markdown rendering** | Markwon library integration for wiki/forum/chat content | 1–2 days |
| **Navigation** | Bottom nav, deep linking, app switching | 2–3 days |
| **Offline support** | Room database, sync queue, conflict resolution | 5–7 days (optional) |

**Subtotal: ~16–23 days (plus ~5–7 optional for offline)**

---

## Total Effort Estimate

| Scope | Days | Calendar Time (1 dev) | Calendar Time (2–3 devs) |
|-------|------|----------------------|--------------------------|
| Infrastructure + Tier 1 | 26–39 | 5–8 weeks | 2–3 weeks |
| + Tier 2 | 50–72 | 10–14 weeks | 4–6 weeks |
| + Tier 3 (full) | 86–123 | 17–25 weeks | 6–10 weeks |
| + Offline support | 91–130 | 18–26 weeks | 7–11 weeks |

### Recommended Phased Approach

**Phase 1 (MVP — 4–6 weeks):** Infrastructure + Login + Home + Chat + Notifications + Settings + People
- Gives you a usable communication client immediately

**Phase 2 (Content — 3–4 weeks):** Feeds + Wikis + Publisher + Forums
- Adds content consumption and creation

**Phase 3 (Productivity — 3–4 weeks):** Projects + CRM + Apps
- Adds productivity tools

**Phase 4 (Specialized — 3–4 weeks):** Repositories + Chess + Go
- Adds specialized/niche apps

---

## Technology Recommendations

### Recommended Android Stack

```
Language:           Kotlin
Min SDK:            API 26 (Android 8.0) — covers 95%+ of devices
UI Framework:       Jetpack Compose (modern declarative UI)
Architecture:       MVVM with Hilt dependency injection
Networking:         Retrofit + OkHttp + Kotlin Serialization
WebSocket:          OkHttp WebSocket client
Local Storage:      Room (SQLite) + DataStore (preferences)
Image Loading:      Coil (Kotlin-first)
Markdown:           Markwon library
Navigation:         Jetpack Navigation Compose
Auth Storage:       EncryptedSharedPreferences / AndroidX Security
Passkeys:           Android Credential Manager API
Push Notifications: Firebase Cloud Messaging (with bridge to Mochi WebPush)
Chess Board:        Custom Compose Canvas
Git Diffs:          Custom syntax-highlighted text rendering
```

### Why NOT Cross-Platform (React Native / Flutter)?

The apps involve several features that benefit from native Android:
- WebAuthn/Passkeys (Credential Manager API)
- Background WebSocket connections (Android foreground services)
- Push notifications (FCM integration)
- File system access for attachments
- Chess board custom rendering

However, **Kotlin Multiplatform (KMP)** could be viable if you later want iOS support — share the networking/data layer while keeping native UI.

---

## Key Simplifying Factors

1. **Content negotiation already works** — Every action returns JSON with the right Accept header. No new backend endpoints needed.

2. **No P2P required for MVP** — The Android client can communicate purely over HTTP/WebSocket. P2P (libp2p) is only needed for federated features and can be deferred.

3. **Rate limits are generous** — 1000 requests/minute per IP is more than enough for a mobile client.

4. **Auth is standard** — JWT + Bearer token is the most common mobile auth pattern. No custom protocol needed.

5. **17 apps but many share patterns** — Most apps are CRUD + list/detail views. Build a reusable entity list/detail scaffold and most apps snap into it.

---

## Key Challenges

1. **No formal API documentation** — Actions are defined in Starlark app manifests. You'd need to reverse-engineer or document each app's JSON responses. Consider adding an `/_/api/schema` endpoint to auto-generate API docs from app manifests.

2. **WebSocket is server-push only** — The current WebSocket implementation only sends data to clients; it doesn't process incoming messages. For chat, you'd POST messages via REST and receive updates via WebSocket.

3. **Attachment handling** — Files are streamed through the server. Large file uploads on mobile need chunked upload support or progress tracking.

4. **Passkey support** — WebAuthn on Android requires the Credential Manager API and FIDO2 integration. This is doable but adds complexity.

5. **Offline mode** — The server has no built-in sync protocol. Building offline support requires designing a sync strategy (last-write-wins, CRDTs, or manual conflict resolution).

6. **App version compatibility** — Apps can have multiple versions. The Android client needs to handle version negotiation or target a minimum app API version.

---

## Conclusion

The effort is **moderate-to-large** but very achievable. The Mochi server architecture is already API-friendly — content negotiation means every action is already a JSON endpoint. The main work is building 17 Android frontends, which range from trivial (Home, Apps) to substantial (Chat, Repositories).

A practical MVP (auth + home + chat + notifications + settings) could be built in **4–6 weeks** by a single developer, providing immediate value while the remaining apps are built incrementally.
