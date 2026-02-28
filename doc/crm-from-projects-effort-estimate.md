# Effort Estimate: Re-implementing CRM Using Projects as a Base

## Key Finding

Both CRM and Projects are **external Starlark apps** — their source code is not in this
repository. They are downloaded as zip packages from a remote publisher via P2P at runtime
and installed into `data_dir/apps/<id>/<version>/`. Each app consists of:

- `app.json` — manifest (actions, events, functions, database schema)
- `.star` files — Starlark business logic
- `templates/en/` — HTML templates
- `labels/` — localization strings

Both apps share the same runtime environment: identical Starlark API surface (`mochi.*`),
identical entity system, identical action handler pattern, identical database access
(`mochi.db`). Neither requires special permissions (both are `nil` in `apps_default`).

---

## What CRM and Projects Have in Common

Both are **entity-backed CRUD apps** built on the same Mochi primitives:

| Shared Pattern | How It Works |
|---------------|-------------|
| Entity ownership | `mochi.entity.create(class, name, privacy)` — one entity per project or CRM account |
| SQLite database | `mochi.db.exec/row/rows` — app-specific tables for domain data |
| Action handlers | Functions that take an `action` object and return JSON or HTML |
| Access control | `mochi.access.allow/check` — per-entity permissions |
| Attachments | `mochi.attachment.*` — file/image management |
| User references | `mochi.user.*` — assigning people to items |
| Scheduling | `mochi.schedule.*` — reminders, recurring tasks |

Both apps fundamentally follow the pattern:
1. Create an entity (a "project" or a "CRM account")
2. Store structured data in SQLite tables scoped to that entity
3. Present list/detail views via actions
4. Support collaboration via access control and messaging

---

## What CRM Needs That Projects Doesn't

| CRM Concept | Projects Equivalent | Adaptation Needed |
|------------|-------------------|------------------|
| **Contacts/Leads** | Tasks/Issues | Rename + different fields (company, email, phone, source, stage) |
| **Companies/Organizations** | (none) | New entity class + table |
| **Deals/Pipeline** | Task status workflow | Different stages (lead → qualified → proposal → won/lost) with $ values |
| **Pipeline visualization** | Kanban board | Same UI pattern, different columns and card content |
| **Activities/Touchpoints** | Comments/Activity log | Similar — timestamped entries linked to a contact or deal |
| **Contact timeline** | Task history | Same pattern, different event types (call, email, meeting, note) |
| **Revenue tracking** | (none) | New fields + aggregation queries |
| **Email integration** | (none) | `mochi.account.*` for email, `mochi.url.*` for API calls |
| **Reporting/dashboards** | (none) | New action handlers with aggregation SQL |
| **Import/Export** | (none) | CSV parsing in Starlark, export action |

---

## Effort Breakdown

### Phase 1: Fork and Adapt (2–3 days)

| Task | Time | Notes |
|------|------|-------|
| Copy Projects app structure | 0.5 day | New app ID, rename labels, update app.json |
| Redesign database schema | 1 day | Replace tasks/issues tables with contacts, companies, deals, activities |
| Adapt entity classes | 0.5 day | Change from `project` class to `crm` (or `account`, `pipeline`) |
| Update schema create/upgrade functions | 0.5 day | Starlark `schema_create()` with new CREATE TABLE statements |

### Phase 2: Core CRUD (3–4 days)

| Task | Time | Notes |
|------|------|-------|
| Contact list/detail actions | 1 day | Adapt task list to contact list — different fields, same pattern |
| Company list/detail actions | 1 day | Similar to contacts but with 1-to-many relationship to contacts |
| Deal list/detail actions | 1 day | Status pipeline instead of task states |
| Activity/note creation | 0.5–1 day | Timestamped entries linked to contacts/deals |

### Phase 3: Pipeline & Visualization (2–3 days)

| Task | Time | Notes |
|------|------|-------|
| Pipeline stage management | 0.5 day | Settings action for customizing stages |
| Pipeline/kanban view | 1–1.5 days | If Projects has a kanban view, adapt columns; if not, build from scratch |
| Deal stage transitions | 0.5 day | Move deals between stages, record stage history |
| Revenue summary | 0.5 day | SQL aggregation queries for pipeline value |

### Phase 4: CRM-Specific Features (3–5 days)

| Task | Time | Notes |
|------|------|-------|
| Contact timeline view | 1 day | Chronological activity feed per contact |
| Search and filtering | 1 day | Full-text search across contacts, companies, deals |
| Basic reporting | 1–2 days | Pipeline summary, conversion rates, activity metrics |
| Import from CSV | 0.5–1 day | File upload action + CSV parsing |
| Email/notification integration | 0.5–1 day | `mochi.account.*` for scheduled follow-up reminders |

### Phase 5: Polish (1–2 days)

| Task | Time | Notes |
|------|------|-------|
| Labels/localization | 0.5 day | CRM-specific strings |
| Access control refinement | 0.5 day | Who can see which contacts/deals |
| Template styling | 0.5–1 day | CRM-appropriate layouts |

---

## Total Estimate

| Scope | Days | Calendar Time |
|-------|------|--------------|
| Minimum viable CRM (CRUD only) | 5–7 days | ~1 week |
| Functional CRM (+ pipeline + timeline) | 10–14 days | ~2 weeks |
| Full-featured CRM (+ reports + import + email) | 14–19 days | ~3 weeks |

---

## Why Using Projects as a Base Saves Significant Time

The Projects app gives you for free:

1. **App manifest structure** — actions, events, database config all wired up
2. **Entity lifecycle** — creation, deletion, privacy, access control
3. **User collaboration** — sharing entities, assigning people
4. **Database patterns** — schema creation, migration functions
5. **List/detail UI patterns** — templates for browsing and editing items
6. **Activity/history tracking** — if Projects tracks task history, this directly adapts
7. **File attachments** — already integrated
8. **Navigation structure** — entity-prefixed routing (`:crm/contacts`, `:crm/deals`)

Without Projects as a base, building CRM from scratch would be roughly **4–6 weeks**
because you'd need to build all the scaffolding. Using Projects cuts that to **2–3 weeks**
by letting you focus only on the CRM-specific domain logic.

---

## Risk Factors

1. **No access to current app source** — The actual Starlark code for both apps lives
   outside this repo (downloaded from a publisher). This estimate assumes Projects has
   standard CRUD patterns (list, detail, create, edit, delete actions with SQLite backing).
   If Projects is simpler than expected, the base provides less value.

2. **Template complexity** — If Projects uses complex client-side JavaScript for its UI
   (e.g., a drag-and-drop kanban board), adapting that for CRM's pipeline view is easier.
   If it's server-rendered HTML only, the pipeline visualization may need more work.

3. **Starlark limitations** — Starlark is intentionally limited (no imports, no classes,
   no exceptions). Complex business logic (e.g., deal scoring, revenue forecasting) may
   require creative workarounds.

---

## Conclusion

**2–3 weeks** for a full-featured CRM, starting from the Projects app as a base. The two
apps are structurally very similar — both are entity-backed CRUD apps with list/detail
views, collaboration, and activity tracking. The main work is replacing the domain model
(tasks → contacts/deals), adding pipeline stages, and building CRM-specific views
(contact timeline, revenue reports). The Mochi platform handles all the hard parts
(auth, access control, attachments, messaging, database, P2P).
