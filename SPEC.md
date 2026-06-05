# Spec: Add `User` entity + `owner` on Project/Settings

## Goal

Introduce a first-class `User` entity (distinct from any authentication identity)
with admin-only CRUD, and give `Project` and `Settings` a required `owner` that
references one `User`.

## Decisions

- **User PK**: `email` (string primary key). `initials` carries a unique index.
- **owner**: `ReferenceField(User)`, **required**, **set only at creation** (no edit UI).
- **Delete**: soft-delete via `status` (ACTIVE/INACTIVE), mirroring `ProjectStatus`/`SettingsStatus`.
  Inactive users stay valid as owners; they are hidden from new-owner dropdowns.
- **Admin**: new **Users** page in nav, write-gated by `?admin=true` (`QueryParams.ADMIN`),
  matching the existing `_FALLBACK` pattern.
- **Backfill**: migration seeds a placeholder user and assigns it to all existing
  Projects/Settings.

## 1. Model — `shared/db/models.py`

- Add `UserStatus` class (`ACTIVE`/`INACTIVE`), next to `ProjectStatus`.
- Add `User(Document)`:
  - `email = StringField(required=True, primary_key=True, max_length=128, regex=EMAIL_REGEX)`
  - `initials = StringField(required=True, max_length=16, unique=True)`
    (`auto_create_index=False`, like Settings, to avoid needing index-write rights on
    read-only connections)
  - `slack_member_id = StringField(max_length=32)`
  - `status = StringField(default=UserStatus.ACTIVE)`
  - `created_at_ = DateTimeField(default=datetime.now)`
- Add `owner = ReferenceField(User, required=True)` to `Project` (replacing the
  `# missing: created by` comment) and to `Settings`.

## 2. CRUD interface — `shared/db/interface.py`

- `add_user(*, email, initials, slack_member_id)` — `force_insert=True`.
- `get_all_users(*, include_inactive=False)`.
- `update_user(email, *, initials=_NO_UPDATE, slack_member_id=_NO_UPDATE)` (email is PK, immutable).
- `deactivate_user(email)` -> sets status INACTIVE (soft-delete).
- Add required `owner_email` param to `add_project(...)` and `create_settings(...)`;
  they resolve `User.objects.get(email=owner_email)` and set `owner`.

## 3. Webapp — new page `webapp/pages_/users.py`

- Register in `webapp/webapp.py` nav as "👤 Users".
- Table of users (resolve status styling like settings). Create form (email, initials,
  slack member id). Edit (initials/slack id) + Deactivate buttons per row — all gated:
  `disable_write = DISABLE_WRITE or not is_query_param_true(QueryParams.ADMIN)`, with a
  banner telling non-admins it's admin-only.
- Basic validation: email regex, initials alphanumeric, `check_for_malicious_content` on inputs.

## 4. Owner in existing create forms

- **projects.py**: add required "Owner" `selectbox` (active users, shown as
  `initials — email`) to the create form; pass `owner_email` to `add_project`. Resolve
  owner to a readable string in the projects table.
- **settings.py**: add required "Owner" selectbox to the create form; pass to
  `create_settings`. Show owner in the settings table.
- Both: if no active users exist, disable creation with a "create a user first" warning.

## 5. Migration — `shared/_migrations/_migrate_add_owner.py`

- Dry-run support (matching `_migrate_add_excluded_field.py`).
- Seed placeholder `User(email="unknown@alphakraken", initials="NA")` if absent.
- Set `owner` -> placeholder on all Project/Settings docs missing it.
- Docstring documents the **required mongosh role update**: grant the webapp role
  `find/insert/update/remove` on the new `user` collection (per the
  `_migrate_project_settings_to_mn.py` precedent and `docs/deployment.md`).

## 6. Tests

- `shared/tests/db/test_interface.py`: add_user, update_user, deactivate_user,
  get_all_users; updated add_project/create_settings with owner.
- `webapp/tests/pages_/test_users.py`: table renders, admin gating disables writes,
  create flow.
- Update existing project/settings page tests for the new owner selectbox.

## 7. Docs

- One line in `docs/deployment.md` MongoDB section: webapp role now also writes the
  `user` collection.

## Risks / things to flag

1. **`df_from_db_data` + ReferenceField**: `to_mongo()` renders `owner` as a `DBRef`,
   not readable. Resolve owner -> `initials`/`email` for display in the project/settings
   tables (small mapping step). Not free, but contained.
2. **Required-on-read is safe**: MongoEngine validates `required` only on `.save()`, and
   models use `strict=False`, so existing reads of owner-less docs won't break before the
   migration runs — but any *save* of an owner-less Project/Settings would. The only save
   paths are the create forms (which set owner), so this is fine.
3. **rest_api / MCP**: read-only over Project/Settings. Adding `owner` shouldn't break
   them, but verify serialization doesn't choke on the DBRef during the verify step.
4. **Email as immutable PK**: editing a user's email isn't possible (it's the PK / the
   reference target). Edits cover initials + slack id only. An email change is a
   delete-and-recreate. Acceptable given the soft-delete model.

## Implementation order

model -> interface -> migration -> UI -> tests, running tests as each slice lands.
