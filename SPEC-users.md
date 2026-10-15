# Spec: users and optional owner on projects/settings

## Objective

Admins manage a list of users (email + initials). Projects and settings can optionally be assigned
an owner when they are created. The owner is a label only: it is displayed and grants no permissions.
With no users configured, the webapp behaves exactly as it does today.

Users are not login identities. Admin is still the single shared password (`service/admin.py`).

## Decisions

1. **`User` document** (`shared/db/models.py`), default ObjectId PK:
   - `email`: required, unique, basic regex, max 128
   - `initials`: required, unique, `[A-Z0-9]`, max 8
   - `status`: `UserStatus.ACTIVE` / `UserStatus.INACTIVE` (soft-delete; mirrors `SettingsStatus`)
   - `created_at_`
   - `meta = {"strict": False, "auto_create_index": False}`. Uniqueness is checked in the interface
     before every write. Concurrent admin edits can race; we accept that, as `create_settings` does
     for `number`.
2. **Owner field**: `owner = ReferenceField(User, required=False, default=None)` on `Project` and
   `Settings`. Set only on create and never changed afterwards. No backfill: existing documents keep
   no owner.
3. **Admin-only writes**: add, edit (email, initials), deactivate and reactivate users. These actions
   live on the existing admin page (`webapp/pages_/admin.py`). Non-admins cannot see that page.
   Deactivate is a soft delete. Reactivate exists so that a mistaken deactivation can be undone.
4. **Inactive users**:
   - are hidden from the owner selectboxes;
   - are still shown on the documents they already own.
5. **Create forms**: projects and settings get an optional "Owner" selectbox. Its first option is
   "(none)", which is the default. The options are the active users, shown as `initials (email)`.
   - When there are no active users, the selectbox is not rendered and both forms look as they do
     today.
   - When you create a new version of an existing settings, the selectbox is prefilled with the
     owner of the latest version. You can change it. If that owner is inactive, the prefill is
     "(none)".
6. **Display**: projects and settings tables get an "owner" column that shows initials, or empty.
   There is no filtering.
7. **Fallback project** (`_FALLBACK`): has no owner and no special handling.

## Out of scope

- Filtering by owner, and permissions based on owner.
- Login per user. Mapping the nginx `X-Remote-User` header to a user.
- Exposing the owner in `rest_api` and `mcp_server`. Verify only that they still work.
- Editing the owner after creation.
- Hard delete.

## Changes

| File | Change |
|---|---|
| `shared/db/models.py` | `UserStatus`, `User`, `owner` on `Project` + `Settings` |
| `shared/db/interface.py` | `add_user`, `update_user`, `set_user_status`, `get_all_users(include_inactive)`; optional `owner_id` on `add_project` / `create_settings` |
| `webapp/pages_/admin.py` | user table + create/edit/(de)activate |
| `webapp/pages_/projects.py`, `settings.py` | owner selectbox + owner column |
| `misc/init-mongo.sh`, `docs/deployment.md` | `webappRole`: `find/insert/update/remove` on collection `user`. Includes the manual `mongosh` command for existing DBs |

No data migration is needed.

## Commands

```
Test:   python -m pytest shared/tests/db webapp/tests
Lint:   pre-commit run --all-files
```

## Code style

Follow the existing interface functions, for example:

```python
def add_user(*, email: str, initials: str) -> User:
    """Add a new active user."""
    connect_db()
    _check_user_unique(email=email, initials=initials)
    user = User(email=email, initials=initials)
    user.save(force_insert=True)
    logging.info(f"Added user: {email=} {initials=}")
    return user
```

- Keyword-only args.
- Constants in `ALL_CAPS`.
- Inputs pass `check_for_malicious_content`.

## Testing strategy

- `shared/tests/db/test_interface.py`, with the existing mocking style:
  - add/update user, including uniqueness violations on both fields;
  - set status;
  - `get_all_users` filtering;
  - `add_project` / `create_settings` with and without `owner_id`.
- `webapp/tests/pages_/test_admin_page.py`: the user form renders for admins, and creating a user calls the interface.
- `test_projects.py`, `test_settings.py`:
  - with no users, there is no owner selectbox and the current tests pass unchanged;
  - with users, the selection is passed through;
  - for a new settings version, the selectbox is prefilled with the previous owner.

## Boundaries

- Always: run the tests listed above before each commit, and keep the current behavior when there are no users.
- Ask first: any index change in `init-mongo.sh`; new dependencies.
- Never: making owner required, backfilling existing docs, touching `_migrations`.

## Success criteria

1. With an empty `user` collection, every existing webapp test passes unchanged, and the create forms
   look the same as today.
2. An admin can create, edit, deactivate and reactivate a user. A non-admin cannot reach any of these
   actions.
3. Duplicate email or initials are rejected with an error toast. This holds for create and for edit.
4. A project or settings created with an owner stores a reference to that owner. Its table row
   shows the owner's initials. When the user's initials are edited, the row shows the new initials.
5. A deactivated user is not offered in the selectboxes, and is still shown on the documents they own.
6. For a new settings version, the owner selectbox is prefilled with the previous version's owner,
   unless that owner is inactive.

## Open questions

1. Initials format: is `[A-Z0-9]{1,8}` OK? Should lowercase input be converted to upper case?
2. Collection name `user`: this is the MongoEngine default. Is it OK, or should it be `users`?
