# Plan: Refactor Project-Settings Relationship from 1:1 to n:1

## Summary

Change the relationship between Projects and Settings from 1:1 (Settings owns Project reference) to n:1 (multiple Projects can share one Settings, Project references Settings).

## Design Decisions (from user input)

- Settings become **standalone entities** (no project ownership)
- Identification: **name + version** together are globally unique
- Project stores an optional **settings ReferenceField** to Settings
- Settings status: **ACTIVE/ARCHIVED** (archived hidden from selection but still works)
- Cannot archive Settings if any Project references it
- Project can change settings **anytime**
- Migration: **Convert 1:1** - each existing Settings becomes standalone
- Settings files stored at `<settings_path>/<settings_name>/`
- Add **description** field to Settings

---

## Phase 1: Model Changes

### File: `shared/db/models.py`

#### 1.1 Add SettingsStatus class (after line 203)

```python
class SettingsStatus:
    """Status of settings."""
    ACTIVE = "active"
    ARCHIVED = "archived"
```

#### 1.2 Modify Settings model (lines 222-254)

- **Remove**: `project = ReferenceField(Project)` (lines 228-230)
- **Add**: `description = StringField(max_length=512)` field
- **Change**: `status` default from `ProjectStatus.ACTIVE` to `SettingsStatus.ACTIVE`
- **Add**: unique index on `(name, version)` in `meta`

```python
class Settings(Document):
    """Schema for quanting settings."""

    meta: ClassVar = {
        "strict": False,
        "indexes": [
            {"fields": ["name", "version"], "unique": True}
        ]
    }
    objects: ClassVar[QuerySet[Settings]]

    name = StringField(required=True, max_length=64)
    version = IntField(min_value=1, default=1)
    description = StringField(max_length=512)

    fasta_file_name = StringField(required=True, max_length=128)
    speclib_file_name = StringField(required=True, max_length=128)
    config_file_name = StringField(required=False, max_length=128)
    config_params = StringField(required=False, max_length=512)

    software_type = StringField(required=True, max_length=128, default="alphadia")
    software = StringField(required=True, max_length=128)

    status = StringField(max_length=64, default=SettingsStatus.ACTIVE)
    created_at_ = DateTimeField(default=datetime.now)
```

#### 1.3 Modify Project model (lines 206-220)

- **Add**: `settings = ReferenceField(Settings, required=False)`

```python
class Project(Document):
    """Schema for a project."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[Project]]

    id = StringField(required=True, primary_key=True, min_length=3, max_length=16)
    name = StringField(required=True, max_length=64)
    description = StringField(max_length=512)

    settings = ReferenceField(Settings, required=False)

    status = StringField(max_length=32, default=ProjectStatus.ACTIVE)
    created_at_ = DateTimeField(default=datetime.now)
```

---

## Phase 2: Interface Function Changes

### File: `shared/db/interface.py`

#### 2.1 Update imports (line 14-23)

Add `SettingsStatus` to imports from `shared.db.models`.

#### 2.2 Modify `get_settings_for_project()` (lines 214-219)

```python
def get_settings_for_project(project_id: str) -> Settings | None:
    """Get the settings assigned to a project."""
    logging.info(f"Getting settings from DB for: {project_id=}")
    connect_db()
    project = Project.objects.get(id=project_id)
    return project.settings
```

#### 2.3 Replace `add_settings()` (lines 222-263) with `create_settings()`

```python
def create_settings(
    *,
    name: str,
    version: int | None = None,
    description: str | None = None,
    fasta_file_name: str,
    speclib_file_name: str,
    config_file_name: str | None,
    config_params: str | None,
    software_type: str,
    software: str,
) -> Settings:
    """Create a new standalone settings entry.

    If version is None, auto-increment based on existing settings with same name.
    """
    connect_db()

    if version is None:
        existing = Settings.objects(name=name).order_by("-version").first()
        version = (existing.version + 1) if existing else 1

    settings = Settings(
        name=name,
        version=version,
        description=description,
        fasta_file_name=fasta_file_name,
        speclib_file_name=speclib_file_name,
        config_file_name=config_file_name,
        config_params=config_params,
        software_type=software_type,
        software=software,
    )
    settings.save(force_insert=True)
    logging.info(f"Created settings: {name=} {version=}")
    return settings
```

#### 2.4 Add new interface functions

```python
def get_all_settings(*, include_archived: bool = False) -> list[Settings]:
    """Get all settings from the database."""
    connect_db()
    if include_archived:
        return list(Settings.objects.all().order_by("-created_at_"))
    return list(Settings.objects(status=SettingsStatus.ACTIVE).order_by("-created_at_"))


def get_settings_by_id(settings_id: str) -> Settings | None:
    """Get a settings entry by its MongoDB ObjectId."""
    connect_db()
    return Settings.objects(id=settings_id).first()


def archive_settings(settings_id: str) -> None:
    """Archive a settings entry. Raises ValueError if referenced by any project."""
    connect_db()
    settings = Settings.objects.get(id=settings_id)

    referencing_projects = Project.objects(settings=settings)
    if referencing_projects.count() > 0:
        project_ids = [p.id for p in referencing_projects]
        raise ValueError(
            f"Cannot archive settings '{settings.name}' v{settings.version}: "
            f"referenced by projects {project_ids}"
        )

    settings.status = SettingsStatus.ARCHIVED
    settings.save()
    logging.info(f"Archived settings: {settings.name} v{settings.version}")


def assign_settings_to_project(project_id: str, settings_id: str | None) -> None:
    """Assign settings to a project, or remove assignment if settings_id is None."""
    connect_db()
    project = Project.objects.get(id=project_id)

    if settings_id is None:
        project.settings = None
    else:
        settings = Settings.objects.get(id=settings_id)
        if settings.status == SettingsStatus.ARCHIVED:
            raise ValueError(f"Cannot assign archived settings '{settings.name}' v{settings.version}")
        project.settings = settings

    project.save()
    logging.info(f"Assigned settings {settings_id} to project {project_id}")


def get_projects_using_settings(settings_id: str) -> list[Project]:
    """Get all projects that reference a specific settings entry."""
    connect_db()
    settings = Settings.objects.get(id=settings_id)
    return list(Project.objects(settings=settings))
```

---

## Phase 3: Processing Pipeline Changes

### File: `airflow_src/dags/impl/processor_impl.py`

#### 3.1 Update `prepare_quanting()` (around line 96)

Change settings path from project-based to settings-name-based:

**Before:**
```python
settings_path = get_path(YamlKeys.Locations.SETTINGS) / project_id_or_fallback
```

**After:**
```python
settings_path = get_path(YamlKeys.Locations.SETTINGS) / settings.name
```

#### 3.2 Update error message (lines 84-87)

```python
if settings is None:
    raise AirflowFailException(
        f"No settings assigned to project '{project_id_or_fallback}'. "
        "Please assign settings to this project in the WebApp."
    )
```

---

## Phase 4: Webapp UI Changes

### File: `webapp/pages_/settings.py`

Major restructure of the settings page:

1. **Update file path message** (lines 91-94): Change to `<settings_path>/<settings name>/`

2. **Replace form flow** (lines 120-330):
   - Remove project selection as first step
   - Add settings name input with validation
   - Add description field
   - Change submission to call `create_settings()`
   - Add separate section for assigning settings to projects

3. **Add settings management section**:
   - Display all settings with their referencing projects
   - Archive button (disabled if projects reference it)
   - Status indicator (ACTIVE/ARCHIVED)

### File: `webapp/pages_/projects.py`

1. **Add settings assignment UI**:
   - Select project from dropdown
   - Show current settings (if any)
   - Dropdown to select from available active settings
   - Button to update assignment

2. **Update project table**: Add column showing assigned settings name/version

### File: `webapp/service/db.py`

Add new service functions to expose the new interface functions to the webapp.

---

## Phase 5: Migration Script

### New file: `shared/migrations/migrate_settings_to_standalone.py`

```python
"""Migration: Convert Settings from project-owned to standalone."""

def migrate_settings_to_standalone(*, dry_run: bool = True) -> dict:
    """
    Migration steps:
    1. For each existing Settings with project reference:
       a. Generate name as "{project_id}_v{version}" (e.g., "P1234_v1")
       b. Store the generated name in Settings.name (if not already set meaningfully)
       c. Record the project_id for step 2
    2. For each Project that had Settings:
       a. Set Project.settings = reference to the Settings
    3. Remove project field from all Settings documents
    4. Move files from <settings>/<project_id>/ to <settings>/<settings_name>/
    5. Convert INACTIVE status to ARCHIVED

    Returns dict with counts and any errors.
    """
```

**Execution order:**
1. Create MongoDB backup
2. Run with `--dry-run` to preview changes
3. Run with `--execute` to apply
4. Run with `--verify` to check integrity
5. Move settings files on filesystem

---

## Phase 6: Test Updates

### File: `shared/tests/db/test_interface.py`

- Remove `test_add_settings_first` and `test_add_settings_second` (lines 259-337)
- Add tests:
  - `test_create_settings_first_version`
  - `test_create_settings_auto_increment_version`
  - `test_get_all_settings_excludes_archived`
  - `test_get_all_settings_includes_archived`
  - `test_archive_settings_success`
  - `test_archive_settings_fails_when_referenced`
  - `test_assign_settings_to_project`
  - `test_get_projects_using_settings`

### File: `webapp/tests/pages_/test_settings.py`

- Update mock data structure: remove `project` column, add `description`

### File: `airflow_src/tests/dags/impl/test_processor_impl.py`

- Update `test_prepare_quanting*` tests for new settings path calculation

---

## Implementation Order

1. **Phase 1**: Model changes (`shared/db/models.py`)
2. **Phase 2**: Interface functions (`shared/db/interface.py`)
3. **Phase 6 (partial)**: Update tests to match new interface
4. **Phase 5**: Write and test migration script
5. **Phase 3**: Processing pipeline changes (`processor_impl.py`)
6. **Phase 4**: Webapp UI changes
7. **Phase 6 (remaining)**: Update remaining tests
8. Run pre-commit and full test suite

---

## Critical Files

| File | Changes |
|------|---------|
| `shared/db/models.py` | SettingsStatus class, Settings model (remove project, add description, add index), Project model (add settings field) |
| `shared/db/interface.py` | Replace add_settings with create_settings, update get_settings_for_project, add 4 new functions |
| `airflow_src/dags/impl/processor_impl.py` | Change settings_path calculation (line ~96) |
| `webapp/pages_/settings.py` | Major UI refactoring for standalone settings |
| `webapp/pages_/projects.py` | Add settings assignment UI |
| `shared/migrations/migrate_settings_to_standalone.py` | New migration script |
