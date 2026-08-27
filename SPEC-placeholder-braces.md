# Spec: Brace-delimited config_params placeholders

## Objective

Placeholders in a settings entry's `config_params` are currently bare words (`RAW_FILE_PATH`).
They become brace-delimited (`{RAW_FILE_PATH}`).

Why: a bare placeholder is a substring of any longer one, so `_substitute_config_params()` depends
on replacement order (`RAW_FILE_PATH` before `RELATIVE_RAW_FILE_PATH`). Braces remove the ambiguity
and make placeholders visually distinct from literal arguments.

User: the admin who writes `config_params` in the webapp settings form.

## Scope

In:
- `shared/config_params.py` (new) — the placeholder registry and the substitution helper
- `airflow_src/dags/impl/processor_impl.py` — substitution keys, obsolete ordering comment,
  raw-`config_params` validation
- `webapp/pages_/settings.py` — raw-`config_params` validation, form placeholder, help text, examples
- `docs/deployment.md` — docker-engine example
- `airflow_src/tests/dags/impl/test_processor_impl.py`, `shared/tests/test_config_params.py` (new)

Out:
- The `RAW_FILE_PATH` / `OUTPUT_PATH` / `NUM_THREADS` **environment variables** used by
  `DockerJobHandler`, `msqc-extractor/`, and `submit_job.sh`. Same names, unrelated mechanism.
- Migration of existing DB settings (decided: clean break, see Boundaries).

## Decisions

1. **Clean break, no compatibility guard.** Only `{PLACEHOLDER}` substitutes. Settings rows stored
   with bare placeholders stop substituting and will hand the literal string to the quanting
   software until an admin saves a new settings version. No detection, no warning.
2. **Validate after substitution only; the webapp substitutes dummies.** `{`/`}` stay out of the
   allowed-character patterns in `shared/validation.py`. Every `check_for_malicious_content()` call
   on `config_params` runs on a string in which the placeholders are already gone.

   The placeholder names move out of `processor_impl.py` into a shared registry so both sides know
   them:

   ```python
   # shared/config_params.py
   CONFIG_PARAM_PLACEHOLDERS: dict[str, str] = {   # name -> help text shown in the webapp
       "PROJECT_ID": "project id",
       ...
   }
   DUMMY_VALUE = "dummy"

   def substitute_placeholders(config_params: str, values: dict[str, str]) -> str: ...
   ```

   - Webapp (save time): substitutes `DUMMY_VALUE` for every known placeholder, then validates the
     result with `allow_spaces=True`. An admin's `{RAW_FILE_PATH}` passes; an absolute path or a
     shell metacharacter they typed themselves still fails, as today.
   - Processor (job submission): validates the real substituted `config_params`, unchanged
     (`processor_impl.py:314-320`), plus the raw params via the same dummy substitution so the
     absolute-path restriction on admin input survives.
   - The webapp help list is generated from the registry, so names cannot drift between the form
     and the substitution.

   Side effect, intended: an unknown placeholder such as `{FOO}` is not substituted by either side,
   so the brace reaches `check_for_malicious_content()` and is rejected — a typo fails at save time
   in the webapp rather than silently reaching the quanting software.

## Commands

```
Test:  pytest airflow_src/tests webapp/tests shared/tests
Lint:  pre-commit run --all-files
```

## Code Style

`_substitute_config_params()` keeps building the values dict and delegates the replacing:

```python
return substitute_placeholders(
    settings.config_params,
    {
        "RAW_FILE_PATH": str(raw_file_path),
        ...
    },
)
```

Order-independent now; the `# mind the order of replacements here` comment is deleted.

## Success Criteria

1. `substitute_placeholders()` substitutes `{X}` for all eight placeholders and leaves a bare `X`
   untouched.
2. Substitution result is independent of the registry's key order.
3. A settings entry with `--f {RAW_FILE_PATH} --threads {NUM_THREADS}` saves in the webapp without
   a validation error; one with `{FOO}` or `--f /etc/passwd` is rejected there.
4. `_check_content()` returns an error when the substituted `config_params` still contains a brace.
5. Webapp help list, form placeholder, and both examples show braces; the help list is derived from
   `CONFIG_PARAM_PLACEHOLDERS`.
6. `docs/deployment.md` docker example shows braces.
7. Full test suite passes.

## Boundaries

- Always: keep the post-substitution validation intact; update tests alongside code.
- Ask first: touching `shared/validation.py` patterns (not needed under decision 2).
- Never: add a backwards-compatibility path for bare placeholders; touch the env-var names in
  `submit_job.sh` / `msqc-extractor` / `DockerJobHandler`.

## Open Questions

None.
