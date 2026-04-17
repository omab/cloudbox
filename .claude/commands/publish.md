# Publish cloudbox to PyPI

Bump the version, tag the release, and publish the package to PyPI.

## Steps

### 1. Determine the next version

Read the current version from `pyproject.toml` (the `version = "..."` line under `[project]`).

Parse it as `MAJOR.MINOR.PATCH`. The default bump is **patch** unless the user passed an argument:
- `$ARGUMENTS` = `major` → bump MAJOR, reset MINOR and PATCH to 0
- `$ARGUMENTS` = `minor` → bump MINOR, reset PATCH to 0
- `$ARGUMENTS` = `patch` or empty → bump PATCH only
- `$ARGUMENTS` = an explicit version like `1.2.3` → use that version directly

### 2. Pre-flight checks

Before making any changes:
- Run `git status` and confirm there are no uncommitted changes. If there are, stop and tell the user to commit or stash them first.
- Run `uv run pytest tests/ -q --no-header 2>&1 | tail -3` to confirm the test suite passes. If tests fail, stop and report the failures.

### 3. Bump the version in pyproject.toml

Edit `pyproject.toml`: replace the current version string with the new version.

### 4. Rebuild the package

Run `uv build` and confirm both `dist/cloudbox-<new_version>.tar.gz` and `dist/cloudbox-<new_version>-py3-none-any.whl` are produced successfully.

### 5. Commit and tag

```
git add pyproject.toml
git commit -m "chore: release v<new_version>"
git tag v<new_version>
```

### 6. Publish to PyPI

Run:
```
uv publish
```

`uv publish` picks up credentials from:
- The `UV_PUBLISH_TOKEN` environment variable, or
- A `~/.pypirc` file, or
- It will prompt interactively for a token.

If publish fails due to missing credentials, tell the user to set `UV_PUBLISH_TOKEN=pypi-...` and re-run `/publish`.

### 7. Report

Print a summary:
- Previous version → new version
- Git tag created
- PyPI URL: `https://pypi.org/project/cloudbox/<new_version>/`

Do not push the commit or tag to the remote — leave that for the user to do with `git push && git push --tags`.
