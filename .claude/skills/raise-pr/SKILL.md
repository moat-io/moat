---
name: raise-pr
description: Prepare and open a GitHub pull request for the current Moat changes, including a SemVer version bump, feature or bugfix branch, commit, and PR targeting main.
---

# Raise a pull request

Use this workflow when asked to raise or create a PR. Complete the version
bump, branch, commit, and GitHub PR steps; do not stop after only suggesting
commands.

## 1. Inspect and determine scope

From the repository root:

1. Inspect `git status --short`, staged and unstaged diffs, and the current
   branch. Read relevant changes to understand the complete intended change.
2. Do not discard, overwrite, or silently include unrelated work. If the
   worktree contains changes that are not clearly part of this PR, ask the
   user which changes to include before staging or committing.
3. Decide whether this is a feature or bug fix:
   - A new feature, view, API, or other new capability is a feature.
   - A small correction that adds no new functionality is a bug fix.
   - If the classification is genuinely unclear, ask the user.
4. Choose a concise branch name describing the change, prefixed with
   `feature/` or `bugfix/` as appropriate. Use lowercase kebab-case.

## 2. Create or verify the branch

Never commit to `main`.

- If currently on `main`, create and switch to a new branch before changing
  `version.txt`, for example:

  ```bash
  git switch -c feature/short-description
  ```

- If already on a `feature/*` or `bugfix/*` branch, continue on that branch.
- If on another branch, create an appropriately named feature or bugfix branch
  before proceeding.
- Verify the current branch after switching.

Creating a branch with `git switch -c` preserves the worktree changes. Never
use a destructive reset or checkout to clean the worktree.

## 3. Increment the version

The version is stored in `<repo-root>/version.txt` and follows
`MAJOR.MINOR.PATCH` semantic versioning.

- Never change the major version (the first number).
- For a feature, increment the minor version and set the patch version to zero:
  `0.25.1` becomes `0.26.0`.
- For a small bug fix with no new functionality, increment only the patch:
  `0.25.1` becomes `0.25.2`.
- Preserve the major version exactly. If `version.txt` is not a plain
  three-part numeric SemVer version, stop and ask how to handle it rather than
  guessing.
- Edit only the version value and include `version.txt` in the PR.

## 4. Validate and commit on the branch

1. Run the relevant tests and checks for the change. For Python tests, follow
   the `run-python-tests` skill; for OPA tests, follow `run-opa-tests`.
2. Review the final diff, including the version change, and check for
   whitespace errors with `git diff --check`.
3. Create a meaningful conventional-style commit subject and concise body that
   accurately describe the included changes.
4. Stage and commit only the intended PR files. Preserve unrelated staged or
   unstaged changes; do not use `git add -A` unless all worktree changes were
   explicitly confirmed as part of this PR.
5. Before committing, verify the current branch is not `main`. Include the
   repository's standard trailer:

   ```text
   Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
   ```

## 5. Open the PR

Use the GitHub CLI (`gh`) to create a PR from the feature/bugfix branch to
`main`. First check whether a PR already exists for the branch. Do not install
`gh` or change repository settings if the CLI is unavailable; report the
blocker.

Use a descriptive title and a body that summarizes the user-visible changes
and lists the tests/checks actually run. For example:

```bash
gh pr create \
  --base main \
  --head "$(git branch --show-current)" \
  --title "feat(scope): concise summary" \
  --body "## Summary
- Describe the change.

## Tests
- List checks actually run."
```

Do not claim checks passed if they were not run or failed. Report the resulting
PR URL and the branch/commit when successful. If pushing or PR creation fails,
state the exact blocker and leave the branch and commit intact.
