---
name: commit-message
description: Write a concise commit message and provide the complete git commit command for the current changes. Use when asked for a commit message or full commit command; do not create the commit unless explicitly requested.
---

# Write a commit message and command

Review the current repository changes and give the user a commit message plus
the complete, copy-pasteable `git commit` command.

## Steps

1. Inspect `git status --short`, `git diff --stat`, and
   `git diff --cached --stat` to understand which files are staged, unstaged,
   added, renamed, or deleted. Read the relevant diffs when needed to identify
   the change accurately. Do not stage, unstage, or modify files.
2. Write a concise imperative subject, preferably following the repository's
   conventional-commit style (`type(scope): summary`) when appropriate. Add a
   short body describing the meaningful changes; avoid listing every file.
3. Return the message inside a fenced `bash` block as a complete command using
   one `-m` per paragraph, for example:

   ```bash
   git commit \
     -m "feat(scope): concise summary" \
     -m "Explain the main behavior or implementation change." \
     -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
   ```

4. Do not run the commit command unless the user explicitly asks you to commit.
   If there are no changes to commit, say so and ask what change the message
   should describe rather than inventing one.
