import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        """
        # Save Notebook Changes

        Commit and push edited notebooks back to GitHub.

        1. Edit any notebook in this Marimo server — changes auto-save to disk.
        2. Come here to review what changed and push to the repo.
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    import os
    import subprocess
    return mo, os, subprocess


@app.cell
def _(mo, os, subprocess):
    _status = subprocess.run(
        ["git", "status", "--short", "notebooks/"],
        capture_output=True, text=True, cwd=os.path.expanduser("~/repo"),
    )
    _diff = subprocess.run(
        ["git", "diff", "notebooks/"],
        capture_output=True, text=True, cwd=os.path.expanduser("~/repo"),
    )

    has_changes = bool(_status.stdout.strip())

    if has_changes:
        mo.md(
            f"### Modified files\n```\n{_status.stdout}```\n"
            f"### Diff\n```diff\n{_diff.stdout}```"
        )
    else:
        mo.md("**No changes detected.** Edit a notebook and come back here.")
    return (has_changes,)


@app.cell
def _(has_changes, mo):
    mo.stop(not has_changes, mo.md(""))

    commit_msg = mo.ui.text(
        value="Update notebooks from Marimo editor",
        label="Commit message",
        full_width=True,
    )
    push_btn = mo.ui.run_button(label="Commit & Push")

    mo.hstack([commit_msg, push_btn])
    return commit_msg, push_btn


@app.cell
def _(commit_msg, mo, os, push_btn, subprocess):
    mo.stop(not push_btn.value)

    _add = subprocess.run(
        ["git", "add", "-A", "notebooks/"],
        capture_output=True, text=True, cwd=os.path.expanduser("~/repo"),
    )
    _commit = subprocess.run(
        ["git", "commit", "-m", commit_msg.value],
        capture_output=True, text=True, cwd=os.path.expanduser("~/repo"),
    )
    _push = subprocess.run(
        ["git", "push"],
        capture_output=True, text=True, cwd=os.path.expanduser("~/repo"),
    )

    if _push.returncode == 0:
        mo.md(f"**Pushed successfully.**\n```\n{_commit.stdout}{_push.stderr}```")
    else:
        mo.md(f"**Push failed.**\n```\n{_commit.stderr}{_push.stderr}```")
    return


if __name__ == "__main__":
    app.run()
