#!/usr/bin/env python3
"""Update .buildkite/pipeline.yml's DRA publishing condition to include a new branch.

The DRA publishing group has an `if:` condition listing the branches that should
produce artifacts (the line marked with `# Add new maintenance branches here`).
When a new release branch is created (e.g. during a minor bump), it must be
added to that list so DRA publishing runs on the branch's pushes.

This script performs that update idempotently: if the branch is already listed,
it makes no change. It fails clearly if the expected structure of the condition
line isn't found -- the inline `if:` is located by matching both the `ci:packaging`
token and the trailing `# Add new maintenance branches here` comment, so it won't
rewrite any other `if:` line that happens to mention `ci:packaging`.

Intended to be invoked from version-bump.sh; kept in a dedicated file so the
logic can be unit tested without a shell.
"""
import sys

ANCHOR_COMMENT = "# Add new maintenance branches here"
DRA_MARKER = "ci:packaging"
PR_LABEL_MARKER = "build.pull_request.labels"


def add_branch_to_dra_condition(content, new_branch):
    """Insert ``new_branch`` into the DRA publishing condition line.

    Returns ``(new_content, message)``. ``new_content`` is unchanged if the
    branch is already listed.

    Raises ``ValueError`` if the DRA condition line can't be located or if the
    ``build.pull_request.labels`` insertion anchor is missing from it.
    """
    lines = content.splitlines(True)
    target_idx = None
    for i, line in enumerate(lines):
        if (
            line.strip().startswith("if:")
            and DRA_MARKER in line
            and ANCHOR_COMMENT in line
        ):
            target_idx = i
            break

    if target_idx is None:
        raise ValueError(
            "could not find DRA branch condition in pipeline.yml "
            f"(expected `if:` line containing both {DRA_MARKER!r} and {ANCHOR_COMMENT!r})"
        )

    line = lines[target_idx]

    # YAML escapes the surrounding double-quotes as \"
    escaped = '\\"'
    if f"{escaped}{new_branch}{escaped}" in line:
        return content, f"Branch {new_branch} already in DRA list, skipping"

    if PR_LABEL_MARKER not in line:
        raise ValueError(
            f"could not find {PR_LABEL_MARKER!r} marker in DRA condition line"
        )

    insertion = f"build.branch == {escaped}{new_branch}{escaped} || "
    lines[target_idx] = line.replace(PR_LABEL_MARKER, insertion + PR_LABEL_MARKER)
    return "".join(lines), "Updated DRA branch list successfully"


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: add_branch_to_dra_list.py PIPELINE_YML_PATH NEW_BRANCH",
            file=sys.stderr,
        )
        sys.exit(2)

    pipeline_yml, new_branch = sys.argv[1], sys.argv[2]

    with open(pipeline_yml) as f:
        content = f.read()

    try:
        new_content, message = add_branch_to_dra_condition(content, new_branch)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if new_content != content:
        with open(pipeline_yml, "w") as f:
            f.write(new_content)
    print(message)


if __name__ == "__main__":
    main()
