#!/usr/bin/env python3
"""Gate auto-approval for human-opened backport PRs to maintenance branches."""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request

BRANCH_RE = re.compile(r"^(\d+)\.(\d+)$")
BACKPORTED_FROM_RE = re.compile(r"Backported from #(\d+)", re.IGNORECASE)
BACKPORT_LABEL = "backport"
ACTIVE_MEMBERSHIP = "active"


def patch_label_re(base_branch: str) -> re.Pattern[str] | None:
    """Return the patch-label regex for a maintenance branch, e.g. 9.5 -> v9.5.N."""
    match = BRANCH_RE.match(base_branch or "")
    if not match:
        return None
    return re.compile(rf"^v{match.group(1)}\.{match.group(2)}\.\d+$")


def parse_backported_from_pr(body: str) -> int | None:
    match = BACKPORTED_FROM_RE.search(body or "")
    if not match:
        return None
    return int(match.group(1))


def _api_get(url: str, token: str) -> dict | list | None:
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="GET",
    )
    with urllib.request.urlopen(request) as response:
        body = response.read().decode()
        return json.loads(body) if body else None


def parse_teams(raw: str) -> list[str]:
    """Split a comma separated team slug list, ignoring blanks."""
    return [team.strip() for team in (raw or "").split(",") if team.strip()]


def is_team_member(org: str, team: str, user: str, token: str) -> bool:
    """True if `user` is an active member of `org/team`.

    A 404 means "not a member". Any other API failure (for example a token
    without org read access) propagates so the caller fails closed instead of
    guessing.
    """
    try:
        membership = _api_get(
            f"https://api.github.com/orgs/{org}/teams/{team}/memberships/{user}",
            token,
        )
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise
    return bool(membership) and membership.get("state") == ACTIVE_MEMBERSHIP


def _deny(message: str) -> None:
    print(message)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("approved=false\n")


def _label_names(pull: dict) -> list[str]:
    return [label["name"] for label in pull.get("labels", [])]


def main() -> int:
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]
    title = os.environ.get("PR_TITLE", "")
    body = os.environ.get("PR_BODY", "") or ""
    pr_number = os.environ.get("PR_NUMBER", "").strip()
    base_branch = os.environ.get("BASE_BRANCH", "").strip()
    if not pr_number.isdigit():
        print("PR_NUMBER must be set.", file=sys.stderr)
        return 1

    label_re = patch_label_re(base_branch)
    if label_re is None:
        _deny(f"Base branch {base_branch!r} is not a maintenance branch.")
        return 0

    if not title.startswith(f"[{base_branch}]"):
        _deny(f"Title must start with [{base_branch}].")
        return 0

    try:
        backport_pr = _api_get(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}",
            token,
        )
    except urllib.error.HTTPError as exc:
        print(f"Failed to load backport PR #{pr_number}: {exc.code}", file=sys.stderr)
        return 1

    if not backport_pr:
        _deny(f"Backport PR #{pr_number} not found.")
        return 0

    if BACKPORT_LABEL not in _label_names(backport_pr):
        _deny(f"PR must have the {BACKPORT_LABEL!r} label.")
        return 0

    head_repo = backport_pr.get("head", {}).get("repo")
    if not head_repo or head_repo.get("fork"):
        _deny("Fork PRs (or PRs whose head repository is gone) are not auto-approved.")
        return 0

    author = os.environ.get("PR_AUTHOR", "").strip()
    teams = parse_teams(os.environ.get("ALLOWED_TEAMS", ""))
    if not author or not teams:
        _deny("PR author and ALLOWED_TEAMS must be set; refusing to auto-approve.")
        return 0
    org = repo.split("/", 1)[0]
    team_token = os.environ.get("TEAM_TOKEN") or token
    try:
        is_member = any(is_team_member(org, team, author, team_token) for team in teams)
    except urllib.error.HTTPError as exc:
        print(
            f"Cannot verify team membership of {author} ({exc.code}); "
            "refusing to auto-approve.",
            file=sys.stderr,
        )
        return 1
    if not is_member:
        _deny(f"Author {author} is not a member of: {', '.join(teams)}.")
        return 0

    source_number = parse_backported_from_pr(body)
    if source_number is None:
        _deny("Body must include: Backported from #<main-pr>")
        return 0

    try:
        source = _api_get(
            f"https://api.github.com/repos/{repo}/pulls/{source_number}",
            token,
        )
    except urllib.error.HTTPError as exc:
        print(f"Failed to load source PR #{source_number}: {exc.code}", file=sys.stderr)
        return 1

    if not source or not source.get("merged"):
        _deny(f"Source PR #{source_number} must be merged.")
        return 0

    if source.get("base", {}).get("ref") != "main":
        _deny(f"Source PR #{source_number} must target main.")
        return 0

    if not any(label_re.match(label) for label in _label_names(source)):
        _deny(
            f"Source PR #{source_number} must have a {base_branch} patch label "
            f"(v{base_branch}.x)."
        )
        return 0

    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("approved=true\n")
        out.write(f"source_pr={source_number}\n")
    print(f"Validation passed for backport PR #{pr_number} (source #{source_number}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
