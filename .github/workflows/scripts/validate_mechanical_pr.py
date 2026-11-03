#!/usr/bin/env python3
"""Approve mechanical PRs to main: Wolfi digest bumps, CVE pin bumps, release version bumps.

Runs from the default branch, never from the PR branch. A PR is approved only if
its author, files and exact changes match one category below and CI is green.
Anything else is left to a human, and any error fails closed (no approval).
"""

from __future__ import annotations

import copy
import json
import os
import re
import sys
import tomllib
import urllib.error
import urllib.request
from typing import Any, Callable

from validate_manual_backport import is_team_member, parse_teams

API = "https://api.github.com"
BOT = "entsearchmachine"
REQUIRED_STATUSES = ("CLA", "buildkite/connectors")
TEAM = "team"  # author must be an active member of ALLOWED_TEAMS

ADVISORY_RE = re.compile(r"\b(CVE-\d{4}-\d{4,}|GHSA(-[0-9a-z]{4}){3})\b", re.I)
VERSION_LINE_RE = re.compile(r"^[+-]\d[\w.+-]*\s*$")
WOLFI_LINE_RE = re.compile(r"^[+-]FROM docker\.elastic\.co/wolfi/python:3\.11-dev@sha256:[0-9a-f]{64}\s*$")
NOTICE_NAME_RE = re.compile(r"^(\S+)\n\d[^\n]*$", re.M)  # a package line is followed by its version
REQ_RE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(\[[^\]]*\])?\s*([^;]*?)\s*(;.*)?$")
PIN_RE = re.compile(r"^==\d[\w.+!-]*$")


# ---------------------------------------------------------------- GitHub API


def api(method: str, path: str, token: str, payload: dict | None = None, raw: bool = False) -> Any:
    request = urllib.request.Request(
        f"{API}/repos/{os.environ['GITHUB_REPOSITORY']}/{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw+json" if raw else "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method=method,
    )
    with urllib.request.urlopen(request) as response:
        body = response.read().decode()
    return body if raw else (json.loads(body) if body else None)


# ------------------------------------------------------------------- helpers


def lines_of(patch: str | None) -> list[str]:
    """Added and removed lines of a PR-files patch."""
    return [x for x in (patch or "").splitlines() if x[:1] in "+-" and not x.startswith("@@")]


def balanced(lines: list[str]) -> bool:
    added = sum(1 for x in lines if x[0] == "+")
    return added > 0 and added == len(lines) - added


def norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def check_notice(files: dict[str, dict]) -> str | None:
    """NOTICE.txt may only swap version numbers."""
    for path, item in files.items():
        if path.endswith("NOTICE.txt"):
            lines = lines_of(item.get("patch"))
            if not (balanced(lines) and all(VERSION_LINE_RE.match(x) for x in lines)):
                return f"{path}: only version numbers may change."
    return None


# --------------------------------------------------------- category checks
# Each gets (files by path, title, read(path, ref) -> text) and returns an error or None.


def check_wolfi(files: dict, title: str, read: Callable) -> str | None:
    lines = lines_of(files["Dockerfile.wolfi"].get("patch")) if "Dockerfile.wolfi" in files else []
    if len(lines) != 2 or not balanced(lines) or not all(WOLFI_LINE_RE.match(x) for x in lines):
        return "Dockerfile.wolfi: only the Wolfi image digest may change."
    return check_notice(files)


def check_version(files: dict, title: str, read: Callable) -> str | None:
    old, new = set(), set()
    for path, item in files.items():
        if path.endswith("/VERSION"):
            lines = lines_of(item.get("patch"))
            if len(lines) != 2 or not balanced(lines) or not all(re.match(r"^[+-]\d+\.\d+\.\d+\s*$", x) for x in lines):
                return f"{path}: must be one version replacement."
            for x in lines:
                (new if x[0] == "+" else old).add(x[1:].strip())
    if len(old) != 1 or len(new) != 1 or title != f"Bump version to {next(iter(new))}":
        return "VERSION files must go from one version to the one in the title."
    for path, item in files.items():
        if path.endswith("NOTICE.txt") and any(x[1:].strip() not in old | new for x in lines_of(item.get("patch"))):
            return f"{path}: only the project's own version may change."
    return check_notice(files)


def requirements(data: dict) -> dict[str, dict[tuple, str]]:
    """{group: {(name, extras, marker): version spec}} from a parsed pyproject.toml."""
    project = data["project"]
    groups = {"": project.get("dependencies", [])} | project.get("optional-dependencies", {})
    result: dict[str, dict[tuple, str]] = {}
    for group, items in groups.items():
        result[group] = {}
        for item in items:
            match = REQ_RE.match(item)
            if not match:
                raise ValueError(f"unparsable requirement {item!r}")
            key = (norm(match.group(1)), match.group(2) or "", match.group(4) or "")
            if key in result[group]:
                raise ValueError(f"duplicate requirement {item!r}")
            result[group][key] = match.group(3)
    return result


def without_requirements(data: dict) -> dict:
    rest = copy.deepcopy(data)
    rest["project"].pop("dependencies", None)
    rest["project"].pop("optional-dependencies", None)
    return rest


def check_deps(files: dict, title: str, read: Callable) -> str | None:
    """Only dependency specs may change: new version of a pinned package, a loose
    spec tightened to `==`, or a pin for an installed package that was not listed."""
    pyprojects = [path for path in files if path.endswith("pyproject.toml")]
    if not pyprojects:
        return "A pyproject.toml pin must change."
    title_names = norm(title)
    for path in pyprojects:
        try:
            old, new = (tomllib.loads(read(path, ref)) for ref in ("base", "head"))
            if without_requirements(old) != without_requirements(new):
                return f"{path}: changes outside the dependency lists."
            old_reqs, new_reqs = requirements(old), requirements(new)
        except (ValueError, KeyError, tomllib.TOMLDecodeError) as exc:
            return f"{path}: {exc}"
        if old_reqs.keys() != new_reqs.keys():
            return f"{path}: dependency groups changed."
        installed = {norm(n) for n in NOTICE_NAME_RE.findall(read(path.rsplit("/", 1)[0] + "/NOTICE.txt", "main"))}
        changed = False
        for group, reqs in new_reqs.items():
            old_names = {key[0] for key in old_reqs[group]}
            if any(key not in reqs for key in old_reqs[group]):
                return f"{path}: a dependency was removed or its extras or marker changed."
            for key, spec in reqs.items():
                if old_reqs[group].get(key) == spec:
                    continue
                changed = True
                if not PIN_RE.match(spec):
                    return f"{path}: {key[0]} must be pinned with '=='."
                if not re.search(rf"(?<![a-z0-9-]){re.escape(key[0])}(?![a-z0-9-])", title_names):
                    return f"{path}: {key[0]} is not named in the PR title."
                if key not in old_reqs[group] and (key[1] or key[0] in old_names or key[0] not in installed):
                    return f"{path}: {key[0]} is not a plain pin of an installed, unlisted package."
        if not changed:
            return f"{path}: no dependency changed."
    return check_notice(files)


# --------------------------------------------------------------- categories

CATEGORIES: dict[str, dict] = {
    "renovate-wolfi": {
        "authors": ["elastic-renovate-prod[bot]"],
        "title": r"^chore\(deps\): update docker\.elastic\.co/wolfi/python:3\.11-dev docker digest",
        "files": ["Dockerfile.wolfi", "app/connectors_service/NOTICE.txt", "libs/connectors_sdk/NOTICE.txt"],
        "check": check_wolfi,
    },
    "dependency-bump": {
        "authors": TEAM,
        "title": r"^fix\(deps\): (bump|pin) ",
        "advisory": True,
        "files": [
            "app/connectors_service/pyproject.toml",
            "app/connectors_service/NOTICE.txt",
            "libs/connectors_sdk/pyproject.toml",
            "libs/connectors_sdk/NOTICE.txt",
        ],
        "check": check_deps,
    },
    "release-version-bump": {
        "authors": ["elastic-vault-github-plugin-prod[bot]"],
        "title": r"^Bump version to \d+\.\d+\.\d+$",
        "files": [
            "app/connectors_service/connectors/VERSION",
            "app/connectors_service/NOTICE.txt",
            "libs/connectors_sdk/connectors_sdk/VERSION",
            "libs/connectors_sdk/NOTICE.txt",
        ],
        "check": check_version,
    },
}


def classify(
    *, title: str, body: str, author: str, is_member: Callable[[str], bool], files: list[dict], read: Callable
) -> tuple[str | None, str]:
    """Return (category, reason); category is None when a human must review."""
    if any(item.get("status") != "modified" for item in files):
        return None, "Only modified files qualify (no adds, deletes or renames)."
    by_path = {item["filename"]: item for item in files}
    reasons = []
    for name, rule in CATEGORIES.items():
        if not re.search(rule["title"], title) or not set(by_path) <= set(rule["files"]):
            continue
        if not (is_member(author) if rule["authors"] == TEAM else author in rule["authors"]):
            reasons.append(f"{name}: author {author} is not allowed.")
        elif rule.get("advisory") and not ADVISORY_RE.search(f"{title}\n{body}"):
            reasons.append(f"{name}: no CVE or GHSA id in the title or body.")
        elif error := rule["check"](by_path, title, read):
            reasons.append(f"{name}: {error}")
        else:
            return name, "Matches the category."
    return None, " ".join(reasons) or "No mechanical category matched."


# ------------------------------------------------------------- orchestration


def ci_is_green(statuses: list[dict]) -> bool:
    state = {item["context"]: item["state"] for item in statuses}
    return all(state.get(context) == "success" for context in REQUIRED_STATUSES)


def bot_approvals(number: int, token: str) -> list[dict]:
    reviews, page = [], 1
    while True:
        chunk = api("GET", f"pulls/{number}/reviews?per_page=100&page={page}", token)
        reviews += chunk
        if len(chunk) < 100:
            return [r for r in reviews if r["user"]["login"] == BOT and r["state"] == "APPROVED"]
        page += 1


def qualifying_category(pull: dict, token: str, team_token: str) -> str | None:
    """The category this PR matches (ignoring CI), or None."""
    repo = os.environ["GITHUB_REPOSITORY"]
    head_repo = pull["head"].get("repo")
    if pull["base"]["ref"] != "main" or pull.get("draft") or not head_repo or head_repo["full_name"] != repo:
        return None
    number, head = pull["number"], pull["head"]["sha"]
    files = api("GET", f"pulls/{number}/files?per_page=100", token)
    if len(files) >= 100:
        return None
    # "base" is the merge base (what the PR diff is against); "main" is the current tip.
    refs = {
        "head": head,
        "base": api("GET", f"compare/main...{head}?per_page=1", token)["merge_base_commit"]["sha"],
        "main": "main",
    }

    def read(path: str, ref: str) -> str:
        return api("GET", f"contents/{path}?ref={refs[ref]}", token, raw=True)

    teams = parse_teams(os.environ.get("ALLOWED_TEAMS", ""))
    org = repo.split("/", 1)[0]
    category, reason = classify(
        title=pull["title"] or "",
        body=pull.get("body") or "",
        author=pull["user"]["login"],
        is_member=lambda user: any(is_team_member(org, team, user, team_token) for team in teams),
        files=files,
        read=read,
    )
    print(f"{category or 'human review'}: {reason}")
    return category


def main() -> int:
    token = os.environ["GITHUB_TOKEN"]
    team_token = os.environ["TEAM_TOKEN"]  # PAT of the approver: reads teams, writes reviews
    number = os.environ.get("PR_NUMBER", "")
    if not number:  # status event: find the open PR whose head is that commit
        sha = os.environ["STATUS_SHA"]
        pulls = api("GET", f"commits/{sha}/pulls", token)
        number = next(
            (str(p["number"]) for p in pulls if p["state"] == "open" and p["base"]["ref"] == "main" and p["head"]["sha"] == sha),
            "",
        )
        if not number:
            print("No open PR with this head commit.")
            return 0

    try:
        pull = api("GET", f"pulls/{number}", token)
        if pull["base"]["ref"] != "main":  # backport approvals belong to approve-manual-backport.yml
            print("Not a PR to main; nothing to do.")
            return 0
        head = pull["head"]["sha"]
        category = qualifying_category(pull, token, team_token)
        if not category:
            # No longer a mechanical PR (e.g. a new commit added other files): drop our approval.
            for review in bot_approvals(int(number), token):
                api("PUT", f"pulls/{number}/reviews/{review['id']}/dismissals", team_token,
                    {"message": "PR no longer qualifies for mechanical auto-approval."})
            return 0
        statuses = api("GET", f"commits/{head}/status", token)["statuses"]
        if not ci_is_green(statuses):
            print("Waiting for CI; a later status event re-checks.")
            return 0
        if api("GET", f"pulls/{number}", token)["head"]["sha"] != head:
            print("The PR changed while it was being checked; the new commit will be checked on its own.")
            return 0
        if any(r["commit_id"] == head for r in bot_approvals(int(number), token)):
            print("Already approved.")
            return 0
        api("POST", f"pulls/{number}/reviews", team_token, {"event": "APPROVE", "commit_id": head})
        print(f"Approved as {category} at {head[:7]}.")
    except urllib.error.HTTPError as exc:
        print(f"GitHub API error {exc.code} for {exc.url}", file=sys.stderr)
        return 1  # fail closed
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
