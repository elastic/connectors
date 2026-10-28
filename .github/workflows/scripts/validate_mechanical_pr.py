#!/usr/bin/env python3
"""Decide whether a mechanical pull request to main may be auto-approved.

Runs from the default branch (never from the PR branch). A PR qualifies only if
it matches one category in .github/auto-approve.yml and the PR diff passes that
category's strict line-by-line check. Everything fails closed.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from validate_manual_backport import is_team_member, parse_teams

API = "https://api.github.com"
SELF_CHECK_NAME = "Mechanical auto-approve"
ReadBase = Callable[[str], "str | None"]
GOOD_CONCLUSIONS = ("success", "skipped", "neutral")

ADVISORY_RE = re.compile(r"\b(CVE-\d{4}-\d{4,}|GHSA-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4})\b", re.I)
REMOVED_DEP_RE = re.compile(r'^-\s+"([A-Za-z0-9_.\-\[\]]+)([^"]*)",?\s*$')
PIN_LINE_RE = re.compile(r'^([+-])\s+"([A-Za-z0-9_.\-\[\]]+)==([^"\s]+)",?\s*$')
NOTICE_LINE_RE = re.compile(r"^([+-])(\d+(?:\.\d+)*)\s*$")
VERSION_LINE_RE = re.compile(r"^([+-])(\d+\.\d+\.\d+)\s*$")
WOLFI_FROM_RE = re.compile(
    r"^([+-])FROM docker\.elastic\.co/wolfi/python:3\.11-dev@sha256:[0-9a-f]{64}\s*$"
)
RENOVATE_LABEL_RE = re.compile(r'^[+-]\s+"v\d+\.\d+\.\d+",?\s*$')
BACKPORTRC_RES = [
    re.compile(r'^[+-]\s*"\d+\.\d+",?\s*$'),
    re.compile(
        r'^[+-]\s*"(maintenanceBranches|renovateVersionLabels)": '
        r'\[("v?\d+\.\d+(\.\d+)?"(, )?)*\],?\s*$'
    ),
    re.compile(r'^[+-]\s*"\^v\d+\.\d+\.\d+\$": "main",?\s*$'),
]
CATALOG_RES = [
    re.compile(r"^[+-]\s+Daily [\w.]+:\s*$"),
    re.compile(r"^[+-]\s+branch: ['\"]?(main|\d+\.\d+)['\"]?\s*$"),
    re.compile(r"^[+-]\s+cronline: '@daily'\s*$"),
    re.compile(r'^[+-]\s+message: "[^"\n]*"\s*$'),
]
DRA_MARKER = "# Add new maintenance branches here"
DRA_BRANCH_RE = re.compile(r'build\.branch == \\"(\d+\.\d+)\\"')


@dataclass
class Result:
    category: str = ""
    approved: bool = False
    qualifies: bool = False  # diff and author match a category (CI not considered)
    reason: str = ""
    head_sha: str = ""


# --------------------------------------------------------------------------
# GitHub API helpers
# --------------------------------------------------------------------------


def _request(method: str, url: str, token: str, payload: dict | None = None) -> Any:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method=method,
    )
    with urllib.request.urlopen(request) as response:
        body = response.read().decode()
        return json.loads(body) if body else None


def _get(url: str, token: str) -> Any:
    return _request("GET", url, token)


def read_base_file(repo: str, path: str, token: str) -> str | None:
    """Content of `path` on main, or None if it does not exist."""
    request = urllib.request.Request(
        f"{API}/repos/{repo}/contents/{path}?ref=main",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.read().decode()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------


def load_config() -> dict[str, Any]:
    import yaml

    path = Path(__file__).resolve().parents[2] / "auto-approve.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("categories"), dict):
        raise RuntimeError(f"Invalid config: {path}")
    return data


# --------------------------------------------------------------------------
# Diff helpers
# --------------------------------------------------------------------------


def changed_lines(patch: str | None) -> list[str]:
    """Added and removed lines of a PR-files API patch (no file headers there)."""
    return [
        line
        for line in (patch or "").splitlines()
        if line[:1] in "+-" and not line.startswith("@@")
    ]


def _norm_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name.split("[", 1)[0]).lower()


def _balanced(lines: list[str]) -> bool:
    plus = sum(1 for line in lines if line[0] == "+")
    return plus > 0 and plus == len(lines) - plus


def _file(files: list[dict], name: str) -> dict | None:
    return next((item for item in files if item["filename"] == name), None)


# --------------------------------------------------------------------------
# Per-category diff checks. Each returns None when OK, else a reason.
# --------------------------------------------------------------------------


def _check_notice(files: list[dict]) -> str | None:
    for item in files:
        if item["filename"].endswith("NOTICE.txt"):
            lines = changed_lines(item.get("patch"))
            if not lines or not all(NOTICE_LINE_RE.match(line) for line in lines):
                return f"{item['filename']}: only version-number lines may change."
            if not _balanced(lines):
                return f"{item['filename']}: every removed version needs an added one."
    return None


def check_renovate_wolfi(files: list[dict], title: str, read_base: ReadBase) -> str | None:
    dockerfile = _file(files, "Dockerfile.wolfi")
    if not dockerfile:
        return "Dockerfile.wolfi must change."
    lines = changed_lines(dockerfile.get("patch"))
    if len(lines) != 2 or not all(WOLFI_FROM_RE.match(line) for line in lines) or not _balanced(lines):
        return "Dockerfile.wolfi: only the pinned Wolfi image digest may change."
    return _check_notice(files)


def check_dependency_bump(files: list[dict], title: str, read_base: ReadBase) -> str | None:
    """Pins may change version, tighten an existing spec to `==`, or pin a package
    that is already installed (listed in the NOTICE.txt on main) but not yet pinned."""
    pyprojects = [item for item in files if item["filename"].endswith("pyproject.toml")]
    if not pyprojects:
        return "A pyproject.toml pin must change."
    normalized_title = re.sub(r"[-_.]+", "-", title).lower()
    for item in pyprojects:
        filename = item["filename"]
        removed: dict[str, str] = {}
        added: dict[str, str] = {}
        lines = changed_lines(item.get("patch"))
        if not lines:
            return f"{filename}: no pin change found."
        for line in lines:
            if line[0] == "+":
                match, target = PIN_LINE_RE.match(line), added
                name, value = (match.group(2), match.group(3)) if match else ("", "")
            else:
                match, target = REMOVED_DEP_RE.match(line), removed
                name, value = (match.group(1), match.group(2)) if match else ("", "")
            if not match:
                return f"{filename}: only dependency lines may change, and added ones must be '\"pkg==version\"'."
            name = _norm_name(name)
            if name in target:
                return f"{filename}: package {name} appears twice."
            target[name] = value
        if not set(removed) <= set(added):
            return f"{filename}: a dependency may not be removed."
        installed: set[str] | None = None
        for name, version in added.items():
            if name not in normalized_title:
                return f"{filename}: {name} is not named in the PR title."
            if name in removed:
                if removed[name] == f"=={version}":
                    return f"{filename}: {name} version is unchanged."
                continue
            if installed is None:
                notice = read_base(filename.rsplit("/", 1)[0] + "/NOTICE.txt") or ""
                installed = {_norm_name(line.strip()) for line in notice.splitlines() if line.strip()}
            if name not in installed:
                return f"{filename}: {name} is not an installed package (not in NOTICE.txt on main)."
    return _check_notice(files)


def check_release_version(files: list[dict], title: str, read_base: ReadBase) -> str | None:
    old_versions: set[str] = set()
    new_versions: set[str] = set()
    for item in files:
        if item["filename"].endswith("/VERSION"):
            lines = changed_lines(item.get("patch"))
            if len(lines) != 2 or not all(VERSION_LINE_RE.match(line) for line in lines) or not _balanced(lines):
                return f"{item['filename']}: must be a single version replacement."
            for line in lines:
                (new_versions if line[0] == "+" else old_versions).add(line[1:].strip())
    if len(new_versions) != 1 or len(old_versions) != 1:
        return "All VERSION files must move from one version to one version."
    new_version = next(iter(new_versions))
    if not title.endswith(f"Bump version to {new_version}"):
        return "Title version does not match the VERSION files."
    error = _check_notice(files)
    if error:
        return error
    allowed = old_versions | new_versions
    for item in files:
        if item["filename"].endswith("NOTICE.txt"):
            for line in changed_lines(item.get("patch")):
                if line[1:].strip() not in allowed:
                    return f"{item['filename']}: only the project's own version may change."
    return None


def check_release_config(files: list[dict], title: str, read_base: ReadBase) -> str | None:
    rules: dict[str, list[re.Pattern[str]]] = {
        "renovate.json": [RENOVATE_LABEL_RE],
        ".backportrc.json": BACKPORTRC_RES,
        "catalog-info.yaml": CATALOG_RES,
    }
    for item in files:
        lines = changed_lines(item.get("patch"))
        patterns = rules[item["filename"]]
        if not lines or not all(any(p.match(line) for p in patterns) for line in lines):
            return f"{item['filename']}: contains changes outside the allowed release-config lines."
    return None


def check_release_dra(files: list[dict], title: str, read_base: ReadBase) -> str | None:
    lines = changed_lines(files[0].get("patch")) if len(files) == 1 else []
    if len(lines) != 2 or not _balanced(lines):
        return "pipeline.yml: exactly one line may change."
    if not all(DRA_MARKER in line and 'if: "(build.branch == \\"main\\"' in line for line in lines):
        return "pipeline.yml: only the maintenance-branch list line may change."
    old_line = next(line for line in lines if line[0] == "-")[1:]
    new_line = next(line for line in lines if line[0] == "+")[1:]
    old = set(DRA_BRANCH_RE.findall(old_line))
    new = set(DRA_BRANCH_RE.findall(new_line))
    if not old < new or len(new - old) != 1:
        return "pipeline.yml: exactly one branch may be added."
    branch = next(iter(new - old))
    if title != f"Add {branch} to DRA branch list":
        return "Title branch does not match the added branch."
    # The new line must be the old line plus one `|| build.branch == "X"` term.
    term = f' || build.branch == \\"{branch}\\"'
    if new_line.count(term) != 1 or new_line.replace(term, "", 1) != old_line:
        return "pipeline.yml: the line may only gain the new branch term."
    return None


CHECKS: dict[str, Callable[[list[dict], str, ReadBase], str | None]] = {
    "renovate-wolfi": check_renovate_wolfi,
    "dependency-bump": check_dependency_bump,
    "release-version-bump": check_release_version,
    "release-config-bump": check_release_config,
    "release-dra-branch": check_release_dra,
}


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


def classify(
    config: dict[str, Any],
    *,
    title: str,
    body: str,
    author: str,
    is_member: Callable[[str], bool],
    files: list[dict],
    read_base: ReadBase,
) -> tuple[str | None, str]:
    """Return (category, reason). category is None when a human must review."""
    for item in files:
        if item.get("status") != "modified":
            return None, f"{item['filename']} is {item.get('status')}; only modified files qualify."
        if any(item["filename"].startswith(p) for p in config.get("forbidden_path_prefixes", [])):
            return None, f"{item['filename']} is in a forbidden path."

    paths = [item["filename"] for item in files]
    reasons: list[str] = []
    for name, category in config["categories"].items():
        if name not in CHECKS or not re.search(category["title_regex"], title):
            continue
        if not all(any(fnmatch.fnmatchcase(p, g) for g in category["allowed_files"]) for p in paths):
            reasons.append(f"{name}: files outside the allowed list.")
            continue
        authors = category["authors"]
        if authors == "team":
            if not is_member(author):
                reasons.append(f"{name}: author {author} is not in an allowed team.")
                continue
        elif author not in authors:
            reasons.append(f"{name}: author {author} is not allowed.")
            continue
        if category.get("require_advisory") and not ADVISORY_RE.search(f"{title}\n{body}"):
            reasons.append(f"{name}: no CVE or GHSA id in the title or body.")
            continue
        error = CHECKS[name](files, title, read_base)
        if error:
            reasons.append(f"{name}: {error}")
            continue
        return name, "Matches the category."
    return None, " ".join(reasons) or "No mechanical category matched."


def ci_is_green(repo: str, sha: str, token: str, contexts: list[str]) -> tuple[bool, str]:
    try:
        status = _get(f"{API}/repos/{repo}/commits/{sha}/status", token)
        runs = _get(f"{API}/repos/{repo}/commits/{sha}/check-runs?per_page=100", token)
    except urllib.error.HTTPError as exc:
        return False, f"Could not read CI state ({exc.code})."
    states = {item["context"]: item["state"] for item in status.get("statuses", [])}
    pending = [c for c in contexts if states.get(c) != "success"]
    if pending:
        return False, f"Waiting for statuses: {', '.join(pending)}."
    for run in runs.get("check_runs", []):
        if run.get("name") == SELF_CHECK_NAME:
            continue
        if run.get("status") != "completed":
            return False, f"Check {run.get('name')!r} is still running."
        if run.get("conclusion") not in GOOD_CONCLUSIONS:
            return False, f"Check {run.get('name')!r} is {run.get('conclusion')!r}."
    return True, "CI is green."


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def evaluate(pull: dict, config: dict[str, Any], repo: str, token: str, team_token: str) -> Result:
    sha = pull["head"]["sha"]
    result = Result(head_sha=sha)

    if pull["base"]["ref"] != "main":
        result.reason = "Only PRs to main are eligible."
        return result
    if pull.get("draft"):
        result.reason = "Draft PRs are not eligible."
        return result
    head_repo = pull["head"].get("repo")
    if not head_repo or head_repo.get("full_name") != repo:
        result.reason = "PRs from forks (or deleted forks) are not eligible."
        return result

    number = pull["number"]
    files = _get(f"{API}/repos/{repo}/pulls/{number}/files?per_page=100", token)
    if not isinstance(files, list) or len(files) >= 100:
        result.reason = "Could not load a complete file list."
        return result

    org = repo.split("/", 1)[0]
    teams = parse_teams(os.environ.get("ALLOWED_TEAMS", ""))
    category, reason = classify(
        config,
        title=pull.get("title") or "",
        body=pull.get("body") or "",
        author=pull["user"]["login"],
        is_member=lambda user: any(is_team_member(org, team, user, team_token) for team in teams),
        files=files,
        read_base=lambda path: read_base_file(repo, path, token),
    )
    result.reason = reason
    if not category:
        return result

    result.category = category
    result.qualifies = True
    label = config.get("label")
    if label and label not in [item["name"] for item in pull.get("labels", [])]:
        _request("POST", f"{API}/repos/{repo}/issues/{number}/labels", token, {"labels": [label]})

    green, ci_reason = ci_is_green(repo, sha, token, config.get("required_status_contexts", []))
    if not green:
        result.reason = ci_reason
        return result

    result.approved = True
    result.reason = f"Validated as `{category}`; approving."
    return result


def _output(**fields: str) -> None:
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as out:
        for key, value in fields.items():
            out.write(f"{key}={' '.join(str(value).split())}\n")


def main() -> int:
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]
    team_token = os.environ.get("TEAM_TOKEN") or token
    number = os.environ.get("PR_NUMBER", "").strip()
    if not number.isdigit():
        print("PR_NUMBER must be set.", file=sys.stderr)
        return 1

    config = load_config()
    try:
        pull = _get(f"{API}/repos/{repo}/pulls/{number}", token)
        result = evaluate(pull, config, repo, token, team_token)
    except urllib.error.HTTPError as exc:
        # Fail closed: no output means no approval step runs.
        print(f"GitHub API error {exc.code} for {exc.url}", file=sys.stderr)
        return 1

    print(result.reason)
    _output(
        approved="true" if result.approved else "false",
        qualifies="true" if result.qualifies else "false",
        category=result.category,
        head_sha=result.head_sha,
        reason=result.reason,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
