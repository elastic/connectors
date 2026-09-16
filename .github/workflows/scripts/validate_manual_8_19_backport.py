#!/usr/bin/env python3
"""Gate auto-approval for human-opened 8.19 backport pull requests."""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request

PATCH_LABEL_RE = re.compile(r"^v8\.19\.\d+$")
BACKPORTED_FROM_RE = re.compile(r"Backported from #(\d+)", re.IGNORECASE)


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


def _deny(message: str) -> None:
    print(message)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("approved=false\n")


def main() -> int:
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]
    title = os.environ.get("PR_TITLE", "")
    body = os.environ.get("PR_BODY", "") or ""

    if not title.startswith("[8.19]"):
        _deny("Title must start with [8.19].")
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

    source_labels = [label["name"] for label in source.get("labels", [])]
    if not any(PATCH_LABEL_RE.match(label) for label in source_labels):
        _deny(f"Source PR #{source_number} must have an 8.19 patch label (v8.19.x).")
        return 0

    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("approved=true\n")
        out.write(f"source_pr={source_number}\n")
    print(f"Validation passed for source PR #{source_number}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
