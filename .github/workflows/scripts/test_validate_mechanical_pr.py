#!/usr/bin/env python3

import os
import unittest
from unittest import mock

import validate_mechanical_pr as v

SHA_A, SHA_B = "a" * 64, "b" * 64
FROM = "FROM docker.elastic.co/wolfi/python:3.11-dev@sha256:"
PYPROJECT = "app/connectors_service/pyproject.toml"
NOTICE = "app/connectors_service/NOTICE.txt"
BASE_NOTICE = "graphql-core\n3.2.3\nMIT\nurllib3\n2.0.0\nMIT\nMIT License\n"
BASE_TOML = """
[project]
name = "x"
dependencies = [
    "graphql-core==3.2.3",
    "pymongo==4.0; platform_machine=='x86_64'",
    "pymongo[srv]==4.0; platform_machine=='aarch64'",
    "elasticsearch[async]==8.0",
    "loose>=1",
]
[project.optional-dependencies]
tests = ["graphql-core==3.2.3"]
[tool.ruff]
line-length = 100
"""


def modified(name, *lines):
    return {"filename": name, "status": "modified", "patch": "@@ -1 +1 @@\n" + "\n".join(lines)}


def toml(*replacements, extra_dep=None):
    text = BASE_TOML
    for old, new in replacements:
        assert old in text, old
        text = text.replace(old, new)
    return text.replace("    \"loose>=1\",\n", f"    \"loose>=1\",\n{extra_dep}" if extra_dep else "    \"loose>=1\",\n")


def classify(title, files, author="dev", body="", member=True, head_toml=BASE_TOML, notice=BASE_NOTICE):
    def read(path, ref):
        if path.endswith("pyproject.toml"):
            return head_toml if ref == "head" else BASE_TOML
        return notice

    return v.classify(title=title, body=body, author=author, is_member=lambda u: member, files=files, read=read)


class TestWolfi(unittest.TestCase):
    TITLE = "chore(deps): update docker.elastic.co/wolfi/python:3.11-dev docker digest to abc"
    BOT = "elastic-renovate-prod[bot]"

    def files(self, *extra):
        return [modified("Dockerfile.wolfi", f"-{FROM}{SHA_A}", f"+{FROM}{SHA_B}"), modified(NOTICE, "-2026.5", "+2026.6"), *extra]

    def test_accepts_digest_swap(self):
        self.assertEqual(classify(self.TITLE, self.files(), self.BOT)[0], "renovate-wolfi")

    def test_rejects_wrong_author_and_cli_style_name(self):
        for author in ("dev", "app/elastic-renovate-prod"):
            self.assertIsNone(classify(self.TITLE, self.files(), author)[0])

    def test_rejects_other_dockerfile_change(self):
        files = [modified("Dockerfile.wolfi", "-USER root", "+USER evil")]
        self.assertIsNone(classify(self.TITLE, files, self.BOT)[0])

    def test_rejects_added_line_and_notice_text(self):
        self.assertIsNone(classify(self.TITLE, [modified("Dockerfile.wolfi", f"+{FROM}{SHA_B}")], self.BOT)[0])
        files = self.files()[:1] + [modified(NOTICE, "-MIT", "+evil")]
        self.assertIsNone(classify(self.TITLE, files, self.BOT)[0])


class TestVersionBump(unittest.TestCase):
    BOT = "elastic-vault-github-plugin-prod[bot]"

    def files(self, new="9.7.0", notice_new="9.7.0"):
        return [
            modified("app/connectors_service/connectors/VERSION", "-9.6.0", f"+{new}"),
            modified(NOTICE, "-9.6.0", f"+{notice_new}"),
        ]

    def test_accepts(self):
        self.assertEqual(classify("Bump version to 9.7.0", self.files(), self.BOT)[0], "release-version-bump")

    def test_rejects_title_author_and_foreign_notice_version(self):
        self.assertIsNone(classify("Bump version to 9.8.0", self.files(), self.BOT)[0])
        self.assertIsNone(classify("Bump version to 9.7.0", self.files(), "dev")[0])
        self.assertIsNone(classify("Bump version to 9.7.0", self.files(notice_new="6.6.6"), self.BOT)[0])


class TestDependencyBump(unittest.TestCase):
    TITLE = "fix(deps): bump graphql-core to 3.2.12 for CVE-2026-75508"

    def run_case(self, head_toml, title=TITLE, **kwargs):
        files = [modified(PYPROJECT, "-x", "+y")]
        return classify(title, files, head_toml=head_toml, **kwargs)[0]

    def test_accepts_version_change_in_every_group(self):
        head = BASE_TOML.replace("graphql-core==3.2.3", "graphql-core==3.2.12")  # runtime and tests
        self.assertEqual(self.run_case(head), "dependency-bump")

    def test_accepts_one_group_only(self):
        head = BASE_TOML.replace('tests = ["graphql-core==3.2.3"]', 'tests = ["graphql-core==3.2.12"]')
        self.assertEqual(self.run_case(head), "dependency-bump")

    def test_accepts_loose_spec_tightened_and_notice_swap(self):
        title = "fix(deps): pin loose to 1.5 for GHSA-6q48-6c7w-9gw2"
        files = [modified(PYPROJECT, "-x", "+y"), modified(NOTICE, "-1.0", "+1.5")]
        head = BASE_TOML.replace("loose>=1", "loose==1.5")
        self.assertEqual(classify(title, files, head_toml=head)[0], "dependency-bump")

    def test_accepts_marker_pins_with_same_name(self):
        title = "fix(deps): bump pymongo to 4.1 for CVE-2026-1234"
        head = BASE_TOML.replace("pymongo==4.0", "pymongo==4.1").replace("pymongo[srv]==4.0", "pymongo[srv]==4.1")
        self.assertEqual(self.run_case(head, title), "dependency-bump")

    def test_accepts_pin_of_installed_unlisted_package(self):
        title = "fix(deps): pin urllib3 to 2.7.0 for CVE-2026-9375"
        head = BASE_TOML.replace('"loose>=1",', '"loose>=1",\n    "urllib3==2.7.0",')
        self.assertEqual(self.run_case(head, title), "dependency-bump")

    def test_rejects_extras_on_a_new_pin(self):
        title = "fix(deps): pin urllib3 to 2.7.0 for CVE-2026-9375"
        head = BASE_TOML.replace('"loose>=1",', '"loose>=1",\n    "urllib3[socks]==2.7.0",')
        self.assertIsNone(self.run_case(head, title))

    def test_title_must_name_the_whole_package(self):
        head = BASE_TOML.replace("loose>=1", "loose==1.5")
        self.assertIsNone(self.run_case(head, "fix(deps): bump loosely to 1.5 for CVE-2026-1234"))

    def test_rejects_package_that_is_not_installed(self):
        title = "fix(deps): pin evil to 1.0 for CVE-2026-9375"
        head = BASE_TOML.replace('"loose>=1",', '"loose>=1",\n    "evil==1.0",')
        self.assertIsNone(self.run_case(head, title))

    def test_license_text_in_notice_is_not_a_package(self):
        title = "fix(deps): pin MIT to 1.0 for CVE-2026-9375"
        head = BASE_TOML.replace('"loose>=1",', '"loose>=1",\n    "MIT==1.0",')
        self.assertIsNone(self.run_case(head, title))

    def test_rejects_unlisted_pin_for_already_listed_name(self):
        title = "fix(deps): pin graphql-core to 3.2.12 for CVE-2026-9375"
        head = BASE_TOML.replace('"loose>=1",', '"loose>=1",\n    "graphql-core==3.2.12; sys_platform==\'x\'",')
        self.assertIsNone(self.run_case(head, title))

    def test_rejects_moving_a_dependency_between_groups(self):
        head = BASE_TOML.replace('tests = ["graphql-core==3.2.3"]', "tests = []").replace(
            '"loose>=1",', '"loose>=1",\n    "graphql-core==3.2.12",'
        ).replace('    "graphql-core==3.2.3",\n', "")
        self.assertIsNone(self.run_case(head))

    def test_rejects_dropped_extras_marker_or_dependency(self):
        for old, new in (
            ("elasticsearch[async]==8.0", "elasticsearch==8.1"),
            ("pymongo==4.0; platform_machine=='x86_64'", "pymongo==4.1"),
            ('    "loose>=1",\n', ""),
        ):
            title = "fix(deps): bump elasticsearch pymongo loose to 8.1 for CVE-2026-1234"
            self.assertIsNone(self.run_case(BASE_TOML.replace(old, new), title), old)

    def test_rejects_changes_outside_dependency_lists(self):
        head = BASE_TOML.replace("graphql-core==3.2.3", "graphql-core==3.2.12").replace("line-length = 100", "line-length = 1")
        self.assertIsNone(self.run_case(head))

    def test_rejects_non_pin_and_missing_title_name_and_no_change(self):
        self.assertIsNone(self.run_case(BASE_TOML.replace("graphql-core==3.2.3", "graphql-core>=3.2.12")))
        self.assertIsNone(self.run_case(BASE_TOML.replace("loose>=1", "loose==2"), self.TITLE))
        self.assertIsNone(self.run_case(BASE_TOML))

    def test_requires_advisory_and_team_member(self):
        head = BASE_TOML.replace("graphql-core==3.2.3", "graphql-core==3.2.12")
        self.assertIsNone(self.run_case(head, "fix(deps): bump graphql-core to 3.2.12"))
        self.assertIsNone(self.run_case(head, member=False))
        self.assertEqual(
            self.run_case(head, "fix(deps): bump graphql-core to 3.2.12", body="GHSA-6q48-6c7w-9gw2"), "dependency-bump"
        )

    def test_rejects_unrelated_file(self):
        files = [modified(PYPROJECT, "-x", "+y"), modified("app/connectors_service/connectors/x.py", "-a", "+b")]
        head = BASE_TOML.replace("graphql-core==3.2.3", "graphql-core==3.2.12")
        self.assertIsNone(classify(self.TITLE, files, head_toml=head)[0])


class TestGlobalRules(unittest.TestCase):
    def test_only_modified_files_with_a_patch_qualify(self):
        title = TestWolfi.TITLE
        for status in ("added", "removed", "renamed"):
            files = TestWolfi().files()
            files[0]["status"] = status
            self.assertIsNone(classify(title, files, TestWolfi.BOT)[0])
        files = [{"filename": "Dockerfile.wolfi", "status": "modified", "patch": None}]
        self.assertIsNone(classify(title, files, TestWolfi.BOT)[0])

    def test_workflow_files_never_qualify(self):
        files = TestWolfi().files(modified(".github/workflows/x.yml", "-a", "+b"))
        self.assertIsNone(classify(TestWolfi.TITLE, files, TestWolfi.BOT)[0])


class TestCi(unittest.TestCase):
    def test_needs_both_statuses_green(self):
        ok = [{"context": "CLA", "state": "success"}, {"context": "buildkite/connectors", "state": "success"}]
        self.assertTrue(v.ci_is_green(ok))
        self.assertFalse(v.ci_is_green(ok[:1]))
        self.assertFalse(v.ci_is_green([ok[0], {"context": "buildkite/connectors", "state": "failure"}]))


class TestMain(unittest.TestCase):
    """The approve and withdraw flow with the GitHub API mocked."""

    GREEN = {"statuses": [{"context": c, "state": "success"} for c in v.REQUIRED_STATUSES]}

    def run_main(self, category="renovate-wolfi", statuses=GREEN, heads=("h1", "h1"), reviews=(), base="main"):
        calls = []
        pulls = iter({"number": 7, "base": {"ref": base}, "head": {"sha": h}} for h in heads)

        def api(method, path, token, payload=None, raw=False):
            calls.append((method, path))
            if path == "pulls/7":
                return next(pulls)
            if path.startswith("commits/") and path.endswith("/pulls"):
                return [{"number": 7, "state": "open", "base": {"ref": "main"}, "head": {"sha": "h1"}}]
            if path.startswith("pulls/7/reviews?"):
                return list(reviews)
            if path.endswith("/status"):
                return statuses

        env = {"GITHUB_TOKEN": "t", "TEAM_TOKEN": "p", "PR_NUMBER": "7", "GITHUB_REPOSITORY": "o/r"}
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(v, "api", api), mock.patch.object(
            v, "qualifying_category", lambda *a: category
        ):
            v.main()
        return [c for c in calls if c[0] in ("POST", "PUT")]

    def bot_review(self, commit="old"):
        return {"id": 5, "commit_id": commit, "state": "APPROVED", "user": {"login": v.BOT}}

    def test_approves_when_qualifying_and_green(self):
        self.assertEqual(self.run_main(), [("POST", "pulls/7/reviews")])

    def test_red_or_pending_ci_does_not_approve_or_withdraw(self):
        self.assertEqual(self.run_main(statuses={"statuses": []}, reviews=[self.bot_review()]), [])

    def test_head_moved_while_checking_does_not_approve(self):
        self.assertEqual(self.run_main(heads=("h1", "h2")), [])

    def test_already_approved_at_this_commit(self):
        self.assertEqual(self.run_main(reviews=[self.bot_review("h1")]), [])

    def test_stops_qualifying_withdraws_bot_approval(self):
        self.assertEqual(self.run_main(category=None, reviews=[self.bot_review()]), [("PUT", "pulls/7/reviews/5/dismissals")])

    def test_backport_prs_are_never_touched(self):
        # approve-manual-backport.yml owns approvals on maintenance branches
        self.assertEqual(self.run_main(category=None, reviews=[self.bot_review()], base="9.5"), [])

    def test_finds_every_review_page(self):
        pages = [[{**self.bot_review(), "user": {"login": "human"}}] * 100, [self.bot_review()]]
        calls = []

        def api(method, path, token, payload=None, raw=False):
            calls.append(path)
            return pages[int(path.rsplit("=", 1)[1]) - 1]

        with mock.patch.object(v, "api", api):
            self.assertEqual(len(v.bot_approvals(7, "t")), 1)
        self.assertEqual(len(calls), 2)

    def test_other_reviewers_are_left_alone(self):
        other = {**self.bot_review(), "user": {"login": "human"}}
        self.assertEqual(self.run_main(category=None, reviews=[other]), [])


class TestQualifyingCategory(unittest.TestCase):
    def pull(self, **overrides):
        pull = {
            "number": 7,
            "title": TestWolfi.TITLE,
            "draft": False,
            "user": {"login": TestWolfi.BOT},
            "base": {"ref": "main", "sha": "b"},
            "head": {"sha": "h", "repo": {"full_name": "o/r"}},
        }
        return pull | overrides

    def run_case(self, pull, files=None):
        files = TestWolfi().files() if files is None else files
        with mock.patch.dict(os.environ, {"GITHUB_REPOSITORY": "o/r"}, clear=True), mock.patch.object(
            v, "api", lambda m, path, *a, **k: {"merge_base_commit": {"sha": "mb"}} if path.startswith("compare/") else files
        ):
            return v.qualifying_category(pull, "t", "p")

    def test_matches(self):
        self.assertEqual(self.run_case(self.pull()), "renovate-wolfi")

    def test_rejects_draft_fork_other_base_and_huge_prs(self):
        for override in (
            {"draft": True},
            {"base": {"ref": "9.5", "sha": "b"}},
            {"head": {"sha": "h", "repo": {"full_name": "fork/r"}}},
            {"head": {"sha": "h", "repo": None}},
        ):
            self.assertIsNone(self.run_case(self.pull(**override)), override)
        self.assertIsNone(self.run_case(self.pull(), files=[modified("a", "+b")] * 100))


if __name__ == "__main__":
    unittest.main()
