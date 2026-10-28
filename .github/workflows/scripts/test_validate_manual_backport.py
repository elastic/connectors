#!/usr/bin/env python3

import io
import os
import tempfile
import unittest
import urllib.error
from unittest import mock

import validate_manual_backport

from validate_manual_backport import (
    _label_names,
    is_team_member,
    parse_backported_from_pr,
    parse_teams,
    patch_label_re,
)


class TestParseBackportedFrom(unittest.TestCase):
    def test_explicit_line(self):
        body = "Ported fix.\n\nBackported from #4437\n"
        self.assertEqual(parse_backported_from_pr(body), 4437)

    def test_case_insensitive(self):
        self.assertEqual(parse_backported_from_pr("backported from #12"), 12)

    def test_rejects_title_only_reference(self):
        self.assertIsNone(parse_backported_from_pr("Fix (#4437) for 8.19"))

    def test_rejects_missing_line(self):
        self.assertIsNone(parse_backported_from_pr("See main PR 4437"))

    def test_label_names(self):
        pull = {"labels": [{"name": "backport"}, {"name": "v8.19.23"}]}
        self.assertEqual(_label_names(pull), ["backport", "v8.19.23"])


class TestPatchLabelRe(unittest.TestCase):
    def test_matches_own_branch_only(self):
        for base, ok, bad in [
            ("8.19", "v8.19.24", "v9.5.6"),
            ("9.4", "v9.4.9", "v9.5.6"),
            ("9.5", "v9.5.6", "v9.4.9"),
        ]:
            regex = patch_label_re(base)
            self.assertIsNotNone(regex)
            self.assertTrue(regex.match(ok))
            self.assertFalse(regex.match(bad))

    def test_minor_is_not_prefix_matched(self):
        self.assertFalse(patch_label_re("9.1").match("v9.10.0"))
        self.assertFalse(patch_label_re("9.5").match("v9.5"))

    def test_rejects_non_maintenance_base(self):
        for base in ("main", "", "feature/x", "9", "9.5.1"):
            self.assertIsNone(patch_label_re(base))


class TestTeams(unittest.TestCase):
    def test_parse_teams(self):
        self.assertEqual(parse_teams("a, b ,,c"), ["a", "b", "c"])
        self.assertEqual(parse_teams(""), [])
        self.assertEqual(parse_teams(None), [])

    def test_is_team_member_states(self):
        with mock.patch.object(validate_manual_backport, "_api_get", lambda u, t: {"state": "active"}):
            self.assertTrue(is_team_member("elastic", "team", "u", "t"))
        with mock.patch.object(validate_manual_backport, "_api_get", lambda u, t: {"state": "pending"}):
            self.assertFalse(is_team_member("elastic", "team", "u", "t"))

    def test_not_found_means_not_a_member(self):
        def fake_get(url, token):
            raise _http_error(404)

        with mock.patch.object(validate_manual_backport, "_api_get", fake_get):
            self.assertFalse(is_team_member("elastic", "team", "u", "t"))

    def test_other_errors_propagate(self):
        def fake_get(url, token):
            raise _http_error(403)

        with mock.patch.object(validate_manual_backport, "_api_get", fake_get):
            with self.assertRaises(urllib.error.HTTPError):
                is_team_member("elastic", "team", "u", "t")


def _http_error(code):
    return urllib.error.HTTPError("https://api.github.com", code, "err", {}, io.BytesIO())


class TestMain(unittest.TestCase):
    """Exercise the approval decision end to end with a stubbed GitHub API."""

    def run_main(
        self,
        base,
        title,
        source_labels,
        source_merged=True,
        source_base="main",
        head_repo={"fork": False},
        author="dev",
        teams="team-a,team-b",
        member_of=("team-b",),
        membership_error=None,
    ):
        backport_pr = {"labels": [{"name": "backport"}], "head": {"repo": head_repo}}
        source_pr = {
            "merged": source_merged,
            "base": {"ref": source_base},
            "labels": [{"name": name} for name in source_labels],
        }

        def fake_get(url, token):
            if "/memberships/" in url:
                if membership_error:
                    raise _http_error(membership_error)
                team = url.split("/teams/")[1].split("/")[0]
                if team in member_of:
                    return {"state": "active"}
                raise _http_error(404)
            return backport_pr if url.endswith("/pulls/10") else source_pr

        with tempfile.NamedTemporaryFile("r+") as out:
            env = {
                "GITHUB_REPOSITORY": "elastic/connectors",
                "GITHUB_TOKEN": "t",
                "GITHUB_OUTPUT": out.name,
                "PR_NUMBER": "10",
                "BASE_BRANCH": base,
                "PR_TITLE": title,
                "PR_BODY": "Backported from #99",
                "PR_AUTHOR": author,
                "ALLOWED_TEAMS": teams,
            }
            with mock.patch.dict(os.environ, env), mock.patch.object(
                validate_manual_backport, "_api_get", fake_get
            ):
                exit_code = validate_manual_backport.main()
            self.exit_code = exit_code
            return "approved=true" in out.read()

    def test_approves_matching_branch_and_label(self):
        self.assertTrue(self.run_main("9.5", "[9.5] fix (#99)", ["v9.5.6"]))
        self.assertEqual(self.exit_code, 0)

    def test_rejects_label_of_other_branch(self):
        self.assertFalse(self.run_main("9.5", "[9.5] fix (#99)", ["v9.4.9"]))

    def test_rejects_title_of_other_branch(self):
        self.assertFalse(self.run_main("9.5", "[9.4] fix (#99)", ["v9.5.6"]))

    def test_rejects_unmerged_or_non_main_source(self):
        self.assertFalse(self.run_main("8.19", "[8.19] x", ["v8.19.24"], source_merged=False))
        self.assertFalse(self.run_main("8.19", "[8.19] x", ["v8.19.24"], source_base="9.5"))

    def test_rejects_non_maintenance_base(self):
        self.assertFalse(self.run_main("main", "[main] x", ["v9.6.0"]))

    def test_rejects_author_outside_allowed_teams(self):
        self.assertFalse(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], member_of=()))
        self.assertEqual(self.exit_code, 0)

    def test_approves_member_of_any_allowed_team(self):
        self.assertTrue(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], member_of=("team-a",)))

    def test_rejects_when_head_repo_is_gone_or_fork(self):
        self.assertFalse(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], head_repo={"fork": True}))
        self.assertFalse(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], head_repo=None))

    def test_fails_closed_without_teams_or_author(self):
        self.assertFalse(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], teams=""))
        self.assertFalse(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], author=""))

    def test_fails_closed_when_membership_cannot_be_verified(self):
        self.assertFalse(self.run_main("9.5", "[9.5] fix", ["v9.5.6"], membership_error=403))
        self.assertEqual(self.exit_code, 1)


if __name__ == "__main__":
    unittest.main()
