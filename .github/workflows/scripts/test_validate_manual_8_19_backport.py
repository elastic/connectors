#!/usr/bin/env python3

import unittest

from validate_manual_8_19_backport import _label_names, parse_backported_from_pr


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


if __name__ == "__main__":
    unittest.main()
