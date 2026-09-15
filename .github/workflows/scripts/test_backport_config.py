#!/usr/bin/env python3

import json
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


class TestBackportConfig(unittest.TestCase):
    def test_renovate_labels_match_backportrc(self):
        backportrc = json.loads((REPO_ROOT / ".backportrc.json").read_text())
        renovate = json.loads((REPO_ROOT / "renovate.json").read_text())

        expected = set(backportrc["renovateVersionLabels"])
        renovate_version_labels = {
            label for label in renovate["labels"] if label.startswith("v")
        }
        self.assertEqual(expected, renovate_version_labels)

    def test_renovate_labels_cover_maintenance_branches(self):
        backportrc = json.loads((REPO_ROOT / ".backportrc.json").read_text())
        renovate_labels = set(backportrc["renovateVersionLabels"])

        for branch in backportrc["maintenanceBranches"]:
            self.assertIn(
                f"v{branch}.0",
                renovate_labels,
                f"missing Renovate label for maintenance branch {branch}",
            )

    def test_maintenance_branches_in_target_branch_choices(self):
        backportrc = json.loads((REPO_ROOT / ".backportrc.json").read_text())
        choices = {
            entry["name"] if isinstance(entry, dict) else entry
            for entry in backportrc["targetBranchChoices"]
        }
        for branch in backportrc["maintenanceBranches"]:
            self.assertIn(
                branch,
                choices,
                f"maintenanceBranches contains {branch} missing from targetBranchChoices",
            )


if __name__ == "__main__":
    unittest.main()
