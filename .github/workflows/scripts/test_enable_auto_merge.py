#!/usr/bin/env python3
"""Run enable_auto_merge.sh against a stubbed `gh` executable."""

import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("enable_auto_merge.sh")

# `gh pr view` prints "<base>\t<cross-repo>\t<label index or null>\t<auto-merge on>".
# `gh pr merge` logs the token it was called with and succeeds only for the
# token named in STUB_OK_TOKEN ("" = never; "any" = always).
GH_STUB = """#!/usr/bin/env bash
if [ "$2" = "view" ]; then
  printf '%s\\t%s\\t%s\\t%s\\n' "$STUB_BASE" "$STUB_CROSS" "$STUB_LABEL" "$STUB_AUTO"
  exit 0
fi
echo "$GH_TOKEN $*" >> "$STUB_LOG"
if [ "$STUB_OK_TOKEN" = "any" ] || [ "$GH_TOKEN" = "$STUB_OK_TOKEN" ]; then exit 0; fi
echo "GraphQL: User is not authorized for this protected branch"
exit 1
"""


class TestEnableAutoMerge(unittest.TestCase):
    def run_script(self, base="9.6", cross="false", label="0", auto="false", ok_token="D"):
        with tempfile.TemporaryDirectory() as tmp:
            gh = Path(tmp, "gh")
            gh.write_text(GH_STUB)
            gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
            log = Path(tmp, "log")
            env = {
                **os.environ,
                "PATH": f"{tmp}:{os.environ['PATH']}",
                "DEFAULT_TOKEN": "D",
                "BOT_TOKEN": "B",
                "GH_REPO": "elastic/connectors",
                "STUB_BASE": base,
                "STUB_CROSS": cross,
                "STUB_LABEL": label,
                "STUB_AUTO": auto,
                "STUB_OK_TOKEN": ok_token,
                "STUB_LOG": str(log),
            }
            result = subprocess.run(
                ["bash", str(SCRIPT), "7"], env=env, capture_output=True, text=True
            )
            calls = log.read_text().splitlines() if log.exists() else []
            return result, calls

    def test_uses_default_token_first(self):
        result, calls = self.run_script(ok_token="D")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].startswith("D "))
        self.assertIn("--squash", calls[0])

    def test_falls_back_to_bot_token(self):
        result, calls = self.run_script(ok_token="B")
        self.assertEqual(result.returncode, 0)
        self.assertTrue(calls[-1].startswith("B "))

    def test_warns_but_does_not_fail_when_nothing_works(self):
        result, calls = self.run_script(ok_token="")
        self.assertEqual(result.returncode, 0)
        self.assertIn("::warning::", result.stdout)
        self.assertEqual(len(calls), 4)

    def test_skips_non_maintenance_base(self):
        for base in ("main", "feature/x", "9"):
            result, calls = self.run_script(base=base, ok_token="any")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(calls, [])

    def test_skips_cross_repo_unlabeled_or_already_enabled(self):
        for kwargs in ({"cross": "true"}, {"label": "null"}, {"auto": "true"}):
            result, calls = self.run_script(ok_token="any", **kwargs)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
