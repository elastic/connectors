# Jira ftest fixture

This fixture serves fake Jira Cloud API responses (via Flask) so the connector
can be run end-to-end against a local Elasticsearch.

## Standard functional test

```bash
cd app/connectors_service
make ftest NAME=jira
```

The harness (`tests/ftest.sh`) automatically runs **two consecutive syncs**
(a full sync, then `remove` some data, then another full sync) and tears down
the stack. This two-sync flow is what makes it useful for catching memory growth
that only shows up *after* the first sync.

## Reproducing the agentless memory-growth / OOM issue locally

In agentless deployments the connector process is long-lived and runs many full
syncs in a row. The reported bug was that process RSS climbed with each full sync
and never returned to baseline, eventually causing an OOM kill. The amount of
growth depends on how much issue/attachment data a sync moves, so the default
fixture corpus (only ~100 issues) is too small to reproduce it.

Two opt-in knobs make the corpus heavy enough to reproduce and to guard against
regressions. They default to "off", so the standard ftest above is unchanged.

| Env var                 | Default | Meaning                                              |
| ----------------------- | ------- | ---------------------------------------------------- |
| `JIRA_ISSUES_COUNT`     | unset   | Total issues emitted across paginated `/search/jql`. |
| `JIRA_FIELD_PADDING_KB` | `0`     | KiB of filler text added to each issue description.  |

### Watching memory across syncs

Run the ftest with `PERF8=yes` to attach the `perf8`/`psutil` profiler. The
`MAX_RSS` threshold in [`.env`](./.env) is the pass/fail ceiling: if the process
exceeds it, the run fails.

```bash
cd app/connectors_service

# Heavy corpus: 20k issues, each with ~8 KiB description.
JIRA_ISSUES_COUNT=20000 \
JIRA_FIELD_PADDING_KB=8 \
PERF8=yes \
make ftest NAME=jira
```

For an allocation-level breakdown (memray flamegraph in the perf8 report):

```bash
JIRA_ISSUES_COUNT=20000 JIRA_FIELD_PADDING_KB=8 PERF8=yes make ftrace NAME=jira
```

### How to interpret results

- **Before the fix:** RSS after the second sync stays well above the post-first-sync
  level (and may breach `MAX_RSS`), reflecting memory that was never returned.
- **After the fix:** RSS returns toward baseline between syncs (the per-sync
  `malloc_trim` releases freed heap back to the OS), and peak usage is lower
  (issues are no longer fetched/retained twice).

### Tips

- Start small (e.g. `JIRA_ISSUES_COUNT=5000`) and scale up; very large corpora
  can exceed the fixture monitor's 20-minute sync timeout.
- To make a regression obvious, lower `MAX_RSS` in `.env` and/or raise the corpus
  size until the unfixed code fails and the fixed code passes.
- These knobs only affect this fixture; they do not change production behavior.
- `ftest.sh` starts the stack with `docker compose up -d`, which **reuses a
  previously built fixture image**. If you change `fixture.py` (or the knobs
  don't seem to take effect), rebuild it first:
  `docker compose -f <this dir>/docker-compose.yml build jira`.
