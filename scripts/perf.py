#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import subprocess
import psutil
import sys
import asyncio
import csv


class WatchedProcess:
    def __init__(self, cmd, report_file, every=10):
        self.cmd = cmd
        self.proc = None
        self.proc_info = None
        self.report_file = open(report_file, "w")
        self.writer = csv.writer(self.report_file)
        self.every = every
        self.rows = (
            "rss",
            "num_fds",
            "num_threads",
            "ctx_switch",
            "cpu_user",
            "cpu_system",
            "cpu_percent",
        )
        # headers
        self.writer.writerow(self.rows)

    async def _probe(self):
        pid = self.proc.pid
        self.proc_info = psutil.Process(pid)

        while self.proc.poll() is None:
            # collect info
            info = self.proc_info.as_dict()
            metrics = (
                info["memory_info"].rss,
                info["num_fds"],
                info["num_threads"],
                info["num_ctx_switches"].voluntary,
                info["cpu_times"].user,
                info["cpu_times"].system,
                info["cpu_percent"],
            )

            self.writer.writerow(metrics)
            self.report_file.flush()
            await asyncio.sleep(self.every)

    async def run(self):
        try:
            self.proc = subprocess.Popen(self.cmd)
            while self.proc.pid is None:
                await asyncio.sleep(1.0)

            await self._probe()
        finally:
            self.report_file.close()


def main():
    report = "report.csv"
    p = WatchedProcess(sys.argv[1:], report)

    asyncio.run(p.run())


main()
