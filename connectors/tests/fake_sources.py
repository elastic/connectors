#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Collection of fake source classes for tests
"""
from functools import partial


class FakeSource:
    """Fakey"""

    service_type = "fake"

    def __init__(self, connector):
        self.connector = connector
        if connector.configuration.has_field("raise"):
            raise Exception("I break on init")
        self.fail = connector.configuration.has_field("fail")

    async def changed(self):
        return True

    async def ping(self):
        pass

    async def close(self):
        pass

    async def _dl(self, doc_id, timestamp=None, doit=None):
        if not doit:
            return
        return {"_id": doc_id, "_timestamp": timestamp, "text": "xx"}

    async def get_docs(self):
        if self.fail:
            raise Exception("I fail while syncing")
        yield {"_id": "1"}, partial(self._dl, "1")

    @classmethod
    def get_default_configuration(cls):
        return []


class FakeSourceTS(FakeSource):
    """Fake source with stable TS"""

    service_type = "fake_ts"
    ts = "2022-10-31T09:04:35.277558"

    async def get_docs(self):
        if self.fail:
            raise Exception("I fail while syncing")
        yield {"_id": "1", "_timestamp": self.ts}, partial(self._dl, "1")


class FailsThenWork(FakeSource):
    """Buggy"""

    service_type = "fail_once"
    fail = True

    async def get_docs(self):
        if FailsThenWork.fail:
            FailsThenWork.fail = False
            raise Exception("I fail while syncing")
        yield {"_id": "1"}, partial(self._dl, "1")


class LargeFakeSource(FakeSource):
    """Phatey"""

    service_type = "large_fake"

    async def get_docs(self):
        for i in range(1001):
            doc_id = str(i + 1)
            yield {"_id": doc_id, "data": "big" * 1024 * 1024}, partial(
                self._dl, doc_id
            )
