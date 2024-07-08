#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
from typing import AsyncIterator, Iterator


class ConnectorBase:
    def __init__(self, data_provider):

        # Check if all fields marked as 'required' in config are present with values, if not raise an exception
        for key, value in data_provider.configuration.items():
            if value.get("value") is None and value.get("required", True):
                raise ValueError(f"Missing required configuration field: {key}")

        self.data_provider = data_provider

    def get_configuration(self):
        return self.data_provider.configuration

    def lazy_load(self) -> Iterator[dict]:
        async_gen = self.alazy_load()
        loop = asyncio.get_event_loop()

        try:
            while True:
                item = loop.run_until_complete(self._next_item(async_gen))
                if item is None:
                    break
                yield item
        except StopAsyncIteration:
            return

    async def _next_item(self, async_gen):
        try:
            return await async_gen.__anext__()
        except StopAsyncIteration:
            return None

    async def alazy_load(
        self,
    ) -> AsyncIterator[dict]:
        async for doc, lazy_download in self.data_provider.get_docs(filtering=None):
            # TODO: not all sources have timestamp field and support downloads
            # data = await lazy_download(doit=True, timestamp=doc[TIMESTAMP_FIELD])
            # doc.update(data)
            yield doc
