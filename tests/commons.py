#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
class AsyncIterator:
    """
    Async documents generator fake class, which records the args and kwargs it was called with.
    """

    def __init__(self, items):
        self.items = items
        self.call_args = []
        self.call_kwargs = []
        self.i = 0
        self.call_count = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.i >= len(self.items):
            raise StopAsyncIteration

        item = self.items[self.i]
        self.i += 1
        return item

    def __call__(self, *args, **kwargs):
        self.call_count += 1

        if args:
            self.call_args.append(args)

        if kwargs:
            self.call_kwargs.append(kwargs)

        return self

    def assert_not_called(self):
        return self.call_count == 0

    def assert_called_once(self):
        if self.call_count != 1:
            raise AssertionError(
                f"Expected one call. Actual number of calls: {self.call_count}."
            )

    def assert_called_once_with(self, *args, **kwargs):
        self.assert_called_once()

        if len(self.call_args) > 0 and self.call_args[0] != args:
            raise AssertionError(
                f"Expected args: {args}. Actual args: {self.call_args[0]}."
            )

        if len(self.call_kwargs) > 0 and self.call_kwargs[0] != kwargs:
            raise AssertionError(
                f"Expected kwargs: {kwargs}. Actual kwargs: {self.call_kwargs[0]}."
            )
