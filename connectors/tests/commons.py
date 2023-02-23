class AsyncIterator:
    """
    Async documents generator fake class, which records the args and kwargs it was called with.
    """

    def __init__(self, *args):
        self.result = args
        self.call_args = []
        self.call_kwargs = []
        self.i = 0

    async def __aiter__(self):
        if len(self.result) == 1:
            yield self.result[0]
        else:
            yield (*self.result,)

    async def __anext__(self):
        if self.i >= len(self.items):
            raise StopAsyncIteration

        item = self.items[self.i]
        self.i += 1
        return item

    def __call__(self, *args, **kwargs):
        if args:
            self.call_args.append(*args)

        if kwargs:
            self.call_kwargs.append(*kwargs.items())

        return self
