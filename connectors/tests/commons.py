class AsyncGeneratorFake:
    """
    Async documents generator fake class, which records the args and kwargs it was called with.
    """

    def __init__(self, docs):
        self.docs = docs
        self.call_args = []
        self.call_kwargs = []

    def __aiter__(self):
        self.i = 0
        return self

    async def __anext__(self):
        if self.i >= len(self.docs):
            raise StopAsyncIteration

        doc = self.docs[self.i]
        self.i += 1
        return doc

    def __call__(self, *args, **kwargs):
        if args:
            self.call_args.append(*args)

        if kwargs:
            self.call_kwargs.append(*kwargs.items())

        return self
