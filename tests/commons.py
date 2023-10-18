#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import math
from functools import cached_property
from random import choices

from faker import Faker


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
        if self.call_count != 0:
            raise AssertionError(
                f"Expected zero calls. Actual number of calls: {self.call_count}."
            )

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


class WeightedFakeProvider:
    def __init__(self, seed=None, weights=None):
        self.seed = seed
        if weights and len(weights) != 4:
            raise Exception(
                f"Exactly 4 weights should be provided. Got {len(weights)}: {weights}"
            )
        self.weights = weights or [0.58, 0.3, 0.1, 0.02]

        if math.fsum(self.weights) != 1:
            raise Exception(
                f"Sum of weights should be equal to 1. Sum of provided weights {self.weights} is {math.fsum(self.weights)}"
            )
        self.fake_provider = FakeProvider(seed=seed)

    @cached_property
    def _texts(self):
        return [
            self.fake_provider.small_text(),
            self.fake_provider.medium_text(),
            self.fake_provider.large_text(),
            self.fake_provider.extra_large_text(),
        ]

    @cached_property
    def fake(self):
        return self.fake_provider.fake

    @cached_property
    def _htmls(self):
        return [
            self.fake_provider.small_html(),
            self.fake_provider.medium_html(),
            self.fake_provider.large_html(),
            self.fake_provider.extra_large_html(),
        ]

    def get_text(self):
        return choices(self._texts, self.weights)[0]

    def get_html(self):
        return choices(self._htmls, self.weights)[0]


class FakeProvider:
    def __init__(self, seed=None):
        self.seed = seed
        self.fake = Faker()
        if seed:
            self.fake.seed_instance(seed)

    @cached_property
    def _cached_random_str(self):
        return self.fake.pystr(min_chars=100 * 1024, max_chars=100 * 1024 + 1)

    def small_text(self):
        # Up to 1KB of text
        return self.generate_text(1 * 1024)

    def medium_text(self):
        # Up to 256KB of text
        return self.generate_text(256 * 1024)

    def large_text(self):
        # Up to 1MB of text
        return self.generate_text(1024 * 1024)

    def extra_large_text(self):
        return self.generate_text(20 * 1024 * 1024)

    def small_html(self):
        # Around 100KB
        return self.generate_html(1)

    def medium_html(self):
        # Around 1MB
        return self.generate_html(1 * 10)

    def large_html(self):
        # Around 8MB
        return self.generate_html(8 * 10)

    def extra_large_html(self):
        # Around 25MB
        return self.generate_html(25 * 10)

    def generate_text(self, max_size):
        return self.fake.text(max_nb_chars=max_size)

    def generate_html(self, images_of_100kb):
        img = self._cached_random_str  # 100kb
        text = self.small_text()

        images = []
        for _ in range(images_of_100kb):
            images.append(f"<img src='{img}'/>")

        return f"<html><head></head><body><div>{text}</div><div>{'<br/>'.join(images)}</div></body></html>"
