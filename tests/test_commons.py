#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from tests.commons import AsyncIterator


@pytest.mark.asyncio
async def test_async_generation():
    items = [1, 2, 3]

    async_generator = AsyncIterator(items)

    yielded_items = []
    async for item in async_generator:
        yielded_items.append(item)

    assert yielded_items == items


@pytest.mark.asyncio
async def test_call_args():
    items = [1]

    async_generator = AsyncIterator(items)

    arg_one = "arg one"
    arg_two = "arg two"

    # first call
    async for _ in async_generator(arg_one, arg_two):
        pass

    arg_three = "arg three"
    arg_four = "arg four"

    # second call
    async for _ in async_generator(arg_three, arg_four):
        pass

    first_call_args = async_generator.call_args[0]
    second_call_args = async_generator.call_args[1]

    assert len(async_generator.call_args) == 2

    assert first_call_args[0] == arg_one
    assert first_call_args[1] == arg_two

    assert second_call_args[0] == arg_three
    assert second_call_args[1] == arg_four


@pytest.mark.asyncio
async def test_call_kwargs():
    items = [1]

    async_generator = AsyncIterator(items)

    kwarg_one_value = "kwarg one value"
    kwarg_two_value = "kwarg two value"

    # first call
    async for _ in async_generator(
        kwarg_one_key=kwarg_one_value, kwarg_two_key=kwarg_two_value
    ):
        pass

    kwarg_three_value = "kwarg three value"
    kwarg_four_value = "kwarg four value"

    # second call
    async for _ in async_generator(
        kwarg_three_key=kwarg_three_value, kwarg_four_key=kwarg_four_value
    ):
        pass

    first_call_kwargs = async_generator.call_kwargs[0]
    second_call_kwargs = async_generator.call_kwargs[1]

    assert len(async_generator.call_kwargs) == 2

    assert first_call_kwargs["kwarg_one_key"] == kwarg_one_value
    assert first_call_kwargs["kwarg_two_key"] == kwarg_two_value

    assert second_call_kwargs["kwarg_three_key"] == kwarg_three_value
    assert second_call_kwargs["kwarg_four_key"] == kwarg_four_value


@pytest.mark.asyncio
async def test_assert_not_called():
    items = []

    async_generator = AsyncIterator(items)
    assert async_generator.assert_not_called()


@pytest.mark.asyncio
async def test_assert_called_once():
    items = []

    async_generator = AsyncIterator(items)

    async for _ in async_generator():
        pass

    # not a direct call on the generator -> call count still 1
    async for _ in async_generator:
        pass

    async_generator.assert_called_once()


@pytest.mark.asyncio
async def test_assert_called_once_with_two_calls():
    items = []

    async_generator = AsyncIterator(items)

    # first call
    async for _ in async_generator():
        pass

    # second call
    async for _ in async_generator():
        pass

    with pytest.raises(AssertionError):
        async_generator.assert_called_once()


@pytest.mark.asyncio
async def test_assert_called_once_with_one_arg():
    items = [1]

    argument = "some argument"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(argument):
        pass

    async_iterator.assert_called_once_with(argument)


@pytest.mark.asyncio
async def test_assert_called_once_with_wrong_arg():
    items = [1]

    argument = "some argument"
    wrong_argument = "wrong argument"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(argument):
        pass

    with pytest.raises(AssertionError):
        async_iterator.assert_called_once_with(wrong_argument)


@pytest.mark.asyncio
async def test_assert_called_once_with_one_arg_and_two_calls():
    items = [1]

    argument = "some argument"

    async_iterator = AsyncIterator(items)

    # first call
    async for _ in async_iterator(argument):
        pass

    # second call
    async for _ in async_iterator(argument):
        pass

    with pytest.raises(AssertionError):
        async_iterator.assert_called_once_with(argument)


@pytest.mark.asyncio
async def test_assert_called_once_with_three_args():
    items = [1]

    argument_one = "some argument one"
    argument_two = "some argument two"
    argument_three = "some argument three"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(argument_one, argument_two, argument_three):
        pass

    async_iterator.assert_called_once_with(argument_one, argument_two, argument_three)


@pytest.mark.asyncio
async def test_assert_called_once_with_one_kwarg():
    items = [1]

    argument = "some argument"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(argument=argument):
        pass

    async_iterator.assert_called_once_with(argument=argument)


@pytest.mark.asyncio
async def test_assert_called_once_with_one_kwarg_and_two_calls():
    items = [1]

    argument = "some argument"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(argument=argument):
        pass

    async for _ in async_iterator(argument=argument):
        pass

    with pytest.raises(AssertionError):
        async_iterator.assert_called_once_with(argument=argument)


@pytest.mark.asyncio
async def test_assert_called_once_with_wrong_kwarg():
    items = [1]

    argument = "some argument"
    wrong_argument = "wrong argument"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(argument=argument):
        pass

    with pytest.raises(AssertionError):
        async_iterator.assert_called_once_with(wrong_argument=wrong_argument)


@pytest.mark.asyncio
async def test_assert_called_once_with_three_kwargs():
    items = [1]

    argument_one = "some argument one"
    argument_two = "some argument two"
    argument_three = "some argument three"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(
        argument_one=argument_one,
        argument_two=argument_two,
        argument_three=argument_three,
    ):
        pass

    async_iterator.assert_called_once_with(
        argument_one=argument_one,
        argument_two=argument_two,
        argument_three=argument_three,
    )


@pytest.mark.asyncio
async def test_assert_called_once_with_args_and_kwargs():
    items = [1]

    argument_one = "some argument one"
    argument_two = "some argument two"
    argument_three = "some argument three"

    async_iterator = AsyncIterator(items)

    async for _ in async_iterator(
        argument_one, argument_two=argument_two, argument_three=argument_three
    ):
        pass

    async_iterator.assert_called_once_with(
        argument_one, argument_two=argument_two, argument_three=argument_three
    )
