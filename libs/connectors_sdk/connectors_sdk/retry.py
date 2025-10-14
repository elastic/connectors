from enum import Enum
import inspect


class RetryStrategy(Enum):
    CONSTANT = 0
    LINEAR_BACKOFF = 1
    EXPONENTIAL_BACKOFF = 2


def retryable(
    retries=3,
    interval=1.0,
    strategy=RetryStrategy.LINEAR_BACKOFF,
    skipped_exceptions=None,
):
    def wrapper(func):
        if skipped_exceptions is None:
            processed_skipped_exceptions = []
        elif not isinstance(skipped_exceptions, list):
            processed_skipped_exceptions = [skipped_exceptions]
        else:
            processed_skipped_exceptions = skipped_exceptions

        if inspect.isasyncgenfunction(func):
            return retryable_async_generator(
                func, retries, interval, strategy, processed_skipped_exceptions
            )
        elif inspect.iscoroutinefunction(func):
            return retryable_async_function(
                func, retries, interval, strategy, processed_skipped_exceptions
            )
        elif inspect.isfunction(func):
            return retryable_sync_function(
                func, retries, interval, strategy, processed_skipped_exceptions
            )
        else:
            msg = f"Retryable decorator is not implemented for {func.__class__}."
            raise NotImplementedError(msg)

    return wrapper


def retryable_async_function(func, retries, interval, strategy, skipped_exceptions):
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        retry = 1
        while retry <= retries:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if retry >= retries or e.__class__ in skipped_exceptions:
                    raise e
                logger.debug(
                    f"Retrying ({retry} of {retries}) with interval: {interval} and strategy: {strategy.name}"
                )
                await sleeps_for_retryable.sleep(
                    time_to_sleep_between_retries(strategy, interval, retry)
                )
                retry += 1

    return wrapped


def retryable_async_generator(func, retries, interval, strategy, skipped_exceptions):
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        retry = 1
        while retry <= retries:
            try:
                async for item in func(*args, **kwargs):
                    yield item
                break
            except Exception as e:
                if retry >= retries or e.__class__ in skipped_exceptions:
                    raise e

                logger.debug(
                    f"Retrying ({retry} of {retries}) with interval: {interval} and strategy: {strategy.name}"
                )
                await sleeps_for_retryable.sleep(
                    time_to_sleep_between_retries(strategy, interval, retry)
                )
                retry += 1

    return wrapped


def retryable_sync_function(func, retries, interval, strategy, skipped_exceptions):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        retry = 1
        while retry <= retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if retry >= retries or e.__class__ in skipped_exceptions:
                    raise e
                logger.debug(
                    f"Retrying ({retry} of {retries}) with interval: {interval} and strategy: {strategy.name}"
                )
                time.sleep(time_to_sleep_between_retries(strategy, interval, retry))
                retry += 1

    return wrapped


def time_to_sleep_between_retries(strategy, interval, retry):
    match strategy:
        case RetryStrategy.CONSTANT:
            return interval
        case RetryStrategy.LINEAR_BACKOFF:
            return interval * retry
        case RetryStrategy.EXPONENTIAL_BACKOFF:
            return interval**retry
        case _:
            raise UnknownRetryStrategyError()
