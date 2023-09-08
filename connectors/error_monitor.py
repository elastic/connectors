#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


class LimitError(Exception):
    pass


class MaxConsecutiveErrorsExceededError(LimitError):
    pass


class MaxErrorsExceededError(LimitError):
    pass


class MaxErrorRatioExceededError(LimitError):
    pass


class ErrorMonitor:
    def __init__(
        self,
        logger_,
        max_errors,
        max_consecutive_errors,
        max_error_ratio,
        error_ratio_window_size,
    ):
        self._logger = logger_
        self.max_errors = max_errors
        self.max_consecutive_errors = max_consecutive_errors
        self.max_error_ratio = max_error_ratio
        self.error_ratio_window_size = error_ratio_window_size
        self.total_error_count = 0
        self.success_count = 0
        self.consecutive_error_count = 0
        self.window_errors = [False] * self.error_ratio_window_size
        self.window_index = 0
        self.last_error = None

    def note_success(self):
        self.consecutive_error_count = 0
        self.success_count += 1
        self.window_errors[self.window_index] = False
        self._increment_window_index()

    def note_error(self, error):
        self.total_error_count += 1
        self.consecutive_error_count += 1
        self.window_errors[self.window_index] = True
        self._increment_window_index()
        self.last_error = error
        self.raise_if_necessary()

    def raise_if_necessary(self):
        if self.consecutive_error_count > self.max_consecutive_errors:
            raise MaxConsecutiveErrorsExceededError(
                f"Exceeded maximum consecutive errors - saw {self.consecutive_error_count} errors in a row."
            ) from self.last_error
        if self.total_error_count > self.max_errors:
            raise MaxErrorsExceededError(
                f"Exceeded maximum number of errors - saw {self.total_error_count} errors in total."
            ) from self.last_error
        if self.error_ratio_window_size > 0 and (
            self.window_errors.count(True) / self.error_ratio_window_size
            > self.max_error_ratio
        ):
            raise MaxErrorRatioExceededError(
                f"Exceeded maximum error ratio of {self.max_error_ratio}. Of the last {self.error_ratio_window_size} documents, {self.window_errors.count(True)} had errors"
            ) from self.last_error

    def _increment_window_index(self):
        self.window_index = (self.window_index + 1) % self.error_ratio_window_size
