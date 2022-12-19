#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Google Cloud Storage module is responsible to check if prerequisites are satisfied for e2e or not.
"""
import os


class PrerequisiteException(Exception):
    """This class is used to generate the custom error exception when prerequisites are not satisfied."""

    def __init__(self, errors):
        super().__init__(
            f"Error while running e2e test for the GCS connector. \nReason: {errors}"
        )
        self.errors = errors


def verify():
    "Method to verify if prerequisites are satisfied for e2e or not"
    storage_emulator_host = os.getenv(key="STORAGE_EMULATOR_HOST", default=None)
    if storage_emulator_host != "http://localhost:4443":
        raise PrerequisiteException(
            "Environment variable STORAGE_EMULATOR_HOST is not set properly.\n"
            "Solution: Please set the environment variable STORAGE_EMULATOR_HOST value with the below command on your terminal:\n"
            "export STORAGE_EMULATOR_HOST=http://localhost:4443 \n"
        )


if __name__ == "__main__":
    verify()
