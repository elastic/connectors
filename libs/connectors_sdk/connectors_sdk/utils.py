#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import hashlib
import os
import platform
import shutil
import subprocess  # noqa S404
import time
from datetime import datetime, timezone
from enum import Enum
from time import strftime

from base64io import Base64IO

from connectors_sdk.logger import logger

_BASE64 = shutil.which("base64")

ISO_ZULU_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Format(Enum):
    VERBOSE = "verbose"
    SHORT = "short"


def iso_utc(when=None):
    if when is None:
        when = datetime.now(timezone.utc)
    return when.isoformat()


def with_utc_tz(ts):
    """Ensure the timestmap has a timezone of UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    else:
        return ts.astimezone(timezone.utc)


def iso_zulu():
    """Returns the current time in ISO Zulu format"""
    return datetime.now(timezone.utc).strftime(ISO_ZULU_TIMESTAMP_FORMAT)


def epoch_timestamp_zulu():
    """Returns the timestamp of the start of the epoch, in ISO Zulu format"""
    return strftime(ISO_ZULU_TIMESTAMP_FORMAT, time.gmtime(0))


def get_file_extension(filename):
    return os.path.splitext(filename)[-1]


def hash_id(_id):
    # Collision probability: 1.47*10^-29
    # S105 rule considers this code unsafe, but we're not using it for security-related
    # things, only to generate pseudo-ids for documents
    return hashlib.md5(_id.encode("utf8")).hexdigest()  # noqa S105


def convert_to_b64(source, target=None, overwrite=False):
    """Converts a `source` file to base64 using the system's `base64`

    When `target` is not provided, done in-place.

    If `overwrite` is `True` and `target` exists, overwrites it.
    If `False` and it exists, raises an `IOError`

    If the `base64` utility could not be found, falls back to pure Python
    using base64io.

    This function blocks -- if you want to avoid blocking the event
    loop, call it through `loop.run_in_executor`

    Returns the target file.
    """
    inplace = target is None
    temp_target = f"{source}.b64"
    if not inplace and not overwrite and os.path.exists(target):
        msg = f"{target} already exists."
        raise IOError(msg)

    if _BASE64 is not None:
        if platform.system() == "Darwin":
            version = int(platform.mac_ver()[0].split(".")[0])
            # MacOS 13 has changed base64 util
            if version >= 13:
                cmd = f"{_BASE64} -i {source} -o {temp_target}"
            else:
                cmd = f"{_BASE64} {source} > {temp_target}"
        else:
            # In Linuces, avoid line wrapping
            cmd = f"{_BASE64} -w 0 {source} > {temp_target}"
        logger.debug(f"Calling {cmd}")
        subprocess.check_call(cmd, shell=True)  # noqa S602
    else:
        # Pure Python version
        with open(source, "rb") as sf, open(temp_target, "wb") as tf:
            with Base64IO(tf) as encoded_target:
                for line in sf:
                    encoded_target.write(line)

    # success, let's move the file to the right place
    if inplace:
        os.remove(source)
        os.rename(temp_target, source)
    else:
        if os.path.exists(target):
            os.remove(target)
        os.rename(temp_target, target)

    return source if inplace else target


def shorten_str(string, shorten_by):
    """
    Shorten a string by removing characters from the middle, replacing them with '...'.

    If balanced shortening is not possible, retains an extra character at the beginning.

    Args:
        string (str): The string to be shortened.
        shorten_by (int): The number of characters to remove from the string.

    Returns:
        str: The shortened string.

    Examples:
        >>> shorten_str("abcdefgh", 1)
        'abcdefgh'
        >>> shorten_str("abcdefgh", 4)
        'ab...gh'
        >>> shorten_str("abcdefgh", 5)
        'ab...h'
        >>> shorten_str("abcdefgh", 1000)
        'a...h'
    """

    if string is None or string == "":
        return ""

    if not string or shorten_by < 3:
        return string

    length = len(string)
    shorten_by = min(length - 2, shorten_by)

    keep = (length - shorten_by) // 2
    balanced_shortening = (shorten_by + length) % 2 == 0

    if balanced_shortening:
        return f"{string[:keep]}...{string[-keep:]}"
    else:
        # keep one more at the front
        return f"{string[:keep + 1]}...{string[-keep:]}"


class Features:
    DOCUMENT_LEVEL_SECURITY = "document_level_security"

    BASIC_RULES_NEW = "basic_rules_new"
    ADVANCED_RULES_NEW = "advanced_rules_new"

    # keep backwards compatibility
    BASIC_RULES_OLD = "basic_rules_old"
    ADVANCED_RULES_OLD = "advanced_rules_old"

    NATIVE_CONNECTOR_API_KEYS = "native_connector_api_keys"

    def __init__(self, features=None):
        if features is None:
            features = {}

        self.features = features

    def incremental_sync_enabled(self):
        return nested_get_from_dict(
            self.features, ["incremental_sync", "enabled"], default=False
        )

    def document_level_security_enabled(self):
        return nested_get_from_dict(
            self.features, ["document_level_security", "enabled"], default=False
        )

    def native_connector_api_keys_enabled(self):
        return nested_get_from_dict(
            self.features, ["native_connector_api_keys", "enabled"], default=True
        )

    def sync_rules_enabled(self):
        return any(
            [
                self.feature_enabled(Features.BASIC_RULES_NEW),
                self.feature_enabled(Features.BASIC_RULES_OLD),
                self.feature_enabled(Features.ADVANCED_RULES_NEW),
                self.feature_enabled(Features.ADVANCED_RULES_OLD),
            ]
        )

    def feature_enabled(self, feature):
        match feature:
            case Features.BASIC_RULES_NEW:
                return nested_get_from_dict(
                    self.features, ["sync_rules", "basic", "enabled"], default=False
                )
            case Features.ADVANCED_RULES_NEW:
                return nested_get_from_dict(
                    self.features, ["sync_rules", "advanced", "enabled"], default=False
                )
            case Features.BASIC_RULES_OLD:
                return self.features.get("filtering_rules", False)
            case Features.ADVANCED_RULES_OLD:
                return self.features.get("filtering_advanced_config", False)
            case _:
                return False


def nested_get_from_dict(dictionary, keys, default=None):
    def nested_get(dictionary_, keys_, default_=None):
        if dictionary_ is None:
            return default_

        if not keys_:
            return dictionary_

        if not isinstance(dictionary_, dict):
            return default_

        return nested_get(dictionary_.get(keys_[0]), keys_[1:], default_)

    return nested_get(dictionary, keys, default)
