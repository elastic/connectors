#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from enum import Enum

WILDCARD = "*"
BLOB = "blob"
FILE = "file"
GITHUB_CLOUD = "github_cloud"
GITHUB_SERVER = "github_server"
PERSONAL_ACCESS_TOKEN = "personal_access_token"  # noqa: S105
GITHUB_APP = "github_app"
PULL_REQUEST_OBJECT = "pullRequest"
REPOSITORY_OBJECT = "repository"

RETRIES = 3
RETRY_INTERVAL = 2
FORBIDDEN = 403
UNAUTHORIZED = 401
# Page size for top-level and dedicated (single-field) paginated queries.
NODE_SIZE = 100
# GitHub GraphQL bills queries by the number of potential nodes they can return,
# which grows multiplicatively for nested connections. To keep the cost of the
# composite pull request/issue/search queries low, nested connections are fetched
# in small eager pages; anything beyond the first page is backfilled on demand via
# the dedicated pagination queries (COMMENT_QUERY, REVIEW_QUERY, etc.).
NESTED_NODE_SIZE = 10

SUPPORTED_EXTENSION = [".markdown", ".md", ".rst"]

FILE_SCHEMA = {
    "name": "name",
    "size": "size",
    "type": "type",
    "path": "path",
    "mode": "mode",
    "extension": "extension",
    "_timestamp": "_timestamp",
}
PATH_SCHEMA = {
    "name": "name",
    "size": "size",
    "type": "type",
    "path": "path",
    "extension": "extension",
    "_timestamp": "_timestamp",
}


class ObjectType(Enum):
    REPOSITORY = "Repository"
    ISSUE = "Issue"
    PULL_REQUEST = "Pull request"
    PR = "pr"
    BRANCH = "branch"
    PATH = "path"


class UnauthorizedException(Exception):
    pass


class NoInstallationAccessTokenException(Exception):
    pass


class ForbiddenException(Exception):
    pass


class RateLimitingError(Exception):
    pass
