#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitLab connector package.

This package provides integration with GitLab Cloud for syncing projects, issues,
merge requests, epics, releases, and README files to Elasticsearch.
"""

from connectors.sources.gitlab.datasource import GitLabDataSource

__all__ = ["GitLabDataSource"]
