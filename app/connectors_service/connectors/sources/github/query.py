#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from enum import Enum

from connectors.sources.github.utils import NODE_SIZE, REVIEWS_COUNT


class GithubQuery(Enum):
    USER_QUERY = """
        query {
        viewer {
            login
        }
    }
    """
    REPOS_QUERY = f"""
    query ($login: String!, $cursor: String) {{
    user(login: $login) {{
        repositories(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            name
            nameWithOwner
            url
            description
            visibility
            primaryLanguage {{
            name
            }}
            defaultBranchRef {{
            name
            }}
            isFork
            stargazerCount
            watchers {{
            totalCount
            }}
            forkCount
            createdAt
            isArchived
        }}
        }}
    }}
    }}
    """
    ORG_REPOS_QUERY = f"""
    query ($orgName: String!, $cursor: String) {{
    organization(login: $orgName) {{
        repositories(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            name
            nameWithOwner
            url
            description
            visibility
            primaryLanguage {{
            name
            }}
            defaultBranchRef {{
            name
            }}
            isFork
            stargazerCount
            watchers {{
            totalCount
            }}
            forkCount
            createdAt
            isArchived
        }}
        }}
    }}
    }}
    """
    REPO_QUERY = """
    query ($owner: String!, $repositoryName: String!) {
    repository(owner: $owner, name: $repositoryName) {
        id
        updatedAt
        name
        nameWithOwner
        url
        description
        visibility
        primaryLanguage {
        name
        }
        defaultBranchRef {
        name
        }
        isFork
        stargazerCount
        watchers {
        totalCount
        }
        forkCount
        createdAt
        isArchived
    }
    }
    """
    BATCH_REPO_QUERY_TEMPLATE = """
    query ({batch_queries}) {{
        {query_body}
    }}
    """
    PULL_REQUEST_QUERY = f"""
    query ($owner: String!, $name: String!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        pullRequests(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            mergedAt
            author {{
            login
            }}
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            reviewRequests(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                requestedReviewer {{
                ... on User {{
                    login
                }}
                }}
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
            reviews(first: {REVIEWS_COUNT}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                id
                author {{
                login
                }}
                state
                body
                comments(first: {NODE_SIZE}) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    body
                }}
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    ISSUE_QUERY = f"""
    query ($owner: String!, $name: String!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        issues(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            author {{
            login
            }}
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
        }}
        }}
    }}
    }}
    """
    COMMENT_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        comments(first: {node_size}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            author {{
                login
            }}
            body
            }}
        }}
        }}
    }}
    }}
    """
    REVIEW_QUERY = f"""
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        pullRequest(number: $number) {{
        reviews(first: {NODE_SIZE}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            id
            author {{
                login
            }}
            state
            body
            comments(first: {NODE_SIZE}) {{
                pageInfo {{
                hasNextPage
                endCursor
                }}
                nodes {{
                body
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    REVIEWERS_QUERY = f"""
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        pullRequest(number: $number) {{
        reviewRequests(first: {NODE_SIZE}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
                requestedReviewer {{
                    ... on User {{
                    login
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    LABELS_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        labels(first: {node_size}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            name
            }}
        }}
        }}
    }}
    }}
    """
    ASSIGNEES_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        assignees(first: {node_size}, after: $cursor) {{
            pageInfo {{
            hasNextPage
            endCursor
            }}
            nodes {{
            login
            }}
        }}
        }}
    }}
    }}
    """
    SEARCH_QUERY = f"""
    query ($filter_query: String!, $cursor: String) {{
    search(query: $filter_query, type: ISSUE, first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
        hasNextPage
        endCursor
        }}
        nodes {{
        ... on Issue {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
        }}
        ... on PullRequest {{
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            mergedAt
            assignees(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                login
            }}
            }}
            labels(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                name
                description
            }}
            }}
            reviewRequests(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                requestedReviewer {{
                ... on User {{
                    login
                }}
                }}
            }}
            }}
            comments(first: {NODE_SIZE}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                author {{
                login
                }}
                body
            }}
            }}
            reviews(first: {REVIEWS_COUNT}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                id
                author {{
                login
                }}
                state
                body
                comments(first: {NODE_SIZE}) {{
                pageInfo {{
                    hasNextPage
                    endCursor
                }}
                nodes {{
                    body
                }}
                }}
            }}
            }}
        }}
        }}
    }}
    }}
    """
    ORG_MEMBERS_QUERY = f"""
    query ($orgName: String!, $cursor: String) {{
    organization(login: $orgName) {{
        membersWithRole(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        edges {{
            node {{
                id
                login
                name
                email
                updatedAt
            }}
        }}
        }}
    }}
    }}
    """
    COLLABORATORS_QUERY = f"""
    query ($orgName: String!, $repoName: String!, $cursor: String) {{
    repository(owner: $orgName, name: $repoName) {{
        collaborators(first: {NODE_SIZE}, after: $cursor) {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        edges {{
            node {{
                id
                login
                email
            }}
        }}
        }}
    }}
    }}
    """
