from enum import Enum


class GithubQuery(Enum):
    USER_QUERY = """
        query {
        viewer {
            login
        }
    }
    """
    REPOS_QUERY = """
    query ($login: String!, $cursor: String) {
    user(login: $login) {
        repositories(first: 100, after: $cursor) {
        pageInfo {
            hasNextPage
            endCursor
        }
        nodes {
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
        }
        }
    }
    }
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
    }
    }
    """
    PULL_REQUEST_QUERY = """
    query ($owner: String!, $name: String!, $cursor: String) {
    repository(owner: $owner, name: $name) {
        pullRequests(first: 100, after: $cursor) {
        pageInfo {
            hasNextPage
            endCursor
        }
        nodes {
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
            assignees(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                login
            }
            }
            labels(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                name
                description
            }
            }
            reviewRequests(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                requestedReviewer {
                ... on User {
                    login
                }
                }
            }
            }
            comments(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                author {
                login
                }
                body
            }
            }
            reviews(first: 45) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                id
                author {
                login
                }
                state
                body
                comments(first: 100) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                nodes {
                    body
                }
                }
            }
            }
        }
        }
    }
    }
    """
    ISSUE_QUERY = """
    query ($owner: String!, $name: String!, $cursor: String) {
    repository(owner: $owner, name: $name) {
        issues(first: 100, after: $cursor) {
        pageInfo {
            hasNextPage
            endCursor
        }
        nodes {
            id
            updatedAt
            number
            url
            createdAt
            closedAt
            title
            body
            state
            assignees(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                login
            }
            }
            labels(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                name
                description
            }
            }
            comments(first: 100) {
            pageInfo {
                hasNextPage
                endCursor
            }
            nodes {
                author {
                login
                }
                body
            }
            }
        }
        }
    }
    }
    """
    COMMENT_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        comments(first: 100, after: $cursor) {{
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
    REVIEW_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {
    repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
        reviews(first: 100, after: $cursor) {
            pageInfo {
            hasNextPage
            endCursor
            }
            nodes {
            id
            author {
                login
            }
            state
            body
            comments(first: 100) {
                pageInfo {
                hasNextPage
                endCursor
                }
                nodes {
                body
                }
            }
            }
        }
        }
    }
    }
    """
    REVIEWERS_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {
    repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
        reviewRequests(first: 100, after: $cursor) {
            pageInfo {
            hasNextPage
            endCursor
            }
            nodes {
                requestedReviewer {
                    ... on User {
                    login
                }
            }
            }
        }
        }
    }
    }
    """
    LABELS_QUERY = """
    query ($owner: String!, $name: String!, $number: Int!, $cursor: String) {{
    repository(owner: $owner, name: $name) {{
        {object_type}(number: $number) {{
        labels(first: 100, after: $cursor) {{
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
        assignees(first: 100, after: $cursor) {{
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
