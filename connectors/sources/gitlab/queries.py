#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GraphQL queries for GitLab API."""

# Pagination sizes for GraphQL queries
NODE_SIZE = 100  # For projects, merge requests
WORK_ITEMS_NODE_SIZE = (
    50  # For work items (lower to avoid complexity limit with widgets)
)
WORK_ITEMS_NESTED_SIZE = (
    50  # For discussions/notes in work items (reduced to lower complexity)
)
NESTED_FIELD_SIZE = 100  # For assignees, labels, discussions in MRs/legacy

# GraphQL query to fetch projects
PROJECTS_QUERY = f"""
query($cursor: String) {{
  projects(membership: true, first: {NODE_SIZE}, after: $cursor) {{
    pageInfo {{
      hasNextPage
      endCursor
    }}
    nodes {{
      id
      name
      path
      fullPath
      description
      visibility
      starCount
      forksCount
      createdAt
      lastActivityAt
      archived
      webUrl
      repository {{
        rootRef
      }}
      group {{
        id
        fullPath
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch merge requests for a project
MERGE_REQUESTS_QUERY = f"""
query($projectPath: ID!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    mergeRequests(first: {NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        iid
        title
        description
        state
        createdAt
        updatedAt
        mergedAt
        closedAt
        webUrl
        sourceBranch
        targetBranch
        author {{
          username
          name
        }}
        assignees(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        reviewers(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        approvedBy(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            username
            name
          }}
        }}
        mergeUser {{
          username
          name
        }}
        labels(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            title
          }}
        }}
        userNotesCount
        discussions(first: {NESTED_FIELD_SIZE}) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          nodes {{
            id
            notes(first: {NESTED_FIELD_SIZE}) {{
              pageInfo {{
                hasNextPage
                endCursor
              }}
              nodes {{
                id
                body
                createdAt
                updatedAt
                system
                position {{
                  newLine
                  oldLine
                  newPath
                  oldPath
                  positionType
                }}
                author {{
                  username
                  name
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining assignees for an issue or MR
ASSIGNEES_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      assignees(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          username
          name
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining labels for an issue or MR
LABELS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      labels(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          title
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining discussions for an issue or MR
DISCUSSIONS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      discussions(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          id
          notes(first: {NESTED_FIELD_SIZE}) {{{{
            pageInfo {{{{
              hasNextPage
              endCursor
            }}}}
            nodes {{{{
              id
              body
              createdAt
              updatedAt
              system
              position {{{{
                newLine
                oldLine
                newPath
                oldPath
                positionType
              }}}}
              author {{{{
                username
                name
              }}}}
            }}}}
          }}}}
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining notes for a specific discussion
NOTES_QUERY = f"""
query($projectPath: ID!, $iid: String!, $discussionId: ID!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      discussions(filter: {{{{discussionIds: [$discussionId]}}}}) {{{{
        nodes {{{{
          notes(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
            pageInfo {{{{
              hasNextPage
              endCursor
            }}}}
            nodes {{{{
              id
              body
              createdAt
              updatedAt
              system
              position {{{{
                newLine
                oldLine
                newPath
                oldPath
                positionType
              }}}}
              author {{{{
                username
                name
              }}}}
            }}}}
          }}}}
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining reviewers for a merge request
REVIEWERS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      reviewers(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          username
          name
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch remaining approvers for a merge request
APPROVEDBY_QUERY = f"""
query($projectPath: ID!, $iid: String!, $cursor: String) {{{{
  project(fullPath: $projectPath) {{{{
    {{issuable_type}}(iid: $iid) {{{{
      approvedBy(first: {NESTED_FIELD_SIZE}, after: $cursor) {{{{
        pageInfo {{{{
          hasNextPage
          endCursor
        }}}}
        nodes {{{{
          username
          name
        }}}}
      }}}}
    }}}}
  }}}}
}}}}
"""

# GraphQL query to fetch work items (Issues, Tasks, etc.) with full widgets
# Using reduced NODE_SIZE (50 instead of 100) to stay under GitLab's 250 complexity limit
WORK_ITEMS_PROJECT_QUERY = f"""
query($projectPath: ID!, $types: [IssueType!], $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(types: $types, first: {WORK_ITEMS_NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        id
        iid
        title
        state
        createdAt
        updatedAt
        closedAt
        webUrl
        author {{
          username
          name
        }}
        workItemType {{
          name
        }}
        widgets {{
          __typename
          ... on WorkItemWidgetDescription {{
            description
          }}
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
          ... on WorkItemWidgetNotes {{
            discussions(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {WORK_ITEMS_NESTED_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    body
                    createdAt
                    updatedAt
                    system
                    position {{
                      newLine
                      oldLine
                      newPath
                      oldPath
                      positionType
                    }}
                    author {{
                      username
                      name
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch work items (Epics) for a group with full widgets
# Using reduced page size like project-level queries
WORK_ITEMS_GROUP_QUERY = f"""
query($groupPath: ID!, $types: [IssueType!], $cursor: String) {{
  group(fullPath: $groupPath) {{
    workItems(types: $types, first: {WORK_ITEMS_NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        id
        iid
        title
        state
        createdAt
        updatedAt
        closedAt
        webUrl
        author {{
          username
          name
        }}
        workItemType {{
          name
        }}
        widgets {{
          __typename
          ... on WorkItemWidgetDescription {{
            description
          }}
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
          ... on WorkItemWidgetNotes {{
            discussions(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {WORK_ITEMS_NESTED_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    body
                    createdAt
                    updatedAt
                    system
                    author {{
                      username
                      name
                    }}
                  }}
                }}
              }}
            }}
          }}
          ... on WorkItemWidgetHierarchy {{
            parent {{
              id
              iid
              title
            }}
            children(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                iid
                title
              }}
            }}
          }}
          ... on WorkItemWidgetLinkedItems {{
            linkedItems(first: {WORK_ITEMS_NESTED_SIZE}) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                linkId
                linkType
                linkCreatedAt
                linkUpdatedAt
                workItem {{
                  id
                  iid
                  title
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining assignees for a work item
WORK_ITEM_ASSIGNEES_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetAssignees {{
            assignees(first: {WORK_ITEMS_NESTED_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining labels for a work item
WORK_ITEM_LABELS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetLabels {{
            labels(first: {WORK_ITEMS_NESTED_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining discussions for a work item
WORK_ITEM_DISCUSSIONS_QUERY = f"""
query($projectPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetNotes {{
            discussions(first: {NESTED_FIELD_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {NESTED_FIELD_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    body
                    createdAt
                    updatedAt
                    system
                    position {{
                      newLine
                      oldLine
                      newPath
                      oldPath
                      positionType
                    }}
                    author {{
                      username
                      name
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining discussions for a group-level work item (e.g., Epic)
WORK_ITEM_GROUP_DISCUSSIONS_QUERY = f"""
query($groupPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  group(fullPath: $groupPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetNotes {{
            discussions(first: {NESTED_FIELD_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                id
                notes(first: {NESTED_FIELD_SIZE}) {{
                  pageInfo {{ hasNextPage endCursor }}
                  nodes {{
                    id
                    body
                    createdAt
                    updatedAt
                    system
                    author {{
                      username
                      name
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining assignees for a group work item (Epic)
WORK_ITEM_GROUP_ASSIGNEES_QUERY = f"""
query($groupPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  group(fullPath: $groupPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetAssignees {{
            assignees(first: {NESTED_FIELD_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                username
                name
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch remaining labels for a group work item (Epic)
WORK_ITEM_GROUP_LABELS_QUERY = f"""
query($groupPath: ID!, $iid: String!, $workItemType: IssueType!, $cursor: String) {{
  group(fullPath: $groupPath) {{
    workItems(iid: $iid, types: [$workItemType]) {{
      nodes {{
        widgets {{
          __typename
          ... on WorkItemWidgetLabels {{
            labels(first: {NESTED_FIELD_SIZE}, after: $cursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                title
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to fetch releases for a project
RELEASES_QUERY = f"""
query($projectPath: ID!, $cursor: String) {{
  project(fullPath: $projectPath) {{
    releases(first: {NODE_SIZE}, after: $cursor) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        tagName
        name
        description
        createdAt
        releasedAt
        author {{
          username
          name
        }}
        commit {{
          sha
          title
          message
        }}
        milestones {{
          nodes {{
            id
            title
          }}
        }}
        assets {{
          count
          links {{
            nodes {{
              name
              url
              linkType
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

# GraphQL query to validate multiple projects exist (max 50 at a time)
VALIDATE_PROJECTS_QUERY = """
query($projectPaths: [String!]!) {
  projects(fullPaths: $projectPaths) {
    nodes {
      fullPath
    }
  }
}
"""
