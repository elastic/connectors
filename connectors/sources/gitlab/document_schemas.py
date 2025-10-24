#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""TypedDict schemas for GitLab Elasticsearch documents."""

from typing import Any, Literal, TypedDict
from typing_extensions import NotRequired



class NoteItem(TypedDict):
    """Schema for note/comment items in issues, merge requests, and work items."""

    id: str
    body: str
    created_at: str
    updated_at: str
    system: bool
    author: str | None
    author_name: str | None
    position: NotRequired[dict[str, Any]]


class EpicChild(TypedDict):
    """Schema for child work items in epic hierarchy."""

    id: str
    iid: int
    title: str


class LinkedWorkItem(TypedDict):
    """Schema for linked work items (related issues, epics, etc.)."""

    link_id: str
    link_type: str
    link_created_at: str
    link_updated_at: str
    work_item_id: str
    work_item_iid: int
    work_item_title: str


class AssetLink(TypedDict):
    """Schema for release asset links."""

    name: str
    url: str
    type: str


class ProjectDocument(TypedDict):
    """Schema for GitLab project documents indexed to Elasticsearch."""

    _id: str
    _timestamp: str
    type: Literal["Project"]
    id: str  # project_id field in Elasticsearch
    name: str
    path: str
    full_path: str
    description: str | None
    visibility: str
    star_count: int
    forks_count: int
    created_at: str
    last_activity_at: str | None
    archived: bool | None
    default_branch: str | None
    web_url: str


class IssueDocument(TypedDict):
    """Schema for GitLab issue documents indexed to Elasticsearch."""

    _id: str
    _timestamp: str
    type: Literal["Issue"]
    project_id: str
    project_path: str
    iid: int
    title: str
    description: str | None
    state: str
    created_at: str
    updated_at: str
    closed_at: str | None
    web_url: str
    author: str | None
    author_name: str | None
    assignees: list[str]
    labels: list[str]
    notes: NotRequired[list[NoteItem] | None]


class MergeRequestDocument(TypedDict):
    """Schema for GitLab merge request documents indexed to Elasticsearch."""

    _id: str
    _timestamp: str
    type: Literal["Merge Request"]
    project_id: str
    project_path: str
    iid: int
    title: str
    description: str | None
    state: str
    created_at: str
    updated_at: str
    merged_at: str | None
    closed_at: str | None
    web_url: str
    source_branch: str
    target_branch: str
    author: str | None
    author_name: str | None
    assignees: list[str]
    reviewers: list[str]
    approved_by: list[str]
    merged_by: str | None
    labels: list[str]
    notes: NotRequired[list[NoteItem] | None]


class WorkItemDocument(TypedDict):
    """Schema for GitLab work item documents (Issues, Epics, Tasks) indexed to Elasticsearch.

    Work items can belong to either a project or a group, so the parent fields are dynamic.
    """

    # Always-required fields
    _id: str
    _timestamp: str
    type: str
    iid: int
    title: str
    description: str | None
    state: str
    created_at: str
    updated_at: str
    closed_at: str | None
    web_url: str
    author: str | None
    author_name: str | None
    assignees: list[str]
    labels: list[str]

    # Optional fields
    notes: NotRequired[list[NoteItem] | None]
    # Dynamic parent fields (project_id/project_path OR group_id/group_path)
    project_id: NotRequired[str]
    project_path: NotRequired[str]
    group_id: NotRequired[str]
    group_path: NotRequired[str]
    # Epic-specific fields
    parent_epic_iid: NotRequired[int | None]
    parent_epic_title: NotRequired[str | None]
    children_count: NotRequired[int | None]
    children: NotRequired[list[EpicChild] | None]
    linked_items_count: NotRequired[int | None]
    linked_items: NotRequired[list[LinkedWorkItem] | None]


class ReleaseDocument(TypedDict):
    """Schema for GitLab release documents indexed to Elasticsearch."""

    _id: str
    _timestamp: str
    type: Literal["Release"]
    project_id: str
    project_path: str
    tag_name: str
    name: str | None
    description: str | None
    created_at: str
    released_at: str | None
    author: str | None
    author_name: str | None
    milestones: list[str]
    asset_count: int
    commit_sha: NotRequired[str | None]
    commit_title: NotRequired[str | None]
    asset_links: NotRequired[list[AssetLink] | None]


class FileDocument(TypedDict):
    """Schema for GitLab file documents (READMEs) indexed to Elasticsearch."""

    _id: str
    _timestamp: str
    type: Literal["File"]
    project_id: str
    project_path: str
    file_path: str
    file_name: str
    extension: str
    web_url: str


# Union type for all GitLab documents
GitLabDocument = (
    ProjectDocument
    | MergeRequestDocument
    | WorkItemDocument
    | ReleaseDocument
    | FileDocument
)
