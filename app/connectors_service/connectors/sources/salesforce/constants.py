#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

SALESFORCE_EMULATOR_HOST = os.environ.get("SALESFORCE_EMULATOR_HOST")
RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

RETRIES = 3
RETRY_INTERVAL = 1

BASE_URL = "https://<domain>.my.salesforce.com"
API_VERSION = "v59.0"
TOKEN_ENDPOINT = "/services/oauth2/token"  # noqa S105
QUERY_ENDPOINT = f"/services/data/{API_VERSION}/query"
SOSL_SEARCH_ENDPOINT = f"/services/data/{API_VERSION}/search"
DESCRIBE_ENDPOINT = f"/services/data/{API_VERSION}/sobjects"
DESCRIBE_SOBJECT_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/<sobject>/describe"
# https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_blob_retrieve.htm
CONTENT_VERSION_DOWNLOAD_ENDPOINT = f"/services/data/{API_VERSION}/sobjects/ContentVersion/<content_version_id>/VersionData"
OFFSET = 200

OBJECT_READ_PERMISSION_USERS = "SELECT AssigneeId FROM PermissionSetAssignment WHERE PermissionSetId IN (SELECT ParentId FROM ObjectPermissions WHERE PermissionsRead = true AND SObjectType = '{sobject}')"
USERNAME_FROM_IDS = "SELECT Name, Email FROM User WHERE Id IN {user_list}"
FILE_ACCESS = "SELECT ContentDocumentId, LinkedEntityId, LinkedEntity.Name FROM ContentDocumentLink WHERE ContentDocumentId = '{document_id}'"
WILDCARD = "*"

STANDARD_SOBJECTS = [
    "Account",
    "Opportunity",
    "Contact",
    "Lead",
    "Campaign",
    "Case",
]

RELEVANT_SOBJECTS = [
    "Account",
    "Campaign",
    "Case",
    "CaseComment",
    "CaseFeed",
    "Contact",
    "ContentDocument",
    "ContentDocumentLink",
    "ContentVersion",
    "EmailMessage",
    "FeedComment",
    "Lead",
    "Opportunity",
    "User",
]
RELEVANT_SOBJECT_FIELDS = [
    "AccountId",
    "BccAddress",
    "BillingAddress",
    "Body",
    "CaseNumber",
    "CcAddress",
    "CommentBody",
    "CommentCount",
    "Company",
    "ContentSize",
    "ConvertedAccountId",
    "ConvertedContactId",
    "ConvertedDate",
    "ConvertedOpportunityId",
    "Department",
    "Description",
    "Email",
    "EndDate",
    "FileExtension",
    "FirstOpenedDate",
    "FromAddress",
    "FromName",
    "IsActive",
    "IsClosed",
    "IsDeleted",
    "LastEditById",
    "LastEditDate",
    "LastModifiedById",
    "LatestPublishedVersionId",
    "LeadSource",
    "LinkUrl",
    "MessageDate",
    "Name",
    "OwnerId",
    "ParentId",
    "Phone",
    "PhotoUrl",
    "Rating",
    "StageName",
    "StartDate",
    "Status",
    "StatusParentId",
    "Subject",
    "TextBody",
    "Title",
    "ToAddress",
    "Type",
    "VersionDataUrl",
    "VersionNumber",
    "Website",
    "UserType",
]
