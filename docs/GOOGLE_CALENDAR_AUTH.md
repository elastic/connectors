# Google Calendar Authentication Guide

This guide explains how to set up service account authentication for the Google Calendar connector.

## Prerequisites

- A Google account with administrative privileges for your Google Workspace domain
- Access to [Google Cloud Console](https://console.cloud.google.com/)

## Step 1: Create a Google Cloud Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Click on the project dropdown at the top of the page
3. Click on "New Project"
4. Enter a name for your project and click "Create"
5. Wait for the project to be created and then select it from the project dropdown

## Step 2: Enable the Google Calendar API

1. In your Google Cloud project, navigate to "APIs & Services" > "Library"
2. Search for "Google Calendar API"
3. Click on the Google Calendar API card
4. Click "Enable"

## Step 3: Create a Service Account

1. Navigate to "APIs & Services" > "Credentials"
2. Click "Create Credentials" and select "Service Account"
3. Enter a name for your service account
4. (Optional) Add a description
5. Click "Create and Continue"
6. In the "Grant this service account access to project" section, you can skip this step by clicking "Continue"
7. In the "Grant users access to this service account" section, you can skip this step by clicking "Done"
8. Your service account has now been created

## Step 4: Create a Service Account Key

1. In the "Service Accounts" section, click on the service account you just created
2. Click on the "Keys" tab
3. Click "Add Key" and select "Create new key"
4. Select "JSON" as the key type
5. Click "Create"
6. The JSON key file will be downloaded to your computer
7. Keep this file secure, as it contains sensitive information

## Step 5: Set Up Domain-Wide Delegation

To allow the service account to access user data in your Google Workspace domain, you need to set up domain-wide delegation:

1. In the Google Cloud Console, navigate to "APIs & Services" > "Credentials"
2. Click on the service account you created
3. Click on the "Details" tab
4. Scroll down to "Show domain-wide delegation"
5. Enable "Domain-wide delegation"
6. Save the changes

Next, you need to authorize the service account in your Google Workspace Admin Console:

1. Go to your [Google Workspace Admin Console](https://admin.google.com/)
2. Navigate to "Security" > "API controls"
3. In the "Domain-wide delegation" section, click "Manage Domain Wide Delegation"
4. Click "Add new"
5. Enter the Client ID of your service account (this is a numeric ID found in the service account details)
6. In the "OAuth scopes" field, enter: `https://www.googleapis.com/auth/calendar.readonly`
7. Click "Authorize"

## Step 6: Configure the Connector

To use the service account with the Google Calendar connector, you need to provide:

1. The service account JSON key file
2. The email address of a user to impersonate

Configure the connector with the following settings:

```yaml
service_account_credentials: |
  {
    "type": "service_account",
    "project_id": "your-project-id",
    "private_key_id": "your-private-key-id",
    "private_key": "your-private-key",
    "client_email": "your-service-account-email",
    "client_id": "your-client-id",
    ...
  }
subject: "user@yourdomain.com"  # Email address of the user to impersonate
include_freebusy: false  # Set to true if you want to include free/busy data
```

Replace the `service_account_credentials` with the contents of your JSON key file, and `subject` with the email address of the user whose calendars you want to access.

## Important Notes

1. **Security**: 
   - Keep your service account key file secure
   - Do not commit it to version control
   - Consider using environment variables or secure vaults to store the credentials

2. **Scopes**:
   - The Google Calendar connector requires the `https://www.googleapis.com/auth/calendar.readonly` scope
   - If you need additional permissions, add the appropriate scopes in the Google Workspace Admin Console

3. **User Impersonation**:
   - The service account will impersonate the user specified in the `subject` field
   - This user must be a member of your Google Workspace domain
   - The connector will only be able to access calendars that this user has permission to view

## Troubleshooting

- **Authentication Failed**: Ensure that the service account has been properly set up with domain-wide delegation and the correct scopes.
- **Access Denied**: Check that the impersonated user has access to the calendars you're trying to fetch.
- **Invalid Service Account JSON**: Verify that the service account JSON is correctly formatted and contains all required fields.
- **Missing Scopes**: Ensure that the service account has been granted the necessary OAuth scopes in the Google Workspace Admin Console.