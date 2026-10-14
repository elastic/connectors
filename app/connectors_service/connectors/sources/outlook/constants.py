#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

OUTLOOK_SERVER = "outlook_server"
OUTLOOK_CLOUD = "outlook_cloud"

DEPRECATION_WARNINGS = {
    OUTLOOK_CLOUD: (
        "The Outlook connector is deprecated and receives no new features. Its "
        "Outlook Cloud mode uses Exchange Web Services (EWS) with the "
        "full_access_as_app permission, which Microsoft is retiring in Exchange "
        "Online. Until then, syncs only work if a tenant admin sets EWSEnabled to "
        "True and lists this connector's application ID in EWSAllowedAppIDs. EWS "
        "is permanently turned off on April 1, 2027. Plan to migrate to the "
        "upcoming Graph-based connector (outlook_cloud service type) when it "
        "is available."
    ),
    OUTLOOK_SERVER: (
        "The Outlook connector is deprecated and receives no new features. For "
        "on-premises Exchange, migrate to the Exchange Server connector "
        "(exchange_server service type), which syncs the same content."
    ),
}
API_SCOPE = "https://graph.microsoft.com/.default"
EWS_ENDPOINT = "https://outlook.office365.com/EWS/Exchange.asmx"
TOP = 999
