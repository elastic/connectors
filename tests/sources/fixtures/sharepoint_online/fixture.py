#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import io
import os
import random
import string

from flask import Flask, escape, request, send_file
from flask_limiter.util import get_remote_address

app = Flask(__name__)

@app.route("/<string:tenant_id>/oauth2/v2.0/token", methods=["POST"])
def get_graph_token(tenant_id):
    return {
        "access_token": f"fake-graph-api-token-{tenant_id}",
        "expires_in": 3699
    }

@app.route("/<string:tenant_id>/tokens/OAuth/2", methods=["POST"])
def get_rest_token(tenant_id):
    return {
        "access_token": f"fake-rest-api-token-{tenant_id}",
        "expires_in": 3699
    }

@app.route("/common/userrealm/", methods=["GET"])
def get_tenant():
    return {"NameSpaceType":"Managed","Login":"cj@something.onmicrosoft.com","DomainName":"something.onmicrosoft.com","FederationBrandName":"Elastic","TenantBrandingInfo":None,"cloud_instance_name":"microsoftonline.com"}


@app.route("/sites/", methods=["GET"])
def get_site_collections():
    return {
                '@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#sites(siteCollection,webUrl)',
                'value': [
                    {
                        'webUrl': 'https://enterprisesearch.sharepoint.com/',
                        'siteCollection': {
                            'hostname': 'enterprisesearch.sharepoint.com',
                            'root': {}
                        }
                    }
                ]
            }

@app.route("/sites/<string:site_id>/sites", methods=["GET"])
def get_sites(site_id):
    return {
        "@odata.context":"https://graph.microsoft.com/v1.0/$metadata#sites",
        "value":[
            {
                "createdDateTime":"2023-05-31T16:08:46Z",
                "description":"It's a private one",
                "id":"enterprisesearch.sharepoint.com,792c7c37-803b-47af-88c2-88d8707aab65,e6ead828-d7a5-4c72-b8e7-0687c6a078e7",
                "lastModifiedDateTime":"2023-05-31T16:08:48Z",
                "name":"PrivateSubsite",
                "webUrl":"http://localhost:10337/sites/ArtemsSiteForTesting",
                "displayName":"Private Subsite"
            }
        ]
    }

@app.route("/sites/<string:site_id>/drives", methods=["GET"])
def get_site_drives(site_id):
    return {
            '@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#drives',
            'value': [
                {
                    'createdDateTime': '2023-05-31T16:08:47Z',
                    'description': '',
                    'id': 'b!N3wseTuAr0eIwojYcHqrZSjY6ual13JMuOcGh8ageOexVaqPcFq1R7TWeUCQv6dv',
                    'lastModifiedDateTime': '2023-05-31T16:08:47Z',
                    'name': 'Documents',
                    'webUrl': 'http://localhost:10337/sites/ArtemsSiteForTesting/Shared Documents',
                    'driveType': 'documentLibrary',
                    'createdBy': {
                        'user': {
                            'email': 'demo@enterprisesearch.onmicrosoft.com', 
                            'id': 'baa37bda-0dd1-4799-ae22-f3476c2cf58d',
                            'displayName': 'Enterprise Search'
                            }
                        },
                    'owner': {
                        'user': {
                            'email': 'demo@enterprisesearch.onmicrosoft.com',
                            'id': 'baa37bda-0dd1-4799-ae22-f3476c2cf58d',
                            'displayName': 'Enterprise Search'
                            }
                        }, 
                    'quota': {
                        'deleted': 79501, 
                        'remaining': 27487790614899,
                        'state': 'normal', 
                        'total': 27487790694400, 
                        'used': 0
                        }
                    }
                ]
            }

@app.route("/drives/<string:drive_id>/root", methods=["GET"])
def get_drive_root(drive_id):
    return {
            '@odata.context': f"https://graph.microsoft.com/v1.0/$metadata#drives('{drive_id}')/root/$entity", 
            'createdDateTime': '2023-05-21T05:23:51Z', 
            'id': '01WDQZWT56Y2GOVW7725BZO354PWSELRRZ', 
            'lastModifiedDateTime': '2023-05-21T05:23:51Z', 
            'name': 'root', 
            'webUrl': 'http://localhost:10337/sites/PrivateSpace/Shared Documents', 
            'size': 0, 
            'parentReference': {
                'driveType': 'documentLibrary', 
                'driveId': drive_id
            }, 
            'fileSystemInfo': {
                'createdDateTime': '2023-05-21T05:23:51Z', 
                'lastModifiedDateTime': '2023-05-21T05:23:51Z'
            }, 
            'folder': {
                'childCount': 0
            }, 
            'root': {}
        }

@app.route("/drives/<string:drive_id>/items/<string:drive_item_id>/children", methods=["GET"])
def get_drive_item_children(drive_id, drive_item_id):
    return {
            '@odata.context': f"https://graph.microsoft.com/v1.0/$metadata#drives('{drive_id}')/items('{drive_item_id}')/children",
            'value': []
        }

@app.route("/sites/<string:site_id>/lists", methods=["GET"])
def get_site_lists(site_id):
    return {
            '@odata.context': "https://graph.microsoft.com/v1.0/$metadata#sites('enterprisesearch.sharepoint.com%2C792c7c37-803b-47af-88c2-88d8707aab65%2Ce6ead828-d7a5-4c72-b8e7-0687c6a078e7')/lists",
            'value': [
                {
                    '@odata.etag': '"8faa55b1-5a70-47b5-b4d6-794090bfa76f,1"',
                    'createdDateTime': '2023-05-31T16:08:47Z',
                    'description': '',
                    'eTag': '"8faa55b1-5a70-47b5-b4d6-794090bfa76f,1"',
                    'id': '8faa55b1-5a70-47b5-b4d6-794090bfa76f',
                    'lastModifiedDateTime': '2023-05-31T16:08:48Z',
                    'name': 'Shared Documents',
                    'webUrl': 'http://localhost:10337/sites/PrivateSubsite/Shared Documents',
                    'displayName': 'Documents',
                    'createdBy': {
                        'user': {
                            'email': 'demo@enterprisesearch.onmicrosoft.com',
                            'id': 'baa37bda-0dd1-4799-ae22-f3476c2cf58d',
                            'displayName': 'Enterprise Search'
                        }
                    }, 
                    'parentReference': {
                        'siteId': 'enterprisesearch.sharepoint.com,792c7c37-803b-47af-88c2-88d8707aab65,e6ead828-d7a5-4c72-b8e7-0687c6a078e7'
                    },
                    'list': {
                        'contentTypesEnabled': False,
                        'hidden': False,
                        'template': 'documentLibrary'
                    }
                }
            ]
        }

@app.route("/sites/<string:site_id>/lists/<string:list_id>/items", methods=["GET"])
def get_site_list_items(site_id, list_id):
    return {
            '@odata.context': "https://graph.microsoft.com/v1.0/$metadata#sites('enterprisesearch.sharepoint.com%2C792c7c37-803b-47af-88c2-88d8707aab65%2C84b8c2b1-3d4a-46f7-93d6-d2c6c4e9523a')/lists('a3f3ce79-e34d-4c03-8f10-e1399e661a65')/items(fields())",
            'value': [
                {
                    '@odata.etag': '"35aef603-c870-4326-91c3-ffdf59c29677,2"',
                    'createdDateTime': '2023-06-06T12:44:01Z',
                    'eTag': '"35aef603-c870-4326-91c3-ffdf59c29677,2"',
                    'id': '1', 
                    'lastModifiedDateTime': '2023-06-06T12:44:01Z',
                    'webUrl': 'http://localhost:10337/sites/ArtemsSiteForTesting/Lists/asldlasdla/1_.000',
                    'createdBy': {
                        'user': {
                            'email': 'demo@enterprisesearch.onmicrosoft.com', 
                            'id': 'baa37bda-0dd1-4799-ae22-f3476c2cf58d', 
                            'displayName': 'Enterprise Search'
                        }
                    }, 
                    'lastModifiedBy': {
                        'user': {
                            'email': 'demo@enterprisesearch.onmicrosoft.com', 
                            'id': 'baa37bda-0dd1-4799-ae22-f3476c2cf58d', 
                            'displayName': 'Enterprise Search'}
                    }, 
                    'parentReference': {
                        'id': 'c3429aae-3cd4-4ba8-a831-c575f8a65aa2', 
                        'siteId': 'enterprisesearch.sharepoint.com,792c7c37-803b-47af-88c2-88d8707aab65,84b8c2b1-3d4a-46f7-93d6-d2c6c4e9523a'
                    }, 
                    'contentType': {
                        'id': '0x0100B239ACAA35349546A923BB0F799FAD6E00C2E8F4FE62D4BC44AD1071D5AA4DE0B9', 
                        'name': 'Item'
                    }, 
                    'fields@odata.context': "https://graph.microsoft.com/v1.0/$metadata#sites('enterprisesearch.sharepoint.com%2C792c7c37-803b-47af-88c2-88d8707aab65%2C84b8c2b1-3d4a-46f7-93d6-d2c6c4e9523a')/lists('a3f3ce79-e34d-4c03-8f10-e1399e661a65')/items('1')/fields/$entity",
                    'fields': {
                        '@odata.etag': '"35aef603-c870-4326-91c3-ffdf59c29677,2"', 
                        'Title': 'Hello world',
                        'LinkTitle': 'Hello world', 
                        'id': '1', 
                        'ContentType': 'Item', 
                        'Modified': '2023-06-06T12:44:01Z', 
                        'Created': '2023-06-06T12:44:01Z', 
                        'AuthorLookupId': '6', 
                        'EditorLookupId': '6', 
                        '_UIVersionString': '2.0', 
                        'Attachments': True, 
                        'Edit': '', 
                        'LinkTitleNoMenu': 'Hello world', 
                        'ItemChildCount': '0', 
                        'FolderChildCount': '0', 
                        '_ComplianceFlags': '', 
                        '_ComplianceTag': '', 
                        '_ComplianceTagWrittenTime': '', 
                        '_ComplianceTagUserId': ''
                        }
                    }
                ]
            }

@app.route("/sites/<string:site_name>/_api/lists/GetByTitle('<string:list_title>')/items(<string:list_item_id>)")
def get_list_item_attachments(site_name, list_title, list_item_id):
    expand = request.args.get(escape('$expand'))
    if expand and "AttachmentFiles" in expand:
        return {
                'odata.metadata': "https://enterprisesearch.sharepoint.com/sites/Artem'sSiteForTesting/_api/$metadata#SP.ListData.Custom_x0020_Made_x0020_ListListItems/@Element",
                'odata.type': 'SP.Data.Custom_x0020_Made_x0020_ListListItem',
                'odata.id': '5484fa3e-6288-4be6-9155-5355bde6a6fc',
                'odata.etag': '"3"',
                'odata.editLink': "Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items(1)",
                'AttachmentFiles@odata.navigationLinkUrl': "Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items(1)/AttachmentFiles",
                'AttachmentFiles': [
                    {
                        'odata.type': 'SP.Attachment',
                        'odata.id': "http://localhost:10337/sites/Artem'sSiteForTesting/_api/Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items(1)/AttachmentFiles('pride-and-prejudice-145.txt')", 
                        'odata.editLink': "Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items(1)/AttachmentFiles('pride-and-prejudice-145.txt')", 
                        'FileName': 'pride-and-prejudice-145.txt', 
                        'FileNameAsPath': {
                            'DecodedUrl': 'pride-and-prejudice-145.txt'
                        },
                        'ServerRelativePath': {
                            'DecodedUrl': "/sites/Artem'sSiteForTesting/Lists/Custom Made List/Attachments/1/pride-and-prejudice-145.txt"
                        }, 
                        'ServerRelativeUrl': "/sites/Artem'sSiteForTesting/Lists/Custom Made List/Attachments/1/pride-and-prejudice-145.txt"
                    }, 
                    {
                        'odata.type': 'SP.Attachment', 
                        'odata.id': "http://localhost:10337/sites/Artem'sSiteForTesting/_api/Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items(1)/AttachmentFiles('txt.log')",
                        'odata.editLink': "Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items(1)/AttachmentFiles('txt.log')",
                        'FileName': 'txt.log',
                        'FileNameAsPath': {
                            'DecodedUrl': 'txt.log'
                        },
                        'ServerRelativePath': {
                            'DecodedUrl': "/sites/Artem'sSiteForTesting/Lists/Custom Made List/Attachments/1/txt.log"
                        },
                        'ServerRelativeUrl': "/sites/Artem'sSiteForTesting/Lists/Custom Made List/Attachments/1/txt.log"
                    }
                ],
                'FileSystemObjectType': 0,
                'Id': 1,
                'ServerRedirectedEmbedUri': None,
                'ServerRedirectedEmbedUrl': '',
                'ContentTypeId': '0x01008A3E09918C44C042809E94957AE584ED0047CD5B88506EC84B8B4A0D415BC12E3D',
                'Title': 'File with attachment',
                'OData__ColorTag': None,
                'ComplianceAssetId': None,
                'ID': 1,
                'Modified': '2023-05-25T14:59:06Z',
                'Created': '2023-05-25T14:58:42Z',
                'AuthorId': 6,
                'EditorId': 6,
                'OData__UIVersionString': '3.0',
                'Attachments': True,
                'GUID': '79db5b37-4672-4826-abfe-8dc847d8baa7'
        }
    else:
        raise Exception("Nope")

@app.route("/sites/<string:site_name>/_api/web/lists/GetByTitle('Site Pages')/items")
def get_list_item_pages(site_name):
       return {
           'odata.metadata': "https://enterprisesearch.sharepoint.com/sites/ArtemsSiteForTesting/_api/$metadata#SP.ListData.SitePagesItems",
           'value': [
               {
                   'odata.type': 'SP.Data.SitePagesItem',
                   'odata.id': 'c1393c35-8dd0-4440-9563-ac8e4d277227',
                   'odata.etag': '"3"',
                   'odata.editLink': "Web/Lists(guid'0deb180c-9812-4ec6-b652-cd80214bb257')/Items(1)",
                   'FileSystemObjectType': 0,
                   'Id': 1,
                   'ServerRedirectedEmbedUri': None,
                   'ServerRedirectedEmbedUrl': '',
                   'ContentTypeId': '0x0101009D1CB255DA76424F860D91F20E6C411800534CF5870D924347B78CC6E21791F9E2',
                   'OData__ColorTag': None,
                   'ComplianceAssetId': None,
                   'WikiField': None,
                   'Title': 'Home',
                   'CanvasContent1': None,
                   'BannerImageUrl': None,
                   'Description': None,
                   'PromotedState': 0.0,
                   'FirstPublishedDate': None,
                   'LayoutWebpartsContent': None,
                   'OData__AuthorBylineId': None,
                   '_AuthorBylineStringId': None,
                   'OData__TopicHeader': None,
                   'OData__SPSitePageFlags': None,
                   'OData__SPCallToAction': None,
                   'OData__OriginalSourceUrl': None,
                   'OData__OriginalSourceSiteId': None,
                   'OData__OriginalSourceWebId': None,
                   'OData__OriginalSourceListId': None,
                   'OData__OriginalSourceItemId': None,
                   'ID': 1,
                   'Created': '2023-05-31T16:08:48Z',
                   'AuthorId': 6,
                   'Modified': '2023-05-31T16:08:48Z',
                   'EditorId': 6,
                   'OData__CopySource': None,
                   'CheckoutUserId': None,
                   'OData__UIVersionString': '1.0',
                   'GUID': 'ed7424f3-23d0-4c4a-926a-def0f36c7e4a'
                   }
               ]
           }

@app.route("/sites/<string:site_name>/_api/Web/Lists(guid'<string:list_id>')/Items(<string:list_item_id>)/AttachmentFiles('<string:file_name>')/$value", methods=["GET"])
def get_list_item_attachment(site_name, list_id, list_item_id, file_name):
    return b"lalala lalala this is some content"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10337)
