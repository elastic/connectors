#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import os
import random
import string
import time

from faker import Faker
from flask import Flask, escape, request
from flask_limiter import HEADERS, Limiter
from flask_limiter.util import get_remote_address
from yattag import Doc

app = Flask(__name__)

THROTTLING = os.environ.get("THROTTLING", False)
PRE_REQUEST_SLEEP = float(os.environ.get("PRE_REQUEST_SLEEP", "0.05"))

if THROTTLING:
    limiter = Limiter(
        get_remote_address,
        app=app,
        storage_uri="memory://",
        application_limits=[
            "6000 per minute",
            "600000 per day",
        ],  # Sharepoint 50k+ licences limits
        retry_after="delta_seconds",
        headers_enabled=True,
        header_name_mapping={
            HEADERS.LIMIT: "RateLimit-Limit",
            HEADERS.RESET: "RateLimit-Reset",
            HEADERS.REMAINING: "RateLimit-Remaining",
        },
    )

seed = 1597463007

TOKEN_EXPIRATION_TIMEOUT = 3699  # seconds

ROOT = os.environ.get(
    "ROOT_HOST_URL", "http://127.0.0.1:10337"
)  # possible to override if hosting somewhere else

TENANT = "functionaltest.sharepoint.fake"

random.seed(seed)
fake = Faker()
fake.seed_instance(seed)

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "extra_small":
        NUMBER_OF_SITES = 1
        NUMBER_OF_DRIVE_ITEMS = 10
        NUMBER_OF_PAGES = 1
        NUMBER_OF_LISTS = 1
        NUMBER_OF_LIST_ITEMS = 5
        NUMBER_OF_LIST_ITEM_ATTACHMENTS = 1
    case "small":
        NUMBER_OF_SITES = 5
        NUMBER_OF_DRIVE_ITEMS = 100
        NUMBER_OF_PAGES = 10
        NUMBER_OF_LISTS = 5
        NUMBER_OF_LIST_ITEMS = 5
        NUMBER_OF_LIST_ITEM_ATTACHMENTS = 1
    case "medium":
        NUMBER_OF_SITES = 10
        NUMBER_OF_DRIVE_ITEMS = 200
        NUMBER_OF_PAGES = 10
        NUMBER_OF_LISTS = 10
        NUMBER_OF_LIST_ITEMS = 10
        NUMBER_OF_LIST_ITEM_ATTACHMENTS = 5
    case "large":
        NUMBER_OF_SITES = 30
        NUMBER_OF_DRIVE_ITEMS = 400
        NUMBER_OF_PAGES = 50
        NUMBER_OF_LISTS = 100
        NUMBER_OF_LIST_ITEMS = 10
        NUMBER_OF_LIST_ITEM_ATTACHMENTS = 5


fake_large_image = fake.pystr(min_chars=1 << 15, max_chars=1 << 15 + 1)


def _generate_html(text, number_of_large_images):
    images = []
    for _ in range(number_of_large_images):
        images.append(f"<img src='{fake_large_image}'/>")

    large_html = f"<html><head></head><body><div>{text}</div><div>{'<br/>'.join(images)}</div></body></html>"

    return large_html


small_text = _generate_html(fake.text(max_nb_chars=500), 0)
medium_text = _generate_html(fake.text(max_nb_chars=5000), 1)
large_text = _generate_html(fake.text(max_nb_chars=10000), 5)

small_text_bytesize = len(small_text.encode("utf-8"))
medium_text_bytesize = len(medium_text.encode("utf-8"))
large_text_bytesize = len(large_text.encode("utf-8"))

fake_binary_image = fake.pystr(min_chars=65536, max_chars=65536 << 1)

TOTAL_RECORD_COUNT = NUMBER_OF_SITES * (
    1 * NUMBER_OF_DRIVE_ITEMS
    + NUMBER_OF_PAGES
    + NUMBER_OF_LISTS * NUMBER_OF_LIST_ITEMS * NUMBER_OF_LIST_ITEM_ATTACHMENTS
)


def get_num_docs():
    print(TOTAL_RECORD_COUNT)


class AutoIncrement:
    def __init__(self):
        self.val = 1

    def get(self):
        value = self.val
        self.val += 1
        return value


class RandomDataStorage:
    """
    RandomDataStorage class is responsible for generation of the
    tree of data for Sharepoint Online.

    When instantiated with a seed, this seed will be used to generate objects.

    As soon as any object is requested or `generate` method is called, data is
    generated.

    Only data that is important for testing is generated - ids, types of files,
    binary file content, timestamps, etc.

    """

    def __init__(self):
        self.autoinc = AutoIncrement()

        self.tenants = []
        self.sites = []
        self.sites_by_site_id = {}
        self.sites_by_drive_id = {}
        self.sites_by_site_name = {}
        self.site_drives = {}
        self.drive_items = {}
        self.drive_item_content = {}
        self.site_pages = {}
        self.site_lists = {}
        self.site_list_items = {}
        self.site_lists_by_list_id = {}
        self.site_lists_by_list_name = {}
        self.list_item_attachment_content = {}

    def generate(self):
        self.tenants = [TENANT]

        for _ in range(NUMBER_OF_SITES):
            # Generate Sites
            site = {
                "id": str(fake.uuid4()),
                "name": fake.company(),
                "description": fake.paragraph(),
            }

            self.sites.append(site)
            self.sites_by_site_id[site["id"]] = site
            self.sites_by_site_name[site["name"]] = site

            # Generate 1 root drive
            drive = {"id": str(fake.uuid4()), "description": fake.paragraph()}

            self.sites_by_drive_id[drive["id"]] = site
            self.site_drives[site["id"]] = [drive]
            self.site_pages[site["id"]] = []
            self.site_lists[site["id"]] = []
            self.drive_items[drive["id"]] = []

            # Generate Drive Items
            for j in range(NUMBER_OF_DRIVE_ITEMS):
                drive_item = {
                    "id": self.generate_sharepoint_id(),
                }

                if j % 20 == 0:  # Every 20th is a folder
                    drive_item["folder"] = True
                    drive_item["name"] = fake.word()
                else:
                    drive_item["folder"] = False
                    drive_item["name"] = fake.file_name(extension="html")

                    if j % 5 == 0:  # Every 5th is medium
                        self.drive_item_content[drive_item["id"]] = medium_text
                        drive_item["size"] = medium_text_bytesize
                    elif j % 17 == 0:  # Every 17th is large
                        self.drive_item_content[drive_item["id"]] = large_text
                        drive_item["size"] = large_text_bytesize
                    else:  # Every other is small
                        self.drive_item_content[drive_item["id"]] = small_text
                        drive_item["size"] = small_text_bytesize

                self.drive_items[drive["id"]].append(drive_item)

            # Generate Site Pages
            for _ in range(NUMBER_OF_PAGES):
                doc, tag, text = Doc().tagtext()

                with tag("html"):
                    with tag("body", id="hello"):
                        with tag("h1"):
                            text(fake.word())
                        with tag("p"):
                            text(fake.paragraph())
                        with tag("p"):
                            text(fake.paragraph())
                        with tag("ul"):
                            with tag("li"):
                                text(fake.word())
                            with tag("li"):
                                text(fake.word())
                            with tag("li"):
                                text(fake.word())
                            doc.stag(
                                "img",
                                src=fake_binary_image,
                            )  # just fake invalid image as if it was base64-encoded png, purely to fill-in some data

                page = {
                    "id": str(self.autoinc.get()),
                    "odata.id": str(fake.uuid4()),
                    "guid": str(fake.uuid4()),
                    "content": doc.getvalue(),
                }

                self.site_pages[site["id"]].append(page)

            # Generate Lists
            for _ in range(NUMBER_OF_LISTS):
                site_list = {
                    "id": str(fake.uuid4()),
                    "name": f"{fake.word()} {fake.word()}",
                    "display_name": fake.word(),
                    "description": fake.paragraph(),
                }
                self.site_lists[site["id"]].append(site_list)
                self.site_lists_by_list_id[site_list["id"]] = site_list
                self.site_lists_by_list_name[site_list["name"]] = site_list
                self.site_list_items[site_list["id"]] = []
                self.list_item_attachment_content[site_list["id"]] = {}

                # Generate List Items
                for k in range(NUMBER_OF_LIST_ITEMS):
                    list_item = {
                        "id": str(k + 1),
                        "guid": str(fake.uuid4()),
                        "title": fake.word(),
                        "attachments": [],
                    }

                    self.list_item_attachment_content[site_list["id"]][
                        list_item["id"]
                    ] = {}

                    # Generate Attachments
                    for m in range(NUMBER_OF_LIST_ITEM_ATTACHMENTS):
                        list_item_attachment = {
                            "title": fake.file_name(extension="txt")
                        }

                        list_item["attachments"].append(list_item_attachment)

                        # Generate Attachment Content
                        generated_content = None
                        if m % 5 == 0:  # Every 5th item
                            generated_content = medium_text
                        elif m % 9 == 0:  # Every 9th item
                            generated_content = large_text
                        else:
                            generated_content = small_text

                        self.list_item_attachment_content[site_list["id"]][
                            list_item["id"]
                        ][list_item_attachment["title"]] = generated_content

                    self.site_list_items[site_list["id"]].append(list_item)

    def get_site_collections(self):
        results = []

        for tenant in self.tenants:
            results.append(
                {
                    "webUrl": f"https://{tenant}/",
                    "siteCollection": {
                        "hostname": tenant,
                        "root": {},
                    },
                }
            )

        return results

    def get_sites(self, skip=0, take=10):
        results = []

        for site in self.sites[skip:][:take]:
            results.append(
                {
                    "id": site["id"],
                    "name": site["name"],
                    "displayName": site["name"],
                    "description": site["description"],
                    "webUrl": f"{ROOT}/sites/{site['name']}",
                    "createdDateTime": "2023-05-31T16:08:46Z",
                    "lastModifiedDateTime": "2023-05-31T16:08:48Z",
                }
            )

        return results

    def get_site_drives(self, site_id):
        results = []

        site = self.sites_by_site_id[site_id]

        for drive in self.site_drives[site_id]:
            results.append(
                {
                    "id": drive["id"],
                    "name": "Documents",
                    "description": drive["description"],
                    "webUrl": f"{ROOT}/sites/{site['name']}/Shared Documents",
                    "createdDateTime": "2023-05-31T16:08:47Z",
                    "lastModifiedDateTime": "2023-05-31T16:08:47Z",
                    "driveType": "documentLibrary",
                    "createdBy": {
                        "user": {
                            "email": "demo@enterprisesearch.onmicrosoft.com",
                            "id": "baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                            "displayName": "Enterprise Search",
                        }
                    },
                    "owner": {
                        "user": {
                            "email": "demo@enterprisesearch.onmicrosoft.com",
                            "id": "baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                            "displayName": "Enterprise Search",
                        }
                    },
                    "quota": {
                        "deleted": 79501,
                        "remaining": 27487790614899,
                        "state": "normal",
                        "total": 27487790694400,
                        "used": 0,
                    },
                }
            )

        return results

    def get_site_pages(self, site_name, skip=0, take=10):
        results = []

        site = self.sites_by_site_name[site_name]
        site_id = site["id"]

        for site_page in self.site_pages[site_id][skip:][:take]:
            results.append(
                {
                    "odata.type": "SP.Data.SitePagesItem",
                    "odata.id": site_page["odata.id"],
                    "odata.etag": '"3"',
                    "odata.editLink": "Web/Lists(guid'0deb180c-9812-4ec6-b652-cd80214bb257')/Items(1)",
                    "FileSystemObjectType": 0,
                    "Id": site_page["id"],
                    "ServerRedirectedEmbedUri": None,
                    "ServerRedirectedEmbedUrl": "",
                    "ContentTypeId": "0x0101009D1CB255DA76424F860D91F20E6C411800534CF5870D924347B78CC6E21791F9E2",
                    "OData__ColorTag": None,
                    "ComplianceAssetId": None,
                    "WikiField": None,
                    "Title": "Home",
                    "CanvasContent1": site_page["content"],
                    "BannerImageUrl": None,
                    "Description": None,
                    "PromotedState": 0.0,
                    "FirstPublishedDate": None,
                    "LayoutWebpartsContent": None,
                    "OData__AuthorBylineId": None,
                    "_AuthorBylineStringId": None,
                    "OData__TopicHeader": None,
                    "OData__SPSitePageFlags": None,
                    "OData__SPCallToAction": None,
                    "OData__OriginalSourceUrl": None,
                    "OData__OriginalSourceSiteId": None,
                    "OData__OriginalSourceWebId": None,
                    "OData__OriginalSourceListId": None,
                    "OData__OriginalSourceItemId": None,
                    "ID": 1,
                    "Created": "2023-05-31T16:08:48Z",
                    "AuthorId": 6,
                    "Modified": "2023-05-31T16:08:48Z",
                    "EditorId": 6,
                    "OData__CopySource": None,
                    "CheckoutUserId": None,
                    "OData__UIVersionString": "1.0",
                    "GUID": site_page["guid"],
                }
            )

        return results

    def generate_sharepoint_id(self):
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=32))

    def get_drive_items(self, drive_id, skip=0, take=100):
        results = []

        site = self.sites_by_drive_id[drive_id]

        for item in self.drive_items[drive_id][skip:][:take]:
            drive_item = {
                "id": item["id"],
                "name": item["name"],
                "webUrl": f"{ROOT}/sites/{site['name']}/Shared Documents",
                "createdDateTime": "2023-05-21T05:23:51Z",
                "lastModifiedDateTime": "2023-05-21T05:23:51Z",
                "parentReference": {
                    "driveType": "documentLibrary",
                    "driveId": drive_id,
                },
                "fileSystemInfo": {
                    "createdDateTime": "2023-05-21T05:23:51Z",
                    "lastModifiedDateTime": "2023-05-21T05:23:51Z",
                },
            }

            if item["folder"]:
                drive_item["folder"] = {"childCount": 0}
                drive_item["size"] = 0
            else:
                drive_item["size"] = item["size"]
                drive_item[
                    "@microsoft.graph.downloadUrl"
                ] = f"{ROOT}/drives/{drive_id}/items/{item['id']}/content"

            results.append(drive_item)

        return results

    def get_drive_item_content(self, drive_item_id):
        return self.drive_item_content[drive_item_id]

    def get_site_lists(self, site_id, skip=0, take=100):
        results = []

        site = self.sites_by_site_id[site_id]

        for site_list in self.site_lists[site_id][skip:][:take]:
            results.append(
                {
                    "@odata.etag": '"8faa55b1-5a70-47b5-b4d6-794090bfa76f,1"',
                    "eTag": '"8faa55b1-5a70-47b5-b4d6-794090bfa76f,1"',
                    "id": site_list["id"],
                    "name": site_list["name"],
                    "description": site_list["description"],
                    "displayName": site_list["display_name"],
                    "createdDateTime": "2023-05-31T16:08:47Z",
                    "lastModifiedDateTime": "2023-05-31T16:08:48Z",
                    "webUrl": f"{ROOT}/sites/{site['name']}/{site_list['name']}",
                    "createdBy": {
                        "user": {
                            "email": "demo@enterprisesearch.onmicrosoft.com",
                            "id": "baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                            "displayName": "Enterprise Search",
                        }
                    },
                    "parentReference": {"siteId": site["id"]},
                    "list": {
                        "contentTypesEnabled": False,
                        "hidden": False,
                        "template": "documentLibrary",
                    },
                }
            )

        return results

    def get_site_list_items(self, site_id, list_id, skip=0, take=100):
        results = []

        list_ = self.site_lists_by_list_id[list_id]
        site = self.sites_by_site_id[site_id]

        for list_item in self.site_list_items[list_id][skip:][:take]:
            results.append(
                {
                    "id": list_item["id"],
                    "@odata.etag": '"35aef603-c870-4326-91c3-ffdf59c29677,2"',
                    "createdDateTime": "2023-06-06T12:44:01Z",
                    "eTag": '"35aef603-c870-4326-91c3-ffdf59c29677,2"',
                    "lastModifiedDateTime": "2023-06-06T12:44:01Z",
                    "webUrl": f"{ROOT}/sites/{site['name']}/Lists/{list_['name']}/1_.000",
                    "createdBy": {
                        "user": {
                            "email": "demo@enterprisesearch.onmicrosoft.com",
                            "id": "baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                            "displayName": "Enterprise Search",
                        }
                    },
                    "lastModifiedBy": {
                        "user": {
                            "email": "demo@enterprisesearch.onmicrosoft.com",
                            "id": "baa37bda-0dd1-4799-ae22-f3476c2cf58d",
                            "displayName": "Enterprise Search",
                        }
                    },
                    "parentReference": {
                        "id": list_id,
                        "siteId": site_id,
                    },
                    "contentType": {
                        "id": "0x0100B239ACAA35349546A923BB0F799FAD6E00C2E8F4FE62D4BC44AD1071D5AA4DE0B9",
                        "name": "Item",
                    },
                    "fields@odata.context": f"https://graph.microsoft.com/v1.0/$metadata#sites('{site['id']}')/lists('{list['id']}')/items('{list_item['id']}')/fields/$entity",
                    "fields": {
                        "@odata.etag": '"35aef603-c870-4326-91c3-ffdf59c29677,2"',
                        "id": list_item["id"],
                        "Title": list_item["title"],
                        "LinkTitle": list_item["title"],
                        "ContentType": "Item",
                        "Modified": "2023-06-06T12:44:01Z",
                        "Created": "2023-06-06T12:44:01Z",
                        "AuthorLookupId": "6",
                        "EditorLookupId": "6",
                        "_UIVersionString": "2.0",
                        "Attachments": True,
                        "Edit": "",
                        "LinkTitleNoMenu": list_item["title"],
                        "ItemChildCount": "0",
                        "FolderChildCount": "0",
                        "_ComplianceFlags": "",
                        "_ComplianceTag": "",
                        "_ComplianceTagWrittenTime": "",
                        "_ComplianceTagUserId": "",
                    },
                }
            )

        return results

    def get_site_list_item_attachments(self, site_name, list_name, list_item_id):
        list_ = self.site_lists_by_list_name[list_name]
        site = self.sites_by_site_name[site_name]
        list_items = self.site_list_items[list_["id"]]

        list_item = list_items[int(list_item_id) - 1]

        result = {
            "odata.metadata": f"{ROOT}/sites/{site['name']}/_api/$metadata#SP.ListData.{list_name}ListItems/@Element",
            "odata.type": "SP.Data.Custom_x0020_Made_x0020_ListListItem",
            "odata.id": list_item["guid"],
            "odata.etag": '"3"',
            "odata.editLink": f"Web/Lists(guid'{list_['id']}')/Items({list_item_id})",
            "AttachmentFiles@odata.navigationLinkUrl": f"Web/Lists(guid'{list_['id']}')/Items({list_item_id})/AttachmentFiles",
            "AttachmentFiles": [],
            "FileSystemObjectType": 0,
            "Id": list_item["id"],
            "ServerRedirectedEmbedUri": None,
            "ServerRedirectedEmbedUrl": "",
            "ContentTypeId": "0x01008A3E09918C44C042809E94957AE584ED0047CD5B88506EC84B8B4A0D415BC12E3D",
            "Title": list_item["title"],
            "OData__ColorTag": None,
            "ComplianceAssetId": None,
            "ID": 1,
            "Modified": "2023-05-25T14:59:06Z",
            "Created": "2023-05-25T14:58:42Z",
            "AuthorId": 6,
            "EditorId": 6,
            "OData__UIVersionString": "3.0",
            "Attachments": True,
            "GUID": list_item["guid"],
        }

        for attachment in list_item["attachments"]:
            result["AttachmentFiles"].append(
                {
                    "odata.type": "SP.Attachment",
                    "odata.id": f"{ROOT}/sites/{site['name']}/_api/Web/Lists(guid'{list_['id']}')/Items({list_item_id})/AttachmentFiles('{attachment['title']}')",
                    "odata.editLink": f"Web/Lists(guid'b84d6e6b-5123-47c4-831d-ab6192fdb88e')/Items({list_item_id})/AttachmentFiles('{attachment['title']}')",
                    "FileName": attachment["title"],
                    "FileNameAsPath": {"DecodedUrl": attachment["title"]},
                    "ServerRelativePath": {
                        "DecodedUrl": f"/sites/{site['name']}/Lists/{list['name']}/Attachments/{list_item_id}/{attachment['title']}"
                    },
                    "ServerRelativeUrl": f"/sites/site['name']/Lists/{list['name']}/Attachments/{list_item_id}/{attachment['title']}",
                }
            )

        return result

    def get_list_item_attachment_content(self, list_id, list_item_id, file_name):
        return self.list_item_attachment_content[list_id][list_item_id][file_name]


data_storage = RandomDataStorage()
data_storage.generate()


@app.before_request
def before_request():
    time.sleep(PRE_REQUEST_SLEEP)


@app.route("/<string:tenant_id>/oauth2/v2.0/token", methods=["POST"])
def get_graph_token(tenant_id):
    return {
        "access_token": f"fake-graph-api-token-{tenant_id}",
        "expires_in": TOKEN_EXPIRATION_TIMEOUT,
    }


@app.route("/<string:tenant_id>/tokens/OAuth/2", methods=["POST"])
def get_rest_token(tenant_id):
    return {
        "access_token": f"fake-rest-api-token-{tenant_id}",
        "expires_in": TOKEN_EXPIRATION_TIMEOUT,
    }


@app.route("/common/userrealm/", methods=["GET"])
def get_tenant():
    return {
        "NameSpaceType": "Managed",
        "Login": "cj@something.onmicrosoft.com",
        "DomainName": "something.onmicrosoft.com",
        "FederationBrandName": "Elastic",
        "TenantBrandingInfo": None,
        "cloud_instance_name": "microsoftonline.com",
    }


@app.route("/sites/", methods=["GET"])
def get_site_collections():
    # No paging as there's always one site collection
    return {
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites(siteCollection,webUrl)",
        "value": data_storage.get_site_collections(),
    }


@app.route("/sites/<string:site_id>/sites", methods=["GET"])
def get_sites(site_id):
    # Sharepoint Online does not use skip/take, but we do it here just for lazy implementation
    skip = int(request.args.get("$skip", 0))
    take = int(request.args.get("$take", 10))

    sites = data_storage.get_sites(skip, take)

    response = {
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites",
        "value": sites,
    }

    if len(sites) == take:
        response[
            "@odata.nextLink"
        ] = f"{ROOT}/sites/site_id/sites?$skip={skip+take}&$take={take}"

    return response


@app.route("/sites/<string:site_id>/drives", methods=["GET"])
def get_site_drives(site_id):
    # I don't bother to page cause it's mostly 1-2 drives
    return {
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#drives",
        "value": data_storage.get_site_drives(site_id),
    }


@app.route("/drives/<string:drive_id>/root/delta", methods=["GET"])
def get_drive_root_delta(drive_id):
    skip = int(request.args.get("$skip", 0))
    take = int(request.args.get("$take", 100))

    drive_items = data_storage.get_drive_items(drive_id, skip, take)
    response = {
        "@odata.context": f"https://graph.microsoft.com/v1.0/$metadata#drives('{drive_id}')/root/$entity",
        "value": drive_items,
    }

    if len(drive_items) == take:
        response[
            "@odata.nextLink"
        ] = f"{ROOT}/drives/{drive_id}/root/delta?$skip={skip+take}&$take={take}"

    return response


@app.route("/drives/<string:drive_id>/items/<string:item_id>/content", methods=["GET"])
def download_drive_item(drive_id, item_id):
    content = data_storage.get_drive_item_content(item_id)

    return content.encode("utf-8")


@app.route("/sites/<string:site_id>/lists", methods=["GET"])
def get_site_lists(site_id):
    skip = int(request.args.get("$skip", 0))
    take = int(request.args.get("$take", 100))

    site_lists = data_storage.get_site_lists(site_id, skip, take)
    response = {
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites('enterprisesearch.sharepoint.com%2C792c7c37-803b-47af-88c2-88d8707aab65%2Ce6ead828-d7a5-4c72-b8e7-0687c6a078e7')/lists",
        "value": site_lists,
    }

    if len(site_lists) == take:
        response[
            "@odata.nextLink"
        ] = f"{ROOT}/sites/{site_id}/lists?$skip={skip+take}&$take={take}"

    return response


@app.route("/sites/<string:site_id>/lists/<string:list_id>/items", methods=["GET"])
def get_site_list_items(site_id, list_id):
    skip = int(request.args.get("$skip", 0))
    take = int(request.args.get("$take", 100))

    site_list_items = data_storage.get_site_list_items(site_id, list_id, skip, take)

    response = {
        "@odata.context": "https://graph.microsoft.com/v1.0/$metadata#sites('enterprisesearch.sharepoint.com%2C792c7c37-803b-47af-88c2-88d8707aab65%2C84b8c2b1-3d4a-46f7-93d6-d2c6c4e9523a')/lists('a3f3ce79-e34d-4c03-8f10-e1399e661a65')/items(fields())",
        "value": site_list_items,
    }

    if len(site_list_items) == take:
        response[
            "@odata.nextLink"
        ] = f"{ROOT}/sites/{site_id}/lists/{list_id}/items?$skip={skip+take}&$take={take}"

    return response


@app.route(
    "/sites/<string:site_name>/_api/lists/GetByTitle('<string:list_title>')/items(<string:list_item_id>)"
)
def get_list_item_attachments(site_name, list_title, list_item_id):
    expand = request.args.get(escape("$expand"))
    if expand and "AttachmentFiles" in expand:
        return data_storage.get_site_list_item_attachments(
            site_name, list_title, list_item_id
        )
    else:
        raise Exception("Nope")


@app.route("/sites/<string:site_name>/_api/web/lists/GetByTitle('Site Pages')/items")
def get_site_pages(site_name):
    skip = int(request.args.get("$skip", 0))
    take = int(request.args.get("$take", 100))

    site_pages = data_storage.get_site_pages(site_name, skip, take)
    response = {
        "odata.metadata": "https://enterprisesearch.sharepoint.com/sites/ArtemsSiteForTesting/_api/$metadata#SP.ListData.SitePagesItems",
        "value": site_pages,
    }
    if len(site_pages) == take:
        response[
            "odata.nextLink"
        ] = f"{ROOT}/sites/{site_name}/_api/web/lists/GetByTitle('Site Pages')/items?$skip={skip+take}&$take={take}"

    return response


@app.route(
    "/sites/<string:site_name>/_api/Web/Lists(guid'<string:list_id>')/Items(<string:list_item_id>)/AttachmentFiles('<string:file_name>')/$value",
    methods=["GET"],
)
def get_list_item_attachment(site_name, list_id, list_item_id, file_name):
    return data_storage.get_list_item_attachment_content(
        list_id, list_item_id, file_name
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10337)
