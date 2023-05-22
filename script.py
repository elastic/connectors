import json
import logging
import os

import msal

path = '/tmp/sharepoint'

import requests

dump_to_terminal = False # if false will dump to {path}

if not dump_to_terminal:
    os.makedirs(path, exist_ok=True)


def dump(what, resp):
    if dump_to_terminal:
        print(f"Loaded {what}: %s" % json.dumps(resp, indent=2))
    else:
        with open(f"{path}/{what}.json", "w") as text_file:
            print(f"Wrote to /tmp/sharepoint/{what}.json")
            text_file.write(json.dumps(resp, indent=2))


def call_graph(query, access_token):
    request_headers = {
        "accept": "application/json",
        "content-type": "application/json",
        'Authorization': 'Bearer ' + access_token
    }

    return requests.get(f"https://graph.microsoft.com/v1.0/{query}",
                              headers=request_headers).json()


def call_sharepoint(url, access_token):
    request_headers = {
        "accept": "application/json",
        "content-type": "application/json",
        'Authorization': 'Bearer ' + access_token
    }

    return requests.get(url,
                              headers=request_headers).json()


def fetch_token(scope):
    authority = f"https://login.microsoftonline.com/{tenant_id}"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority)

    result = app.acquire_token_for_client(scopes=[scope])

    if "access_token" in result:
        access_token = result['access_token']
        return access_token
    else:
        raise Exception(result.get("error"))

def fetch_sharepoint_token(client_id, client_secret, tenant_id, tenant_name):
    url = f"https://accounts.accesscontrol.windows.net/{tenant_id}/tokens/OAuth/2"
    # GUID in resource is always a constant used to create access token
    data = {
        "grant_type": "client_credentials",
        "resource": f"00000003-0000-0ff1-ce00-000000000000/{tenant_name}.sharepoint.com@{tenant_id}",
        "client_id": f"{client_id}@{tenant_id}",
        "client_secret": client_secret
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url,data=data, headers=headers).json()

    return response["access_token"]
    
def load_site_collections(access_token):
    # Calling graph using the access token
    filter_ = requests.utils.quote('siteCollection/root ne null')
    select = "siteCollection,webUrl"

    graph_data = call_graph(f"/sites/?$filter={filter_}&$select={select}", access_token)

    dump("site-collections", graph_data['value'])

def load_sites(access_token, sharepoint_token):
    select = ""

    graph_data = call_graph(f"sites/?search=*&$select={select}", access_token)

    for site in graph_data["value"]:
        dump(f"site-{site['id']}", site)
        load_site_pages(sharepoint_token, site)
        load_site_drives(access_token, site)
        load_site_lists(access_token, site)
        break

def load_site_pages(sharepoint_token, site):
    site_url = site['webUrl']
    select = "Title,CanvasContent1,FileLeafRef"
    url = f"{site_url}/_api/web/lists/getbytitle('Site%20Pages')/items?$select="

    graph_data = call_sharepoint(url, sharepoint_token)    

    dump(f"site-{site['id']}-pages", graph_data['value'])

def load_site_drives(access_token, site):
    select = ""
    
    graph_data = call_graph(f"sites/{site['id']}/drives?$select={select}", access_token)

    dump(f"site-{site['id']}-drives", graph_data['value'])

    for drive in graph_data['value']:
        load_drive_items(access_token, site, drive)

def load_drive_items(access_token, site, drive):
    select = ""

    root_graph_data = call_graph(f"drives/{drive['id']}/root?$select={select}", access_token)
    children_graph_data = call_graph(f"drives/{drive['id']}/root/children?$select={select}", access_token) 

    dump(f"site-{site['id']}-drive-{drive['id']}-items-root", root_graph_data)  # only if I knew why it's not in value 
    dump(f"site-{site['id']}-drive-{drive['id']}-items-children", children_graph_data['value']) 

def load_site_lists(access_token, site):
    select = ""

    graph_data = call_graph(f"sites/{site['id']}/lists?$select={select}", access_token)

    for list_ in graph_data['value']:
        dump(f"site-{site['id']}-list-{list_['id']}", list_)
        load_list_items(access_token, site, list_)

def load_list_items(access_token, site, list_):
    select = ""

    graph_data = call_graph(f"sites/{site['id']}/lists/{list_['id']}/items?$select={select}", access_token)

    dump(f"site-{site['id']}-list-{list_['id']}-items", graph_data['value'])

client_id = '#'
client_secret = '#'  
tenant_id = '#'
tenant_name = '#'

graph_token = fetch_token("https://graph.microsoft.com/.default")
sharepoint_token = fetch_sharepoint_token(client_id, client_secret, tenant_id, tenant_name)

load_site_collections(graph_token)
load_sites(graph_token, sharepoint_token) 
