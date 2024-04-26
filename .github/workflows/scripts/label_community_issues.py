#!/usr/bin/env python

import aiohttp
import asyncio
import os
from gidgethub.aiohttp import GitHubAPI
from gidgethub import BadRequest

ACTOR = os.getenv("ACTOR")
NUMBER = os.getenv("NUMBER")
REPO = os.getenv("REPO")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

BASE_URL = "https://api.github.com"
LABELS = ["community-driven", "needs-triage"]

async def main():
    async with aiohttp.ClientSession() as session:
        gh = GitHubAPI(session, requester="", base_url=BASE_URL, oauth_token=GITHUB_TOKEN)

        print("********")
        print(f"ACTOR: {ACTOR}")
        print(f"NUMBER: {NUMBER}")
        print(f"REPO: {REPO}")
        print("********")

        employees = []
        print("Fetching employees...")
        teams = await gh.getitem(f"/repos/{REPO}/teams")
        for team in teams:
            members_url = team.get("members_url").replace(BASE_URL, "").replace("{/member}", "")
            members = await gh.getitem(members_url)
            employees.extend(list(map(lambda member: member["login"], members)))

        print(f"Found {len(set(employees))} employees.")
        if ACTOR in employees:
            print("User is an employee, not applying labels.")
            return

        print("Fetching collaborators...")
        internal = await gh.getitem(f"/repos/{REPO}/collaborators")
        external = await gh.getitem(f"/repos/{REPO}/collaborators?affiliation=outside")
        collaborators = list(map(lambda collaborator: collaborator["login"], internal + external))

        print(f"Found {len(set(collaborators))} collaborators.")
        if ACTOR in collaborators:
            print("User is a collaborator, not applying labels.")
            return

        print("User is not an amployee nor a collaborator, applying labels...")
        await gh.post(f"/repos/{REPO}/issues/{NUMBER}/labels", data={"labels": LABELS})

if __name__ == "__main__":
    asyncio.run(main())
