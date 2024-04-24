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

LABELS = ["community-driven", "needs-triage"]

async def main():
    async with aiohttp.ClientSession() as session:
        gh = GitHubAPI(session, requester="", base_url="https://api.github.com", oauth_token=GITHUB_TOKEN)

        print("********")
        print(f"ACTOR: {ACTOR}")
        print(f"NUMBER: {NUMBER}")
        print(f"REPO: {REPO}")
        print("********")

        try:
            # this API returns a None response, but will raise if the user isn't a collaborator
            await gh.getitem(f"/repos/{REPO}/collaborators/{ACTOR}")
            print(f"User is a collaborator, not applying labels.")
        except BadRequest as e:
            # if this fails we want it to be noisy, so no try/except
            print(f"User is not a collaborator, applying labels...")
            await gh.post(f"/repos/{REPO}/issues/{NUMBER}/labels", data={"labels": LABELS})

if __name__ == "__main__":
    asyncio.run(main())
