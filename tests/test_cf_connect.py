"""
CF Connectivity Test — Run locally to verify credentials work.

Usage:
    CF_API_URL=https://api.cf.example.com CF_ADMIN_USER=admin CF_ADMIN_PASS=secret \
        python tests/test_cf_connect.py

Authenticates to CF UAA, lists orgs/spaces/apps, and lists tasks.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Add orchestrator to path
sys.path.insert(0, str(Path(__file__).parent.parent / "orchestrator"))

from cf_client import CFClient, CFConfig


async def main():
    config = CFConfig(
        api_url=os.environ.get("CF_API_URL", ""),
        admin_user=os.environ.get("CF_ADMIN_USER", ""),
        admin_password=os.environ.get("CF_ADMIN_PASS", ""),
        skip_ssl_verify=os.environ.get("CF_SKIP_SSL", "false").lower() == "true",
    )

    if not config.api_url:
        print("ERROR: CF_API_URL not set")
        sys.exit(1)

    print(f"CF API: {config.api_url}")
    print(f"User:   {config.admin_user}")
    print()

    async with CFClient(config) as cf:
        print("--- Orgs ---")
        orgs = await cf.get_orgs()
        for org in orgs:
            print(f"  {org['name']} (guid={org['guid']})")

        print("\n--- Spaces ---")
        spaces = await cf.get_spaces()
        for space in spaces:
            print(f"  {space['name']} (guid={space['guid']})")

        if spaces:
            space_guid = spaces[0]["guid"]
            print(f"\n--- Apps in space '{spaces[0]['name']}' ---")
            url = f"{config.api_url}/v3/apps"
            resp = await cf._authed("GET", url, params={"space_guids": space_guid})
            apps = resp.json().get("resources", [])
            for a in apps:
                print(f"  {a['name']} (guid={a['guid']}, state={a['state']})")

            if apps:
                app_guid = apps[0]["guid"]
                print(f"\n--- Tasks for '{apps[0]['name']}' ---")
                tasks = await cf.list_tasks(app_guid)
                if tasks:
                    for t in tasks[:5]:
                        print(f"  {t['name']} state={t['state']} guid={t['guid']}")
                else:
                    print("  (no tasks)")

    print("\n[OK] CF connectivity verified!")


if __name__ == "__main__":
    asyncio.run(main())
