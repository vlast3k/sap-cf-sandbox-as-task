"""
CF API Client — Spawns CF Tasks using the v3 API.

Authenticates via password grant (admin/Space Developer user), then uses
POST /v3/apps/:guid/tasks to create isolated task containers.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import httpx

log = logging.getLogger(__name__)


@dataclass
class CFConfig:
    """CF API connection configuration."""
    api_url: str                 # e.g. https://api.cf.example.com
    admin_user: str              # CF user with SpaceDeveloper on the sandbox space
    admin_password: str
    app_guid: str = ""           # GUID of THIS app (resolved at startup from VCAP_APPLICATION)
    org: str = ""                # Target org (optional, for discovery)
    space: str = ""              # Target space (optional, for discovery)
    skip_ssl_verify: bool = False


@dataclass
class TokenCache:
    """Cached OAuth token with expiry."""
    access_token: str = ""
    expires_at: float = 0.0

    @property
    def is_valid(self) -> bool:
        return bool(self.access_token) and time.time() < (self.expires_at - 60)


class CFClient:
    """
    Cloud Foundry v3 API client.

    Authenticates via password grant, then can:
    - Discover the app GUID from VCAP_APPLICATION
    - Create CF Tasks on the app
    - List/cancel tasks
    - Get task status
    """

    def __init__(self, config: CFConfig):
        self.config = config
        self._token = TokenCache()
        self._login_url: Optional[str] = None
        self._http = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def start(self):
        """Initialize the async HTTP client."""
        self._http = httpx.AsyncClient(
            verify=not self.config.skip_ssl_verify,
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
        )
        await self._discover_login_url()

    async def close(self):
        if self._http:
            await self._http.aclose()

    # =========================================================================
    # Authentication
    # =========================================================================

    async def _discover_login_url(self):
        """Get the UAA login endpoint from CF API root."""
        resp = await self._http.get(f"{self.config.api_url}/")
        if resp.status_code == 200:
            data = resp.json()
            self._login_url = data.get("links", {}).get("login", {}).get("href")
            if not self._login_url:
                self._login_url = data.get("links", {}).get("uaa", {}).get("href")

        if not self._login_url:
            self._login_url = self.config.api_url.replace("://api.", "://login.")

        log.info(f"CF login endpoint: {self._login_url}")

    async def _get_token(self) -> str:
        """Get a valid OAuth token (cached, auto-refreshes)."""
        if self._token.is_valid:
            return self._token.access_token

        log.info(f"Obtaining CF token for user {self.config.admin_user}...")

        token_url = f"{self._login_url}/oauth/token"
        resp = await self._http.post(
            token_url,
            data={
                "grant_type": "password",
                "username": self.config.admin_user,
                "password": self.config.admin_password,
                "client_id": "cf",
                "client_secret": "",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "Authorization": "Basic Y2Y6",  # base64("cf:")
            },
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"CF login failed ({resp.status_code}): {resp.text[:500]}"
            )

        data = resp.json()
        self._token = TokenCache(
            access_token=data["access_token"],
            expires_at=time.time() + data.get("expires_in", 3600),
        )
        log.info("CF token obtained successfully")
        return self._token.access_token

    async def _authed(self, method: str, url: str, **kwargs):
        """Make an authenticated request to the CF API."""
        token = await self._get_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        headers.setdefault("Content-Type", "application/json")
        return await self._http.request(method, url, headers=headers, **kwargs)

    # =========================================================================
    # App Discovery
    # =========================================================================

    async def get_app_guid(self, app_name: str, space_guid: str = "") -> str:
        """Resolve an app name to its GUID."""
        url = f"{self.config.api_url}/v3/apps"
        params = {"names": app_name}
        if space_guid:
            params["space_guids"] = space_guid
        resp = await self._authed("GET", url, params=params)
        resources = resp.json().get("resources", [])
        if not resources:
            raise RuntimeError(f"App '{app_name}' not found")
        return resources[0]["guid"]

    async def get_spaces(self) -> list[dict]:
        """List available spaces."""
        resp = await self._authed("GET", f"{self.config.api_url}/v3/spaces")
        return resp.json().get("resources", [])

    async def get_orgs(self) -> list[dict]:
        """List available orgs."""
        resp = await self._authed("GET", f"{self.config.api_url}/v3/organizations")
        return resp.json().get("resources", [])

    # =========================================================================
    # Task Management
    # =========================================================================

    async def create_task(
        self,
        app_guid: str,
        command: str,
        name: str = "",
        memory_mb: int = 512,
        disk_mb: int = 4096,
    ) -> dict:
        """
        Create a CF Task on the given app.
        Returns the task record from CF API.
        """
        url = f"{self.config.api_url}/v3/apps/{app_guid}/tasks"

        body: dict[str, Any] = {
            "command": command,
            "memory_in_mb": memory_mb,
            "disk_in_mb": disk_mb,
        }
        if name:
            body["name"] = name

        resp = await self._authed("POST", url, json=body)

        if resp.status_code not in (201, 202):
            raise RuntimeError(
                f"Failed to create task ({resp.status_code}): {resp.text[:500]}"
            )

        task = resp.json()
        log.info(f"Task created: {task['guid']} (name={task.get('name', '')})")
        return task

    async def get_task(self, task_guid: str) -> dict:
        """Get task details by GUID."""
        url = f"{self.config.api_url}/v3/tasks/{task_guid}"
        resp = await self._authed("GET", url)
        return resp.json()

    async def cancel_task(self, task_guid: str) -> dict:
        """Cancel a running task."""
        url = f"{self.config.api_url}/v3/tasks/{task_guid}/actions/cancel"
        resp = await self._authed("POST", url, json={})
        if resp.status_code not in (200, 202):
            raise RuntimeError(f"Failed to cancel task: {resp.text[:500]}")
        log.info(f"Task {task_guid} cancelled")
        return resp.json()

    async def list_tasks(
        self, app_guid: str, states: Optional[list[str]] = None
    ) -> list[dict]:
        """List tasks for an app, optionally filtered by state."""
        url = f"{self.config.api_url}/v3/apps/{app_guid}/tasks"
        params: dict[str, Any] = {"per_page": 50, "order_by": "-created_at"}
        if states:
            params["states"] = ",".join(states)
        resp = await self._authed("GET", url, params=params)
        return resp.json().get("resources", [])

    # =========================================================================
    # Convenience
    # =========================================================================

    @staticmethod
    def build_task_command(base_command: str, env_vars: dict[str, str]) -> str:
        """
        Build a task command with env vars prepended.
        CF Tasks inherit the app env but don't support per-task vars via API.
        Values are single-quoted to prevent shell interpretation of special chars.
        """
        env_prefix = " ".join(f"{k}='{v}'" for k, v in env_vars.items())
        return f"{env_prefix} {base_command}" if env_prefix else base_command
