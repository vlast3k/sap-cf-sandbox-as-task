"""
Sandbox Orchestrator

A FastAPI app that:
1. Accepts requests for different users (identified by ?user= param)
2. Spawns a CF Task per user as their isolated sandbox
3. Tasks connect back via WebSocket
4. API routes commands to the correct task via the WebSocket channel
5. No DB for communication — pure WebSocket

Usage:
    uvicorn app:app --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
import pathlib
from fastapi.responses import JSONResponse, HTMLResponse

from cf_client import CFClient, CFConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("app")


# =============================================================================
# User Session — tracks the task and WebSocket for each user
# =============================================================================

MAX_RESULTS_PER_SESSION = 200  # Cap stored results to prevent memory exhaustion
MAX_ACTIVITY_LOG = 500  # Keep last N commands for admin UI
activity_log: list = []  # Global command activity log


@dataclass
class UserSession:
    """Represents a user's sandbox session."""
    user_id: str
    task_guid: Optional[str] = None
    task_state: str = "none"  # none, starting, running, stopped
    websocket: Optional[WebSocket] = None
    pending_requests: dict = field(default_factory=dict)  # request_id -> Future
    created_at: float = field(default_factory=time.time)
    results: list = field(default_factory=list)
    connect_token: str = field(default_factory=lambda: "")
    start_requested_at: float = 0.0  # when task creation was requested
    startup_duration_ms: Optional[float] = None  # ms from request to WS connect


# =============================================================================
# Session Manager
# =============================================================================

class SessionManager:
    def __init__(self):
        self._sessions: dict[str, UserSession] = {}

    def get_or_create(self, user_id: str) -> UserSession:
        if user_id not in self._sessions:
            self._sessions[user_id] = UserSession(user_id=user_id)
        return self._sessions[user_id]

    def get(self, user_id: str) -> Optional[UserSession]:
        return self._sessions.get(user_id)

    def remove(self, user_id: str):
        self._sessions.pop(user_id, None)

    @property
    def active_users(self) -> list[str]:
        return [uid for uid, s in self._sessions.items() if s.task_state == "running"]

    @property
    def all_sessions(self) -> dict[str, UserSession]:
        return self._sessions


# =============================================================================
# Global state
# =============================================================================

sessions = SessionManager()
cf: Optional[CFClient] = None


# =============================================================================
# App Lifespan
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global cf
    config = CFConfig(
        api_url=os.environ.get("CF_API_URL", ""),
        admin_user=os.environ.get("CF_ADMIN_USER", ""),
        admin_password=os.environ.get("CF_ADMIN_PASS", ""),
        skip_ssl_verify=os.environ.get("CF_SKIP_SSL", "false").lower() == "true",
    )
    if not config.api_url:
        log.warning("CF_API_URL not set - task spawning will be disabled")
        yield
        return
    cf = CFClient(config)
    await cf.start()
    log.info(f"CF client connected to {config.api_url}")
    vcap = os.environ.get("VCAP_APPLICATION", "")
    if vcap:
        import json as _json
        vcap_data = _json.loads(vcap)
        cf.config.app_guid = vcap_data.get("application_id", "")
        log.info(f"App GUID (from VCAP): {cf.config.app_guid}")
    yield
    await cf.close()


# =============================================================================
# FastAPI App
# =============================================================================

app = FastAPI(
    title="Sandbox Orchestrator",
    description="Spawns per-user CF Task sandboxes, communicates via WebSocket",
    lifespan=lifespan,
)


# =============================================================================
# Helper: ensure task is running for a user
# =============================================================================

async def _ensure_task(session: UserSession) -> Optional[JSONResponse]:
    """Spawn a task if not running. Returns error response or None on success."""
    if session.task_state == "running":
        return None

    if not cf:
        raise HTTPException(503, "CF client not configured")
    if not cf.config.app_guid:
        raise HTTPException(503, "App GUID not resolved - are we running on CF?")

    session.task_state = "starting"
    session.start_requested_at = time.time()
    session.startup_duration_ms = None
    connect_token = uuid.uuid4().hex

    app_url = os.environ.get("APP_URL", "")
    if not app_url:
        vcap = os.environ.get("VCAP_APPLICATION", "{}")
        import json as _json
        vcap_data = _json.loads(vcap)
        uris = vcap_data.get("application_uris", [])
        if uris:
            app_url = f"wss://{uris[0]}"

    task_env = {
        "USER_ID": session.user_id,
        "API_WS_URL": f"{app_url}/ws/task?user={session.user_id}&token={connect_token}",
    }
    session.connect_token = connect_token

    sandbox_guid = os.environ.get("SANDBOX_APP_GUID", cf.config.app_guid)
    if sandbox_guid != cf.config.app_guid:
        worker_cmd = "python3 /sandbox/worker.py"
    else:
        worker_cmd = "python task_worker.py"

    task_command = CFClient.build_task_command(worker_cmd, task_env)

    try:
        task = await cf.create_task(
            app_guid=sandbox_guid,
            command=task_command,
            name=f"sandbox-{session.user_id}-{uuid.uuid4().hex[:6]}",
            memory_mb=512,
        )
        session.task_guid = task["guid"]
    except Exception as e:
        session.task_state = "none"
        raise HTTPException(500, f"Failed to spawn task: {e}")

    for _ in range(60):
        if session.websocket is not None:
            break
        await asyncio.sleep(0.5)
    else:
        return JSONResponse(
            status_code=202,
            content={"status": "starting", "message": "Task spawning, not yet connected"},
        )
    return None


async def _send_command(session: UserSession, op: str, params: dict) -> dict:
    """Send a typed command to the task and wait for result."""
    if not session.websocket:
        raise HTTPException(503, f"Task for user {session.user_id} is not connected")

    request_id = str(uuid.uuid4())
    future = asyncio.get_event_loop().create_future()
    session.pending_requests[request_id] = future

    await session.websocket.send_text(json.dumps({
        "type": "command",
        "request_id": request_id,
        "op": op,
        "params": params,
    }))

    try:
        result = await asyncio.wait_for(future, timeout=60.0)
        return result
    except asyncio.TimeoutError:
        raise HTTPException(504, "Command timed out")
    finally:
        session.pending_requests.pop(request_id, None)


# =============================================================================
# WebSocket endpoint — Tasks connect here on startup
# =============================================================================

@app.websocket("/ws/task")
async def ws_task_endpoint(websocket: WebSocket, user: str = Query(...), token: str = Query("")):
    session = sessions.get(user)
    if not session or not session.connect_token:
        await websocket.close(code=4001, reason="No pending session")
        return
    if not token or token != session.connect_token:
        await websocket.close(code=4003, reason="Invalid token")
        return
    session.connect_token = ""
    await websocket.accept()
    log.info(f"Task connected for user: {user}")
    session.websocket = websocket
    session.task_state = "running"
    # Record startup duration
    if session.start_requested_at:
        session.startup_duration_ms = round((time.time() - session.start_requested_at) * 1000, 1)
        log.info(f"Sandbox {user} started in {session.startup_duration_ms}ms")

    try:
        msg_count = 0
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            msg_type = msg.get("type")
            msg_count += 1
            if msg_count > 100:
                await asyncio.sleep(0.01)
                msg_count = 0

            if msg_type == "result":
                request_id = msg.get("request_id")
                if request_id and request_id in session.pending_requests:
                    session.pending_requests[request_id].set_result(msg)
                if len(session.results) < MAX_RESULTS_PER_SESSION:
                    session.results.append(msg)
            elif msg_type == "error":
                request_id = msg.get("request_id")
                if request_id and request_id in session.pending_requests:
                    session.pending_requests[request_id].set_result(msg)
                if len(session.results) < MAX_RESULTS_PER_SESSION:
                    session.results.append(msg)
            elif msg_type == "event":
                if len(session.results) < MAX_RESULTS_PER_SESSION:
                    session.results.append(msg)
            elif msg_type == "heartbeat":
                pass
    except WebSocketDisconnect:
        log.info(f"Task disconnected for user: {user}")
    except Exception as e:
        log.error(f"WebSocket error for user {user}: {e}")
    finally:
        session.websocket = None
        session.task_state = "stopped"


# =============================================================================
# REST Endpoints
# =============================================================================

@app.post("/run")
async def run_command(user: str = Query(...), command: str = Query(...)):
    """Execute a bash command in the user's sandbox (convenience endpoint)."""
    session = sessions.get_or_create(user)
    err = await _ensure_task(session)
    if err:
        return err
    return await _send_command(session, "bash", {"command": command})


@app.post("/exec")
async def exec_operation(user: str = Query(...), request: dict = None):
    """Execute a typed operation: {"op": "bash|read|write|glob|grep", "params": {...}}"""
    if request is None:
        raise HTTPException(400, "Request body required: {op, params}")
    op = request.get("op", "")
    params = request.get("params", {})
    if op not in ("bash", "read", "write", "glob", "grep"):
        raise HTTPException(400, f"Unknown op: {op}. Valid: bash, read, write, glob, grep")
    session = sessions.get_or_create(user)
    err = await _ensure_task(session)
    if err:
        return err
    result = await _send_command(session, op, params)
    # Log to activity feed for admin UI
    import datetime
    entry = {
        "user": user,
        "op": op,
        "params": {k: (v[:2000] if isinstance(v, str) else v) for k, v in params.items()},
        "result": result if isinstance(result, dict) else None,
        "time": datetime.datetime.utcnow().strftime("%H:%M:%S"),
        "ts": time.time(),
    }
    activity_log.append(entry)
    if len(activity_log) > MAX_ACTIVITY_LOG:
        activity_log[:] = activity_log[-MAX_ACTIVITY_LOG:]
    return result


@app.get("/status")
async def get_status(user: str = Query(...)):
    session = sessions.get(user)
    if not session:
        return {"user": user, "status": "none", "task_guid": None}
    return {
        "user": user,
        "status": session.task_state,
        "task_guid": session.task_guid,
        "connected": session.websocket is not None,
        "results_count": len(session.results),
    }


@app.get("/results")
async def get_results(user: str = Query(...), last: int = Query(default=10)):
    session = sessions.get(user)
    if not session:
        return {"user": user, "results": []}
    return {"user": user, "results": session.results[-last:]}


@app.post("/stop")
async def stop_task(user: str = Query(...)):
    session = sessions.get(user)
    if not session or not session.task_guid:
        raise HTTPException(404, f"No task found for user {user}")
    if cf and session.task_guid:
        try:
            await cf.cancel_task(session.task_guid)
        except Exception as e:
            log.warning(f"Failed to cancel task: {e}")
    if session.websocket:
        await session.websocket.close()
    session.task_state = "stopped"
    return {"user": user, "status": "stopped"}


@app.get("/sessions")
async def list_sessions():
    result = {}
    for uid, s in sessions.all_sessions.items():
        # Count commands for this session from the activity log
        cmd_count = sum(1 for e in activity_log if e.get("user") == uid)
        result[uid] = {
            "user": uid,
            "task_state": s.task_state,
            "connected": s.websocket is not None,
            "task_guid": s.task_guid,
            "task_id": s.task_guid[:8] if s.task_guid else None,
            "created_at": s.created_at,
            "startup_ms": s.startup_duration_ms,
            "results_count": len(s.results),
            "cmd_count": cmd_count,
        }
    return {"sessions": result}


@app.get("/health")
async def health():
    return {"status": "ok", "cf_connected": cf is not None}

# =============================================================================
# Admin UI + Activity Log
# =============================================================================

ADMIN_HTML = pathlib.Path(__file__).parent / "admin.html"


@app.get("/activity")
async def get_activity(user: Optional[str] = None):
    """Return recent command activity for admin UI. Optionally filter by user."""
    if user:
        filtered = [e for e in activity_log if e.get("user") == user]
        return {"commands": filtered[-200:]}
    return {"commands": activity_log[-200:]}


@app.get("/admin")
async def admin_page():
    """Live admin dashboard showing sandbox activity."""
    if ADMIN_HTML.exists():
        return HTMLResponse(content=ADMIN_HTML.read_text())
    return HTMLResponse(content="<h1>admin.html not found</h1>", status_code=404)
