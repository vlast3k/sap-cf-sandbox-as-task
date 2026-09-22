"""
Sandbox worker — connects back to orchestrator via WebSocket and exposes
typed file/shell operations (bash, read, write, glob, grep).
"""

import asyncio
import glob as globmod
import json
import logging
import os
import pathlib
import signal
import subprocess
import sys
import time

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sandbox-worker")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_ID = os.environ.get("USER_ID", "anonymous")
API_WS_URL = os.environ.get("API_WS_URL", "ws://localhost:8765")
WORKSPACE = os.environ.get("WORKSPACE", "/workspace")
HEARTBEAT_INTERVAL = 20  # seconds
MAX_OUTPUT_BYTES = 100 * 1024  # 100KB
MAX_CONCURRENT = 4
RECONNECT_BASE = 1.0
RECONNECT_MAX = 30.0
IDLE_TIMEOUT = 60  # seconds — exit if no command received within this window

# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------


def _resolve_path(path: str) -> str:
    """Resolve a path relative to WORKSPACE unless absolute."""
    if os.path.isabs(path):
        return path
    return os.path.join(WORKSPACE, path)


async def op_bash(params: dict) -> dict:
    command = params.get("command", "")
    timeout = params.get("timeout", 300)
    cwd = params.get("cwd", WORKSPACE)
    cwd = _resolve_path(cwd)

    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass
        return {
            "stdout": "",
            "stderr": f"Command timed out after {timeout}s",
            "exit_code": -1,
            "truncated": False,
        }

    truncated = False
    stdout = stdout_bytes[:MAX_OUTPUT_BYTES]
    stderr = stderr_bytes[:MAX_OUTPUT_BYTES]
    if len(stdout_bytes) > MAX_OUTPUT_BYTES or len(stderr_bytes) > MAX_OUTPUT_BYTES:
        truncated = True

    return {
        "stdout": stdout.decode("utf-8", errors="replace"),
        "stderr": stderr.decode("utf-8", errors="replace"),
        "exit_code": proc.returncode,
        "truncated": truncated,
    }


async def op_read(params: dict) -> dict:
    path = _resolve_path(params.get("path", ""))
    offset = params.get("offset", None)
    limit = params.get("limit", None)

    if not os.path.isfile(path):
        raise FileNotFoundError(f"File not found: {path}")

    with open(path, "r", errors="replace") as f:
        lines = f.readlines()

    total = len(lines)

    if offset is not None:
        lines = lines[offset:]
    if limit is not None:
        lines = lines[:limit]

    return {
        "content": "".join(lines),
        "lines": total,
    }


async def op_write(params: dict) -> dict:
    path = _resolve_path(params.get("path", ""))
    content = params.get("content", "")

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        written = f.write(content)

    return {"bytes_written": written}


async def op_glob(params: dict) -> dict:
    pattern = params.get("pattern", "")
    if not os.path.isabs(pattern):
        pattern = os.path.join(WORKSPACE, pattern)

    matches = globmod.glob(pattern, recursive=True)
    # Return relative to workspace when possible
    rel = []
    for m in sorted(matches):
        if m.startswith(WORKSPACE + "/"):
            rel.append(m[len(WORKSPACE) + 1:])
        else:
            rel.append(m)
    return {"files": rel}


async def op_grep(params: dict) -> dict:
    pattern = params.get("pattern", "")
    path = _resolve_path(params.get("path", WORKSPACE))
    include = params.get("include", None)
    max_results = params.get("max_results", 100)

    cmd = ["grep", "-rn", "--binary-files=without-match"]
    if include:
        cmd.extend(["--include", include])
    cmd.extend([pattern, path])

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    except asyncio.TimeoutError:
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass
        return {"matches": [], "truncated": True}

    matches = []
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        if len(matches) >= max_results:
            break
        # format: path:line_no:text
        parts = line.split(":", 2)
        if len(parts) >= 3:
            file_path = parts[0]
            if file_path.startswith(WORKSPACE + "/"):
                file_path = file_path[len(WORKSPACE) + 1:]
            try:
                line_no = int(parts[1])
            except ValueError:
                line_no = 0
            matches.append({"path": file_path, "line": line_no, "text": parts[2]})

    return {"matches": matches}


OPERATIONS = {
    "bash": op_bash,
    "read": op_read,
    "write": op_write,
    "glob": op_glob,
    "grep": op_grep,
}

# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------


async def handle_command(msg: dict, ws) -> None:
    request_id = msg.get("request_id", "unknown")
    op = msg.get("op", "")
    params = msg.get("params", {})

    handler = OPERATIONS.get(op)
    if not handler:
        await ws.send(json.dumps({
            "type": "error",
            "request_id": request_id,
            "error": f"Unknown operation: {op}",
            "error_class": "unknown_op",
        }))
        return

    try:
        result = await handler(params)
        result["type"] = "result"
        result["request_id"] = request_id
        result["op"] = op
        await ws.send(json.dumps(result))
    except FileNotFoundError as e:
        await ws.send(json.dumps({
            "type": "error",
            "request_id": request_id,
            "error": str(e),
            "error_class": "not_found",
        }))
    except PermissionError as e:
        await ws.send(json.dumps({
            "type": "error",
            "request_id": request_id,
            "error": str(e),
            "error_class": "permission_denied",
        }))
    except Exception as e:
        await ws.send(json.dumps({
            "type": "error",
            "request_id": request_id,
            "error": str(e),
            "error_class": "internal",
        }))


async def run_worker() -> None:
    os.makedirs(WORKSPACE, exist_ok=True)

    shutdown_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    backoff = RECONNECT_BASE
    last_activity = time.time()  # track idle timeout

    while not shutdown_event.is_set():
        try:
            # API_WS_URL already includes user/token params from orchestrator
            url = API_WS_URL
            log.info("Connecting to %s", url)

            async with websockets.connect(url, ping_interval=None) as ws:
                log.info("Connected to orchestrator (idle timeout=%ds)", IDLE_TIMEOUT)
                backoff = RECONNECT_BASE  # reset on success
                last_activity = time.time()

                # Heartbeat task (also checks idle timeout)
                async def heartbeat():
                    nonlocal last_activity
                    while not shutdown_event.is_set():
                        try:
                            await asyncio.wait_for(
                                shutdown_event.wait(), timeout=HEARTBEAT_INTERVAL
                            )
                            break
                        except asyncio.TimeoutError:
                            # Check idle timeout
                            idle = time.time() - last_activity
                            if idle >= IDLE_TIMEOUT:
                                log.info("Idle timeout reached (%.0fs). Shutting down.", idle)
                                shutdown_event.set()
                                await ws.close()
                                break
                            await ws.send(json.dumps({
                                "type": "heartbeat",
                                "user_id": USER_ID,
                                "timestamp": time.time(),
                            }))

                hb_task = asyncio.create_task(heartbeat())

                try:
                    async for raw in ws:
                        if shutdown_event.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            log.warning("Invalid JSON received: %s", raw[:200])
                            continue

                        if msg.get("type") == "command":
                            last_activity = time.time()
                            async def _run(m=msg):
                                async with semaphore:
                                    await handle_command(m, ws)
                            asyncio.create_task(_run())
                        elif msg.get("type") == "ping":
                            await ws.send(json.dumps({"type": "pong"}))
                        else:
                            log.debug("Ignored message type: %s", msg.get("type"))
                finally:
                    hb_task.cancel()
                    try:
                        await hb_task
                    except asyncio.CancelledError:
                        pass

        except websockets.ConnectionClosed as e:
            log.warning("Connection closed: %s", e)
        except (OSError, ConnectionRefusedError) as e:
            log.warning("Connection failed: %s", e)
        except Exception as e:
            log.error("Unexpected error: %s", e, exc_info=True)

        if not shutdown_event.is_set():
            log.info("Reconnecting in %.1fs ...", backoff)
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, RECONNECT_MAX)

    log.info("Worker shutting down.")


def main():
    log.info("Sandbox worker starting (user=%s, ws=%s)", USER_ID, API_WS_URL)
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
