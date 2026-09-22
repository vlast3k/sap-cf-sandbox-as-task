"""
CF Task Worker (buildpack variant) — Runs inside a CF Task spawned from a
buildpack-based app.

For Docker-based sandboxes, see worker/worker.py instead. This simpler variant
is for cases where you don't need a custom Docker image — the task runs on the
same droplet as the orchestrator app.

On startup:
1. Reads USER_ID and API_WS_URL from environment
2. Connects to the orchestrator via WebSocket
3. Waits for commands, executes them, sends results back
4. Reconnects with exponential backoff if the connection drops
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import signal

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("task_worker")

try:
    import websockets
except ImportError:
    log.error("websockets library not installed")
    sys.exit(1)


# =============================================================================
# Configuration from environment
# =============================================================================

USER_ID = os.environ.get("USER_ID", "unknown")
API_WS_URL = os.environ.get("API_WS_URL", "")

if not API_WS_URL:
    log.error("API_WS_URL not set - cannot connect back to API")
    sys.exit(1)


# =============================================================================
# Command execution
# =============================================================================

async def execute_command(command: str) -> dict:
    """Execute a shell command and return the result."""
    try:
        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
        return {
            "stdout": stdout.decode(errors="replace"),
            "stderr": stderr.decode(errors="replace"),
            "exit_code": proc.returncode,
        }
    except asyncio.TimeoutError:
        return {"error": "Command timed out after 300s", "exit_code": -1}
    except Exception as e:
        return {"error": str(e), "exit_code": -1}


# =============================================================================
# Main worker loop
# =============================================================================

_shutdown = False


def handle_signal(signum, frame):
    global _shutdown
    _shutdown = True
    log.info(f"Received signal {signum}, shutting down...")


async def worker_main():
    """Main worker loop with reconnection."""
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log.info(f"Task worker starting for user={USER_ID}")
    log.info(f"Connecting to: {API_WS_URL}")

    reconnect_delay = 1.0
    max_delay = 30.0

    while not _shutdown:
        try:
            async with websockets.connect(
                API_WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                log.info(f"Connected to API (user={USER_ID})")
                reconnect_delay = 1.0  # Reset on successful connect

                # Send initial heartbeat
                await ws.send(json.dumps({
                    "type": "heartbeat",
                    "user_id": USER_ID,
                    "timestamp": time.time(),
                }))

                # Receive and execute commands
                async for message in ws:
                    if _shutdown:
                        break

                    msg = json.loads(message)
                    msg_type = msg.get("type")

                    if msg_type == "command":
                        request_id = msg.get("request_id", "")
                        command = msg.get("command", "")
                        log.info(f"Executing: {command}")

                        # Send progress event
                        await ws.send(json.dumps({
                            "type": "event",
                            "event_type": "started",
                            "request_id": request_id,
                            "command": command,
                            "timestamp": time.time(),
                        }))

                        # Execute
                        result = await execute_command(command)

                        # Send result
                        await ws.send(json.dumps({
                            "type": "result",
                            "request_id": request_id,
                            "command": command,
                            **result,
                            "timestamp": time.time(),
                        }))

                        log.info(f"Completed: {command} (exit={result.get('exit_code')})")

                    elif msg_type == "shutdown":
                        log.info("Received shutdown command")
                        break

        except websockets.ConnectionClosed:
            log.warning("Connection closed by server")
        except ConnectionRefusedError:
            log.warning("Connection refused - server not ready?")
        except Exception as e:
            log.error(f"Connection error: {e}")

        if _shutdown:
            break

        # Reconnect with backoff
        log.info(f"Reconnecting in {reconnect_delay:.1f}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_delay)

    log.info("Task worker exiting")


if __name__ == "__main__":
    asyncio.run(worker_main())
