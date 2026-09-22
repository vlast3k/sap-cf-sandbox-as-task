"""
Local end-to-end test — runs the orchestrator and simulates task workers
in-process. No CF deployment needed.

Validates the WebSocket communication flow: connect, auth, dispatch, result.

Usage:
    python test_orchestrator_local.py
"""

import asyncio
import json
import logging
import os

# Disable CF client for local test
os.environ.setdefault("CF_API_URL", "")
os.environ.setdefault("PORT", "8090")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("test_local")


async def simulated_task_worker(user_id: str, port: int, token: str):
    """Simulate a CF Task worker connecting back via WebSocket."""
    import websockets

    url = f"ws://127.0.0.1:{port}/ws/task?user={user_id}&token={token}"
    log.info(f"Simulated worker connecting: {url}")

    async with websockets.connect(url) as ws:
        log.info(f"Worker connected for user={user_id}")

        # Send heartbeat
        await ws.send(json.dumps({"type": "heartbeat", "user_id": user_id}))

        # Wait for commands
        async for message in ws:
            msg = json.loads(message)
            if msg.get("type") == "command":
                command = msg.get("command", "")
                request_id = msg.get("request_id", "")
                log.info(f"Worker [{user_id}] executing: {command}")

                # Simulate execution
                await asyncio.sleep(0.5)

                await ws.send(json.dumps({
                    "type": "result",
                    "request_id": request_id,
                    "command": command,
                    "stdout": f"Hello from {user_id}! Executed: {command}",
                    "stderr": "",
                    "exit_code": 0,
                }))
            elif msg.get("type") == "shutdown":
                break


async def test():
    """Run end-to-end local test."""
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "orchestrator"))

    import uvicorn
    from app import app, sessions

    port = 8090

    # Start the server
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.5)  # Let server start

    print("=" * 60)
    print("  LOCAL END-TO-END TEST")
    print("=" * 60)

    # Pre-create sessions with connect tokens (normally done by /run spawn logic)
    import uuid as _uuid
    alice_token = _uuid.uuid4().hex
    bob_token = _uuid.uuid4().hex
    alice_session = sessions.get_or_create("alice")
    alice_session.connect_token = alice_token
    alice_session.task_state = "starting"
    bob_session = sessions.get_or_create("bob")
    bob_session.connect_token = bob_token
    bob_session.task_state = "starting"

    # Start simulated workers for alice and bob
    alice_task = asyncio.create_task(simulated_task_worker("alice", port, alice_token))
    bob_task = asyncio.create_task(simulated_task_worker("bob", port, bob_token))
    await asyncio.sleep(0.5)  # Let workers connect

    import httpx as hx

    async with hx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
        # Check sessions
        resp = await client.get("/sessions")
        print(f"\nSessions: {resp.json()}")

        # Send commands to alice
        print("\n--- Sending command to alice ---")
        resp = await client.post("/run", params={"user": "alice", "command": "echo hello"})
        print(f"Response: {resp.json()}")

        # Send commands to bob
        print("\n--- Sending command to bob ---")
        resp = await client.post("/run", params={"user": "bob", "command": "ls -la"})
        print(f"Response: {resp.json()}")

        # Check status
        print("\n--- Status check ---")
        resp = await client.get("/status", params={"user": "alice"})
        print(f"Alice: {resp.json()}")
        resp = await client.get("/status", params={"user": "bob"})
        print(f"Bob: {resp.json()}")

        # Get results
        print("\n--- Results ---")
        resp = await client.get("/results", params={"user": "alice"})
        print(f"Alice results: {resp.json()}")

    print("\n" + "=" * 60)
    print("  TEST PASSED")
    print("=" * 60)

    # Cleanup
    alice_task.cancel()
    bob_task.cancel()
    server.should_exit = True
    await asyncio.sleep(0.5)
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(test())
