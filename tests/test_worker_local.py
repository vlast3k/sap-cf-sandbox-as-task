"""
Local integration test for the sandbox worker.

Starts a fake orchestrator WebSocket server, spawns worker.py as a subprocess,
and exercises all 5 operations (bash, read, write, glob, grep).

No CF deployment needed. Run from the repo root:
    python tests/test_worker_local.py
"""

import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import uuid

import websockets

PORT = 18765
WS_URL = f"ws://localhost:{PORT}"

# Use a temp workspace so tests don't pollute the real filesystem
WORKSPACE = tempfile.mkdtemp(prefix="sandbox-test-")


class OrchestratorServer:
    """Fake orchestrator that sends commands and collects responses."""

    def __init__(self):
        self.responses: dict[str, dict] = {}
        self.connected = asyncio.Event()
        self._ws = None

    async def handler(self, ws):
        self._ws = ws
        self.connected.set()
        try:
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("type") in ("result", "error"):
                    self.responses[msg["request_id"]] = msg
                elif msg.get("type") == "heartbeat":
                    pass
                elif msg.get("type") == "pong":
                    pass
        except websockets.ConnectionClosed:
            pass

    async def send_command(self, op: str, params: dict, timeout: float = 10.0) -> dict:
        request_id = str(uuid.uuid4())
        msg = {
            "type": "command",
            "request_id": request_id,
            "op": op,
            "params": params,
        }
        await self._ws.send(json.dumps(msg))

        # Wait for response
        deadline = time.time() + timeout
        while time.time() < deadline:
            if request_id in self.responses:
                return self.responses.pop(request_id)
            await asyncio.sleep(0.05)
        raise TimeoutError(f"No response for {op} (request_id={request_id})")


async def run_tests():
    server = OrchestratorServer()
    async with websockets.serve(server.handler, "localhost", PORT):
        # Spawn worker subprocess
        env = os.environ.copy()
        env["USER_ID"] = "test-user"
        env["API_WS_URL"] = WS_URL
        env["WORKSPACE"] = WORKSPACE

        worker_script = os.path.join(os.path.dirname(__file__), "..", "worker", "worker.py")
        proc = subprocess.Popen(
            [sys.executable, worker_script],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            await asyncio.wait_for(server.connected.wait(), timeout=5.0)
            print("Worker connected.\n")

            passed = 0
            failed = 0

            # --- Test 1: bash ---
            print("TEST 1: bash (echo)")
            resp = await server.send_command("bash", {"command": "echo hello world"})
            assert resp["type"] == "result", f"Expected result, got {resp['type']}"
            assert resp["op"] == "bash"
            assert resp["exit_code"] == 0
            assert "hello world" in resp["stdout"]
            print(f"  PASS: stdout={resp['stdout'].strip()!r}, exit_code={resp['exit_code']}\n")
            passed += 1

            # --- Test 2: write ---
            print("TEST 2: write")
            test_content = "line1\nline2\nline3\ndef main():\n    pass\n"
            resp = await server.send_command("write", {
                "path": "testdir/hello.py",
                "content": test_content,
            })
            assert resp["type"] == "result", f"Expected result, got {resp}"
            assert resp["op"] == "write"
            assert resp["bytes_written"] > 0
            print(f"  PASS: bytes_written={resp['bytes_written']}\n")
            passed += 1

            # --- Test 3: read ---
            print("TEST 3: read")
            resp = await server.send_command("read", {"path": "testdir/hello.py"})
            assert resp["type"] == "result"
            assert resp["content"] == test_content
            assert resp["lines"] == 5
            print(f"  PASS: lines={resp['lines']}, content matches\n")
            passed += 1

            # --- Test 3b: read with offset/limit ---
            print("TEST 3b: read with offset and limit")
            resp = await server.send_command("read", {
                "path": "testdir/hello.py",
                "offset": 1,
                "limit": 2,
            })
            assert resp["type"] == "result"
            assert resp["content"] == "line2\nline3\n"
            print(f"  PASS: got 2 lines starting from offset 1\n")
            passed += 1

            # --- Test 4: glob ---
            print("TEST 4: glob")
            await server.send_command("write", {
                "path": "testdir/sub/other.py",
                "content": "# other\n",
            })
            resp = await server.send_command("glob", {
                "pattern": f"{WORKSPACE}/**/*.py",
            })
            assert resp["type"] == "result"
            assert len(resp["files"]) >= 2
            print(f"  PASS: found {len(resp['files'])} files: {resp['files']}\n")
            passed += 1

            # --- Test 5: grep ---
            print("TEST 5: grep")
            resp = await server.send_command("grep", {
                "pattern": "def main",
                "path": WORKSPACE,
                "include": "*.py",
            })
            assert resp["type"] == "result"
            assert len(resp["matches"]) >= 1
            match = resp["matches"][0]
            assert "def main" in match["text"]
            print(f"  PASS: {len(resp['matches'])} match(es): {match}\n")
            passed += 1

            # --- Test 6: bash error ---
            print("TEST 6: bash (nonzero exit)")
            resp = await server.send_command("bash", {"command": "exit 42"})
            assert resp["type"] == "result"
            assert resp["exit_code"] == 42
            print(f"  PASS: exit_code={resp['exit_code']}\n")
            passed += 1

            # --- Test 7: read nonexistent file ---
            print("TEST 7: read nonexistent (error)")
            resp = await server.send_command("read", {"path": "/nonexistent/file.txt"})
            assert resp["type"] == "error"
            assert resp["error_class"] == "not_found"
            print(f"  PASS: error_class={resp['error_class']}, error={resp['error']}\n")
            passed += 1

            # --- Test 8: unknown op ---
            print("TEST 8: unknown operation (error)")
            resp = await server.send_command("dance", {"style": "salsa"})
            assert resp["type"] == "error"
            assert resp["error_class"] == "unknown_op"
            print(f"  PASS: error_class={resp['error_class']}\n")
            passed += 1

            # --- Test 9: bash with cwd ---
            print("TEST 9: bash with cwd")
            resp = await server.send_command("bash", {
                "command": "pwd",
                "cwd": "testdir",
            })
            assert resp["type"] == "result"
            assert "testdir" in resp["stdout"]
            print(f"  PASS: cwd respected, pwd={resp['stdout'].strip()}\n")
            passed += 1

            # Summary
            total = passed + failed
            print("=" * 50)
            print(f"RESULTS: {passed}/{total} passed, {failed} failed")
            if failed:
                sys.exit(1)

        finally:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

            subprocess.run(["rm", "-rf", WORKSPACE], check=False)


def main():
    asyncio.run(run_tests())


if __name__ == "__main__":
    main()
