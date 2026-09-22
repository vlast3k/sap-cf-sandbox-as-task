# Ephemeral Sandboxes on Cloud Foundry

Per-user isolated containers using CF Tasks and reverse WebSocket connections. No external orchestration, no Kubernetes, no job scheduler — just the CF v3 API.

---

## The Problem

You need to give untrusted code a place to run — per-user, isolated, short-lived. Think: AI agent tool execution, CI job runners, interactive coding environments, security testing. Each session needs its own filesystem, process tree, and network namespace. Containers should start in seconds and clean up after themselves.

CF already has the primitives: Tasks run one-off commands in isolated containers. Docker image support gives full control over the runtime. The missing piece is a way to connect back to the controlling process — CF Tasks have no inbound routes.

## Architecture

```
                    REST API
User / Agent ──────────────────► Orchestrator App
                                      │
                                      │  POST /v3/apps/{guid}/tasks
                                      │  (CF API — spawns container)
                                      │
                                      │◄──── WebSocket (outbound from task)
                                      │
                                Sandbox Container
                                (CF Task, Docker image)
```

Three components:

1. **Orchestrator** — a long-running CF app (Python/FastAPI). Manages sessions, spawns tasks, relays commands over WebSocket.
2. **Sandbox template** — a Docker-based CF app scaled to zero instances. CF Tasks run against its image. Each task is a fresh container.
3. **Worker** — a Python script baked into the Docker image. On startup, opens a WebSocket back to the orchestrator. Executes commands, returns results.

The orchestrator and sandbox template live in separate CF spaces. This separation enables independent security group policies — the sandbox space can have restricted or no egress while the orchestrator retains full connectivity.

## How Tasks Connect Back

CF Tasks can make outbound connections but cannot receive inbound ones (no route, no port mapping). The reverse WebSocket pattern solves this:

1. Orchestrator generates a one-time token (UUID).
2. Orchestrator calls `POST /v3/apps/{sandbox_app_guid}/tasks` with a command that embeds the callback URL and token as environment variables:
   ```
   USER_ID='alice-742' API_WS_URL='wss://orchestrator.example.com/ws/task?user=alice-742&token=a1b2c3' python3 /sandbox/worker.py
   ```
3. The CF Task starts the Docker container. The worker reads `API_WS_URL` and connects.
4. The orchestrator validates the token on the WebSocket handshake, clears it (single-use), and the bidirectional channel is open.

CF v3 Tasks do not support per-task environment variables. The workaround is prepending shell assignments to the command string — `KEY='value' command`. The CF API passes this entire string to the container's shell.

## The Orchestrator

A FastAPI app with REST endpoints for session management and a WebSocket endpoint for worker connections.

### Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/exec?user=X` | Send a typed operation (bash, read, write, glob, grep) |
| `POST` | `/run?user=X&command=Y` | Shorthand for bash execution |
| `POST` | `/stop?user=X` | Cancel the CF Task and close the WebSocket |
| `GET` | `/sessions` | List all active sessions with state, timing, command counts |
| `GET` | `/health` | Liveness check including CF API connectivity |

### Session Lifecycle

Each user gets a `UserSession` that tracks:
- Task GUID and state (`none` → `starting` → `running` → `stopped`)
- The WebSocket connection (set when the worker connects back)
- A one-time connect token
- Pending request futures (for request/response correlation)
- Startup duration (measured from task creation to WebSocket connect)

### Task Creation Flow

```
POST /exec?user=alice
  │
  ├─ Session exists and running? → dispatch command immediately
  │
  └─ No running task?
       ├─ Generate connect_token (UUID hex)
       ├─ Build callback URL: wss://orchestrator/ws/task?user=alice&token=<token>
       ├─ Call CF API: POST /v3/apps/{sandbox_guid}/tasks
       │     body: { command: "USER_ID='alice' API_WS_URL='wss://...' python3 /sandbox/worker.py",
       │             memory_in_mb: 512, disk_in_mb: 4096,
       │             name: "sandbox-alice-a3f2c1" }
       ├─ Poll for up to 30s (0.5s intervals) waiting for worker WebSocket
       └─ Worker connects → dispatch command
```

### Command Dispatch

Each command gets a `request_id` (UUID). The orchestrator sends it over WebSocket, creates an `asyncio.Future`, and awaits the response with a 60-second timeout. The worker replies with the same `request_id`, resolving the future. This gives clean request/response semantics over a persistent connection.

```json
// Orchestrator → Worker
{"type": "command", "request_id": "a1b2c3", "op": "bash", "params": {"command": "ls -la"}}

// Worker → Orchestrator
{"type": "result", "request_id": "a1b2c3", "op": "bash",
 "stdout": "total 8\ndrwxr-xr-x 2 root root 4096 ...\n", "stderr": "", "exit_code": 0}
```

### CF API Client

The orchestrator authenticates to the CF API using password grant (`POST /oauth/token` with `grant_type=password`, `client_id=cf`). Tokens are cached with a 60-second safety margin before expiry.

API calls used:
- `POST /v3/apps/{guid}/tasks` — create task
- `GET /v3/tasks/{guid}` — check task state
- `POST /v3/tasks/{guid}/actions/cancel` — stop task

## The Worker

A single Python file (`worker.py`, ~300 lines) that runs inside the Docker container.

### Operations

| Op | Params | Notes |
|----|--------|-------|
| `bash` | `command`, `timeout` (default 300s), `cwd` | Uses `asyncio.create_subprocess_shell`. Output truncated at 100KB. |
| `read` | `path`, `offset`, `limit` | Line-based offset/limit. |
| `write` | `path`, `content` | Creates parent directories automatically. |
| `glob` | `pattern` | `recursive=True`. Returns paths relative to workspace. |
| `grep` | `pattern`, `path`, `include`, `max_results` (default 100) | Wraps `grep -rn`. 30-second timeout. |

### Concurrency and Limits

- Max 4 concurrent operations (semaphore)
- 100KB output truncation per command
- 60-second idle timeout — if no command arrives, the worker closes the WebSocket and exits. Only `command` messages reset the timer; pings and heartbeats do not.
- Heartbeat every 20 seconds

### Error Protocol

```json
{"type": "error", "request_id": "a1b2c3", "error": "No such file: /foo",
 "error_class": "not_found"}
```

Error classes: `unknown_op`, `not_found`, `permission_denied`, `internal`.

### Shutdown

The worker handles `SIGTERM` and `SIGINT` via an `asyncio.Event`. On signal or idle timeout, it closes the WebSocket and exits. The CF Task transitions to `SUCCEEDED` (clean exit) or `FAILED` (non-zero). The orchestrator detects the disconnect in its WebSocket handler and marks the session as `stopped`.

## The Docker Image

Based on `python:3.12-slim`. Contains:

- The worker script at `/sandbox/worker.py`
- A `/workspace` directory for user files
- System packages: `git`, `gcc`, `make`, `curl`, `wget`, `iproute2`, `net-tools`, `traceroute`
- Python packages: `websockets`, `numpy`, `pandas`, `httpx`, `matplotlib`, `pyyaml`, `jinja2`
- Whatever else the use case requires — the image is fully customizable

The CF app manifest for the sandbox template:
```yaml
applications:
- name: sandbox-template
  docker:
    image: registry.example.com/sandbox:latest
  instances: 0       # never runs as a web process
  memory: 256M
  no-route: true     # no HTTP route needed
```

`instances: 0` means the app never runs a web process. It exists solely so CF Tasks can be created against it. Each task gets the configured memory (512MB in this implementation, configurable per-task via the API).

## Security

### Token-Based WebSocket Auth

The connect token is generated per-session, embedded in the task command, validated on WebSocket handshake, and cleared after first use. This prevents:
- Unauthorized connections to the orchestrator's WebSocket endpoint
- Replay attacks (token is single-use)
- Cross-session impersonation (token is bound to a specific user session)

WebSocket close codes:
- `4001` — no pending session for this user
- `4003` — invalid or missing token

### Container Isolation

Each CF Task runs in its own container with:
- Separate filesystem (copy-on-write from the Docker image)
- Separate process namespace
- Separate network namespace with ASG-enforced egress rules
- No shared state between sessions

### Network Isolation via ASGs

CF Application Security Groups control container egress at the iptables level. The sandbox space can have a restricted ASG policy:

```json
[
  { "protocol": "tcp", "destination": "10.0.0.5/32", "ports": "443", "log": true }
]
```

With `public_networks` and `dns` unbound from the sandbox space, containers have no internet access unless explicitly granted. ASG changes propagate dynamically (~60 seconds) — no container restarts required.

The orchestrator space retains normal egress since it needs to reach the CF API and external services.

### What's Not Covered

- The orchestrator's REST endpoints have no authentication. In production, add JWT validation or CF UAA token verification.
- CF admin credentials are passed as environment variables. Use a bound service instance or credential store instead.
- No TLS between worker and orchestrator beyond what the gorouter provides (the `wss://` connection routes through the CF gorouter, which terminates TLS).

## Performance

Measured on a landscape with Docker image cached on the Diego cells:

| Metric | Value |
|--------|-------|
| Task creation → WebSocket connected | ~5 seconds (Docker, cached image) |
| Task creation → WebSocket connected | ~8 seconds (buildpack) |
| Command round-trip (simple bash) | <100ms |
| Concurrent sandboxes per orchestrator | Limited by CF Task quotas, not orchestrator |

First pull of a Docker image adds 10-30 seconds depending on image size and registry latency. Subsequent tasks on the same cell use the cached image.

## Scaling Considerations

- **Orchestrator sessions are in-memory.** A restart loses all active sessions. For durability, persist session state to a bound database and implement reconnection logic in the worker.
- **One orchestrator instance handles many sandboxes.** The bottleneck is the CF Task quota (per-space or per-org), not the orchestrator's connection capacity.
- **Task cleanup is automatic.** CF Tasks that exit (SUCCEEDED or FAILED) are garbage-collected by Diego. The idle timeout in the worker ensures abandoned sessions don't accumulate.
- **The WebSocket goes through the gorouter.** Gorouter's `websocket_idle_timeout` (default 15 minutes in CF) applies. The worker's 20-second heartbeat keeps the connection alive well within this window.

## What CF Gives You for Free

| Concern | CF mechanism |
|---------|-------------|
| Container isolation | Diego cells, Garden containers (user namespaces, cgroups, seccomp) |
| Network egress control | Application Security Groups (iptables rules per space) |
| Resource limits | Per-task memory and disk via v3 API |
| Image distribution | Docker image cached on cells after first pull |
| Task lifecycle | Diego handles placement, health monitoring, cleanup |
| TLS termination | Gorouter terminates TLS for the WebSocket callback |
| Audit trail | CF events API records task creation, cancellation |
| Multi-tenancy | Orgs and spaces with RBAC for who can create tasks |

No sidecar processes, service meshes, or external schedulers. The CF platform does the container orchestration; this system just adds the session management and communication layer on top.
