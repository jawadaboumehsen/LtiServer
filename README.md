# LtiRomServer API Documentation

**Protocol Version**: `1.2.0` (Backwards-compatible with `1.1.0`)

`LtiRomServer` is a lightweight, embedded Kotlin/Ktor server running natively within Linux/WSL environments to execute Android ROM toolchain operations, orchestrate durable pipeline runs, and stream sequenced events back to LtiRom Studio.

---

## Authentication & Protocol Headers

All requests under `/api/v1` (except unauthenticated `/api/v1/health`) require authentication:
- **Header**: `Authorization: Bearer <AUTH_TOKEN>` or query parameter `?token=<AUTH_TOKEN>`.
- **Protocol Header**: `X-LtiRom-Protocol-Version: 1.2.0` (Server supports `1.1.0` and `1.2.0`).

---

## Durable Runs API (v1.2.0)

Durable runs decouple client execution from the server process. If the client disconnects or GUI restarts, runs continue executing on the server with full journal persistence and resume capabilities.

### 1. Start a Durable Run
- **Route**: `POST /api/v1/runs`
- **Request Body**:
  ```json
  {
    "id": "run-12345",
    "target": { ... },
    "pipeline": {
      "steps": [ ... ]
    },
    "workingDirectory": "/home/user/LtiRomWorkDir/workspaces/ws_001",
    "idempotencyKey": "setup:build:avbtool",
    "workspaceLock": "ws_001"
  }
  ```
- **Responses**:
  - `201 Created`: New run started. Returns `RunHandle`.
  - `200 OK`: Idempotent match found. Returns existing `RunHandle`.
  - `409 Conflict`: Conflicting run in progress holding the workspace lock.
  - `422 Unprocessable Entity`: Invalid or inaccessible working directory.

### 2. Inspect a Run
- **Route**: `GET /api/v1/runs/{runId}`
- **Response**: `200 OK` with full `BuildRun` state (status, current step, exit code, start/finish times). Returns `404 Not Found` if the run ID is unknown.

### 3. List Runs
- **Route**: `GET /api/v1/runs[?workspaceLock=<lock>]`
- **Response**: `200 OK` with `List<BuildRun>`.

### 4. Attach to a Run Stream
- **Route**: `WS /api/v1/runs/{runId}/attach?fromSeq=<sequenceNumber>`
- **Behavior**:
  - Replays events starting from `fromSeq` (default `1`).
  - Streams live `SequencedStreamEvent` items in real-time.
  - Emits periodic heartbeat frames (`StreamEvent.Heartbeat`).
  - Gracefully closes when the run completes.

### 5. Cancel a Run
- **Route**: `POST /api/v1/runs/{runId}/cancel[?signal=SIGTERM|SIGKILL]`
- **Response**: `200 OK` with `ProcessCancellationResponse`.

---

## Tool Execution & Utilities

### Dynamic Tool Discovery
- `GET /api/v1/tools`: Returns catalog of discovered binaries, paths, and metadata.
- `POST /api/v1/tools/refresh`: Rescans environment tools and returns refreshed catalog.

### Discrete Tool Execution
- `POST /api/v1/tools/execute`: Executes a discrete tool synchronously.
  ```json
  {
    "toolName": "printenv",
    "arguments": ["HOME"],
    "workingDirectory": "/home/user",
    "timeoutMs": 15000
  }
  ```

### File Upload
- `POST /api/v1/binary/upload?path=<destinationPath>`
  - Streams binary payload directly to filesystem.
  - Destination must reside within configured allowed base directories (`~/LtiRomWorkDir`, `~/.ltirom`).

### Path Translation
- `POST /api/v1/path/translate`
  - Translates paths between Windows host (`\\wsl.localhost\...`, `C:\...`) and WSL Linux paths.

### Health Check
- `GET /api/v1/health`
  - Unauthenticated diagnostic probe returning host distribution, uptime, tool count, and daemon status.

---

## Deprecated Endpoints (v1.1.0 Legacy)

> [!WARNING]
> The following endpoints are deprecated in Protocol 1.2.0 and return HTTP header `Deprecation: true`. They will be removed in a future release.

1. **`WS /api/v1/tools/stream`**
   - *Deprecated*: Use `POST /api/v1/runs` and `WS /api/v1/runs/{runId}/attach` instead.
2. **`POST /api/v1/tools/execute/{executionId}/cancel`**
   - *Deprecated*: Use `POST /api/v1/runs/{runId}/cancel` instead.
