# Shim + Daemon Design

## Context

`gocacheprog` works well in direct mode when a CI step runs a single `go` process:

- one helper process
- one preload decision
- one local cache owner
- one `cache-used` upload at shutdown

This breaks down when a single job starts many `go` processes in parallel, for example:

- `golangci-lint` invoking `go/packages`
- generators or wrapper scripts spawning multiple `go list` / `go test` / `go vet` calls
- multiple tools in one shell step sharing the same `GOCACHEPROG` and local cache dir

With direct mode, every `go` process starts its own helper. That leads to:

- repeated preload requests for the same cache scope
- races around "is local cache empty?"
- multiple short-lived owners of the same local cache dir
- multiple writers competing to refresh the same rolling `changes-id` manifest

This document describes the intended replacement architecture.

## Goals

1. One local owner of the cache dir per CI job/workspace.
2. One preload flow per local cache scope, not one per `go` process.
3. No local preload storm when many `go` processes start at once.
4. Preserve the current remote behavior:
   - same HTTP protocol to `gocacheprogd`
   - same remote batching behavior
   - same preload semantics
   - same `cache-used` manifests
5. Keep local responses low-latency.
6. Keep local daemon cache files uncompressed so returned `DiskPath` is directly usable by the Go tool.
7. Make the daemon a multiplexor over one shared `Proxy`, not a second cache implementation.

## Non-goals

1. Changing the `cmd/go` `GOCACHEPROG` protocol.
2. Replacing the remote HTTP server protocol.
3. Making local front-side traffic batched for roundtrip reduction.
4. Supporting cross-job daemon reuse.

## High-level architecture

```text
cmd/go
  -> shim (stdio cacheprog protocol)
  -> local daemon (HTTP over Unix socket)
  -> remote gocacheprogd (HTTP over TCP/HTTPS)
```

The important architectural rule is:

- the daemon owns one shared `local.Proxy`
- all shim sessions feed requests into that one proxy instance
- the daemon must not introduce an alternate remote-fetch path that bypasses proxy batching/barriers

In other words, shim mode is not a different cache algorithm. It is a transport topology change that gives many `cmd/go` processes one shared proxy.

### Roles

#### Shim

The shim is the executable pointed to by `GOCACHEPROG`.

Responsibilities:

- speak the official cacheprog JSON protocol over stdin/stdout
- forward requests immediately to the local daemon
- return daemon-provided `DiskPath` values transparently to `cmd/go`

Non-responsibilities:

- no preload
- no local cache ownership
- no remote batching
- no manifest upload
- no local filesystem policy

The shim must stay thin.

#### Local daemon

The daemon is the single owner of the local cache dir for one job/workspace.

Responsibilities:

- own local uncompressed cache storage
- multiplex many shim sessions onto one shared `Proxy`
- serve local hits and local `DiskPath`s
- run preload at most once per daemon lifetime / cache scope
- aggregate `used ActionID`s across all shim sessions
- preserve current remote-facing batching behavior by reusing proxy batching, not by re-implementing remote fetches
- upload `cache-used` manifests at shutdown

Non-responsibilities:

- no stdio cacheprog protocol
- no separate local per-process state

#### Remote server

The remote `gocacheprogd` remains unchanged conceptually:

- HTTP transport
- optional compression
- preload source resolution
- manifest storage
- eviction/status/auth

## Local transport

Use HTTP over Unix socket between shim and daemon.

Reasons:

- reuse the existing HTTP protocol and client/server code
- avoid inventing a second internal RPC
- avoid port collisions
- keep job-local scoping simple

The shim should use a dedicated Unix-socket HTTP client.

This transport is only for session multiplexing and request routing.
It must not change how remote misses are resolved once inside the daemon.

## Request flow

### Put

```text
cmd/go -> shim -> daemon -> local store -> async/queued remote put
```

Rules:

1. Shim forwards the body as plain local bytes.
2. Daemon stores the object in local uncompressed storage immediately.
3. Daemon returns the local `DiskPath` to the shim immediately.
4. Daemon schedules the remote upload using the existing remote-side batching/compression behavior.

Invariant:

- local daemon storage is always uncompressed
- remote storage may compress according to current policy

### Get

```text
cmd/go -> shim -> daemon
  local hit => immediate local DiskPath
  local miss => daemon waits for shared proxy resolution, stores locally uncompressed, returns local DiskPath
```

Rules:

1. Shim must not implement its own local cache.
2. Daemon is the only local cache owner.
3. A local miss must go through the daemon's shared `Proxy` batching/barrier machinery.
4. The daemon must not call upstream directly in a special shim-only miss path.
5. If the remote object is compressed, the daemon decompresses it before writing local disk.
6. Shim returns the daemon-local `DiskPath` unchanged.

Invariant:

- any `DiskPath` returned to `cmd/go` points to uncompressed daemon-local storage

This is the most important behavioral constraint of the redesign:

- shim requests may block waiting for the daemon's normal proxy batch to resolve
- that is expected and correct
- the daemon should answer only after the action is either materialized locally or known to be a miss

The daemon therefore behaves like a multiplexed session router over one proxy, not like a metadata service.

## Compression model

Compression policy belongs only on the remote-facing side.

### Shim -> daemon

- no compression policy
- treat body as plain local content

### Daemon local cache

- uncompressed only

### Daemon -> remote

- current compression behavior is preserved
- sufficiently large entries may be compressed for upload/storage/download

## Preload model

Preload belongs only to the daemon.

### Required behavior

1. Shim never starts preload.
2. Daemon may start preload once during startup.
3. Daemon must serve requests immediately even if preload is still running.
4. Preload is opportunistic, not a startup barrier for the whole job.

### Motivation

This removes the current direct-mode race where many short-lived helpers all see an empty cache dir and all trigger preload.

## Batching model

Batching behavior must differ across boundaries.

### Shim -> daemon

- no timer-based batching
- no artificial barrier delay
- requests should be forwarded immediately

Reason:

- localhost / Unix socket roundtrips are cheap
- batching here only adds latency

### Daemon -> remote

- preserve current remote batching/barrier behavior

Reason:

- this is where roundtrip reduction still matters

Important clarification:

- "preserve current remote batching" means reuse the same proxy batching implementation as direct mode
- it does not mean "add a second daemon-specific batching layer beside proxy"
- it also does not mean "perform eager direct upstream fetches for shim requests"

## Manifest model

The daemon is the only writer of local-session manifest state.

### Upload behavior

The daemon aggregates `ActionID`s used across all shim sessions and uploads once at shutdown.

This removes the direct-mode race where many helper processes compete to overwrite/append the same rolling `changes-id` manifest.

### Semantics

Existing semantics should remain:

- `commit`: exact manifest bucket
- `changes-id`: rolling latest manifest bucket
- `build-type`: scope boundary
- `base-commit` / `parent-commit`: preload lookup inputs only

## Lifecycle

### Startup

1. CI starts the daemon explicitly in the background.
2. CI exports `GOCACHEPROG` pointing to shim mode.
3. All Go tools in the job talk to the same daemon.

### Shutdown

1. CI sends graceful termination in an `always()` step.
2. Daemon:
   - stops accepting new work
   - flushes pending remote puts if needed
   - uploads aggregated `cache-used`
   - closes local resources

### Job cancellation

Hard cancellation may skip graceful shutdown.
That is acceptable on ephemeral GitHub-hosted runners.

## Configuration model

The daemon, not the shim, owns the remote/cache policy flags:

- `-cache-dir`
- `-remote-url`
- `-auth-token`
- `-preload`
- `-max-file-bytes`
- `-commit`
- `-changes-id`
- `-build-type`
- `-base-commit`
- `-parent-commit`

The shim should need only enough configuration to find the daemon:

- `-daemon-socket`

Potentially also optional local logging flags.

## Testing strategy

Tests must use the same extracted facades the CLI uses.

Bad pattern:

- rebuild remote/daemon/shim topology manually inside each test

Required pattern:

- extract runtime construction under `internal/...`
- tests call those constructors/facades directly

Code placement rule:

- `cmd/gocacheprog/main.go` should stay relatively minimal and mostly perform flag parsing plus wiring
- runtime/session/server/shim/proxy-facing code should live under `internal/local`
- tests should primarily target those `internal/local` facades instead of rebuilding behavior through `main.go`

Required topology rule:

- tests must validate the daemon as a shared-proxy multiplexor
- tests must not accidentally validate a shim-only bypass path that production code should not have

### Test layers

1. Unit tests
   - local store compression semantics
   - proxy behavior
   - manifest upload logic
   - shim protocol adaptation

2. Runtime integration tests
   - direct <-> remote put/get with compression assertions
   - daemon <-> remote put/get with compression assertions
   - shim -> daemon -> remote put/get end to end

3. Minimal CLI smoke tests
   - only enough to prove wiring, not to duplicate runtime tests

## Implementation constraints

1. Do not mix test-only runtime setup with production runtime setup.
2. Keep `cmd/gocacheprog/main.go` thin; it should mainly parse flags and call extracted runtime code.
3. Keep daemon-local storage policy explicit and testable.
4. Keep shim intentionally dumb.
5. Avoid incremental layering of daemon logic on top of direct-mode ad hoc code without first extracting shared runtime pieces.
6. Do not add shim-only remote lookup paths that bypass `local.Proxy`.
7. Do not introduce pseudo-`HEAD` or metadata-only APIs as a substitute for true shared proxy resolution.
8. Prefer placing new runtime logic under `internal/local` rather than growing `cmd/gocacheprog/main.go`.

## Rollout plan

1. Keep current direct-only implementation stable.
2. Extract server/runtime helpers first, while still direct-only.
3. Extract a shared cacheprog session runner abstraction so direct and shim do not fork the JSON loop logic ad hoc.
4. Add daemon runtime as "one shared proxy behind many sessions".
5. Add shim runtime as a thin transport adapter to that daemon.
6. Add end-to-end runtime tests against extracted facades.
7. Only then wire daemon/shim flags back into the CLI and sample workflow.

## Acceptance criteria

The redesign is successful if:

1. A CI job with many parallel `go` invocations causes one local preload, not many.
2. The daemon is the only local owner of the cache dir.
3. Local responses do not pay remote-style batching delays.
4. Remote batching and compression still work.
5. Returned `DiskPath`s always point to daemon-local uncompressed files.
6. Runtime tests use the same setup facades as the CLI implementation.
