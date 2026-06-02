# gocacheprog

[![Build Status](https://github.com/vearutop/gocacheprog/workflows/test-unit/badge.svg)](https://github.com/vearutop/gocacheprog/actions?query=branch%3Amaster+workflow%3Atest-unit)
[![Coverage Status](https://codecov.io/gh/vearutop/gocacheprog/branch/master/graph/badge.svg)](https://codecov.io/gh/vearutop/gocacheprog)
[![GoDevDoc](https://img.shields.io/badge/dev-doc-00ADD8?logo=go)](https://pkg.go.dev/github.com/vearutop/gocacheprog)
[![Time Tracker](https://wakatime.com/badge/github/vearutop/gocacheprog.svg)](https://wakatime.com/badge/github/vearutop/gocacheprog)
![Code lines](https://sloc.xyz/github/vearutop/gocacheprog/?category=code)
![Comments](https://sloc.xyz/github/vearutop/gocacheprog/?category=comments)

`gocacheprog` can act as:

- a direct `GOCACHEPROG` helper
- a local daemon + shim pair for CI
- a batch restore/save tool for native `GOCACHE`
- a remote HTTP cache server when started with `-listen`

The project is aimed at large CI workloads where:

- remote Go cache reuse matters
- preload can materially reduce cold-start time
- multiple jobs and multiple `go` invocations need to share cache state safely

## What It Does

At a high level:

- `gocacheprog -listen ...` stores cached Go action results and serves them over HTTP
- `gocacheprog` keeps a local on-disk cache and proxies misses to the remote server
- `gocacheprog -restore-cache` and `-save-cache` sync native `GOCACHE` files in batch mode
- preload can pull a relevant working set into the local cache before the build starts
- cache usage manifests store the list of cached entries actually used by a build, so later runs can preload only likely-needed entries

The design supports:

- exact commit reuse via `-commit`
- rolling branch/PR reuse via `-changes-id`
- optional fallback relevance via `-base-commit`
- optional build partitioning via `-build-type`

## Install

```bash
go install github.com/vearutop/gocacheprog@latest
```

Or download binaries from [releases](https://github.com/vearutop/gocacheprog/releases).

Example on Linux AMD64:

```bash
wget https://github.com/vearutop/gocacheprog/releases/latest/download/linux_amd64.tar.gz
tar xf linux_amd64.tar.gz
rm linux_amd64.tar.gz
./gocacheprog -help
```

## Modes

`gocacheprog` has four practical modes:

1. Direct mode
2. Daemon mode
3. Shim mode
4. Native `GOCACHE` batch mode

Important flags:

- `-cache-dir`
- `-remote-url`
- `-auth-token`
- `-preload`
- `-preload-size`
- `-commit`
- `-changes-id`
- `-build-type`
- `-base-commit`
- `-parent-commit`
- `-max-file-bytes`
- `-canonicalize-timestamps`

Important batch-mode flags:

- `-restore-cache`
- `-save-cache`
- `-max-file-bytes`
- `-save-cache-chunk-bytes`
- `-cache-dir`
- `-remote-url`
- `-commit`
- `-changes-id`
- `-build-type`
- `-base-commit`
- `-parent-commit`

### Direct Mode

This is the original mode where each `go` invocation starts its own helper:

```bash
export GOCACHEPROG="/path/to/gocacheprog \
  -cache-dir ./build-cache \
  -remote-url https://cache.example.com \
  -preload \
  -preload-size 3000000 \
  -commit $COMMIT \
  -changes-id $CHANGES_ID \
  -build-type unit \
  -base-commit $BASE_COMMIT"
go test ./...
```

This works well for simple flows with one Go invocation per step.

### Daemon + Shim Mode

This is the preferred CI setup for complex jobs.

Start a local daemon once:

```bash
/path/to/gocacheprog \
  -listen unix:///tmp/gocacheprog.sock \
  -cache-dir ./build-cache \
  -remote-url https://cache.example.com \
  -preload \
  -preload-size 3000000 \
  -commit "$COMMIT" \
  -changes-id "$CHANGES_ID" \
  -build-type unit \
  -base-commit "$BASE_COMMIT" \
  > /tmp/gocacheprog-daemon.log 2>&1 &
```

Then point `GOCACHEPROG` at the shim:

```bash
export GOCACHEPROG="/path/to/gocacheprog -remote-url unix:///tmp/gocacheprog.sock"
go test ./...
```

### Explicit Preload Then Daemon

If you want preload in a separate CI step, use `-preload-only` against the same cache dir:

```bash
/path/to/gocacheprog \
  -cache-dir ./build-cache \
  -remote-url https://cache.example.com \
  -preload-only \
  -preload-size 3000000 \
  -changes-id "$CHANGES_ID" \
  -build-type lint \
  -base-commit "$BASE_COMMIT"
```

This preloads into `./build-cache` and exits without uploading `cache-used`.

Then start daemon + shim later with the same cache dir:

```bash
/path/to/gocacheprog \
  -listen unix:///tmp/gocacheprog.sock \
  -cache-dir ./build-cache \
  -remote-url https://cache.example.com \
  -skip-preload \
  -commit "$COMMIT" \
  -changes-id "$CHANGES_ID" \
  -build-type lint \
  -base-commit "$BASE_COMMIT" \
  > /tmp/gocacheprog-daemon.log 2>&1 &
```

The daemon will skip preload explicitly and still upload `cache-used` on shutdown.

Why this is better:

- one local owner of `./build-cache`
- one preload per job/cache scope instead of one per `go` invocation
- no local preload storm when one script starts many `go` commands
- daemon preserves the remote batching behavior
- shim talks to daemon over Unix socket with no local batching delay
- daemon returns its own local `DiskPath`, and shim passes that through to the Go tool

### Native `GOCACHE` Batch Mode

This mode does not use `GOCACHEPROG` during the build.

Instead:

1. `gocacheprog -restore-cache` preloads likely-needed native cache files into a local `GOCACHE` directory
2. Go commands run against that local `GOCACHE` normally
3. `gocacheprog -save-cache` uploads freshly created native cache files after the build

Example:

```bash
GOCACHE_DIR=./build-gocache

/path/to/gocacheprog \
  -restore-cache \
  -cache-dir "${GOCACHE_DIR}" \
  -remote-url https://cache.example.com \
  -commit "$COMMIT" \
  -changes-id "$CHANGES_ID" \
  -build-type unit \
  -base-commit "$BASE_COMMIT"

export GOCACHE="${GOCACHE_DIR}"
go test ./...

/path/to/gocacheprog \
  -save-cache \
  -cache-dir "${GOCACHE_DIR}" \
  -remote-url https://cache.example.com \
  -commit "$COMMIT" \
  -changes-id "$CHANGES_ID" \
  -build-type unit \
  -base-commit "$BASE_COMMIT"
```

Why this mode exists:

- no remote cache roundtrips on the build hot path
- native local `GOCACHE` behavior during the build
- finer-grained restore/save than archive-style CI cache blobs
- remote storage still uses compression and manifest-targeted restore

How it works:

- manifests contain relative native `GOCACHE` file paths, not `ActionID`s
- restore streams matching files from the remote server and materializes native cache files locally
- local restore preserves file contents and executable permission bits, but intentionally does not restore historical mtimes
- `-max-file-bytes` can skip pathological large single cache files during both native restore and native save
- save walks the local `GOCACHE` tree, skips files that were already restored in this job, compresses payloads client-side, and streams them to the server
- the server stores compressed file objects and merges uploaded file paths into the relevant manifests; when the server also runs with `-max-file-bytes`, oversized objects are silently skipped on save and treated as misses on restore
- large `-save-cache` uploads are split into outer HTTP chunks, so the mode can work behind restrictive reverse proxies such as default nginx `client_max_body_size=1m`

In `GOCACHEPROG` mode, `-max-file-bytes` also skips remote `Put` uploads for oversized cache entries while still keeping them in the local cache. When the server is started with the same flag, oversized entries are also not stored in or served from the remote cache.

## Recommended CI Shape

For daemon + shim mode, the sample workflow in [sample-workflow.yml](sample-workflow.yml) shows the intended GitHub Actions setup:

1. download `gocacheprog`
2. canonicalize timestamps
3. start daemon in background
4. export `GOCACHEPROG` as shim mode
5. run all Go tools
6. stop daemon in an `always()` step

For native batch mode, [sample-gocache-workflow.yml](sample-gocache-workflow.yml) shows the corresponding shape:

1. download `gocacheprog`
2. canonicalize timestamps
3. restore native `GOCACHE`
4. export `GOCACHE`
5. run all Go tools normally
6. save native `GOCACHE` in an `always()` step

## Timestamp Canonicalization

Fresh CI checkouts often get unstable mtimes, which can cause false cache invalidation.

`gocacheprog` can canonicalize them deterministically:

```bash
gocacheprog -canonicalize-timestamps .
```

This:

- walks the repo
- includes hidden paths too
- assigns deterministic file mtimes based on file content
- normalizes directory mtimes

This is useful when:

- `git-restore-mtime` is too slow
- shallow history makes git-based restore inaccurate
- repo-local non-Go files participate in test inputs

## Manifest Model

The server stores manifest sidecars separately from cache entries.

Each manifest is a newline-delimited list of `ActionID`s that were actually used by a build or test run.

In native `GOCACHE` batch mode, manifests instead contain relative native cache file paths.

Scopes:

- `commit`
- `changes-id`
- `build-type`

Preload source resolution order is:

1. `commit`
2. `parent`
3. `changes`
4. `base`

Interpretation:

- `commit`
  - strongest signal for exact reruns
- `parent`
  - optional precise git-history hint
- `changes-id`
  - stable rolling label such as `owner/repo#123`
- `base`
  - fallback relevance from target branch state

### `-changes-id`

`-changes-id` is intentionally free-form. A common PR label is:

```bash
owner/repo#123
```

or in GitHub Actions:

```bash
${{ github.repository }}#${{ github.event.pull_request.number }}
```

It is the preferred long-lived reuse key for PR-style CI.

In both modes, `changes-id` is a rolling reuse key. In native `GOCACHE` batch mode it currently merges like `commit` rather than replacing prior state.

### `-build-type`

Use `-build-type` to isolate incompatible cache manifest streams, for example:

- `unit`
- `lint`
- `gen`
- `race`

This keeps a `lint` manifest from clobbering a `unit` manifest for the same `changes-id`.

## Compression

Remote cache traffic and remote cache storage support compression.

That reduces:

- upload size
- download size
- remote storage usage

In daemon mode, this is intentionally separate from the local cache layout:

- daemon-local cache files stay uncompressed so they can be passed directly back to the Go tool as local disk paths
- remote uploads, downloads, and remote storage use compression automatically for sufficiently large cache entries

## Cold vs Warm Manifest Updates

There is a subtle CI edge case when one script runs many `go` invocations.

Behavior in `GOCACHEPROG` mode:

- if local cache was empty at helper startup:
  - `changes-id` manifest is replaced on upload
  - this refreshes the rolling manifest for that change stream
- if local cache was already populated:
  - `changes-id` manifest is merged/appended
  - this avoids secondary `go` invocations in the same job clobbering the first one

`commit` manifests are merged/appended rather than treated as rolling pointers.

In native `GOCACHE` batch mode, both `commit` and `changes-id` manifests are merged.

In daemon mode this becomes naturally simpler because one daemon owns the whole local session.

## Local Cache Layout

Remote entries and manifests are separated on disk.

Cache objects live under:

```text
entries/<prefix>/<output-id>
```

Manifests live under:

```text
manifests/<scope>/...
```

This avoids mixing actual cached blobs with sidecar metadata.

## Eviction

Server mode supports a total on-disk cache size limit:

```bash
gocacheprog -listen :8080 -max-disk-bytes 5000000000
```

Native `GOCACHE` storage has a separate quota:

```bash
gocacheprog -listen :8080 -gocache-max-disk-bytes 5000000000
```

Native `GOCACHE` storage also has an age-based retirement policy, defaulting to `48h`:

```bash
gocacheprog -listen :8080 -gocache-max-age 48h
```

Set `-gocache-max-age 0` to disable age-based retirement.

Eviction policy:

- LRU
- delayed background cleanup
- not inline on `Put`

The implementation schedules cleanup after a delay so active CI jobs are less likely to get disrupted by immediate eviction work.

## Authentication

Optional bearer token auth is supported on both client and server.

Server:

```bash
gocacheprog -listen :8080 -auth-token secret-token
```

Client:

```bash
gocacheprog -auth-token secret-token ...
```

If auth is wrong or missing, client startup reports:

```text
authentication failed: -auth-token <value> is missing or incorrect
```

## Observability

### `/status`

Server mode exposes:

```text
/status
```

It returns JSON with:

- `store`
  - hits, misses, puts, index size
  - disk usage
  - eviction state
- `http`
  - preload counters and concurrency limit
- `runtime`
  - heap in use

Byte sizes are also humanized in the JSON.

### Native cache admin endpoints

When server mode has native `GOCACHE` storage enabled, two authenticated admin endpoints are available:

- `/inspect?build-type=...`
- `/clear?build-type=...`

Optional identity narrowing:

- `commit=...`
- `changes-id=...`

Examples:

```text
/inspect?build-type=backend-generate-check
/inspect?build-type=backend-unit&changes-id=owner/repo-123
/clear?build-type=backend-unit&commit=abcdef123456
```

`/inspect` returns JSON with:

- manifest count
- referenced file count
- total compressed and uncompressed size
- max file size
- count and total size for files in the top 10% size band

`/clear` removes matching manifests and deletes native cache objects only when they are no longer referenced by any remaining manifest.

### Native restore/save summaries

Native batch mode logs completion summaries that include:

- file count
- transfer time
- compressed and uncompressed bytes
- restore sources
- server-side timing fields such as `server_prepare_time` and `server_total_time` when available

### Preload logs

Preload logs distinguish:

- `queue_wait`
- `prepare_time`
- `total_time`

This is important because a long preload line can mean:

- actual slow preparation
or
- just waiting behind the preload semaphore

### `/version` session metadata

The initial version probe from `gocacheprog` carries session metadata:

- session id
- start time
- pid
- cache dir
- commit / parent / changes / build type / base

This helps debug CI floods caused by many short-lived helpers starting in parallel.

## Concurrency Notes

### Remote preload concurrency

The server limits concurrent preload preparation with `-preload-limit`.

Default:

```text
2
```

This protects the server from thrashing, but under heavy multi-session startup it can create a backlog.

If needed, raise it moderately, for example:

```bash
gocacheprog -listen :8080 -preload-limit 4
```

Do not jump straight to very high values without observing:

- queue wait
- preparation time
- server I/O behavior

### Why daemon mode exists

One CI job may start many `go` invocations:

- `go test`
- `go list`
- `go vet`
- `golangci-lint` internals
- generators

In direct mode, each helper can decide to preload independently, which creates:

- many local sessions
- repeated `/version`
- repeated `/preload`
- queueing on the remote server

Daemon mode collapses those into one local owner and one preload stream.

## Edge Cases

### GitHub Actions shallow checkout is not enough by itself

Git-based mtime restoration with shallow history may be too inaccurate for stable test cache keys.

Deterministic timestamp canonicalization turned out to be more effective and much cheaper than `fetch-depth: 0`.

### Same-commit reruns and new commits behave differently

Exact reruns are best served by `commit`.

PR evolution is better served by `changes-id`, because GitHub Actions often evaluates pull requests through synthetic or moving PR/base relationships rather than a stable previous commit chain. In practice, the effective merge/base context used by GitHub can change underneath the PR even when the branch is not rebased, so exact parent/merge-commit identities are a weaker rolling cache key than a stable PR-scoped label.

`base-commit` remains useful as a fallback, especially when a PR has no good recent manifest yet.

### Multiple Go invocations in one script

This is the main reason for daemon mode.

Direct mode had two tricky behaviors:

- repeated preloads
- manifest overwrites/appends depending on startup order

Daemon mode resolves the local race where many short-lived helpers can start in parallel, all decide to preload, and compete to refresh the same rolling manifest state.

### Repo-local non-Go files can still matter

Changes in files like:

- `.github/...`
- `.gitignore`
- test fixtures
- config files

can still affect test cache inputs.

That is one reason timestamp normalization includes hidden paths too.

## Status of the Design

The direction is:

- remote HTTP cache server
- local daemon + shim for CI
- selective preload via manifests
- deterministic timestamp canonicalization
- job/build-type scoped rolling manifests

That is the setup reflected in the sample workflow and the codebase.
