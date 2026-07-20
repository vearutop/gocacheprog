# gocacheprog

[![Build Status](https://github.com/vearutop/gocacheprog/workflows/test-unit/badge.svg)](https://github.com/vearutop/gocacheprog/actions?query=branch%3Amaster+workflow%3Atest-unit)
[![Coverage Status](https://codecov.io/gh/vearutop/gocacheprog/branch/master/graph/badge.svg)](https://codecov.io/gh/vearutop/gocacheprog)
[![GoDevDoc](https://img.shields.io/badge/dev-doc-00ADD8?logo=go)](https://pkg.go.dev/github.com/vearutop/gocacheprog)
[![Time Tracker](https://wakatime.com/badge/github/vearutop/gocacheprog.svg)](https://wakatime.com/badge/github/vearutop/gocacheprog)
![Code lines](https://sloc.xyz/github/vearutop/gocacheprog/?category=code)
![Comments](https://sloc.xyz/github/vearutop/gocacheprog/?category=comments)

`gocacheprog` is a remote Go build/test cache purpose-built for CI, and a drop-in replacement for
`actions/cache` when what you're actually caching is `GOCACHE`/`GOPATH` build output: precise
commit/PR-scoped reuse, preload before the build starts, and no tarball-shaped cache blobs.

In GitHub Actions it comes down to one setup call and one teardown call:

```yaml
- run: gocacheprog -github-actions-init "https://gocache.example.com?auth=${{ secrets.GOCACHE_AUTH }}"
- run: go test ./...
- run: gocacheprog -github-actions-done
  if: ${{ always() }}
```

Everything else — commit/PR scoping, preload, daemon lifecycle, native `GOCACHE` restore/save — is
handled for you with sane defaults. See [ADVANCED.md](ADVANCED.md) for manual/low-level control
and internals.

## Server Setup

You need a running `gocacheprog` server somewhere your CI can reach — it's the same binary,
started with `-http`/`-https` instead of `-github-actions-init`.

```bash
go install github.com/vearutop/gocacheprog@latest
```

Or download a binary from [releases](https://github.com/vearutop/gocacheprog/releases):

```bash
wget https://github.com/vearutop/gocacheprog/releases/latest/download/linux_amd64.tar.gz
tar xf linux_amd64.tar.gz
rm linux_amd64.tar.gz
./gocacheprog -http :8080 -auth-token secret-token -max-disk-bytes 2000000000
```

See [ADVANCED.md](ADVANCED.md#self-hosting-the-server) for running the server via Docker/Docker
Compose, disk/age-based eviction, native `GOCACHE` storage quotas, and observability endpoints
(`/status`, `/inspect`, `/clear`).

## Quick Start

Once you have a server URL, add three steps to your job:

```yaml
- name: Init gocacheprog
  run: |
    wget -q -O /tmp/gocacheprog.tar.gz https://github.com/vearutop/gocacheprog/releases/latest/download/linux_amd64.tar.gz
    tar xf /tmp/gocacheprog.tar.gz -C /tmp
    rm /tmp/gocacheprog.tar.gz
    /tmp/gocacheprog -github-actions-init "https://gocache.example.com?auth=${{ secrets.GOCACHE_AUTH }}&build_type=unit"

- name: Test
  run: go test ./...

- name: Finalize gocacheprog
  if: ${{ always() }}
  run: /tmp/gocacheprog -github-actions-done
```

`-github-actions-init` takes a single DSN — the remote server URL plus optional query
parameters — and:

- derives commit / PR / base-commit scoping automatically from the GitHub Actions environment
  (no `${{ github.event... }}` plumbing needed in your YAML)
- canonicalizes repo timestamps (stable cache keys on fresh checkouts)
- preloads the likely-needed cache working set in bulk
- sets `GOCACHEPROG` (or `GOCACHE`, depending on mode — see [Modes](#modes) below) via
  `$GITHUB_ENV` so the rest of the job just runs `go` normally, quietly: in `direct`/`shim` mode
  the `GOCACHEPROG` helper instances that run under `go build`/`go test` pass `-quiet`, so only a
  fatal error ever prints — routine cache logging doesn't clutter your test output

`-github-actions-done` reverses whatever `-github-actions-init` set up, then prints a final cache
summary: hits/misses/puts, total wall-clock time since `-github-actions-init` started, and — where
a remote round trip is involved — bytes read/written, how many round trips it took, and time spent
on them. The round trip count is usually much lower than the miss count: misses are batched
through a short barrier before each batch is resolved in a single round trip (see
[ADVANCED.md](ADVANCED.md#remote-batch-resolution-concurrency)), and those batches themselves now
run concurrently rather than one at a time — a real speedup for shim mode in particular, where one
daemon fields batches from many `go` invocations over the life of a job.

- `shim` mode: stops the daemon and reports its job-wide cumulative stats, including how many
  separate `go` invocations shared that one daemon session, and `forced_closes` — a count of shim
  clients that hit their close-wait safety timeout instead of a clean shutdown (see
  [ADVANCED.md](ADVANCED.md#a-lost-shim-response-is-bounded-by-a-close-wait-timeout-not-a-hang)).
  Normally `0`; if it's not, the job still finished, but it's worth investigating why a response
  was ever late or lost.
- `gocache` mode: uploads freshly-built cache entries and reports combined restore + save stats
- `direct` mode: no daemon to stop, but each `-quiet` invocation appends its stats (plus its
  parent process's PID and, on Linux, command line) to a small file next to the cache dir, so
  `-github-actions-done` still reports a summary aggregated across invocations if the job ran
  `go` more than once — and, when it did, breaks the count down by parent command, so an
  unexpectedly high invocation count (e.g. `go tool covdata` calls fanned out by `-coverprofile`)
  is traceable straight from the job log

Run it in an `if: ${{ always() }}` step so it also finalizes on test failure.

See [sample-workflow.yml](sample-workflow.yml) for a minimal example.

### DSN Parameters

Only the base URL is required; everything else has a default.

| Parameter                 | Default            | Meaning                                                                 |
|---------------------------|--------------------|--------------------------------------------------------------------------|
| `auth`                     | (none)             | bearer token for the remote server (and the local daemon socket in shim mode) |
| `mode`                     | `shim`             | `direct`, `shim`, `gocache`, or `local-gocache` — see [Modes](#modes)    |
| `cache_dir`                | automatic          | local cache / native `GOCACHE` directory; `~/foo` resolves against `$HOME` |
| `preload_size`             | `3000000`          | maps to `-max-file-bytes`: max size of a single preloaded/cached file    |
| `build_type`               | (none)             | maps to `-build-type`, e.g. `unit`, `race`, `lint` — always prefixed with the repository name (see below) |
| `canonicalize_timestamps`  | `.`                | repo root to canonicalize before anything else                          |
| `skip_canonicalize_timestamps` | `false`        | skip timestamp canonicalization entirely                                |
| `skip_preload`             | `false`            | skip the explicit preload pass entirely (direct/shim only)               |
| `max_cache_bytes`          | `0` (unlimited)    | `local-gocache` mode only; total cache dir size limit, enforced by evicting the oldest files on `-github-actions-done` |

Timestamp canonicalization runs against the repo root by default, since fresh CI checkouts almost
always need it for stable cache keys (see [ADVANCED.md](ADVANCED.md#timestamp-canonicalization)
for why). Set `canonicalize_timestamps=<path>` to canonicalize somewhere other than the checkout
root, or `skip_canonicalize_timestamps=true` to disable it entirely.

`build_type` is always prefixed with `$GITHUB_REPOSITORY` (e.g. `owner-repo-unit`, or just
`owner-repo` if you don't set one) so that pointing several repositories at the same server keeps
their manifests — and the `/inspect`/`/clear` admin endpoints — isolated per repository.

## Modes

`-github-actions-init`'s `mode=` parameter picks one of four underlying strategies. Pick based on
how many `go` invocations your job runs, how large the cache working set is, and whether the
runner has a remote server to talk to at all:

### `mode=direct`

One `gocacheprog` helper per `go` invocation, talking to the remote server directly. No background
process, nothing to tear down beyond the (no-op) `-github-actions-done` call.

Use this for jobs with a single Go invocation per step, e.g. one `go test ./...` or `go build ./...`
step and nothing else.

```yaml
- run: gocacheprog -github-actions-init "https://gocache.example.com?auth=${{ secrets.GOCACHE_AUTH }}&mode=direct"
- run: go test ./...
- run: gocacheprog -github-actions-done
```

### `mode=shim` (default)

A background daemon starts once, preloads once, and serves every subsequent `go` invocation in the
job over a local unix socket — `GOCACHEPROG` just points at the socket.

Use this when a job runs multiple `go` invocations that should share one warm cache session, e.g.
`go generate` followed by `go vet` followed by `go test`, or `golangci-lint` alongside `go test`.
Direct mode would otherwise preload independently (and redundantly) for each invocation.

```yaml
- run: gocacheprog -github-actions-init "https://gocache.example.com?auth=${{ secrets.GOCACHE_AUTH }}&mode=shim"
- run: go generate ./... && go vet ./... && go test ./...
- run: gocacheprog -github-actions-done
  if: ${{ always() }}
```

### `mode=gocache`

Restores/saves native `GOCACHE` files directly — no `GOCACHEPROG` helper runs during the build at
all, so there's no per-lookup protocol overhead.

Use this for large builds with heavy cache lookup traffic, where `GOCACHEPROG` round-trips (even to
a local daemon) become a measurable fraction of build time. The trade-off is coarser reuse: it
restores/saves whole native cache files rather than individual `ActionID` results.

```yaml
- run: gocacheprog -github-actions-init "https://gocache.example.com?auth=${{ secrets.GOCACHE_AUTH }}&mode=gocache"
- run: go test ./...
- run: gocacheprog -github-actions-done
  if: ${{ always() }}
```

### `mode=local-gocache`

Points `GOCACHE` straight at `cache_dir` — no remote server involved at all, no restore, no
preload, no upload on `-github-actions-done`. The remote URL in the DSN is ignored in this mode.

Use this on self-hosted runners with a persistent home directory across jobs, where the cache dir
itself survives between runs on disk: this is the fastest possible option there's no round trip to
pay for, remote or local. `-github-actions-init` logs the cache dir's current file count/size on
the way in, and `-github-actions-done` logs it again on the way out, so growth is visible in the
job log.

```yaml
- run: gocacheprog -github-actions-init "?mode=local-gocache&cache_dir=~/.cache/gocacheprog&max_cache_bytes=5000000000"
- run: go test ./...
- run: gocacheprog -github-actions-done
  if: ${{ always() }}
```

Since the same persistent cache dir can end up shared across several repos/build types on one
self-hosted runner, `local-gocache` mode also keeps a small `gocacheprog.json` right in `cache_dir`:
a per-`build_type` count/first-used/last-used, so you can tell what's actually been using that
runner's disk. Both `-github-actions-init` and `-github-actions-done` print it in full; `-done` is
also what updates it (it re-reads the file immediately before writing, to keep the race window
small when several jobs finish around the same time on the same runner).

Set `max_cache_bytes` to cap the cache dir's total size. It's checked only on
`-github-actions-done` (a single scan of `cache_dir` covers both the size log and eviction, so
setting it doesn't add a second directory walk): once the dir exceeds the limit, the oldest files
(by mtime, wherever they sit in `cache_dir`) are deleted first until the total drops strictly below
90% of the limit — e.g. `max_cache_bytes=10000000000` (10GB) trims down to just under 9GB, not
exactly 9GB and not exactly 10GB. Trimming to that margin rather than to the limit itself avoids
evicting again on almost every subsequent job once the cache settles near the cap. Deleting
individual files out of a native `GOCACHE` is always
safe: it's content-addressed, so a missing file just costs a cache miss, never a corruption.

## Embedding

The entire CLI lives in the importable [`cli`](cli/cli.go) package; `main.go` at the repo root is
just:

```go
func main() {
	if err := cli.Main(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
```

A third-party binary can call `cli.Main()` directly from its own `main` to embed gocacheprog
as-is, or vendor/soft-fork the `cli` package to change behavior without forking the rest of the
module — `internal/...` stays off-limits to external importers either way, but `cli.Main()` is the
whole CLI surface. `cli.Main(options ...func(o *cli.Options))` takes functional options; currently
`Options.VersionLabel` lets an embedding binary append its own identifier to the `-version` output.

## Further Reading

[ADVANCED.md](ADVANCED.md) covers everything below `-github-actions-init`/`-done`:

- manual CLI flags for direct/shim/native modes, for power users who don't want the wrapper
- the full `-h` flag reference
- manifest model, compression, eviction, authentication internals
- observability (`/status`, `/inspect`, `/clear`, logs)
- concurrency notes and known edge cases
