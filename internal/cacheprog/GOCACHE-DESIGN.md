# Native GOCACHE Batch Sync Design

## Context

The existing `gocacheprog` architecture optimizes `GOCACHEPROG` usage by:

- keeping a local cache store
- batching remote lookups
- preloading likely-needed entries
- uploading cache-usage manifests after the run

That works well when remote miss latency is acceptable and when `ActionID`-level accounting is valuable.

For large codebases with many cache lookups, especially when builds involve many remote roundtrips, even an optimized `GOCACHEPROG` path can still lose to native local `GOCACHE`.

This document describes a separate mode that does not use `GOCACHEPROG` at all.

## Goal

Treat `GOCACHE` as a native local directory during the build, and use `gocacheprog` only in two batch phases:

1. restore likely-needed cache files into local `GOCACHE`
2. save newly-created cache files back to remote storage

The main purpose is to remove network roundtrips from the build hot path while still preserving:

- targeted restore instead of whole-cache archive restore
- remote deduplication
- compressed remote storage
- manifest-based preload relevance

## Mental Model

This mode is not a `GOCACHEPROG` helper.

It is a managed remote-backed `GOCACHE` replication system:

- local side: ordinary native `GOCACHE`
- remote side: compressed object store of native cache files
- control plane: restore selected files in, save selected files out

This is conceptually similar to GitHub Actions cache restore/save, but with much finer granularity:

- object identity is per cache file, not per archive
- manifests can restore only likely-needed files
- remote storage deduplicates naturally across jobs and branches

## Non-goals

This mode does not:

- speak the `GOCACHEPROG` protocol
- track or upload `ActionID` usage
- handle online cache misses during the build
- try to make remote storage a byte-for-byte public `GOCACHE` filesystem

The build itself should use plain native `GOCACHE`.

## Lifecycle

The intended job flow is:

1. `gocacheprog -restore-cache ...`
2. run `go test`, `go build`, or other Go commands with native `GOCACHE`
3. `gocacheprog -save-cache ...`

There is no daemon, no shim, and no runtime cache protocol involvement.

## Identity Model

In this mode, `ActionID` has no place.

The stable identity is:

- object key: relative file path inside `GOCACHE`

This means:

- manifests contain relative file paths
- restore requests resolve sets of relative file paths
- save uploads regular files keyed by relative file path

`ActionID` and `OutputID` are specific to `GOCACHEPROG` mode and should not be mixed into this design.

## Cache Identity

The manifest scoping model from current preload remains useful:

- `commit`
- `changes-id`
- `build-type`
- `base-commit`
- `parent-commit`

In this mode, those values no longer identify sets of `ActionID`s`.
They identify manifests of native cache file paths.

Preload source resolution should remain:

1. `commit`
2. `parent`
3. `changes`
4. `base`

The resulting file-path sets are unioned before restore.

No extra Go-version namespace is required for correctness.
If a Go version change makes old cache files irrelevant, those files will simply stop being reused and will eventually be evicted.

## Object Model

Each remote object represents one native cache file.

Suggested metadata:

- `Path`: relative path under `GOCACHE`
- `Size`: original file size
- `WireSize`: transferred/stored size
- `ModTime`: original file mtime
- `IsCompressed`: whether payload is compressed in transit/at rest

Compression is a storage and transport concern, not an identity concern.

Important separation:

- manifest layer decides which file paths matter
- object layer decides how file contents are stored efficiently

## Compression

Compression should follow the same principle as current remote cache entries:

- local `GOCACHE` files remain native and uncompressed
- remote storage may compress sufficiently large files
- network transfer should reuse compressed payloads when available
- restore must decompress before materializing local files

This preserves:

- lower remote storage usage
- lower network bandwidth
- exact native local cache files for `go`

## Restore

Restore is conceptually the same procedure as current preload, but with different input and output.

Current preload:

- input: manifest of `ActionID`s
- output: local proxy store entries

Native restore:

- input: manifest of relative `GOCACHE` file paths
- output: native files written into local `GOCACHE`, with mtimes restored

### Restore Flow

1. Read manifests using `commit` / `parent` / `changes-id` / `base-commit`.
2. Union file paths.
3. Stream matching objects from remote storage.
4. Write them into local `GOCACHE`.
5. Restore their mtimes.

Restore should be best-effort only for cache misses:

- missing manifest files or missing cache objects are not logical cache failures

But I/O or protocol errors during restore are real failures.

### Lazy Manifest Sanitization

Restore may lazily sanitize manifests:

- if a manifest references missing remote objects, those paths can be dropped
- stale manifest contents can be rewritten during restore preparation

This keeps manifests healthy without needing a separate maintenance pass.

## Save

After the build finishes, the tool should upload newly-created native cache files.

### Fresh-file Selection

A simple initial rule is:

- select files with `mtime > jobStart`

Optionally, save may further exclude files that were already present in the original preload set.
That refinement is not required for the first version.

### Save Flow

1. Walk local `GOCACHE`.
2. Select regular files considered fresh for this job.
3. Upload missing remote objects, preserving file mtimes.
4. Merge uploaded file paths into relevant manifests.

Save is the analogue of current `cache-used` upload, except the signal is newly-created native cache files instead of used `ActionID`s`.

## Manifest Update Semantics

Because this mode has no exact "cache used" signal, `replace` does not provide strong value.

Manifest updates should therefore behave like merge:

- `commit` manifests merge file paths
- `changes-id` manifests also effectively merge file paths

This differs from the current `changes-id` overwrite model in `GOCACHEPROG` mode.

The server should own manifest mutation during save.

## Remote API Shape

This mode should use separate endpoints from the `GOCACHEPROG` protocol path.

Suggested endpoints:

- `/restore-cache?...`
- `/save-cache?...`

### `/restore-cache`

Input:

- cache identity parameters such as `commit`, `changes-id`, `build-type`, `base-commit`, `parent-commit`

Behavior:

- resolve manifests
- union file paths
- lazily sanitize stale references
- stream matching cache-file objects back to the client

### `/save-cache`

Input:

- cache identity parameters
- streamed batch of selected local cache files with path, mtime, and content

Behavior:

- store missing objects in remote storage
- preserve mtimes in remote metadata
- update relevant manifests by merging uploaded file paths

This endpoint is the save-side equivalent of current `cache-used`.

## Storage Layout

Remote storage does not need to replicate a genuine public `GOCACHE` directory layout.

Conceptually this is a managed `GOCACHE`, but in practice the remote side may:

- store files compressed
- organize files according to its own local storage rules
- keep separate manifest files and metadata sidecars

What matters is preserving:

- file identity by relative `GOCACHE` path
- original file contents after restore
- original mtimes after restore

## File Selection Rules

Client-side scanning should:

- walk the local `GOCACHE` directory tree
- consider regular files only
- skip symlinks
- skip directories as manifest objects

Parent directories can be created implicitly during restore.
Empty directories do not need to be preserved intentionally.

This keeps the protocol simple and avoids odd filesystem behavior.

## Failure Semantics

Restore/save behavior should be strict:

- not found is not a failure
- actual restore/save I/O or protocol errors are critical

CI can suppress command failures explicitly if desired, but the tool itself should report them as errors.

## Eviction

This mode should have its own remote disk quota, separate from `GOCACHEPROG` mode.

Reason:

- separate tuning
- simpler eviction logic
- no interference between fundamentally different storage models

Eviction policy should follow the current disk-usage approach:

- keep total remote storage under a configured quota
- when over budget, delete the oldest objects first

The relevant recency signal is object mtime, which approximates reuse over time.

This is preferable to a fixed hard age cutoff as the primary policy, because it adapts to actual storage pressure.

Manifests can tolerate lazy cleanup:

- if an evicted object is still referenced, restore can skip it
- restore preparation can prune that stale reference from manifests

## Why This Mode Exists

This design intentionally trades exact online cache accounting for lower build-path latency.

Compared with `GOCACHEPROG` mode:

- no network roundtrips on local cache miss
- no runtime protocol mediation
- no `ActionID`-level usage tracking
- native `GOCACHE` behavior during the build

Compared with archive-based CI cache restore/save:

- much better precision
- much better deduplication
- no giant archive churn on small cache changes
- targeted restore based on current manifest identity rules

## Relationship To Existing Code

This mode should remain clearly separate from the current `GOCACHEPROG` flow.

Safe areas of reuse:

- HTTP streaming primitives
- compression policy and helpers
- manifest scope resolution patterns
- disk quota / oldest-first eviction practices

Semantics that should remain separate:

- `ActionID`/`OutputID`
- runtime `Get`/`Put` cache protocol
- shim/daemon flow
- cache-used accounting

## Summary

The new mode can be summarized as:

- native `GOCACHE` during the build
- batch restore before the build
- batch save after the build
- file-path manifests instead of `ActionID` manifests
- compressed remote object storage
- preserved mtimes end-to-end
- separate quota and eviction from `GOCACHEPROG`

This keeps the successful storage and transport ideas from the current implementation while removing remote latency from the build hot path.
