# rustc_worker

The persistent worker behind rules_rust's pipelined-worker mode: it compiles
rmeta+rlib with ONE rustc invocation per crate, plus a worker-owned compile
cache. Both actions of a pipelined crate are WorkRequests to the same worker
pool:

- **`RustcMetadata`**: rustc runs **through process_wrapper** (which owns
  substitution, env-files and env_clear, exactly like the one-shot path) with
  `--out-dir` redirected to `<rlib>.rustc_scratch`. The param file laid out
  by rustc.bzl IS the wrapper invocation; the worker only strips its own
  leading flags and rewrites `--out-dir`. process_wrapper is not put into
  output-processing mode, so rustc's stderr streams through verbatim: the
  worker replies at the rmeta artifact notification (the only safe "rmeta is
  complete" signal) while rustc keeps running.
- **`Rustc`** (`--rlib-waiter`): waits IN-PROCESS for the paired compile to
  finish (same worker key, so both requests normally land in the same
  multiplex worker process), then installs the rlib. A status-file fallback
  covers requests landing on a different worker process.

## Compile cache

Keys are opaque: sha256 over the WorkRequest's Bazel-computed input digests
plus the raw request arguments. Param/env file contents are covered by their
own input digests, and `${pwd}`-style placeholders are hashed verbatim, so
keys are machine-portable. Backends (selected via the worker process env,
e.g. `--action_env` + `--host_action_env`):

- `RUSTC_WORKER_REMOTE_CACHE=grpc://host:port` — REAPI gRPC via
  remote-apis-sdks (tested against [bazel-remote]): one ActionCache entry
  referencing the {rmeta, rlib} blobs in CAS. Hits are all-or-nothing — the
  server-side AC deps check turns any entry with an evicted blob into a clean
  miss, and the crate recompiles + refills.
- `RUSTC_WORKER_CACHE_DIR=/abs/path` — a local directory cache.

## Wiring

```
--@rules_rust//rust/settings:pipelined_compilation=True
--@rules_rust//rust/settings:pipelined_compilation_worker=True
```

The worker is this in-tree target (not configurable). Because gazelle only
accepts `gazelle_override` tags from the root module, consuming workspaces
carry the two remote-apis overrides documented in MODULE.bazel.

Status: proof of concept. Known gaps: metadata-phase warnings are not
replayed on cache hits; no auth on the gRPC dial (NoSecurity); the go_deps
repos are fetched eagerly once the worker target is analyzed.

[bazel-remote]: https://github.com/buchgr/bazel-remote
