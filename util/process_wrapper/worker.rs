// Bazel persistent-worker mode for process_wrapper.
//
// When invoked with `--persistent_worker`, process_wrapper enters this module
// instead of the one-shot path in main.rs. It reads `WorkRequest` objects from
// stdin (JSON, brace-delimited streaming), pairs metadata + link phases of the
// same rust_library into a single rustc invocation, and writes `WorkResponse`
// objects to stdout.
//
// Design summary (see also rustc.bzl):
//   * One rustc per (target, config) regardless of phase. The metadata phase
//     returns to Bazel the moment rustc emits `--json=artifacts emit=metadata`;
//     rustc keeps running in the background for the link phase to harvest the
//     `.rlib`.
//   * No SIGKILL: the kill-on-rmeta workaround that process_wrapper uses in
//     one-shot mode is replaced with "return ResponseOK early, keep rustc alive"
//     here. Fixes #3383 (Windows pipelining) by construction.
//   * Phase is signalled to the worker via a unique `--cfg` marker in the rustc
//     param file (which IS the WorkRequest.arguments content). The marker is
//     stripped before invoking rustc so rmeta and rlib stay byte-equivalent.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use tinyjson::JsonValue;

use crate::options::{env_from_files, prepare_arg, Options};
use crate::rustc::{self, ErrorFormat};

// ---------- per-request wrapper flag scanner -----------------------------
//
// In worker mode, Bazel routes ONLY the rustc-flags param file into
// WorkRequest.arguments; the pre-`--` wrapper flags become part of the worker's
// startup argv and are therefore invariant across requests. To carry per-action
// wrapper-equivalent signals (phase, where to write the diagnostic sidecar,
// per-crate env vars), rustc.bzl puts them INSIDE the param file using the
// *same* flag names process_wrapper accepts in one-shot mode. We scan + apply +
// strip them here so rustc only sees its own args.
//
// Recognised flags (all sourced from rustc_flags param file when worker is on):
//   `--rustc-quit-on-rmeta true`  -> metadata phase
//   `--output-file <path>`         -> path to write captured rustc stderr
//   `--env-file <path>`            -> read `key=value\n...` and setenv on rustc

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    Metadata,
    Link,
}

#[derive(Default)]
struct RequestFlags {
    phase: Phase,
    output_file: Option<String>,
    env_files: Vec<String>,
    /// Each entry is the path of an `--arg-file <path>` referenced inside the
    /// per-request param file. Its contents are inlined into rustc_args after
    /// extraction (mirrors one-shot mode's `args_from_file` in options.rs).
    arg_files: Vec<String>,
}

impl Default for Phase {
    fn default() -> Self {
        Phase::Link
    }
}

/// Expand any `@response-file` args by inlining each line of the file as a
/// separate arg. Worker-mode rustc.bzl puts per-action wrapper flags
/// (`--rustc-quit-on-rmeta`, `--output-file`, `--env-file`) INSIDE the
/// rustc-flags param file — they're not in the WorkRequest.arguments directly
/// because the worker-startup argv is invariant across phases. Without this
/// expansion `extract_request_flags` would never see those flags and every
/// request would default to Phase::Link, with both phases ending up sharing
/// one rustc that only produces the artifacts of whichever action arrived
/// first.
///
/// Recursive @file references are not expanded; rustc itself doesn't support
/// nesting them either.
fn expand_response_files(args: &[String]) -> io::Result<Vec<String>> {
    let mut out: Vec<String> = Vec::with_capacity(args.len());
    for a in args {
        if let Some(path) = a.strip_prefix('@') {
            let content = std::fs::read_to_string(path)?;
            for line in content.lines() {
                if !line.is_empty() {
                    out.push(line.to_string());
                }
            }
        } else {
            out.push(a.clone());
        }
    }
    Ok(out)
}

fn extract_request_flags(args: &mut Vec<String>) -> RequestFlags {
    let mut out = RequestFlags::default();
    let mut filtered: Vec<String> = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        match a.as_str() {
            "--rustc-quit-on-rmeta" => {
                if args.get(i + 1).map(|s| s.as_str()) == Some("true") {
                    out.phase = Phase::Metadata;
                }
                i += 2;
                continue;
            }
            "--output-file" => {
                if let Some(p) = args.get(i + 1) {
                    out.output_file = Some(p.clone());
                }
                i += 2;
                continue;
            }
            "--env-file" => {
                if let Some(p) = args.get(i + 1) {
                    out.env_files.push(p.clone());
                }
                i += 2;
                continue;
            }
            "--arg-file" => {
                if let Some(p) = args.get(i + 1) {
                    out.arg_files.push(p.clone());
                }
                i += 2;
                continue;
            }
            _ => {}
        }
        if let Some(rest) = a.strip_prefix("--rustc-quit-on-rmeta=") {
            if rest == "true" {
                out.phase = Phase::Metadata;
            }
            i += 1;
            continue;
        }
        if let Some(rest) = a.strip_prefix("--output-file=") {
            out.output_file = Some(rest.to_owned());
            i += 1;
            continue;
        }
        if let Some(rest) = a.strip_prefix("--env-file=") {
            out.env_files.push(rest.to_owned());
            i += 1;
            continue;
        }
        if let Some(rest) = a.strip_prefix("--arg-file=") {
            out.arg_files.push(rest.to_owned());
            i += 1;
            continue;
        }
        filtered.push(a.clone());
        i += 1;
    }
    *args = filtered;
    out
}

/// Replace the `--out-dir <path>` value in `args` with `new_dir`, returning
/// the original path. Both space-separated (`--out-dir foo`) and equals
/// (`--out-dir=foo`) forms are supported. rustc emits all artifacts named by
/// `--emit=...` (including `.rmeta` / `.rlib` / `.d`) into this directory.
fn rewrite_out_dir(args: &mut [String], new_dir: &str) -> Option<PathBuf> {
    let mut i = 0;
    let mut orig: Option<PathBuf> = None;
    while i < args.len() {
        if args[i] == "--out-dir" {
            if let Some(v) = args.get(i + 1) {
                orig = Some(PathBuf::from(v.clone()));
                args[i + 1] = new_dir.to_string();
            }
            return orig;
        }
        if let Some(rest) = args[i].strip_prefix("--out-dir=") {
            orig = Some(PathBuf::from(rest.to_owned()));
            args[i] = format!("--out-dir={}", new_dir);
            return orig;
        }
        i += 1;
    }
    orig
}

/// Re-materialize the phase's artifact from `scratch_dir` into `orig_out_dir`.
/// Invoked at WorkResponse time so the artifact is present at the
/// action-declared output path immediately before bazel's existence check —
/// defensive against bazel post-action output management between when the
/// stderr thread first copied the artifact and when the request handler
/// replies (especially on Link REUSE, where the gap can be substantial).
/// Returns Ok(()) if there is no recorded artifact for this phase yet (rustc
/// hadn't emitted it before we got here — let the existing-from-emit-time
/// copy stand).
fn materialize_phase_artifact(inflight: &CrateInFlight, phase: Phase) -> io::Result<()> {
    let kind = match phase {
        Phase::Metadata => "metadata",
        Phase::Link => "link",
    };
    let artifact_path = {
        let map = inflight.artifacts.lock().unwrap();
        match map.get(kind) {
            Some(p) => p.clone(),
            None => return Ok(()),
        }
    };
    copy_artifact(&artifact_path, &inflight.scratch_dir, &inflight.orig_out_dir).map(|_| ())
}

/// Copy `scratch/<basename(artifact_path)>` into `orig_out_dir/`. The dst is
/// unlinked first so we can overwrite files that bazel finalized to 0o555
/// after a prior build (worker mode is not sandboxed, so the action-declared
/// output path may already contain a read-only file from an earlier action).
fn copy_artifact(artifact_path: &str, scratch: &Path, orig_out_dir: &Path) -> io::Result<PathBuf> {
    let filename = Path::new(artifact_path)
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "artifact has no filename"))?;
    let src = scratch.join(filename);
    let dst = orig_out_dir.join(filename);

    // Multiple worker pools may concurrently materialize the same crate
    // (until we route every per-crate `process_wrapper_flags` entry through
    // the per-request param file, Bazel partitions us). Each pool's worker
    // copies into the same `dst`; if we did `remove_file(dst) + copy(src, dst)`
    // there's a window where `dst` doesn't exist or is half-written. Instead
    // copy to a `dst.<pid>.<tid>.tmp` sibling, then rename to `dst`. Rename is
    // atomic on POSIX, so the final `dst` is always either the previous
    // worker's full bytes or this worker's full bytes — never empty.
    let tid = thread::current()
        .name()
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{:?}", thread::current().id()));
    let tmp = orig_out_dir.join(format!(
        "{}.{}.{}.tmp",
        filename.to_string_lossy(),
        std::process::id(),
        tid,
    ));
    let bytes = match fs::copy(&src, &tmp) {
        Ok(b) => b,
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
    };
    if let Err(e) = fs::rename(&tmp, &dst) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    let _ = bytes;
    Ok(dst)
}

// ---------- per-crate orchestration state ---------------------------------

#[derive(Default)]
struct ArtifactSlot {
    ready: bool,
    err: Option<String>,
    consumed: bool,
}

struct CrateInFlight {
    rmeta: Mutex<ArtifactSlot>,
    rmeta_cv: Condvar,
    rlib: Mutex<ArtifactSlot>,
    rlib_cv: Condvar,
    stderr_buf: Mutex<String>,
    // Per-crate worker-private scratch dir: rustc is invoked with
    // `--out-dir=<scratch_dir>` so artifacts land here first, then we copy
    // them out to `orig_out_dir` once they're complete. This sidesteps the
    // bazel-out 555-perm finalization issue (worker mode is not sandboxed,
    // so writing directly to the action-declared location collides with
    // outputs left there by earlier non-worker / sandboxed builds).
    scratch_dir: PathBuf,
    // Where rustc *would* have written, if we weren't redirecting via
    // `--out-dir`. This is the action-declared output directory, where the
    // .rmeta / .rlib must live by the time we send the WorkResponse.
    orig_out_dir: PathBuf,
    // emit kind ("metadata" | "link" | "dep-info" | ...) -> scratch filename.
    // Populated by the stderr thread when rustc emits an artifact JSON line.
    // Used to re-copy artifacts on REUSE requests, because bazel finalizes a
    // metadata-phase action's declared output to 0o555 immediately after the
    // metadata WorkResponse — that finalization can race with rustc's own
    // continued writes (the link emit) and with subsequent action handling
    // for the link phase. Copying on every WorkRequest reply guarantees the
    // file is present at the action-declared path right when bazel checks.
    artifacts: Mutex<HashMap<String, String>>,
}

impl CrateInFlight {
    fn new(scratch_dir: PathBuf, orig_out_dir: PathBuf) -> Self {
        Self {
            rmeta: Mutex::new(ArtifactSlot::default()),
            rmeta_cv: Condvar::new(),
            rlib: Mutex::new(ArtifactSlot::default()),
            rlib_cv: Condvar::new(),
            stderr_buf: Mutex::new(String::new()),
            scratch_dir,
            orig_out_dir,
            artifacts: Mutex::new(HashMap::new()),
        }
    }

    fn signal_rmeta(&self, err: Option<String>) {
        let mut s = self.rmeta.lock().unwrap();
        if !s.ready {
            s.ready = true;
            s.err = err;
            self.rmeta_cv.notify_all();
        }
    }

    fn signal_rlib(&self, err: Option<String>) {
        let mut s = self.rlib.lock().unwrap();
        if !s.ready {
            s.ready = true;
            s.err = err;
            self.rlib_cv.notify_all();
        }
    }

    fn wait_rmeta(&self) -> Result<(), String> {
        let mut s = self.rmeta.lock().unwrap();
        while !s.ready {
            s = self.rmeta_cv.wait(s).unwrap();
        }
        s.consumed = true;
        s.err.clone().map_or(Ok(()), Err)
    }

    fn wait_rlib(&self) -> Result<(), String> {
        let mut s = self.rlib.lock().unwrap();
        while !s.ready {
            s = self.rlib_cv.wait(s).unwrap();
        }
        s.consumed = true;
        s.err.clone().map_or(Ok(()), Err)
    }

    fn both_consumed(&self) -> bool {
        self.rmeta.lock().unwrap().consumed && self.rlib.lock().unwrap().consumed
    }
}

type StateMap = Arc<Mutex<HashMap<u64, Arc<CrateInFlight>>>>;

// ---------- state key (input + rustc-argv hash) --------------------------

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn is_params_file(path: &str) -> bool {
    path.ends_with(".params") || path.contains("/_internal_") || path.contains("/internal/_")
}

fn compute_key(inputs: &[(String, String)], rustc_argv: &[String]) -> u64 {
    let mut buf: Vec<u8> = Vec::new();
    for (p, d) in inputs.iter().filter(|(p, _)| !is_params_file(p)) {
        buf.extend_from_slice(p.as_bytes());
        buf.push(0);
        buf.extend_from_slice(d.as_bytes());
        buf.push(0);
    }
    buf.push(b'|');
    for a in rustc_argv {
        buf.extend_from_slice(a.as_bytes());
        buf.push(0);
    }
    fnv1a(&buf)
}

// ---------- WorkRequest / WorkResponse JSON -------------------------------

#[derive(Debug)]
struct WorkRequest {
    arguments: Vec<String>,
    inputs: Vec<(String, String)>,
    request_id: i64,
    cancel: bool,
    // Set by Bazel to 10 when `--worker_verbose` is passed. Workers are
    // expected to use this to enable per-request diagnostic logging on stderr
    // (which Bazel captures in `bazel-workers/multiplex-worker-N-*.log`).
    verbosity: i64,
}

fn parse_work_request(json: &JsonValue) -> Result<WorkRequest, String> {
    let obj = match json {
        JsonValue::Object(o) => o,
        _ => return Err("WorkRequest is not a JSON object".to_string()),
    };
    let arguments = match obj.get("arguments") {
        Some(JsonValue::Array(a)) => a
            .iter()
            .map(|v| match v {
                JsonValue::String(s) => Ok(s.clone()),
                _ => Err("argument is not a string".to_string()),
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => Vec::new(),
    };
    let inputs = match obj.get("inputs") {
        Some(JsonValue::Array(a)) => a
            .iter()
            .map(|v| match v {
                JsonValue::Object(io_obj) => {
                    let p = match io_obj.get("path") {
                        Some(JsonValue::String(s)) => s.clone(),
                        _ => return Err("input.path missing".to_string()),
                    };
                    let d = match io_obj.get("digest") {
                        Some(JsonValue::String(s)) => s.clone(),
                        _ => String::new(),
                    };
                    Ok((p, d))
                }
                _ => Err("input is not an object".to_string()),
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => Vec::new(),
    };
    let request_id = match obj.get("requestId") {
        Some(JsonValue::Number(n)) => *n as i64,
        _ => 0,
    };
    let cancel = matches!(obj.get("cancel"), Some(JsonValue::Boolean(true)));
    let verbosity = match obj.get("verbosity") {
        Some(JsonValue::Number(n)) => *n as i64,
        _ => 0,
    };
    Ok(WorkRequest {
        arguments,
        inputs,
        request_id,
        cancel,
        verbosity,
    })
}

fn write_response(
    stdout_lock: &Mutex<io::Stdout>,
    request_id: i64,
    exit_code: i32,
    output: &str,
) -> io::Result<()> {
    // Bazel's JSON worker parser is strict about integer-typed numeric fields;
    // hand-roll the JSON to guarantee `requestId` / `exitCode` are integers.
    let mut buf = Vec::with_capacity(64 + output.len());
    buf.extend_from_slice(b"{\"requestId\":");
    buf.extend_from_slice(request_id.to_string().as_bytes());
    buf.extend_from_slice(b",\"exitCode\":");
    buf.extend_from_slice(exit_code.to_string().as_bytes());
    buf.extend_from_slice(b",\"output\":");
    let escaped = JsonValue::String(output.to_string())
        .stringify()
        .expect("string stringify");
    buf.extend_from_slice(escaped.as_bytes());
    buf.extend_from_slice(b"}\n");
    let mut out = stdout_lock.lock().unwrap();
    out.write_all(&buf)?;
    out.flush()
}

// ---------- streaming JSON reader (brace-counted) ------------------------

fn read_json_value<R: Read>(reader: &mut io::BufReader<R>) -> io::Result<Option<String>> {
    let mut buf = String::new();
    let mut depth: i32 = 0;
    let mut in_str = false;
    let mut esc = false;
    let mut started = false;
    let mut byte = [0u8; 1];
    loop {
        let n = reader.read(&mut byte)?;
        if n == 0 {
            if started {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF mid-JSON",
                ));
            }
            return Ok(None);
        }
        let c = byte[0] as char;
        if !started {
            if c.is_whitespace() {
                continue;
            }
            if c != '{' {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected '{{' got {:?}", c),
                ));
            }
            started = true;
        }
        buf.push(c);
        if in_str {
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(Some(buf));
                }
            }
            _ => {}
        }
    }
}

// ---------- rustc spawn + stderr orchestration ---------------------------

fn spawn_and_drive(
    rustc_path: &str,
    env: &HashMap<String, String>,
    rustc_args: &[String],
    inflight: Arc<CrateInFlight>,
    output_format: ErrorFormat,
) -> io::Result<()> {
    let mut cmd = Command::new(rustc_path);
    cmd.args(rustc_args);
    cmd.env_clear().envs(env);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child: Child = cmd.spawn()?;
    let stderr = child.stderr.take().expect("piped stderr");
    let stdout = child.stdout.take().expect("piped stdout");

    let stderr_inflight = inflight.clone();
    let stderr_thread = thread::spawn(move || -> io::Result<()> {
        let r = io::BufReader::new(stderr);
        for line in r.lines() {
            let line = line?;
            // Try to parse as JSON; rustc with --json=artifacts emits an
            // `{"$message_type":"artifact","artifact":"<path>","emit":"<kind>"}`
            // line each time it finishes writing one of the requested
            // `--emit=...` outputs (metadata before codegen, link after).
            // For those, copy the file out of our scratch dir to the action's
            // declared `--out-dir` and then signal the matching slot.
            let parsed: Result<JsonValue, _> = line.parse();
            let mut handled_as_artifact = false;
            if let Ok(JsonValue::Object(obj)) = &parsed {
                // rustc's `--json=artifacts` emits `{"artifact":"<path>","emit":"<kind>"}`.
                // Some rustc versions also add `$message_type:"artifact"`, but
                // it's not required — match on the presence of both `emit` and
                // `artifact` keys.
                if let (Some(JsonValue::String(kind)), Some(JsonValue::String(artifact))) =
                    (obj.get("emit"), obj.get("artifact"))
                {
                    handled_as_artifact = true;
                    // Remember the emit→scratch-path mapping for later REUSE
                    // requests, which need to re-materialize the artifact at
                    // the action-declared path (bazel may delete or finalize
                    // an in-place output between when we copy it here and
                    // when bazel actually checks the link-phase action's
                    // declared output).
                    stderr_inflight
                        .artifacts
                        .lock()
                        .unwrap()
                        .insert(kind.clone(), artifact.clone());

                    // Initial copy. Downstream actions whose inputs include
                    // *this* crate's `.rmeta` (pipelining) need it on disk
                    // ASAP — that's why we copy on emit rather than waiting
                    // for the WorkResponse round-trip.
                    let copied = copy_artifact(
                        artifact,
                        &stderr_inflight.scratch_dir,
                        &stderr_inflight.orig_out_dir,
                    );
                    match copied {
                        Ok(_) => match kind.as_str() {
                            "metadata" => stderr_inflight.signal_rmeta(None),
                            "link" => stderr_inflight.signal_rlib(None),
                            _ => {}
                        },
                        Err(e) => {
                            let msg = format!(
                                "worker: copy {} (emit={}) failed: {}",
                                artifact, kind, e
                            );
                            let mut buf = stderr_inflight.stderr_buf.lock().unwrap();
                            buf.push_str(&msg);
                            buf.push('\n');
                            match kind.as_str() {
                                "metadata" => stderr_inflight.signal_rmeta(Some(msg)),
                                "link" => stderr_inflight.signal_rlib(Some(msg)),
                                _ => {}
                            }
                        }
                    }
                }
            }
            if handled_as_artifact {
                continue;
            }

            // Non-artifact line: route through the existing rustc-JSON
            // diagnostic parser to render or pass through diagnostics. We
            // never use the Terminate signal in worker mode — keep rustc
            // running to produce the rlib.
            let mut metadata_seen = false;
            let result = rustc::stop_on_rmeta_completion(
                line.clone(),
                output_format,
                &mut metadata_seen,
            );
            match result {
                Ok(crate::output::LineOutput::Message(msg)) => {
                    let mut buf = stderr_inflight.stderr_buf.lock().unwrap();
                    buf.push_str(&msg);
                    if !msg.ends_with('\n') {
                        buf.push('\n');
                    }
                }
                Ok(crate::output::LineOutput::Skip)
                | Ok(crate::output::LineOutput::Terminate) => {
                    // Filter out residual artifact-notification frames.
                }
                Err(_) => {
                    let mut buf = stderr_inflight.stderr_buf.lock().unwrap();
                    buf.push_str(&line);
                    buf.push('\n');
                }
            }
        }
        Ok(())
    });

    let stdout_thread = thread::spawn(move || -> io::Result<()> {
        let mut r = io::BufReader::new(stdout);
        let mut tmp = String::new();
        while r.read_line(&mut tmp)? > 0 {
            tmp.clear();
        }
        Ok(())
    });

    thread::spawn(move || {
        let status = child.wait();
        let _ = stderr_thread.join();
        let _ = stdout_thread.join();
        match status {
            Ok(s) if s.success() => {
                inflight.signal_rmeta(None);
                inflight.signal_rlib(None);
            }
            Ok(s) => {
                let msg = format!("rustc exited with status {:?}", s.code());
                inflight.signal_rmeta(Some(msg.clone()));
                inflight.signal_rlib(Some(msg));
            }
            Err(e) => {
                let msg = format!("rustc wait error: {}", e);
                inflight.signal_rmeta(Some(msg.clone()));
                inflight.signal_rlib(Some(msg));
            }
        }
    });
    Ok(())
}

// ---------- request handler ----------------------------------------------

struct WorkerCtx {
    rustc_path: String,
    cwd: String,
    env: HashMap<String, String>,
    subst_mappings: Vec<(String, String)>,
    output_format: ErrorFormat,
}

fn handle_request(state: &StateMap, ctx: &WorkerCtx, req: &WorkRequest) -> (i32, String) {
    if req.cancel {
        return (0, String::new());
    }

    // Apply --subst substitutions (e.g., `${pwd}` -> cwd) using process_wrapper's
    // existing helper.
    let substituted: Vec<String> = req
        .arguments
        .iter()
        .map(|a| {
            let with_pwd = a.replace("${pwd}", &ctx.cwd);
            prepare_arg(with_pwd, &ctx.subst_mappings)
        })
        .collect();
    // Inline @response-files BEFORE scanning for worker flags — they live
    // inside the rustc-flags param file in worker mode.
    let mut rustc_args = match expand_response_files(&substituted) {
        Ok(a) => a,
        Err(e) => return (1, format!("response file read error: {}", e)),
    };
    let req_flags = extract_request_flags(&mut rustc_args);

    // Inline `--arg-file <path>` contents directly into rustc_args. These are
    // build-script-emitted extra rustc flags (e.g. cargo links="X" emits
    // `-l X`). In one-shot mode `process_wrapper` reads them via
    // `args_from_file` and appends them to the rustc argv. In worker mode the
    // `--arg-file` flag lives inside the per-request param file (routed there
    // by rustc.bzl to keep `process_wrapper_flags` invariant), so we have to
    // expand it here. Substitute `${pwd}` etc. on each inlined arg as the
    // one-shot path does via `prepare_arg`.
    for arg_file in &req_flags.arg_files {
        let content = match std::fs::read_to_string(arg_file) {
            Ok(c) => c,
            Err(e) => return (1, format!("arg-file {} read error: {}", arg_file, e)),
        };
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            let with_pwd = line.replace("${pwd}", &ctx.cwd);
            rustc_args.push(prepare_arg(with_pwd, &ctx.subst_mappings));
        }
    }

    // Merge any --env-file contents on top of the worker-startup env (which
    // already holds workspace/toolchain-level vars from options()).
    //
    // The env-file may carry `${pwd}` / `${exec_root}` placeholders (e.g.
    // build-script OUT_DIR uses `${pwd}/bazel-out/...`). One-shot mode applies
    // these via `environment_block` in options.rs; do the same here so rustc
    // doesn't see literal `${pwd}` in env values.
    let mut env = ctx.env.clone();
    if !req_flags.env_files.is_empty() {
        match env_from_files(req_flags.env_files.clone()) {
            Ok(extra) => {
                for (k, v) in extra {
                    let v = v.replace("${pwd}", &ctx.cwd);
                    let v = prepare_arg(v, &ctx.subst_mappings);
                    env.insert(k, v);
                }
            }
            Err(e) => return (1, format!("env-file error: {}", e)),
        }
    }

    // Compute key BEFORE rewriting --out-dir to scratch. Metadata + Link
    // phases of the same crate ship the same original --out-dir, so the
    // pre-rewrite args hash matches between phases.
    let key = compute_key(&req.inputs, &rustc_args);

    let inflight = {
        let mut map = state.lock().unwrap();
        if let Some(existing) = map.get(&key) {
            existing.clone()
        } else {
            // First request for this crate: set up a scratch out-dir and
            // redirect rustc's `--out-dir` to it. The original `--out-dir` is
            // remembered on CrateInFlight so the stderr thread can copy each
            // emit artifact out of scratch and into the action-declared path.
            //
            // The scratch lives under the worker's bazel-allocated working
            // directory (`ctx.cwd`, the execroot for non-sandboxed multiplex
            // workers), so it's covered by bazel clean / output_base cleanup
            // and doesn't leak across runs into /tmp.
            let scratch_dir = std::path::PathBuf::from(&ctx.cwd)
                .join("bazel-out")
                .join("_rules_rust_worker_scratch")
                .join(format!("{}.{:016x}", std::process::id(), key));
            if let Err(e) = fs::create_dir_all(&scratch_dir) {
                return (1, format!("failed to create scratch dir: {}", e));
            }
            let orig_out_dir = match rewrite_out_dir(
                &mut rustc_args,
                &scratch_dir.to_string_lossy(),
            ) {
                Some(p) => p,
                None => {
                    let _ = fs::remove_dir_all(&scratch_dir);
                    return (
                        1,
                        "rustc args missing --out-dir; worker mode requires it".to_string(),
                    );
                }
            };
            let new = Arc::new(CrateInFlight::new(scratch_dir.clone(), orig_out_dir));
            map.insert(key, new.clone());
            if let Err(e) = spawn_and_drive(
                &ctx.rustc_path,
                &env,
                &rustc_args,
                new.clone(),
                ctx.output_format,
            ) {
                map.remove(&key);
                let _ = fs::remove_dir_all(&scratch_dir);
                return (1, format!("failed to spawn rustc: {}", e));
            }
            new
        }
    };

    let result = match req_flags.phase {
        Phase::Metadata => inflight.wait_rmeta(),
        Phase::Link => inflight.wait_rlib(),
    };

    // Re-copy the phase's artifact from scratch to the action-declared
    // output path. If bazel deleted it between the stderr thread's
    // emit-time copy and now (which can happen on Link REUSE after the
    // metadata phase's WorkResponse already triggered output finalization),
    // this restores it before bazel runs its existence check.
    let materialize_err = if result.is_ok() {
        materialize_phase_artifact(&inflight, req_flags.phase).err()
    } else {
        None
    };

    let stderr_snapshot = inflight.stderr_buf.lock().unwrap().clone();

    if inflight.both_consumed() {
        state.lock().unwrap().remove(&key);
        // Best-effort scratch cleanup; we don't fail the request on this.
        let _ = fs::remove_dir_all(&inflight.scratch_dir);
    }

    if let Some(e) = materialize_err {
        let msg = format!("worker: materialize {:?} failed: {}", req_flags.phase, e);
        return (
            1,
            if stderr_snapshot.is_empty() {
                msg
            } else {
                format!("{}\n{}", stderr_snapshot, msg)
            },
        );
    }

    // Per-action diagnostic file (the .rlib.json / .rmeta.json sidecar Bazel
    // declares as an action output). Write the captured stderr there.
    if let Some(path) = req_flags.output_file {
        if let Ok(mut f) = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
        {
            let _ = f.write_all(stderr_snapshot.as_bytes());
        }
    }

    match result {
        Ok(()) => (0, stderr_snapshot),
        Err(e) => (
            1,
            if stderr_snapshot.is_empty() {
                e
            } else {
                format!("{}\n{}", stderr_snapshot, e)
            },
        ),
    }
}

// ---------- worker entry --------------------------------------------------

pub(crate) fn run(opts: Options) -> io::Result<()> {
    // Surface panics in worker threads to stderr (the per-worker log Bazel
    // archives under `bazel-workers/`). Unconditional — panics are always
    // exceptional and the worst case is one extra line per crash.
    std::panic::set_hook(Box::new(|info| {
        eprintln!("[worker pid={}] PANIC: {}", std::process::id(), info);
    }));

    let cwd = std::env::current_dir()?
        .to_string_lossy()
        .into_owned();
    let ctx = Arc::new(WorkerCtx {
        rustc_path: opts.executable.clone(),
        cwd,
        env: opts.child_environment.clone(),
        subst_mappings: opts.subst_mappings.clone(),
        output_format: opts.rustc_output_format.unwrap_or(ErrorFormat::Rendered),
    });

    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let stdout_lock = Arc::new(Mutex::new(io::stdout()));
    let state: StateMap = Arc::new(Mutex::new(HashMap::new()));
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    loop {
        let json_str = match read_json_value(&mut reader) {
            Ok(Some(s)) => s,
            Ok(None) => {
                for h in handles {
                    let _ = h.join();
                }
                return Ok(());
            }
            Err(e) => {
                eprintln!("[worker pid={}] stdin read error: {}", std::process::id(), e);
                return Err(e);
            }
        };
        let json: JsonValue = match json_str.parse() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[process_wrapper worker] parse error: {}", e);
                continue;
            }
        };
        let req = match parse_work_request(&json) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("[process_wrapper worker] bad WorkRequest: {}", e);
                continue;
            }
        };

        let state_for_thread = state.clone();
        let stdout_for_thread = stdout_lock.clone();
        let ctx_for_thread = ctx.clone();
        let request_id = req.request_id;
        let verbose = req.verbosity > 0;
        if verbose {
            eprintln!(
                "[worker pid={}] req={} arguments={} inputs={}",
                std::process::id(),
                request_id,
                req.arguments.len(),
                req.inputs.len(),
            );
        }
        let h = thread::spawn(move || {
            let (code, msg) = handle_request(&state_for_thread, &ctx_for_thread, &req);
            if verbose {
                eprintln!(
                    "[worker pid={}] reply req={} code={}",
                    std::process::id(),
                    request_id,
                    code
                );
            }
            if let Err(e) = write_response(&stdout_for_thread, request_id, code, &msg) {
                eprintln!(
                    "[worker pid={}] write_response req={} error: {}",
                    std::process::id(),
                    request_id,
                    e
                );
            }
        });
        handles.push(h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_request_flags_two_arg_form() {
        let mut args = vec![
            "lib.rs".to_string(),
            "--rustc-quit-on-rmeta".to_string(),
            "true".to_string(),
            "--output-file".to_string(),
            "/path/to/out.json".to_string(),
            "--env-file".to_string(),
            "/path/to/foo.env".to_string(),
            "--crate-name=foo".to_string(),
        ];
        let f = extract_request_flags(&mut args);
        assert_eq!(f.phase, Phase::Metadata);
        assert_eq!(f.output_file.as_deref(), Some("/path/to/out.json"));
        assert_eq!(f.env_files, vec!["/path/to/foo.env".to_string()]);
        assert_eq!(args, vec!["lib.rs".to_string(), "--crate-name=foo".to_string()]);
    }

    #[test]
    fn extract_request_flags_equals_form() {
        let mut args = vec![
            "--rustc-quit-on-rmeta=true".to_string(),
            "--output-file=/x.json".to_string(),
            "--env-file=/y.env".to_string(),
            "lib.rs".to_string(),
        ];
        let f = extract_request_flags(&mut args);
        assert_eq!(f.phase, Phase::Metadata);
        assert_eq!(f.output_file.as_deref(), Some("/x.json"));
        assert_eq!(f.env_files, vec!["/y.env".to_string()]);
        assert_eq!(args, vec!["lib.rs".to_string()]);
    }

    #[test]
    fn extract_request_flags_link_default() {
        let mut args = vec!["lib.rs".to_string()];
        let f = extract_request_flags(&mut args);
        assert_eq!(f.phase, Phase::Link);
        assert_eq!(f.output_file, None);
        assert!(f.env_files.is_empty());
    }

    #[test]
    fn key_is_phase_independent_modulo_params_files() {
        let rustc_argv = vec!["rustc".to_string(), "--crate-name=foo".to_string()];
        let meta = vec![
            ("src/lib.rs".to_string(), "abc".to_string()),
            ("bazel-out/.../rustc-meta.params".to_string(), "deadbeef".to_string()),
        ];
        let link = vec![
            ("src/lib.rs".to_string(), "abc".to_string()),
            ("bazel-out/.../rustc-link.params".to_string(), "feedface".to_string()),
        ];
        assert_eq!(
            compute_key(&meta, &rustc_argv),
            compute_key(&link, &rustc_argv),
        );
    }

    #[test]
    fn streaming_reader_packs_objects() {
        let data = br#"{"a":1}{"b":2}"#;
        let mut r = io::BufReader::new(&data[..]);
        let a = read_json_value(&mut r).unwrap().unwrap();
        let b = read_json_value(&mut r).unwrap().unwrap();
        assert!(a.contains("\"a\""));
        assert!(b.contains("\"b\""));
        assert!(read_json_value(&mut r).unwrap().is_none());
    }
}
