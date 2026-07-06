package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

// compileState is the in-memory rendezvous between a compile request and its
// paired waiter request (see waiter.go). done is closed when rustc exits.
type compileState struct {
	done     chan struct{}
	exitCode int
	diag     string
}

type registry struct {
	mu sync.Mutex
	m  map[string]*compileState // keyed by rlib output path
}

func (r *registry) register(rlib string) *compileState {
	st := &compileState{done: make(chan struct{})}
	r.mu.Lock()
	r.m[rlib] = st
	r.mu.Unlock()
	return st
}

func (r *registry) lookup(rlib string) *compileState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[rlib]
}

func (r *registry) drop(rlib string) {
	r.mu.Lock()
	delete(r.m, rlib)
	r.mu.Unlock()
}

// handle dispatches one WorkRequest: a compile or (for --rlib-waiter) a wait.
func (w *worker) handle(req *workRequest) (int, string) {
	expanded, err := expandResponseFiles(req.Arguments)
	if err != nil {
		return 1, err.Error()
	}
	rf, err := parseRequestFlags(expanded)
	if err != nil {
		return 1, err.Error()
	}
	if rf.rlibWaiter {
		return w.handleWait(rf, w.cancelChan(req.RequestID))
	}
	return w.handleCompile(req, rf, w.cancelChan(req.RequestID))
}

func (w *worker) handleCompile(req *workRequest, rf *requestFlags, cancelled <-chan struct{}) (int, string) {
	if rf.linkRlib == "" || rf.processWrapper == "" {
		return 1, "rustc_worker: --worker-link-rlib and --process-wrapper are required"
	}
	key, cacheable := cacheKeyOf(w.backend, req)
	crate := crateName(rf.pwArgs)
	scratch := scratchDir(rf.linkRlib)
	status := statusPath(rf.linkRlib)
	st := w.reg.register(rf.linkRlib)

	// finish publishes the compile result: in-memory for the same-process
	// waiter, status file for later builds. EVERY exit path of this compile
	// goes through it — an unclosed done channel would hang the waiter.
	finish := func(exitCode int, diag string) {
		atomicWrite(linkStderrPath(rf.linkRlib), []byte(diag))
		atomicWrite(status, []byte(fmt.Sprintf("%d\n", exitCode)))
		st.exitCode = exitCode
		st.diag = diag
		close(st.done)
	}
	fail := func(msg string) (int, string) {
		finish(1, msg)
		return 1, msg
	}

	// Stale-state hygiene: a leftover sentinel from an earlier build must not
	// release the waiter with stale results.
	os.Remove(status)
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		return fail(fmt.Sprintf("create scratch %s: %v", scratch, err))
	}
	os.Remove(linkStderrPath(rf.linkRlib))

	origOutDir, ok := rewriteOutDir(rf.pwArgs, scratch)
	if !ok {
		return fail("rustc args missing --out-dir")
	}

	rlibScratch := filepath.Join(scratch, filepath.Base(rf.linkRlib))
	if cacheable {
		// The fetch is cancellable with the request; temp files land in the
		// scratch dir (same filesystem as both install destinations).
		ctx, stop := context.WithCancel(context.Background())
		go func() {
			select {
			case <-cancelled:
				stop()
			case <-ctx.Done():
			}
		}()
		pair, err := w.backend.fetch(ctx, key, scratch)
		stop()
		if err != nil {
			log.Printf("fetch error %s: %v", crate, err)
		}
		if pair != nil {
			rmetaBase := strings.TrimSuffix(filepath.Base(rf.linkRlib), ".rlib") + ".rmeta"
			err := move(pair.rmeta, filepath.Join(origOutDir, rmetaBase))
			if err == nil {
				err = move(pair.rlib, rlibScratch)
			}
			if err == nil {
				if rf.outputFile != "" {
					atomicWrite(rf.outputFile, nil)
				}
				finish(0, "")
				log.Printf("HIT %s %s", crate, key.hash[:16])
				return 0, ""
			}
			// Treat a failed install as a miss; the compile below rewrites
			// both destinations.
			os.Remove(pair.rmeta)
			os.Remove(pair.rlib)
			log.Printf("install error %s: %v", crate, err)
		}
		log.Printf("MISS %s %s", crate, key.hash[:16])
	}

	// Everything after the request's `--` is a ready-to-run process_wrapper
	// invocation laid out by rustc.bzl. process_wrapper is deliberately NOT
	// in output-processing mode: rustc's stderr streams through verbatim,
	// and the rmeta artifact notification on it is the only safe "rmeta is
	// complete" signal.
	cmd := exec.Command(rf.processWrapper, rf.pwArgs...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fail(fmt.Sprintf("stderr pipe: %v", err))
	}
	if err := cmd.Start(); err != nil {
		return fail(fmt.Sprintf("process_wrapper spawn: %v", err))
	}

	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 1<<20), 64<<20)
	var rmetaBuf strings.Builder
	rmetaArtifact := ""
	for scanner.Scan() {
		if art, isArt := scanLine(scanner.Text(), &rmetaBuf); isArt && art != "" {
			rmetaArtifact = art
			break
		}
	}
	if rmetaArtifact == "" {
		for scanner.Scan() {
			scanLine(scanner.Text(), &rmetaBuf)
		}
		cmd.Wait()
		return fail(strings.TrimRight(rmetaBuf.String(), "\n") + "\nrustc exited before emitting rmeta")
	}

	rmetaDst := filepath.Join(origOutDir, filepath.Base(rmetaArtifact))
	if err := atomicCopy(rmetaArtifact, rmetaDst); err != nil {
		return fail(fmt.Sprintf("copy rmeta %s -> %s: %v", rmetaArtifact, rmetaDst, err))
	}
	if rf.outputFile != "" {
		atomicWrite(rf.outputFile, []byte(rmetaBuf.String()))
	}

	// Reply now; a goroutine drains the link phase, fills the cache, and
	// releases the waiter.
	go func() {
		var linkBuf strings.Builder
		for scanner.Scan() {
			scanLine(scanner.Text(), &linkBuf)
		}
		exitCode := 0
		if err := cmd.Wait(); err != nil {
			exitCode = 1
			if ee, ok := err.(*exec.ExitError); ok {
				exitCode = ee.ExitCode()
			}
		}
		if exitCode == 0 && cacheable {
			// The fill deliberately outlives the request (and the build):
			// its inputs are worker-owned scratch files, not Bazel state.
			if err := w.backend.fill(context.Background(), key, rmetaArtifact, rlibScratch); err != nil {
				log.Printf("fill error %s: %v", crate, err)
			} else {
				log.Printf("FILL %s %s", crate, key.hash[:16])
			}
		}
		finish(exitCode, linkBuf.String())
	}()

	return 0, rmetaBuf.String()
}

func cacheKeyOf(backend cacheBackend, req *workRequest) (cacheKeyDigest, bool) {
	if backend == nil {
		return cacheKeyDigest{}, false
	}
	key, ok := cacheKey(req)
	if !ok {
		log.Printf("SKIP (uncacheable request, inputs=%d)", len(req.Inputs))
	}
	return key, ok
}

func crateName(args []string) string {
	for _, a := range args {
		if c, ok := strings.CutPrefix(a, "--crate-name="); ok {
			return c
		}
	}
	return "unknown"
}

// rustcMessage is the subset of rustc's --json output the worker cares about:
// artifact notifications and rendered diagnostics.
type rustcMessage struct {
	Emit     string `json:"emit"`
	Artifact string `json:"artifact"`
	Rendered string `json:"rendered"`
}

// scanLine appends rendered diagnostics to buf; returns the rmeta artifact
// path when the emit=metadata notification is seen.
func scanLine(line string, buf *strings.Builder) (rmetaArtifact string, isArtifact bool) {
	var m rustcMessage
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		buf.WriteString(line)
		buf.WriteByte('\n')
		return "", false
	}
	if m.Artifact != "" {
		if m.Emit == "metadata" {
			return m.Artifact, true
		}
		return "", true
	}
	if m.Rendered != "" {
		buf.WriteString(m.Rendered)
		if !strings.HasSuffix(m.Rendered, "\n") {
			buf.WriteByte('\n')
		}
	}
	return "", false
}
