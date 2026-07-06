package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// handleWait implements the paired `Rustc` action as a WorkRequest to the
// same worker pool. No polling: the waiter is only dispatched after the
// paired compile has replied, and same worker key means same worker process,
// so either the compile is in flight right here (registry) or it finished in
// an earlier build and its status file already exists. Anything else is an
// error — a missing file will never appear.
func (w *worker) handleWait(rf *requestFlags, cancelled <-chan struct{}) (int, string) {
	rlib := rf.waiterRlib
	if rlib == "" {
		return 1, "--waiter-rlib is required"
	}

	var exitCode int
	var diag string
	if st := w.reg.lookup(rlib); st != nil {
		// The compile is running (or just finished) in this process.
		select {
		case <-st.done:
		case <-cancelled:
			return cancelledExit, ""
		}
		exitCode, diag = st.exitCode, st.diag
		w.reg.drop(rlib)
	} else if content, err := os.ReadFile(statusPath(rlib)); err == nil {
		// The compile finished in an earlier build; its results persist.
		exitCode, err = strconv.Atoi(strings.TrimSpace(string(content)))
		if err != nil {
			return selfHeal(rlib, fmt.Sprintf("bad status %q: %v", content, err))
		}
		d, _ := os.ReadFile(linkStderrPath(rlib))
		diag = string(d)
	} else {
		return selfHeal(rlib, fmt.Sprintf("no compile in flight and no status for %s; the paired RustcMetadata request never ran here", rlib))
	}

	// Materialize the declared diagnostics sidecar (when any) regardless of
	// the outcome.
	if rf.waiterStderr != "" {
		if err := atomicWrite(rf.waiterStderr, []byte(diag)); err != nil {
			return selfHeal(rlib, err.Error())
		}
	}
	if exitCode != 0 {
		// The compile itself failed; both actions fail and both re-run once
		// their inputs change, so no self-heal is needed.
		return exitCode, diag
	}

	src := filepath.Join(scratchDir(rlib), filepath.Base(rlib))
	if _, err := os.Stat(src); err != nil {
		return selfHeal(rlib, fmt.Sprintf("compile reported success but rlib %s is missing", src))
	}
	if err := move(src, rlib); err != nil {
		return selfHeal(rlib, err.Error())
	}
	return 0, ""
}

// selfHeal fails the waiter AND removes the paired rmeta so the next build
// re-runs the RustcMetadata action (Bazel re-executes an action whose declared
// output is missing). Without this a waiter failure is permanent: no-cache
// only bypasses the remote/disk caches, NOT Bazel's local incremental action
// cache, so a compile whose worker died between the rmeta reply and the status
// write (or whose scratch rlib was already moved) would replay as a stale
// success on every following build while the rlib never materializes.
func selfHeal(rlib string, msg string) (int, string) {
	if err := os.Remove(rmetaPath(rlib)); err != nil && !os.IsNotExist(err) {
		msg = fmt.Sprintf("%s (removing %s to force a recompile also failed: %v)", msg, rmetaPath(rlib), err)
	}
	return 1, msg
}
