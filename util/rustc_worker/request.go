package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

type workInput struct {
	Path   string `json:"path"`
	Digest []byte `json:"digest"` // Bazel-computed content hash; opaque token
}

type workRequest struct {
	Arguments []string    `json:"arguments"`
	Inputs    []workInput `json:"inputs"`
	RequestID int64       `json:"requestId"`
	Cancel    bool        `json:"cancel"`
}

type workResponse struct {
	RequestID    int64  `json:"requestId"`
	ExitCode     int    `json:"exitCode"`
	Output       string `json:"output"`
	WasCancelled bool   `json:"wasCancelled,omitempty"`
}

// Paths derived from the rlib output path — the contract shared with
// rustc.bzl.
func scratchDir(rlib string) string { return rlib + ".rustc_scratch" }
func statusPath(rlib string) string { return rlib + ".rustc_status" }
func linkStderrPath(rlib string) string {
	return scratchDir(rlib) + "/link.stderr"
}
func rmetaPath(rlib string) string {
	return strings.TrimSuffix(rlib, ".rlib") + ".rmeta"
}

// requestFlags is the worker's own header of a request's expanded args; for
// compile requests everything after the first `--` (pwArgs) is a ready-to-run
// process_wrapper invocation laid out by rustc.bzl.
type requestFlags struct {
	rlibWaiter     bool
	waiterRlib     string
	waiterStderr   string
	processWrapper string
	linkRlib       string
	outputFile     string
	pwArgs         []string
}

func parseRequestFlags(args []string) (*requestFlags, error) {
	fs := flag.NewFlagSet("request", flag.ContinueOnError)
	rf := &requestFlags{}
	fs.BoolVar(&rf.rlibWaiter, "rlib-waiter", false, "wait for a compile instead of starting one")
	fs.StringVar(&rf.waiterRlib, "waiter-rlib", "", "rlib output path to install")
	fs.StringVar(&rf.waiterStderr, "waiter-stderr", "", "declared diagnostics sidecar")
	fs.StringVar(&rf.processWrapper, "process-wrapper", "", "path to process_wrapper")
	fs.StringVar(&rf.linkRlib, "worker-link-rlib", "", "the paired waiter's rlib output path")
	fs.StringVar(&rf.outputFile, "output-file", "", "rmeta-phase diagnostics sidecar")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	rf.pwArgs = fs.Args()
	return rf, nil
}

func expandResponseFiles(args []string) ([]string, error) {
	var out []string
	for _, a := range args {
		if strings.HasPrefix(a, "@") {
			data, err := os.ReadFile(a[1:])
			if err != nil {
				return nil, fmt.Errorf("response file %s: %w", a[1:], err)
			}
			for _, line := range strings.Split(string(data), "\n") {
				if line != "" {
					out = append(out, line)
				}
			}
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

func rewriteOutDir(args []string, newDir string) (string, bool) {
	for i, a := range args {
		if val, ok := strings.CutPrefix(a, "--out-dir="); ok {
			args[i] = "--out-dir=" + newDir
			return val, true
		}
		if a == "--out-dir" && i+1 < len(args) {
			orig := args[i+1]
			args[i+1] = newDir
			return orig, true
		}
	}
	return "", false
}
