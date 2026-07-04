// rustc_worker is the persistent worker behind rules_rust's pipelined-worker
// mode. Both actions of a pipelined crate are WorkRequests to the same worker
// pool:
//
//   - RustcMetadata compiles rmeta+rlib with ONE rustc, run through
//     process_wrapper, with --out-dir redirected to `<rlib>.rustc_scratch`.
//     The WorkResponse is sent as soon as the rmeta artifact notification
//     appears (downstream unblocks) while rustc keeps running.
//   - Rustc (--rlib-waiter) waits in-process for that compile to finish and
//     installs the rlib; a status-file fallback covers requests landing on a
//     different worker process.
//
// A worker-owned compile cache (REAPI gRPC or a local directory) short-cuts
// the whole compile on a hit. See cache.go.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

func main() {
	log.SetPrefix("[rustc_worker] ")
	log.SetFlags(0)
	// The worker is launched with no flags of its own; Bazel appends
	// --persistent_worker and every request carries its full payload.
	for _, a := range os.Args[1:] {
		if a != "--persistent_worker" {
			log.Fatalf("unexpected startup arg %q", a)
		}
	}
	w := &worker{
		backend: pickBackend(context.Background()),
		reg:     &registry{m: map[string]*compileState{}},
		stdout:  json.NewEncoder(os.Stdout),
		cancels: map[int64]chan struct{}{},
	}
	if w.backend != nil {
		log.Printf("cache backend: %s", w.backend.name())
	}

	dec := json.NewDecoder(os.Stdin)
	var wg sync.WaitGroup
	for {
		var req workRequest
		if err := dec.Decode(&req); err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("bad WorkRequest: %v", err)
			break
		}
		if req.Cancel {
			// Cancellation of an in-flight request; the original goroutine
			// replies with wasCancelled.
			w.cancel(req.RequestID)
			continue
		}
		wg.Add(1)
		go func(req workRequest) {
			defer wg.Done()
			defer w.dropCancel(req.RequestID)
			code, out := w.handle(&req)
			if code == cancelledExit {
				w.replyCancelled(req.RequestID)
				return
			}
			w.reply(req.RequestID, code, out)
		}(req)
	}
	wg.Wait()
}

type worker struct {
	backend cacheBackend
	reg     *registry
	stdout  *json.Encoder
	mu      sync.Mutex

	cmu     sync.Mutex
	cancels map[int64]chan struct{}
}

// cancelledExit is an internal sentinel: the request observed a cancel and
// its reply must carry wasCancelled.
const cancelledExit = -128

// cancelChan returns the cancellation signal for a request id.
func (w *worker) cancelChan(id int64) chan struct{} {
	w.cmu.Lock()
	defer w.cmu.Unlock()
	if c, ok := w.cancels[id]; ok {
		return c
	}
	c := make(chan struct{})
	w.cancels[id] = c
	return c
}

func (w *worker) cancel(id int64) {
	w.cmu.Lock()
	defer w.cmu.Unlock()
	if c, ok := w.cancels[id]; ok {
		select {
		case <-c:
		default:
			close(c)
		}
	} else {
		c := make(chan struct{})
		close(c)
		w.cancels[id] = c
	}
}

func (w *worker) dropCancel(id int64) {
	w.cmu.Lock()
	delete(w.cancels, id)
	w.cmu.Unlock()
}

func (w *worker) replyCancelled(id int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.stdout.Encode(workResponse{RequestID: id, ExitCode: 1, WasCancelled: true}); err != nil {
		log.Printf("write cancel response req=%d: %v", id, err)
	}
}

func (w *worker) reply(id int64, code int, output string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.stdout.Encode(workResponse{RequestID: id, ExitCode: code, Output: output}); err != nil {
		log.Printf("write response req=%d: %v", id, err)
	}
}
