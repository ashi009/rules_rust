package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cacheKey hashes the request OPAQUELY: the Bazel-computed input digests plus
// the raw arguments, both in the deterministic order Bazel sends them.
// Param/env/arg file CONTENTS are covered by their own input digests;
// `${pwd}`-style placeholders are hashed verbatim, so keys are
// machine-portable.
type cacheKeyDigest struct {
	hash string
	size int64 // bytes hashed; makes a well-formed REAPI digest
}

type countingHash struct {
	h hash.Hash
	n int64
}

func (c *countingHash) write(b []byte) {
	c.h.Write(b)
	c.n += int64(len(b))
}

func cacheKey(req *workRequest) (cacheKeyDigest, bool) {
	if len(req.Inputs) == 0 {
		return cacheKeyDigest{}, false
	}
	ch := &countingHash{h: sha256.New()}
	for _, in := range req.Inputs {
		if len(in.Digest) == 0 {
			return cacheKeyDigest{}, false
		}
		ch.write([]byte(in.Path))
		ch.write([]byte{0})
		ch.write(in.Digest)
		ch.write([]byte{0})
	}
	ch.write([]byte{1})
	for _, a := range req.Arguments {
		ch.write([]byte(a))
		ch.write([]byte{0})
	}
	return cacheKeyDigest{hash: hex.EncodeToString(ch.h.Sum(nil)), size: ch.n}, true
}

// cachedPair is a fetched {rmeta, rlib} pair as temp files owned by the
// caller: install them with move() or delete them.
type cachedPair struct {
	rmeta string
	rlib  string
}

type cacheBackend interface {
	// fetch retrieves the pair for key into fresh temp files under tmpDir.
	// (nil, nil) is a miss. On any error nothing is left behind; on success
	// the caller owns both temp files.
	fetch(ctx context.Context, key cacheKeyDigest, tmpDir string) (*cachedPair, error)
	// fill stores the pair for key; both files are read in place.
	fill(ctx context.Context, key cacheKeyDigest, rmetaSrc, rlibSrc string) error
	name() string
}

func pickBackend(ctx context.Context) cacheBackend {
	if ep := os.Getenv("RUSTC_WORKER_REMOTE_CACHE"); ep != "" {
		service := strings.TrimPrefix(strings.TrimPrefix(ep, "grpc://"), "grpcs://")
		c, err := client.NewClient(ctx, "", client.DialParams{
			Service:    service,
			NoSecurity: true,
		})
		if err != nil {
			log.Printf("remote cache dial %s: %v (caching disabled)", service, err)
			return nil
		}
		return &grpcCache{c: c}
	}
	if dir := os.Getenv("RUSTC_WORKER_CACHE_DIR"); dir != "" {
		return &dirCache{dir: dir}
	}
	return nil
}

// grpcCache speaks REAPI (ActionCache + CAS) via remote-apis-sdks. The
// server-side AC deps check (default in bazel-remote) makes hits atomic: an
// entry with any evicted blob is served as NotFound.
type grpcCache struct {
	c *client.Client
}

func (g *grpcCache) name() string { return "grpc" }

func (g *grpcCache) fetch(ctx context.Context, key cacheKeyDigest, tmpDir string) (*cachedPair, error) {
	ar, err := g.c.GetActionResult(ctx, &repb.GetActionResultRequest{
		ActionDigest: &repb.Digest{Hash: key.hash, SizeBytes: key.size},
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	var rmeta, rlib *repb.OutputFile
	for _, of := range ar.GetOutputFiles() {
		switch filepath.Ext(of.GetPath()) {
		case ".rmeta":
			rmeta = of
		case ".rlib":
			rlib = of
		}
	}
	if rmeta == nil || rlib == nil {
		return nil, fmt.Errorf("AC entry incomplete (%d files)", len(ar.GetOutputFiles()))
	}
	read := func(of *repb.OutputFile) (string, error) {
		f, err := os.CreateTemp(tmpDir, of.GetPath()+".fetch*")
		if err != nil {
			return "", err
		}
		f.Close()
		d := digest.Digest{Hash: of.GetDigest().GetHash(), Size: of.GetDigest().GetSizeBytes()}
		if _, err := g.c.ReadBlobToFile(ctx, d, f.Name()); err != nil {
			os.Remove(f.Name())
			return "", err
		}
		return f.Name(), nil
	}
	// A race with eviction between the deps check and the reads surfaces as
	// an error here; the caller treats it as a miss.
	rmetaTmp, err := read(rmeta)
	if err != nil {
		return nil, err
	}
	rlibTmp, err := read(rlib)
	if err != nil {
		os.Remove(rmetaTmp)
		return nil, err
	}
	return &cachedPair{rmeta: rmetaTmp, rlib: rlibTmp}, nil
}

func (g *grpcCache) fill(ctx context.Context, key cacheKeyDigest, rmetaSrc, rlibSrc string) error {
	rmetaDg, err := digest.NewFromFile(rmetaSrc)
	if err != nil {
		return err
	}
	rlibDg, err := digest.NewFromFile(rlibSrc)
	if err != nil {
		return err
	}
	// UploadIfMissing streams from the files and dedups against the server.
	if _, _, err := g.c.UploadIfMissing(ctx,
		uploadinfo.EntryFromFile(rmetaDg, rmetaSrc),
		uploadinfo.EntryFromFile(rlibDg, rlibSrc),
	); err != nil {
		return err
	}
	_, err = g.c.UpdateActionResult(ctx, &repb.UpdateActionResultRequest{
		ActionDigest: &repb.Digest{Hash: key.hash, SizeBytes: key.size},
		ActionResult: &repb.ActionResult{
			OutputFiles: []*repb.OutputFile{
				{Path: filepath.Base(rmetaSrc), Digest: rmetaDg.ToProto()},
				{Path: filepath.Base(rlibSrc), Digest: rlibDg.ToProto()},
			},
		},
	})
	return err
}

// dirCache is a local directory backend (one subdir per key, no eviction).
type dirCache struct{ dir string }

func (d *dirCache) name() string { return "disk" }

func (d *dirCache) fetch(_ context.Context, key cacheKeyDigest, tmpDir string) (*cachedPair, error) {
	entry := filepath.Join(d.dir, key.hash)
	des, err := os.ReadDir(entry)
	if err != nil {
		return nil, nil
	}
	var rmeta, rlib string
	for _, de := range des {
		switch filepath.Ext(de.Name()) {
		case ".rmeta":
			rmeta = filepath.Join(entry, de.Name())
		case ".rlib":
			rlib = filepath.Join(entry, de.Name())
		}
	}
	if rmeta == "" || rlib == "" {
		return nil, nil
	}
	read := func(src string) (string, error) {
		f, err := os.CreateTemp(tmpDir, filepath.Base(src)+".fetch*")
		if err != nil {
			return "", err
		}
		f.Close()
		if err := atomicCopy(src, f.Name()); err != nil {
			os.Remove(f.Name())
			return "", err
		}
		return f.Name(), nil
	}
	rmetaTmp, err := read(rmeta)
	if err != nil {
		return nil, err
	}
	rlibTmp, err := read(rlib)
	if err != nil {
		os.Remove(rmetaTmp)
		return nil, err
	}
	return &cachedPair{rmeta: rmetaTmp, rlib: rlibTmp}, nil
}

func (d *dirCache) fill(_ context.Context, key cacheKeyDigest, rmetaSrc, rlibSrc string) error {
	entry := filepath.Join(d.dir, key.hash)
	if _, err := os.Stat(entry); err == nil {
		return nil
	}
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp(d.dir, key.hash+".tmp*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp) // no-op once renamed
	if err := atomicCopy(rmetaSrc, filepath.Join(tmp, filepath.Base(rmetaSrc))); err != nil {
		return err
	}
	if err := atomicCopy(rlibSrc, filepath.Join(tmp, filepath.Base(rlibSrc))); err != nil {
		return err
	}
	if err := os.Rename(tmp, entry); err != nil {
		if _, statErr := os.Stat(entry); statErr == nil {
			return nil // lost a benign race
		}
		return err
	}
	return nil
}
