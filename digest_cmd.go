// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"cloudeng.io/algo/digests"
	"cloudeng.io/sync/errgroup"
)

type DigestCmd struct{}

type DigestComputeFlags struct {
	Algorithm   string `subcmd:"algo,sha1,'the hash algorithm to use (eg. sha1, md5, sha256, sha512)'"`
	Format      string `subcmd:"format,hex,'the output format for the digest (eg. base64, hex)'"`
	Concurrency int    `subcmd:"concurrency,0,the number of concurrent digest calculations"`
}

func (c DigestCmd) Compute(ctx context.Context, flags any, args []string) error {
	fl := flags.(*DigestComputeFlags)
	if fl.Concurrency <= 0 {
		fl.Concurrency = runtime.NumCPU()
	}
	if fl.Format != "base64" && fl.Format != "hex" {
		return fmt.Errorf("unsupported format: %s: need one of base64, hex", fl.Format)
	}
	g := &errgroup.T{}
	g = errgroup.WithConcurrency(g, fl.Concurrency)

	if !digests.IsSupported(fl.Algorithm) {
		return fmt.Errorf("unsupported hash algorithm: %s: need one of %v", fl.Algorithm, digests.Supported())
	}
	var mu sync.Mutex
	for _, file := range args {
		g.Go(func() error {
			h, d, err := c.calculateDigest(fl.Algorithm, file)
			if err != nil {
				return err
			}
			var out string
			if fl.Format == "base64" {
				out = digests.ToBase64(d)
			} else {
				out = digests.ToHex(d)
			}
			mu.Lock()
			fmt.Printf("%s: %s (%s)\n", file, out, h.Algo)
			mu.Unlock()
			return nil
		})
	}
	return g.Wait()
}

func (c DigestCmd) calculateDigest(algo, file string) (digests.Hash, []byte, error) {
	h, err := digests.New(algo, nil)
	if err != nil {
		return digests.Hash{}, nil, err
	}

	f, err := os.Open(file)
	if err != nil {
		return digests.Hash{}, nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return digests.Hash{}, nil, err
	}

	n, err := io.Copy(h, f)
	if err != nil {
		return digests.Hash{}, nil, err
	}
	if n != fi.Size() {
		return digests.Hash{}, nil, fmt.Errorf("file size mismatch: expected %d bytes, got %d bytes", fi.Size(), n)
	}
	// Read the file and update the hash.
	return h, h.Sum(nil), f.Close()
}
