// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"cloudeng.io/algo/digests"
	"cloudeng.io/cloudstream/bulkspec"
	"cloudeng.io/errors"
	"cloudeng.io/file/largefile"
	"cloudeng.io/file/localfs"
	"cloudeng.io/google/cloud/gdrive"
	"cloudeng.io/net/http/httpfs"
	"google.golang.org/api/drive/v3"
)

// PerFileInfo contains information about a file to be downloaded, it is created
// from flags and bulkspec.Files.
type PerFileInfo struct {
	DownloadPath string       // Full name of file to download.
	Name         string       // Name of the file to download.
	Output       string       // Output file name.
	Cache        string       // Cache file for the downloaded file.
	Index        string       // Index file for the cache.
	Digest       digests.Hash // Algorithm used for the digest, nil of no digest is specified.
}

// PerFile creates a slice of PerFileInfo from the bulkspec.Files specification.
func PerFile(spec bulkspec.Files) ([]PerFileInfo, LargeFileOpenFunc, error) {
	u, err := url.Parse(spec.Prefix)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse prefix %s: %w", spec.Prefix, err)
	}
	if u.Scheme == "" {
		return nil, nil, fmt.Errorf("prefix %s does not have a scheme", spec.Prefix)
	}
	openFunc, ok := OpenerForScheme(u.Scheme)
	if !ok {
		return nil, nil, fmt.Errorf("unsupported scheme %q for opening largefile files", u.Scheme)
	}
	files := make([]PerFileInfo, len(spec.Files))
	for i, f := range spec.Files {
		fn := f.FileID
		if len(fn) == 0 {
			fn, err = url.JoinPath(u.Path, f.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to join prefix %s with file name %s: %w", spec.Prefix, f.Name, err)
			}
		}
		files[i] = PerFileInfo{
			DownloadPath: fn,
			Name:         f.Name,
			Output:       f.Output,
			Cache:        f.Cache,
			Index:        f.Index,
		}
		files[i].Digest, err = f.Digest()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create digest for file %s: %w", f.Name, err)
		}
	}
	return files, openFunc, nil
}

type LargeFileOpenFunc func(ctx context.Context, config bulkspec.Config, pf PerFileInfo) (largefile.Reader, error)

var openLargeFileFuncs = map[string]LargeFileOpenFunc{
	"http":   newHTTPLargeFile,
	"https":  newHTTPLargeFile,
	"file":   newLocalLargeFile,
	"gdrive": newGoogleDriveLargeFile,
}

// OpenerForScheme returns a LargeFileOpenFunc for the given scheme if it exists.
func OpenerForScheme(scheme string) (LargeFileOpenFunc, bool) {
	if fn, ok := openLargeFileFuncs[scheme]; ok {
		return fn, true
	}
	return nil, false
}

func newHTTPTransport(config bulkspec.Config) *http.Transport {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Minute,
			KeepAlive: 2 * time.Minute,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   config.Connections,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   20 * time.Second,
		ExpectContinueTimeout: 10 * time.Second,
		MaxConnsPerHost:       config.Connections,
		ReadBufferSize:        config.Connections * config.Concurrency * int(config.BlockSize),
	}
	return transport
}

func newHTTPLargeFile(ctx context.Context, config bulkspec.Config, pf PerFileInfo) (largefile.Reader, error) {
	lf, err := httpfs.NewLargeFile(ctx, pf.DownloadPath,
		httpfs.WithLargeFileBlockSize(config.BlockSize),
		httpfs.WithLargeFileDigest(pf.Digest),
		httpfs.WithLargeFileTransport(newHTTPTransport(config)))
	if err != nil {
		if errors.Is(err, httpfs.ErrNoRangeSupport) {
			return nil, fmt.Errorf("file %s does not support range requests: %w", pf.DownloadPath, err)
		}
		return nil, fmt.Errorf("failed to create large file reader for %s: %w", pf.DownloadPath, err)
	}
	return lf, nil
}

func newLocalLargeFile(ctx context.Context, config bulkspec.Config, pf PerFileInfo) (largefile.Reader, error) {
	f, err := os.Open(pf.DownloadPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open local file %s: %w", pf.DownloadPath, err)
	}
	lf, err := localfs.NewLargeFile(f, config.BlockSize, pf.Digest)
	if err != nil {
		return nil, fmt.Errorf("failed to create local large file reader for %s: %w", pf.DownloadPath, err)
	}
	return lf, nil
}

func newGoogleDriveLargeFile(ctx context.Context, config bulkspec.Config, pf PerFileInfo) (largefile.Reader, error) {
	sys := OpenerInfo(ctx)
	if sys == nil {
		return nil, fmt.Errorf("Google Drive service is not available in context")
	}
	srv, ok := sys.(*drive.Service)
	if !ok {
		return nil, fmt.Errorf("expected Google Drive service in context, got %T", sys)
	}
	lf, err := gdrive.NewReader(ctx, srv, pf.DownloadPath,
		gdrive.WithBlockSize(config.BlockSize))
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Drive large file reader for %s: %w", pf.DownloadPath, err)
	}
	return lf, nil
}

type openerCtxKey struct{}

// ContextWithOpenerInfo returns a new context with the provided system information
// associated with the openerCtxKey. This is used to pass information about the
// opener (e.g., Google Drive service) to the large file reader functions.
func ContextWithOpenerInfo(ctx context.Context, sys any) context.Context {
	return context.WithValue(ctx, openerCtxKey(struct{}{}), sys)
}

// OpenerInfo retrieves the opener information from the context.
// It returns nil if no opener information is found.
func OpenerInfo(ctx context.Context) any {
	if v := ctx.Value(openerCtxKey(struct{}{})); v != nil {
		return v
	}
	return nil
}
