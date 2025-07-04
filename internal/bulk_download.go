// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"

	"cloudeng.io/algo/digests"
	"cloudeng.io/cloudstream/bulkspec"
	"cloudeng.io/errors"
	"cloudeng.io/file/largefile"
	"cloudeng.io/logging/ctxlog"
	"cloudeng.io/sync/errgroup"
)

type downloadOptions struct {
	withProgress         bool
	waitForCompletion    bool
	outstandingDownloads int
	verifyChecksum       bool
}

type DownloadOption func(*downloadOptions)

// WithDownloadWaitForCompletion sets whether the download should wait for completion
// and iterate until the download is complete, retrying errors as necessary.
func WithDownloadWaitForCompletion(wait bool) func(*downloadOptions) {
	return func(opts *downloadOptions) {
		opts.waitForCompletion = wait
	}
}

// WithDownloadProgress sets whether the download should display progress
// information during the download process.
func WithDownloadProgress(withProgress bool) func(*downloadOptions) {
	return func(opts *downloadOptions) {
		opts.withProgress = withProgress
	}
}

// WithOutstandingDownloads sets the number of outstanding downloads that can be
// processed concurrently.
func WithOutstandingDownloads(outstanding int) func(*downloadOptions) {
	return func(opts *downloadOptions) {
		opts.outstandingDownloads = outstanding
	}
}

// WithVerifyChecksum sets whether the download should verify the checksum of the
// downloaded file against the expected digest.
func WithVerifyChecksum(verify bool) func(*downloadOptions) {
	return func(opts *downloadOptions) {
		opts.verifyChecksum = verify
	}
}

// BulkDownload manages the bulk download of files.
type BulkDownload struct {
	downloadOptions
	bulkspec.Config
	logger  *slog.Logger
	display *ProgressDisplay // Assuming a type for displaying progress.
}

// NewBulkDownload creates a new BulkDownload instance with the provided configuration
// and options.
func NewBulkDownload(ctx context.Context, config bulkspec.Config, opts ...DownloadOption) *BulkDownload {
	bc := &BulkDownload{
		Config: config,
	}
	for _, opt := range opts {
		opt(&bc.downloadOptions)
	}
	if bc.outstandingDownloads <= 0 {
		bc.outstandingDownloads = runtime.NumCPU()
	}
	bc.logger = ctxlog.Logger(ctx)
	if bc.withProgress {
		bc.display = NewProgressDisplay()
	}
	return bc
}

// Cache downloads files using a cache that support resuming downloads.
func (bc *BulkDownload) Cache(ctx context.Context, spec bulkspec.Files) error {
	return bc.run(ctx, spec, bc.downloadFile)
}

// Stream downloads files and streams them to the output, typically stdout.
func (bc *BulkDownload) Stream(ctx context.Context, spec bulkspec.Files) error {
	return bc.run(ctx, spec, bc.streamFile)
}

type downloadFunc func(ctx context.Context, opener LargeFileOpenFunc, pf PerFileInfo) (bool, error)

func (bc *BulkDownload) run(ctx context.Context, spec bulkspec.Files, downloader downloadFunc) error {
	pf, opener, err := PerFile(spec)
	if err != nil {
		return fmt.Errorf("failed to process bulk spec: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := make(chan PerFileInfo, bc.outstandingDownloads)
	g := &errgroup.T{}
	g = errgroup.WithConcurrency(g, bc.outstandingDownloads+2)

	if bc.withProgress {
		bc.display.Run(ctx)
	}

	g.Go(func() error {
		for _, p := range pf {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- p:
			}
		}
		close(ch)
		return nil
	})

	for range bc.outstandingDownloads {
		g.Go(func() error {
			for p := range ch {
				if earlyExit, err := downloader(ctx, opener, p); err != nil {
					if earlyExit {
						cancel()
					}
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if bc.withProgress {
		bc.display.Wait()
	}
	return nil
}

func downloadedBytes(st largefile.DownloadStats) int64 {
	return st.DownloadedBytes
}

func cachedBytes(st largefile.DownloadStats) int64 {
	return st.CachedOrStreamedBytes
}

// PreferHeaderDigest sets the digest from the largefile.Reader if it is set,
// otherwise it leaves the PerFileInfo's Digest as is.
func (pf *PerFileInfo) PreferHeaderDigest(lf largefile.Reader) {
	if lf.Digest().IsSet() {
		pf.Digest = lf.Digest()

	}
}

const ChecksumBlockSize = 16 * 1024 // 32 * 1024 * 1024

func (bc *BulkDownload) downloadFile(ctx context.Context, opener LargeFileOpenFunc, pf PerFileInfo) (bool, error) {
	lf, err := opener(ctx, bc.Config, pf)
	if err != nil {
		return true, fmt.Errorf("failed to open large file %s: %w", pf.DownloadURI, err)
	}
	pf.PreferHeaderDigest(lf)

	contentSize, blockSize := lf.ContentLengthAndBlockSize()

	sameSize, err := existsAndIsOfCorrectSize(pf.Output, contentSize)
	if err != nil {
		return true, fmt.Errorf("failed to check if output file %s exists and has correct size: %w", pf.Output, err)
	}
	if sameSize {
		return true, fmt.Errorf("%s already exists and has the correct size %d, skipping download", pf.Output, contentSize)
	}

	opts := []largefile.DownloadOption{
		largefile.WithDownloadConcurrency(bc.Concurrency),
		largefile.WithDownloadWaitForCompletion(bc.waitForCompletion),
	}
	if bc.verifyChecksum && pf.Digest.IsSet() {
		opts = append(opts, largefile.WithDownloadDigest(pf.Digest))
	}

	cache, err := useCacheIfPresent(pf)
	if err != nil {
		return true, err
	}

	var reserveCh chan<- int64
	var digestCh chan<- int64
	if bc.withProgress {
		dlCh := make(chan largefile.DownloadStats, bc.Concurrency)
		remainingDownloadBytes := contentSize
		if cache == nil {
			rb := bc.display.AddBytesBar(pf.Name, "reserve", contentSize)
			rb.Run(ctx)
			reserveCh = rb.UpdateCh()
		} else {
			cached, _ := cache.CachedBytesAndBlocks()
			remainingDownloadBytes -= cached
		}
		dp := bc.display.NewDownloadProgressBars(dlCh)
		if remainingDownloadBytes > 0 {
			dp.AddDownloadMetric(pf.Name, "downloaded", remainingDownloadBytes, downloadedBytes)
		}
		dp.AddDownloadMetric(pf.Name, "cached", contentSize, cachedBytes)

		if pf.Digest.Hash != nil {
			db := bc.display.AddBytesBar(pf.Name, pf.Digest.Algo, contentSize)
			db.Run(ctx)
			digestCh = db.UpdateCh()
		}
		dp.Run(ctx)
		opts = append(opts, largefile.WithDownloadProgress(dlCh))
	}

	if cache == nil {
		if err := largefile.CreateNewFilesForCache(ctx, pf.Cache, pf.Index, contentSize, blockSize, bc.Concurrency, reserveCh); err != nil {
			return true, fmt.Errorf("failed to create cache files %s and %s: %w", pf.Cache, pf.Index, err)
		}
		cache, err = useCachePresent(pf)
		if err != nil {
			return true, fmt.Errorf("failed to create cache files %s and %s: %w", pf.Cache, pf.Index, err)
		}
	}

	dl, err := largefile.NewCachingDownloader(lf, cache, opts...)
	if err != nil {
		return true, fmt.Errorf("failed to create caching downloader for file %s: %w", pf.DownloadURI, err)
	}

	var digestErrCh chan error
	if digestCh != nil {
		digestErrCh = make(chan error, 1)
		cd := calculateDigest{
			cache:      cache,
			hasher:     pf.Digest,
			progressCh: digestCh,
			buf:        make([]byte, ChecksumBlockSize),
		}
		go func() {
			err := cd.run(ctx)
			if err != nil {
				err = fmt.Errorf("failed to validate digest for file %s: %w", pf.DownloadURI, err)
			}
			digestErrCh <- err
			close(digestErrCh)
			close(digestCh)
		}()
	}
	var errs errors.M
	st, err := dl.Run(ctx)
	errs.Append(err)
	if digestErrCh != nil {
		err := <-digestErrCh
		errs.Append(err)
	}

	if err := errs.Err(); err != nil {
		return false, err
	}

	if st.DownloadSize != contentSize {
		return false, fmt.Errorf("downloaded size %d does not match expected size %d for file %s",
			st.DownloadSize, contentSize, pf.DownloadURI)
	}

	return false, os.Rename(pf.Cache, pf.Output) // Rename the cache file to the output file.
}

type calculateDigest struct {
	cache      largefile.DownloadCache
	hasher     digests.Hash
	progressCh chan<- int64
	buf        []byte
}

func (cd calculateDigest) run(ctx context.Context) error {
	var total int64
	var lastFrom int64
	contentSize, _ := cd.cache.ContentLengthAndBlockSize()
	for {
		contig := cd.cache.Tail(ctx)
		if contig.From == -1 {
			return ctx.Err()
		}
		for r := range largefile.Ranges(lastFrom, contig.To, ChecksumBlockSize) {
			buf := cd.buf[:r.Size()]
			n, err := cd.cache.ReadAt(buf, r.From)
			if err != nil {
				return fmt.Errorf("failed to read range %v: %w", r, err)
			}
			cd.hasher.Write(buf)
			total += int64(n)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case cd.progressCh <- total:
			default:
			}
		}
		lastFrom = contig.To + 1
		if contig.To == contentSize-1 {
			break
		}
	}
	err := cd.validate()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case cd.progressCh <- total: // Finalize the progress channel.
	}
	return err
}

func (cd calculateDigest) validate() error {
	valid := cd.hasher.Validate()
	if !valid {
		return fmt.Errorf("digest validation failed expected %s %s != %s",
			cd.hasher.Algo,
			digests.ToHex(cd.hasher.Digest),
			digests.ToHex(cd.hasher.Sum(nil)))
	}
	return nil
}

// useCacheIfPresent attempts to open existing cache files, and if present will use it,
// but if not present it will return nil without error.
func useCacheIfPresent(pf PerFileInfo) (*largefile.LocalDownloadCache, error) {
	dataRW, indexRW, err := largefile.OpenCacheFiles(pf.Cache, pf.Index)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to open cache files %s and %s: %w", pf.Cache, pf.Index, err)
		}
		return nil, nil // No cache present
	}
	cache, err := largefile.NewLocalDownloadCache(dataRW, indexRW)
	if err != nil {
		return nil, fmt.Errorf("failed to create local download cache: %w", err)
	}
	return cache, nil
}

// useCachePresent assumes that the cache files are present and creates a cache
// from them.
func useCachePresent(pf PerFileInfo) (*largefile.LocalDownloadCache, error) {
	dataRW, indexRW, err := largefile.OpenCacheFiles(pf.Cache, pf.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to open cache files %s and %s: %w", pf.Cache, pf.Index, err)
	}
	cache, err := largefile.NewLocalDownloadCache(dataRW, indexRW)
	if err != nil {
		return nil, fmt.Errorf("failed to create local download cache: %w", err)
	}
	return cache, nil
}

func existsAndIsOfCorrectSize(path string, size int64) (bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // File does not exist
		}
		return false, fmt.Errorf("failed to stat file %s: %w", path, err)
	}
	if fi.Size() != size {
		return false, fmt.Errorf("file %s exists but has size %d, expected %d", path, fi.Size(), size)
	}
	return true, nil // File exists and has the correct size
}

func (bc *BulkDownload) streamFile(ctx context.Context, opener LargeFileOpenFunc, pf PerFileInfo) (bool, error) {
	output := os.Stdout
	closeOutput := func() error {
		return nil
	}
	if pf.Output != "" && pf.Output != "-" {
		var err error
		output, err = os.Create(pf.Output)
		if err != nil {
			return true, fmt.Errorf("failed to create output file %s: %w", pf.Output, err)
		}
		closeOutput = func() error {
			return output.Close()
		}
	}

	lf, err := opener(ctx, bc.Config, pf)
	if err != nil {
		return true, err
	}
	pf.PreferHeaderDigest(lf)
	contentSize, _ := lf.ContentLengthAndBlockSize()

	opts := []largefile.DownloadOption{
		largefile.WithDownloadConcurrency(bc.Concurrency),
		largefile.WithDownloadWaitForCompletion(bc.waitForCompletion),
	}
	if bc.verifyChecksum && pf.Digest.IsSet() {
		opts = append(opts, largefile.WithDownloadDigest(pf.Digest))
	}

	if bc.withProgress {
		dlCh := make(chan largefile.DownloadStats, bc.Concurrency)
		dp := bc.display.NewDownloadProgressBars(dlCh)
		dp.AddDownloadMetric(pf.Name, "downloaded", contentSize, downloadedBytes)
		dp.AddDownloadMetric(pf.Name, "ordered", contentSize, cachedBytes)
		dp.Run(ctx)
		opts = append(opts, largefile.WithDownloadProgress(dlCh))
	}

	dl := largefile.NewStreamingDownloader(lf, opts...)

	writtenCh := make(chan int64, 1)

	var errs errors.M

	go func() {
		n, err := io.Copy(output, dl)
		errs.Append(err)
		writtenCh <- n
	}()

	st, err := dl.Run(ctx)
	errs.Append(err)
	if err := closeOutput(); err != nil {
		errs.Append(fmt.Errorf("failed to close output file %s: %w", pf.Output, err))
	}

	written := <-writtenCh

	if st.DownloadSize != contentSize {
		return false, fmt.Errorf("downloaded bytes %d do not match expected size %d for file %s",
			st.DownloadSize, contentSize, pf.DownloadURI)
	}
	if written != contentSize {
		return false, fmt.Errorf("written bytes %d do not match expected size %d for file %s",
			st.DownloadSize, contentSize, pf.DownloadURI)
	}

	return false, errs.Err()
}
