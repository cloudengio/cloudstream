// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"

	"cloudeng.io/cloudstream/bulkspec"
	"cloudeng.io/cloudstream/internal"
)

func cacheFiles(ctx context.Context, fl DownloadFlags, outstandingConnections int, spec bulkspec.Files) error {
	bc := internal.NewBulkDownload(ctx, spec.Config,
		internal.WithDownloadWaitForCompletion(fl.WaitForCompletion),
		internal.WithDownloadProgress(fl.WithProgress),
		internal.WithOutstandingDownloads(outstandingConnections))
	return bc.Cache(ctx, spec)
}

func streamFiles(ctx context.Context, fl DownloadFlags, verifyChecksum bool, outstandingConnections int, spec bulkspec.Files) error {
	bc := internal.NewBulkDownload(ctx, spec.Config,
		internal.WithDownloadWaitForCompletion(fl.WaitForCompletion),
		internal.WithDownloadProgress(fl.WithProgress),
		internal.WithVerifyChecksum(verifyChecksum),
		internal.WithOutstandingDownloads(outstandingConnections))
	return bc.Stream(ctx, spec)
}
