// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"

	"cloudeng.io/cloudstream/bulkspec"
	"cloudeng.io/logging/ctxlog"
)

type CacheCmd struct{}

type CacheFileFlags struct {
	LoggingFlags
	DownloadFlags
	DigestFlags
	OutputFlags
	CacheFlags
}

func (cmd *CacheCmd) File(ctx context.Context, flags any, args []string) error {
	fl := flags.(*CacheFileFlags)
	ctx = ctxlog.WithLogger(ctx, fl.Logger())
	fl.DownloadFlags = fl.SetDefaults()
	spec, err := fl.BulkSpecForSingleURI(args[0], fl.OutputFlags)
	if err != nil {
		return err
	}
	spec.Files[0].Cache = fl.cacheFile(spec.Files[0].Output) + ".cache"
	spec.Files[0].Index = fl.cacheFile(spec.Files[0].Output) + ".index"
	spec.Files[0].DigestBase64 = fl.DigestBase64
	spec.Files[0].DigestHex = fl.DigestHex
	return cacheFiles(ctx, fl.DownloadFlags, 1, spec)
}

type CacheBulkFlags struct {
	LoggingFlags
	DownloadFlags
	BulkFlags
}

func (cmd *CacheCmd) Bulk(ctx context.Context, flags any, args []string) error {
	fl := flags.(*CacheBulkFlags)
	if fl.DisplaySpec {
		fmt.Println(bulkspec.Example())
		return nil
	}
	ctx = ctxlog.WithLogger(ctx, fl.Logger())
	spec, err := bulkspec.Parse(args[0])
	if err != nil {
		return err
	}
	spec.Config = fl.PreferFlags(spec.Config)
	return cacheFiles(ctx, fl.DownloadFlags, fl.OutstandingDownloads, spec)
}
