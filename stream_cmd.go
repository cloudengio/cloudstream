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

type StreamCmd struct{}

type StreamFileFlags struct {
	LoggingFlags
	DownloadFlags
	DigestFlags
	OutputFlags
	StreamFlags
}

func (cmd *StreamCmd) File(ctx context.Context, flags any, args []string) error {
	fl := flags.(*StreamFileFlags)
	ctx = ctxlog.WithLogger(ctx, fl.Logger())
	fl.DownloadFlags = fl.SetDefaults()
	spec, err := fl.BulkSpecForSingleURI(args[0], fl.OutputFlags)
	if err != nil {
		return err
	}
	spec.Files[0].DigestBase64 = fl.DigestBase64
	spec.Files[0].DigestHex = fl.DigestHex
	if fl.Output != "" {
		spec.Files[0].Output = fl.Output
	} else {
		spec.Files[0].Output = "-"
		fl.WithProgress = false
	}
	return streamFiles(ctx, fl.DownloadFlags, fl.VerifyChecksum, 1, spec)
}

type StreamBulkFlags struct {
	LoggingFlags
	DownloadFlags
	BulkFlags
	StreamFlags
}

func (cmd *StreamCmd) Bulk(ctx context.Context, flags any, args []string) error {
	fl := flags.(*StreamBulkFlags)
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
	return streamFiles(ctx, fl.DownloadFlags, fl.VerifyChecksum, fl.OutstandingDownloads, spec)
}
