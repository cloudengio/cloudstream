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
	CommonFlags
	DownloadFlags
	DigestFlags
	OutputFlags
	StreamFlags
}

type StreamBulkFlags struct {
	CommonFlags
	DownloadFlags
	BulkFlags
	StreamFlags
}

func (cmd *StreamCmd) File(ctx context.Context, flags any, args []string) error {
	fl := flags.(*StreamFileFlags)
	ctx = ctxlog.WithLogger(ctx, fl.CommonFlags.Logger())
	fl.DownloadFlags = fl.DownloadFlags.SetDefaults()
	spec, err := fl.DownloadFlags.BulkSpecForSingleURI(args[0], fl.OutputFlags)
	if err != nil {
		return err
	}
	spec.Files[0].DigestBase64 = fl.DigestFlags.DigestBase64
	spec.Files[0].DigestHex = fl.DigestFlags.DigestHex
	if fl.OutputFlags.Output != "" {
		spec.Files[0].Output = fl.OutputFlags.Output
	} else {
		spec.Files[0].Output = "-"
		fl.DownloadFlags.WithProgress = false
	}
	return streamFiles(ctx, fl.DownloadFlags, fl.VerifyChecksum, 1, spec)
}

func (cmd *StreamCmd) Bulk(ctx context.Context, flags any, args []string) error {
	fl := flags.(*StreamBulkFlags)
	if fl.DisplaySpec {
		fmt.Println(bulkspec.Example())
		return nil
	}
	ctx = ctxlog.WithLogger(ctx, fl.CommonFlags.Logger())
	spec, err := bulkspec.Parse(args[0])
	if err != nil {
		return err
	}
	spec.Config = fl.DownloadFlags.PreferFlags(spec.Config)
	return streamFiles(ctx, fl.DownloadFlags, fl.VerifyChecksum, fl.OutstandingDownloads, spec)
}
