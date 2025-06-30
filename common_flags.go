// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path"
	"runtime"

	"cloudeng.io/cloudstream/bulkspec"
)

type CommonFlags struct {
	Logfile string `subcmd:"log-file,,'log file name, if not set, logs are written to stdout'"`
}

func (cf CommonFlags) Logger() *slog.Logger {
	if cf.Logfile == "" {
		return slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	f, err := os.OpenFile(cf.Logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Errorf("failed to open log file %s: %w", cf.Logfile, err))
	}
	return slog.New(slog.NewJSONHandler(f, nil))
}

type DownloadFlags struct {
	BlockSize         int  `subcmd:"block-size,,size of each byte range for downloads"`
	Concurrency       int  `subcmd:"concurrency,10,number of concurrent Reads"`
	Connections       int  `subcmd:"connections,10,number of connections to use for downloads"`
	WithProgress      bool `subcmd:"with-progress,true,display progress of the download"`
	WaitForCompletion bool `subcmd:"wait-for-completion,true,iterate until download has successfully completed"`
}

func (fl DownloadFlags) BulkConfig() bulkspec.Config {
	return bulkspec.Config{
		BlockSize:   fl.BlockSize,
		Concurrency: fl.Concurrency,
		Connections: fl.Connections,
	}
}

func (fl DownloadFlags) PreferFlags(c bulkspec.Config) bulkspec.Config {
	bs := bulkspec.Config{}
	if fl.BlockSize > 0 {
		bs.BlockSize = fl.BlockSize
	}
	if fl.Concurrency > 0 {
		bs.Concurrency = fl.Concurrency
	}
	if fl.Connections > 0 {
		bs.Connections = fl.Connections
	}
	return bs
}

func (fl DownloadFlags) SetDefaults() DownloadFlags {
	if fl.Concurrency <= 0 {
		fl.Concurrency = runtime.NumCPU()
	}
	return fl
}

type OutputFlags struct {
	Output string `subcmd:"output,,'output file name, if not set, the file is written to the current directory with the same name as the URL\\'s last path segment'"`
}

func (dl DownloadFlags) BulkSpecForSingleURI(uri string, of OutputFlags) (bulkspec.Files, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return bulkspec.Files{}, fmt.Errorf("failed to parse URI %s: %w", uri, err)
	}
	root := u.Scheme + "://" + u.Host + "/" + path.Dir(u.Path)
	bn := path.Base(u.Path)
	output := of.Output
	if output == "" {
		output = bn
	}
	return bulkspec.Files{
		Prefix: root,
		Config: dl.BulkConfig(),
		Files: []bulkspec.File{
			{
				Name:   bn,
				Output: output,
			},
		},
	}, nil
}

type DigestFlags struct {
	DigestBase64 string `subcmd:"digest-b64,,'expected digest of the file, (rfc1930 Repr-Digest format), if set, the file is verified against this checksum after download'"`
	DigestHex    string `subcmd:"digest-hex,,'expected hex digest of the file, if set, the file is verified against this checksum after download'"`
}

type CacheFlags struct {
	Cache string `subcmd:"cache,,'cache file name, if not set, a cache file in the current directory is used.'"`
}

func (fl CacheFlags) cacheFile(name string) string {
	if fl.Cache != "" {
		return fl.Cache
	}
	return name
}

type BulkFlags struct {
	OutstandingDownloads int  `subcmd:"outstanding-downloads,10,number of outstanding downloads to run concurrently"`
	DisplaySpec          bool `subcmd:"display-spec,false,display the spec of the JSON file"`
}

type StreamFlags struct {
	VerifyChecksum bool `subcmd:"verify-checksum,false,verify the checksum of downloaded files"`
}
