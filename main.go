// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"

	"cloudeng.io/cmdutil/subcmd"
)

const cmdSpec = `name: cloudstream
summary: cloudstream is tool for concurrent and resumable downloads from
  web or cloud services. It aims to be the fastest and most reliable
  tool for downloading large files or files from unreliable, slow or rate-limited
  services.
commands:
  - name: cache
    summary: Download files from various services with local caching and resumption support. Downloads rely on byte range requests and are designed to be as fast as possible. Support for rate control is also provided.
    commands:
      - name: file
        summary: Download a single file as specified by a URI.
        arguments:
          - <uri>
      - name: bulk
        summary: |
          Download multiple files as specified in a JSON file.
          Run ` + "`cloudstream fetch bulk-cache --display-spec`" + ` to see the expected format.
        arguments:
          - <json-spec-file>
      - name: cached
        summary: Display the cached byte ranges in the index file.
        arguments:
          - <index-file>
      - name: outstanding
        summary: Display the outstanding byte ranges in the index file.
        arguments:
          - <index-file>
      - name: summary
        summary: Display a summary of the cached and outstanding byte ranges in the index file.
        arguments:
          - <index-file>

  - name: stream
    summary: Stream files from various services that do not require authentication. The files may be processed as they are downloaded, allowing for real-time processing of the data.
    commands:
      - name: file
        summary: Stream a single file as specified by a URI.
        arguments:
          - <uri>
      - name: bulk
        summary: |
          Stream multiple files as specified in a JSON file.
          Run ` + "`cloudstream fetch bulk-stream --display-spec`" + ` to see the expected format.
        arguments:
          - <json-spec-file>

  - name: google-drive
    summary: Download files from Google Drive.
    commands:
      - name: stat
        summary: Get the metadata of a file in Google Drive.
        arguments:
          - <file-name>
      - name: get
        summary: Download a file from Google Drive.
        arguments:
          - <file-id>
      - name: bulk-get
        summary: |
          Download multiple files from Google Drive as specified in a JSON file.
          Run ` + "`cloudstream google-drive bulk-get --display-spec`" + ` to see the expected format.
        arguments:
          - <json-spec-file>
      - name: stream
        summary: Stream a file from Google Drive.
        arguments:
          - <file-id>
      - name: bulk-stream
        summary: |
          Stream multiple files from Google Drive as specified in a JSON file.
          Run ` + "`cloudstream google-drive bulk-stream --display-spec`" + ` to see the expected format.
        arguments:
          - <json-spec-file>

  - name: digest
    summary: Compute/compared the hash digests of files.
    commands:
      - name: compute
        summary: Compute the hash digest of files
        arguments:
          - <files>...
          `

func cli() *subcmd.CommandSetYAML {
	cmd := subcmd.MustFromYAML(cmdSpec)

	cacheCmd := &CacheCmd{}
	cmd.Set("cache", "file").MustRunner(cacheCmd.File, &CacheFileFlags{})
	cmd.Set("cache", "bulk").MustRunner(cacheCmd.Bulk, &CacheBulkFlags{})
	cmd.Set("cache", "cached").MustRunner(cacheCmd.Cached, &CacheIndexFlags{})
	cmd.Set("cache", "outstanding").MustRunner(cacheCmd.Outstanding, &CacheIndexFlags{})
	cmd.Set("cache", "summary").MustRunner(cacheCmd.Summary, &CacheIndexFlags{})

	streamCmd := &StreamCmd{}
	cmd.Set("stream", "file").MustRunner(streamCmd.File, &StreamFileFlags{})
	cmd.Set("stream", "bulk").MustRunner(streamCmd.Bulk, &StreamBulkFlags{})

	gdriveCmd := &GoogleDriveCmd{}
	cmd.Set("google-drive", "get").MustRunner(gdriveCmd.Get, &GoogleDriveCacheGetFlags{})
	cmd.Set("google-drive", "stat").MustRunner(gdriveCmd.Stat, &GoogleDriveStatFlags{})

	/*  httpCmd := &HTTPCmd{}
	cmd.Set("http", "get").MustRunner(httpCmd.Get, &HTTPGetFlags{})
	cmd.Set("http", "bulk-get").MustRunner(httpCmd.BulkGet, &HTTPBulkGetFlags{})
	cmd.Set("http", "stream").MustRunner(httpCmd.Stream, &HTTPStreamFlags{})
	cmd.Set("http", "bulk-stream").MustRunner(httpCmd.BulkStream, &HTTPBulkStreamFlags{})

	idxCmd := &IndexCmd{}
	cmd.Set("index", "cached").MustRunner(idxCmd.Cached, &IndexFlags{})
	cmd.Set("index", "outstanding").MustRunner(idxCmd.Outstanding, &IndexFlags{})
	cmd.Set("index", "summary").MustRunner(idxCmd.Summary, &IndexFlags{})

	*/

	digestCmd := &DigestCmd{}
	cmd.Set("digest", "compute").MustRunner(digestCmd.Compute, &DigestComputeFlags{})
	return cmd
}

func main() {
	subcmd.Dispatch(context.Background(), cli())
}
