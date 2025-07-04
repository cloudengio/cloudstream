// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package bulkspec

import (
	"encoding/json"
	"fmt"
	"os"

	"cloudeng.io/algo/digests"
	"cloudeng.io/net/http/httpfs/rfc9530"
)

// File defines the specification for a single file to be downloaded in
// a bulk operation.
type File struct {
	NameOrID     string `json:"name_or_id"`           // The name or ID of the file to download, relative to the prefix in the parent BulkSpec.
	DigestBase64 string `json:"digest_b64,omitempty"` // Optional digest of the file. rfc9530 format is expected.
	DigestHex    string `json:"digest_hex,omitempty"` // Optional hex digest of the file.
	Output       string `json:"output,omitempty"`     // Optional local output file name, the default is the Name of the file above.
	Cache        string `json:"cache,omitempty"`      // Optional cache file name.
	Index        string `json:"index,omitempty"`      // Optional index file name.
}

// StreamSink rerepresents a sink for streaming data, each streamed file will
// be written to the stdin of an instance of the binary specified in
// BinaryName, with the arguments specified in Args. Environment variables
// are expanded in the Args, and the following environment variables are
// defined:
//   - STREAM_URI: the full URI of the file streamed.
//   - STREAM_NAME: the name of the file streamed, as specified in the File
type StreamSink struct {
	Dir        string   `json:"dir"`  // The directory where the binary will be executed.
	BinaryName string   `json:"name"` // The name of the binary to use for processing.
	Args       []string `json:"args"` // Additional arguments to pass to the binary.
}

// Config represents the Config of a bulk download specification and other
// global parameters that apply to all files in the bulk download.
type Config struct {
	// Optional stream sink for processing the files as they are downloaded.
	Sink        *StreamSink `json:"sink,omitempty"`
	BlockSize   int         `json:"block_size,omitempty"`  // Size of each byte range for downloads.
	Concurrency int         `json:"concurrency,omitempty"` // Number of concurrent downloads.
	Connections int         `json:"connections,omitempty"` // Number of connections to use for downloads.
}

type Files struct {
	Config
	Prefix string `json:"prefix"` // The prefix for the bulk download, typically a URL or a file system path.
	Files  []File `json:"files"`  // List of files to download.
}

var exampleSpec = Files{
	Prefix: "memfs://my-bucket/data",
	Config: Config{
		Sink: &StreamSink{
			Dir:        "memfs://my-bucket/data",
			BinaryName: "streamHandlerBinary",
			Args:       []string{"arg1", "$STREAM_NAME"},
		},
	},
	Files: []File{
		{
			NameOrID:     "file1.txt",
			DigestBase64: "sha1=:<base64-digest>:",
			DigestHex:    "sha1=<hex-digits>..",
			Output:       "file1.txt",
			Cache:        "file1.cache",
			Index:        "file1.index",
		},
		{
			NameOrID:     "file2.txt",
			DigestBase64: "sha256=:<base64-digest>:",
			DigestHex:    "sha256=<hex-digits>..",
		},
	},
}

func Example() string {
	out, err := json.MarshalIndent(exampleSpec, "  ", "  ")
	if err != nil {
		panic(fmt.Sprintf("failed to marshal example spec: %v", err))
	}
	return string(out)
}

func DigestFromRFC9530(digest string) (digests.Hash, error) {
	algo, _, bytesDigest, err := rfc9530.ParseAlgoDigest(digest)
	if err != nil {
		return digests.Hash{}, fmt.Errorf("failed to parse RFC1930 digest: %w", err)
	}
	return digests.New(algo, bytesDigest)
}

func DigestFromHex(digest string) (digests.Hash, error) {
	algo, hexDigest, err := digests.ParseHex(digest)
	if err != nil {
		return digests.Hash{}, fmt.Errorf("failed to parse hex digest: %w", err)
	}
	dbytes, err := digests.FromHex(hexDigest)
	if err != nil {
		return digests.Hash{}, fmt.Errorf("failed to decode hex digest: %w", err)
	}
	return digests.New(algo, dbytes)
}

func (f File) Digest() (digests.Hash, error) {
	if f.DigestBase64 != "" {
		return DigestFromRFC9530(f.DigestBase64)
	}
	if f.DigestHex != "" {
		return DigestFromHex(f.DigestHex)
	}
	return digests.Hash{}, nil
}

func Parse(path string) (Files, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Files{}, fmt.Errorf("failed to read bulk spec file %s: %w", path, err)
	}
	var spec Files
	if err := json.Unmarshal(data, &spec); err != nil {
		return Files{}, fmt.Errorf("failed to unmarshal bulk spec file %s: %w", path, err)
	}
	if len(spec.Files) == 0 {
		return Files{}, fmt.Errorf("no files to download in bulk spec file %s", path)
	}
	return spec, nil
}
