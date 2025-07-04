// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"slices"

	"cloudeng.io/cloudstream/bulkspec"
)

// StreamProcessor processes a single file stream.
type StreamProcessor struct {
	prefix  string
	config  bulkspec.Config // Configuration for the stream processing.
	file    bulkspec.File
	logger  *slog.Logger // Optional logger for logging messages.
	uri     string
	args    []string
	environ []string
}

// NewStreamProcessor creates a new StreamProcessor for the given prefix and file
// specification.
func NewStreamProcessor(prefix string, config bulkspec.Config, file bulkspec.File, logger *slog.Logger) (*StreamProcessor, error) {
	uri, err := url.JoinPath(prefix, file.NameOrID)
	if err != nil {
		return nil, err
	}
	sp := &StreamProcessor{
		prefix: prefix,
		config: config,
		file:   file,
		uri:    uri,
		logger: logger.With("file", file.NameOrID, "uri", uri),
	}
	args := make([]string, 0, len(config.Sink.Args))
	for _, v := range config.Sink.Args {
		args = append(args, os.Expand(v, sp.envMap))
	}
	sp.args = args
	sp.environ = slices.Clone(os.Environ())
	sp.environ = append(sp.environ, "STREAM_URI="+sp.uri, "STREAM_NAME="+file.NameOrID)
	return sp, nil
}

func (sp *StreamProcessor) envMap(name string) string {
	switch name {
	case "STREAM_URI":
		return sp.uri
	case "STREAM_NAME":
		return sp.file.NameOrID
	default:
		return os.ExpandEnv(name)
	}
}

func (sp *StreamProcessor) Write(buf []byte) (int, error) {
	buflen := len(buf)
	for idx := bytes.Index(buf, []byte("\n")); idx >= 0; idx = bytes.Index(buf, []byte("\n")) {
		if idx > 0 {
			// Print the line up to the newline character.
			sp.logger.Info("stream output", "message", string(buf[:idx]))
		}
		buf = buf[idx+1:] // Move past the newline character.
	}
	if len(buf) > 0 {
		sp.logger.Info("stream output", "message", string(buf))
	}
	return buflen, nil
}

func (sp *StreamProcessor) Run(ctx context.Context, rd io.Reader) error {
	out, err := os.Create(sp.file.Output)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	cmd := exec.CommandContext(ctx, sp.config.Sink.BinaryName, sp.args...)
	cmd.Stdin = rd
	cmd.Stdout = out
	cmd.Stderr = sp // catch stderr to the StreamProcessor for error handling.
	cmd.Dir = sp.config.Sink.Dir
	cmd.Env = sp.environ
	err = cmd.Run()
	if err != nil {
		out.Close() //nolint:errcheck
		return fmt.Errorf("failed to run command: %w", err)
	}
	return out.Close()
}
