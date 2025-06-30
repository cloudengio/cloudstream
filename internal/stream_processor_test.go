// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"cloudeng.io/cloudstream/bulkspec"
)

var streamHandlerBinary string

func TestMain(m *testing.M) {
	// Build the test binary
	tmpDir, err := os.MkdirTemp("", "stream-processor-test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir) //nolint:errcheck

	streamHandlerBinary = filepath.Join(tmpDir, "stream_handler")
	cmd := exec.Command("go", "build", "-o", streamHandlerBinary, "./testdata/stream_handler")
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build test binary: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()
	os.Exit(code)
}

func TestStreamProcessor(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	outputFilename := "output.txt"

	var capturedStderr bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&capturedStderr, nil))

	prefix := "memfs://my-bucket/data"

	prefixSpec := bulkspec.Config{
		Sink: &bulkspec.StreamSink{
			Dir:        tmpDir,
			BinaryName: streamHandlerBinary,
			Args:       []string{"arg1", "$STREAM_NAME"},
		},
	}

	fileSpec := bulkspec.File{
		Name:   "test-file.txt",
		Output: outputFilename,
	}

	sp, err := NewStreamProcessor(prefix, prefixSpec, fileSpec, logger)
	if err != nil {
		t.Fatalf("NewStreamProcessor failed: %v", err)
	}

	inputData := "hello world"
	inputReader := strings.NewReader(inputData)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sp.Run(ctx, inputReader)
	}()

	runErr := <-errCh

	if runErr != nil {
		t.Fatalf("Run() failed: %v", runErr)
	}

	// 1. Verify the output file content.
	outputData, err := os.ReadFile(outputFilename)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	expectedOutput := "HELLO WORLD"
	if got, want := string(outputData), expectedOutput; got != want {
		t.Errorf("output file content mismatch: got %q, want %q", got, want)
	}

	// 2. Verify the captured stderr.
	type logMsg struct {
		File    string `json:"file"`
		URI     string `json:"uri"`
		Message string `json:"message"`
	}
	text := []string{}
	sc := bufio.NewScanner(bytes.NewReader(capturedStderr.Bytes()))
	for sc.Scan() {
		line := sc.Text()
		var lm logMsg
		if err := json.Unmarshal([]byte(line), &lm); err != nil {
			t.Errorf("failed to unmarshal log message: %v", err)
			continue
		}
		if got, want := lm.File, fileSpec.Name; got != want {
			t.Errorf("log message file mismatch: got %q, want %q", got, want)
		}
		if got, want := lm.URI, "memfs://my-bucket/data/test-file.txt"; got != want {
			t.Errorf("log message URI mismatch: got %q, want %q", got, want)
		}
		text = append(text, lm.Message)
	}

	expectedMessages := []string{
		"Received args: [arg1 test-file.txt]",
		"STREAM_NAME=test-file.txt, STREAM_URI=memfs://my-bucket/data/test-file.txt",
	}
	if !slices.Equal(text, expectedMessages) {
		t.Errorf("stderr messages mismatch:\ngot:  %v\nwant: %v", text, expectedMessages)
	}

}
