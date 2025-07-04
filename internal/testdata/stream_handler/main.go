// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// stream_handler is a simple program for testing the StreamProcessor. It reads
// from stdin, converts the text to uppercase, and writes it to stdout. It also
// prints its arguments and specific environment variables to stderr.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

func main() {
	fmt.Fprintf(os.Stderr, "Received args: %v\n", os.Args[1:])
	fmt.Fprintf(os.Stderr, "STREAM_NAME=%s, STREAM_URI=%s\n", os.Getenv("STREAM_NAME"), os.Getenv("STREAM_URI"))

	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read from stdin: %v\n", err)
		os.Exit(1)
	}

	output := bytes.ToUpper(input)
	if _, err := os.Stdout.Write(output); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write to stdout: %v\n", err)
		os.Exit(1)
	}
}
