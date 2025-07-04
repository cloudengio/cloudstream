// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"net/url"
	"testing"

	"cloudeng.io/cloudstream/bulkspec"
)

func TestOpenerForScheme(t *testing.T) {
	// Known schemes
	for _, scheme := range []string{"http", "https", "file", "gdrive"} {
		fn, ok := OpenerForScheme(scheme)
		if !ok || fn == nil {
			t.Errorf("expected opener for scheme %q", scheme)
		}
	}
	// Unknown scheme
	if fn, ok := OpenerForScheme("nosuch"); ok || fn != nil {
		t.Errorf("expected no opener for unknown scheme")
	}
}

func TestPerFile(t *testing.T) {
	spec := bulkspec.Files{
		Prefix: "file:///tmp",
		Files: []bulkspec.File{
			{NameOrID: "foo.txt", Output: "foo.out"},
		},
	}
	files, openFunc, err := PerFile(spec)
	if err != nil {
		t.Fatalf("PerFile failed: %v", err)
	}
	if len(files) != 1 || files[0].Name != "foo.txt" {
		t.Errorf("unexpected files: %+v", files)
	}
	if openFunc == nil {
		t.Errorf("expected openFunc")
	}

	// Bad prefix
	spec.Prefix = "://missing"
	_, _, err = PerFile(spec)
	if err == nil {
		t.Errorf("expected error for missing scheme")
	}

	// Unknown scheme
	spec.Prefix = "nosuch://foo"
	_, _, err = PerFile(spec)
	if err == nil {
		t.Errorf("expected error for unknown scheme")
	}
}

func TestContextWithOpenerInfoAndOpenerInfo(t *testing.T) {
	ctx := context.Background()
	val := "testsys"
	ctx2 := ContextWithOpenerInfo(ctx, val)
	got := OpenerInfo(ctx2)
	if got != val {
		t.Errorf("OpenerInfo did not return expected value")
	}
	if OpenerInfo(context.Background()) != nil {
		t.Errorf("OpenerInfo should return nil if not set")
	}
}

func TestNewLocalLargeFile_Error(t *testing.T) {
	u, err := url.Parse("file:///tmp/nonexistentfile")
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}
	// Should fail to open non-existent file
	pf := PerFileInfo{DownloadURI: u}
	_, err = newLocalLargeFile(context.Background(), bulkspec.Config{}, pf)
	if err == nil {
		t.Errorf("expected error for missing file")
	}
}

func TestNewHTTPLargeFile_Error(t *testing.T) {
	// Should fail for invalid URL
	u, err := url.Parse("http://localhost:0/doesnotexist")
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}
	// Should fail to open non-existent HTTP file
	pf := PerFileInfo{DownloadURI: u}
	_, err = newHTTPLargeFile(context.Background(), bulkspec.Config{}, pf)
	if err == nil {
		t.Errorf("expected error for invalid HTTP file")
	}
}

func TestNewGoogleDriveLargeFile_Error(t *testing.T) {
	// Should fail if context has no drive.Service
	u, err := url.Parse("gdrive://fileid")
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}
	// Create a PerFileInfo without a drive service
	pf := PerFileInfo{DownloadURI: u}
	_, err = newGoogleDriveLargeFile(context.Background(), bulkspec.Config{}, pf)
	if err == nil {
		t.Errorf("expected error for missing drive service")
	}
}
