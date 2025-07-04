package internal_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cloudeng.io/cloudstream/bulkspec"
	"cloudeng.io/cloudstream/internal"
)

func TestBulkDownload_Cache(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "testfile")
	if err := os.WriteFile(testFile, []byte("0123456789abcdef"), 0600); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	spec := bulkspec.Files{
		Prefix: "file://" + tmpDir,
		Files: []bulkspec.File{
			{
				NameOrID: "testfile",
				Output:   filepath.Join(tmpDir, "output"),
				Cache:    filepath.Join(tmpDir, "cache"),
				Index:    filepath.Join(tmpDir, "index"),
			},
		},
	}

	bd := internal.NewBulkDownload(ctx, spec.Config)

	err := bd.Cache(ctx, spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buf, err := os.ReadFile(filepath.Join(tmpDir, "output"))
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if string(buf) != "0123456789abcdef" {
		t.Errorf("unexpected output: %q", buf)
	}
}

func TestBulkDownload_Stream(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "testfile")
	if err := os.WriteFile(testFile, []byte("0123456789abcdef"), 0600); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	spec := bulkspec.Files{
		Prefix: "file://" + tmpDir,
		Files: []bulkspec.File{
			{
				NameOrID: "testfile",
				Output:   filepath.Join(tmpDir, "output"),
			},
		},
	}
	bd := internal.NewBulkDownload(ctx, spec.Config)

	err := bd.Stream(ctx, spec)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// Check output file exists and content matches
	got, err := os.ReadFile(filepath.Join(tmpDir, "output"))
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	want := "0123456789abcdef"
	if string(got) != want {
		t.Errorf("output mismatch: got %q, want %q", got, want)
	}
}
