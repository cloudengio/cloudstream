// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"

	"cloudeng.io/cloudstream/bulkspec"
	"cloudeng.io/cloudstream/internal"
	"cloudeng.io/google/cloud/gdrive"
	"cloudeng.io/logging/ctxlog"
	"google.golang.org/api/drive/v3"
)

type GoogleDriveCmd struct{}

// GoogleDriveFlags contains flags common to all Google Drive commands.
type GoogleDriveFlags struct {
	LoggingFlags
	CredentialsFile string `subcmd:"credentials-file,google-drive-credentials.json,Google Drive service account credentials file"`
	FileID          bool   `subcmd:"file-id,false,interpret arguments as file IDs rather than file names"`
}

func (cmd *GoogleDriveCmd) createService(ctx context.Context, credentialsFile string) (*drive.Service, error) {
	// Read the credentials file.
	credentialsJSON, err := os.ReadFile(credentialsFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read credentials file: %v", err)
	}
	srv, err := gdrive.ServiceFromJSON(ctx, credentialsJSON, drive.DriveReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("unable to create Drive service from file %v: %v", credentialsFile, err)
	}
	return srv, nil
}

// byNameOrID resolves a file by its name or ID according to the asID flag.
// The returned file contains only the ID and name fields.
func (cmd *GoogleDriveCmd) byNameOrID(ctx context.Context, srv *drive.Service, asID bool, identifier string) (*drive.File, error) {
	if asID {
		return gdrive.GetWithFields(ctx, srv, identifier, "id", "name")
	}
	return gdrive.GetFileID(ctx, srv, fmt.Sprintf("name=%q", identifier))
}

func (cmd *GoogleDriveCmd) byNameOrIDStat(ctx context.Context, srv *drive.Service, asID bool, identifier string) (*drive.File, error) {
	file, err := cmd.byNameOrID(ctx, srv, asID, identifier)
	if err != nil {
		return nil, fmt.Errorf("unable to resolve file ID or name: %w", err)
	}
	return gdrive.GetWithFields(ctx, srv, file.Id, "id", "name", "size", "md5Checksum", "sha1Checksum", "sha256Checksum", "parents")
}

type GoogleDriveStatFlags struct {
	GoogleDriveFlags
}

func (cmd *GoogleDriveCmd) Stat(ctx context.Context, flags any, args []string) error {
	fl := flags.(*GoogleDriveStatFlags)

	srv, err := cmd.createService(ctx, fl.GoogleDriveFlags.CredentialsFile)
	if err != nil {
		log.Fatalf("Unable to create Drive service: %v", err)
	}

	file, err := cmd.byNameOrIDStat(ctx, srv, fl.GoogleDriveFlags.FileID, args[0])
	if err != nil {
		log.Fatalf("Unable to retrieve file metadata: %v", err)
	}
	fmt.Printf("File ID: %s\n", file.Id)
	fmt.Printf("File Name: %s\n", file.Name)
	fmt.Printf("File Size: %d bytes\n", file.Size)
	if len(file.Parents) > 0 {
		fmt.Printf("Parent Folder ID: %s\n", file.Parents[0])
		parent, err := gdrive.GetWithFields(ctx, srv, file.Parents[0], "id", "name")
		if err == nil {
			fmt.Printf("Parent Folder Name: %s\n", parent.Name)
		} else {
			fmt.Printf("Unable to retrieve parent folder name: %v\n", err)
		}
	}

	if file.Md5Checksum != "" {
		fmt.Printf("MD5 Checksum: %s\n", file.Md5Checksum)
	}
	if file.Sha1Checksum != "" {
		fmt.Printf("SHA1 Checksum: %s\n", file.Sha1Checksum)
	}
	if file.Sha256Checksum != "" {
		fmt.Printf("SHA256 Checksum: %s\n", file.Sha256Checksum)
	}

	return nil
}

type GoogleDriveCacheGetFlags struct {
	GoogleDriveFlags
	DownloadFlags
	OutputFlags
	CacheFlags
}

func (cmd *GoogleDriveCmd) Get(ctx context.Context, flags any, args []string) error {
	fl := flags.(*GoogleDriveCacheGetFlags)
	ctx = ctxlog.WithLogger(ctx, fl.LoggingFlags.Logger())
	fl.DownloadFlags = fl.DownloadFlags.SetDefaults()

	srv, err := cmd.createService(ctx, fl.GoogleDriveFlags.CredentialsFile)
	if err != nil {
		log.Fatalf("Unable to create Drive service: %v", err)
	}

	file, err := cmd.byNameOrID(ctx, srv, fl.FileID, args[0])
	if err != nil {
		log.Fatalf("Unable to resolve file ID: %v", err)
	}

	name := path.Base(file.Name)
	output := fl.OutputFlags.Output
	if output == "" {
		output = name
	}
	bulkSpec := bulkspec.Files{
		Prefix: "gdrive://",
		Config: fl.DownloadFlags.BulkConfig(),
		Files: []bulkspec.File{
			{
				NameOrID: file.Id,
				Output:   output,
				Cache:    fl.cacheFile(output) + ".cache",
				Index:    fl.cacheFile(output) + ".index",
			},
		}}

	ctx = internal.ContextWithOpenerInfo(ctx, srv)
	s := internal.OpenerInfo(ctx)
	if s != srv {
		panic("Google Drive service in context does not match the created service")
	}
	return cacheFiles(ctx, fl.DownloadFlags, 1, bulkSpec)
}

type GoogleDriveCacheBulkFlags struct {
	GoogleDriveFlags
	DownloadFlags
	OutputFlags
	CacheFlags
	BulkFlags
}
