// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"

	"cloudeng.io/file/diskusage"
	"cloudeng.io/file/largefile"
)

type CacheIndexFlags struct {
}

func (cmd *CacheCmd) Cached(_ context.Context, _ any, args []string) error {
	return cmd.display(args[0], true)
}

func (cmd *CacheCmd) Outstanding(_ context.Context, _ any, args []string) error {
	return cmd.display(args[0], false)
}

func (cmd *CacheCmd) Summary(_ context.Context, _ any, args []string) error {
	drs, err := cmd.load(args[0])
	if err != nil {
		return err
	}
	cached := 0
	outstanding := 0
	for br := range drs.AllSet(0) {
		cached += int(br.Size())
	}
	for br := range drs.AllClear(1) {
		outstanding += int(br.Size())
	}
	fmt.Printf("Cached: %d\n", diskusage.Binary(cached))
	fmt.Printf("Outstanding: %d\n", diskusage.Binary(outstanding))
	return nil
}

func (cmd *CacheCmd) load(idx string) (*largefile.ByteRanges, error) {
	data, err := os.ReadFile(idx)
	if err != nil {
		return nil, err
	}
	drs := &largefile.ByteRanges{}
	if err := drs.UnmarshalJSON(data); err != nil {
		return nil, err
	}
	return drs, nil
}

func (cmd *CacheCmd) display(idx string, cached bool) error {
	drs, err := cmd.load(idx)
	if err != nil {
		return fmt.Errorf("failed to load index file %q: %w", idx, err)
	}
	it := drs.AllSet(0)
	if !cached {
		it = drs.AllClear(1)
	}
	for br := range it {
		fmt.Printf("%v: %v bytes\n", br, br.Size())
	}
	return nil
}
