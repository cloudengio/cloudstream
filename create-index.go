// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"os"

	"cloudeng.io/file/largefile"
)

func main() {
	const bs = 16 * 1024 // 16 KiB
	prev := int64(-bs)
	for r := range largefile.Ranges(0, 245065909770, bs) {
		if r.From > 245065909770 {
			fmt.Printf("R: %v\n", r)
		}
		if r.From != prev+bs {
			fmt.Printf("R: %v\n", r)
			if prev != 0 {
				fmt.Printf("Expected %d, got %d\n", prev+bs, r.From)
			}
			os.Exit(1)
		}
		prev = r.From
	}
	return
	br := largefile.NewByteRanges(245065909770, 1048576)
	fbr := largefile.NewByteRanges(245065909770, 1048576)
	for r := range br.AllClear(0) {
		fbr.Set(r.From)
	}

	data, err := json.Marshal(fbr)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("test.json", data, 0644); err != nil {
		panic(err)
	}

}
