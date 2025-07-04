package internal

import (
	"context"
	"sync"
	"testing"
	"time"

	"cloudeng.io/file/largefile"
)

func TestProgressDisplay_AddBytesBarAndUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd := NewProgressDisplay()
	pd.Run(ctx)

	bar := pd.AddBytesBar("testfile", "download", 100)
	bar.Run(ctx)

	ch := bar.UpdateCh()
	ch <- 50
	ch <- 100

	// Allow some time for updates to propagate.
	time.Sleep(100 * time.Millisecond)
	pd.Wait()
}

func TestProgressDisplay_NewDownloadProgressBars(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd := NewProgressDisplay()
	pd.Run(ctx)

	statsCh := make(chan largefile.DownloadStats, 2)
	dp := pd.NewDownloadProgressBars(statsCh)
	var mu sync.Mutex
	received := []int64{}

	dp.AddDownloadMetric("testfile", "download", 100, func(st largefile.DownloadStats) int64 {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, st.DownloadedBytes)
		return st.DownloadedBytes
	})

	dp.Run(ctx)

	statsCh <- largefile.DownloadStats{DownloadedBytes: 10}
	statsCh <- largefile.DownloadStats{DownloadedBytes: 100}
	close(statsCh)

	// Allow some time for updates to propagate.
	time.Sleep(100 * time.Millisecond)
	pd.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 || received[0] != 10 || received[1] != 100 {
		t.Errorf("unexpected received stats: %v", received)
	}
}

func TestProgressDisplay_Wait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pd := NewProgressDisplay()
	pd.Run(ctx)
	go func() {
		time.Sleep(50 * time.Millisecond)
		pd.Wait()
	}()
	// Should not deadlock.
}
