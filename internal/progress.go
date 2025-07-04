// Copyright 2025 cloudeng llc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package internal

import (
	"context"
	"sync"
	"time"

	"cloudeng.io/algo/container/list"
	"cloudeng.io/file/largefile"
	"cloudeng.io/logging/ctxlog"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// ProgressDisplay manages multiple progress bars.
type ProgressDisplay struct {
	mu        sync.Mutex
	progress  *mpb.Progress
	bars      map[int]*mpb.Bar
	nextID    int
	ch        chan progressReport
	doneCh    chan struct{}
	runDoneCh chan struct{}
}

// NewProgressDisplay creates a new ProgressDisplay instance.
func NewProgressDisplay() *ProgressDisplay {
	return &ProgressDisplay{
		bars:      make(map[int]*mpb.Bar),
		ch:        make(chan progressReport, 100),
		doneCh:    make(chan struct{}),
		runDoneCh: make(chan struct{}),
	}
}

// Run starts the progress display which will contain all the progress bars.
func (pd *ProgressDisplay) Run(ctx context.Context) {
	pd.progress = mpb.NewWithContext(ctx, mpb.WithRefreshRate(100*time.Millisecond))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-pd.doneCh:
				close(pd.runDoneCh)
				return
			case report, ok := <-pd.ch:
				if !ok {
					return
				}
				pd.updateBar(ctx, report)
			}
		}
	}()
}

// Wait blocks until all progress bars are completed and the display is done.
func (pd *ProgressDisplay) Wait() {
	pd.progress.Wait()
	close(pd.doneCh)
	<-pd.runDoneCh

}

func (pd *ProgressDisplay) updateBar(ctx context.Context, report progressReport) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	bar, exists := pd.bars[report.id]
	if !exists {
		ctxlog.Error(ctx, "progress bar with does not exist", "id", report.id)
		return
	}
	bar.EwmaSetCurrent(report.written, report.took)
}

type progressBar struct {
	id    int
	bar   *mpb.Bar
	barCh chan<- progressReport
}

// BytesProgressBar is a specialized progress bar that tracks the number of bytes
// written to a file or downloaded.
type BytesProgressBar struct {
	progressBar
	bytesCh chan int64
}

// Run starts the goroutine that listens for byte updates and sends them to
// the progress bar channel.
func (pd BytesProgressBar) Run(ctx context.Context) {
	go func() {
		then := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case written, ok := <-pd.bytesCh:
				if !ok {
					return
				}
				took := time.Since(then)
				then = time.Now()
				select {
				case pd.barCh <- progressReport{id: pd.id, written: written, took: took}:
				default:
				}
			}
		}
	}()
}

func (pd BytesProgressBar) UpdateCh() chan<- int64 {
	return pd.bytesCh
}

type downloadMetric struct {
	metric MetricFunc
	barId  int
}

// DownloadProgressBars tracks either downloaded bytes or the bytes written to
// a cache file.
type DownloadProgressBars struct {
	pd         *ProgressDisplay
	downloadCh <-chan largefile.DownloadStats
	mu         sync.RWMutex
	metrics    *list.Double[downloadMetric]
}

type progressReport struct {
	id      int
	written int64
	took    time.Duration
}

func (pd *ProgressDisplay) getNextID() int {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	id := pd.nextID
	pd.nextID++
	return id
}

func (pd *ProgressDisplay) newBar(name, op string, total int64) (int, *mpb.Bar) {
	id := pd.getNextID()
	bar := pd.progress.New(total,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding("-").Rbound("]"),
		mpb.BarFillerClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name(name, decor.WC{C: decor.DindentRight | decor.DextraSpace}),
			decor.OnComplete(
				decor.Name(op, decor.WCSyncSpaceR),
				op+" done!"),
			decor.OnComplete(decor.CountersKibiByte("% .2f / % .2f"), ""),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.EwmaSpeed(decor.SizeB1024(0), "% .2f", 30), ""),
		),
	)
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.bars[id] = bar
	return id, bar
}

// AddBytesBar creates a new BytesProgressBar and adds it to the ProgressDisplay.
func (pd *ProgressDisplay) AddBytesBar(name, op string, total int64) *BytesProgressBar {
	id, bar := pd.newBar(name, op, total)
	pbar := &BytesProgressBar{
		progressBar: progressBar{
			id:    id,
			bar:   bar,
			barCh: pd.ch,
		},
		bytesCh: make(chan int64, 1),
	}
	return pbar
}

// NewDownloadProgressBars creates a new DownloadProgressBars instance that listens
// for download updates on the provided channel. There are two progress bars,
// one that tracks downloaded bytes and the other that tracks cached bytes.
func (pd *ProgressDisplay) NewDownloadProgressBars(ch <-chan largefile.DownloadStats) *DownloadProgressBars {
	return &DownloadProgressBars{
		pd:         pd,
		downloadCh: ch,
		metrics:    list.NewDouble[downloadMetric](),
	}
}

type MetricFunc func(st largefile.DownloadStats) int64

func (dp *DownloadProgressBars) AddDownloadMetric(name, op string, total int64, metric MetricFunc) {
	id, _ := dp.pd.newBar(name, op, total)
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.metrics.Append(downloadMetric{
		metric: metric,
		barId:  id,
	})
}

// Run starts the goroutine that listens for download updates and sends them to
// the progress bar channel.
func (dp *DownloadProgressBars) Run(ctx context.Context) {
	go func() {
		then := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case status, ok := <-dp.downloadCh:
				if !ok {
					return
				}
				took := time.Since(then)
				then = time.Now()
				dp.mu.RLock()
				for dm := range dp.metrics.Forward() {
					written := dm.metric(status)
					select {
					case dp.pd.ch <- progressReport{id: dm.barId, written: written, took: took}:
					default:
					}
				}
				dp.mu.RUnlock()
			}
		}
	}()
}
