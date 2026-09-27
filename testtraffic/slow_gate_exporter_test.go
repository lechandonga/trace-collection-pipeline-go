package testtraffic

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/trace-collection-pipeline-go/export"
	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// slowGateExporter 模拟“短暂不可用后恢复”的导出端：
// 在 open 关闭前阻塞导出（最多 freeze），到点由后台自动放开，
// 从而既制造稳定的背压窗口，又保证在途等待者最终被排空，不死锁。
type slowGateExporter struct {
	open      chan struct{}
	active    atomic.Int64
	peak      atomic.Int64
	mu        sync.Mutex
	collected []*trace.Span
}

// newSlowGateExporter 创建闸门并安排 freeze 后自动放开。
func newSlowGateExporter(freeze time.Duration) *slowGateExporter {
	g := &slowGateExporter{open: make(chan struct{})}
	go func() {
		time.Sleep(freeze)
		close(g.open)
	}()
	return g
}

func (g *slowGateExporter) ExportSpans(ctx context.Context, spans []*trace.Span) error {
	cur := g.active.Add(1)
	defer g.active.Add(-1)
	for {
		p := g.peak.Load()
		if cur <= p || g.peak.CompareAndSwap(p, cur) {
			break
		}
	}
	select {
	case <-g.open:
	case <-ctx.Done():
		return &export.FailureError{Reason: export.FailureTimeout, Err: ctx.Err()}
	}
	g.mu.Lock()
	g.collected = append(g.collected, spans...)
	g.mu.Unlock()
	return nil
}

func (g *slowGateExporter) Shutdown(ctx context.Context) error { return nil }

func (g *slowGateExporter) snapshot() []*trace.Span {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*trace.Span, len(g.collected))
	copy(out, g.collected)
	return out
}

func (g *slowGateExporter) peakInFlight() int64 { return g.peak.Load() }
