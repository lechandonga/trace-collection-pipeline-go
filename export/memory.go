package export

import (
	"context"
	"sync"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// MemoryExporter 是进程内内存导出器：按导出成功批次保留跨度快照，
// 供测试对“最终导出内容”做一致性断言。
type MemoryExporter struct {
	mu       sync.Mutex
	batches  [][]*trace.Span
	shutdown bool
}

// NewMemoryExporter 创建内存导出器。
func NewMemoryExporter() *MemoryExporter { return &MemoryExporter{} }

// ExportSpans 保存一批跨度的浅拷贝快照。
func (m *MemoryExporter) ExportSpans(ctx context.Context, spans []*trace.Span) error {
	if err := ctx.Err(); err != nil {
		return &FailureError{Reason: FailureTimeout, Err: err}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shutdown {
		return &FailureError{Reason: FailureShutdown, Err: errExporterShutdown}
	}
	cp := make([]*trace.Span, len(spans))
	copy(cp, spans)
	m.batches = append(m.batches, cp)
	return nil
}

// Shutdown 标记关闭。
func (m *MemoryExporter) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shutdown = true
	return nil
}

// Spans 返回全部已导出跨度（跨批次合并）的快照。
func (m *MemoryExporter) Spans() []*trace.Span {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, b := range m.batches {
		total += len(b)
	}
	out := make([]*trace.Span, 0, total)
	for _, b := range m.batches {
		out = append(out, b...)
	}
	return out
}

// BatchCount 返回成功导出的批次数。
func (m *MemoryExporter) BatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}
