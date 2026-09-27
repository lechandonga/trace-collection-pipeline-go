package tracing

import (
	"context"
	"errors"
	"sync/atomic"
)

// gatedExporter 在 gate 打开前表现为"暂时不可用"（可重试失败），不阻塞调用，
// 用于在测试中真实打满有界队列，同时不干扰关闭流程。
type gatedExporter struct {
	opened atomic.Bool
	inner  *ControllableExporter
}

func newGatedExporter() *gatedExporter {
	return &gatedExporter{inner: NewControllableExporter()}
}

func (g *gatedExporter) ExportSpans(ctx context.Context, batch []SpanData) error {
	if !g.opened.Load() {
		return &ExportError{
			Reason:    ExportFailureUnavailable,
			Retryable: true,
			Err:       errors.New("gate closed: backend unavailable in test"),
		}
	}
	return g.inner.ExportSpans(ctx, batch)
}

func (g *gatedExporter) Shutdown(ctx context.Context) error { return g.inner.Shutdown(ctx) }
func (g *gatedExporter) open() {
	g.opened.Store(true)
}
func (g *gatedExporter) Exported() []SpanData { return g.inner.Exported() }
