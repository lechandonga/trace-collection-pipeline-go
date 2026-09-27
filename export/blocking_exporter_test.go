package export

import (
	"context"
	"sync/atomic"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// gateExporter 在 open 为 nil/关闭前对每次导出自旋等待 open，
// 用于在测试中冻结后台消费。与 blockingExporter 不同，它支持“先填满、
// 后放行”的两阶段控制：初始化时 open 是一个未关闭的 channel。
type gateExporter struct {
	open  chan struct{}
	calls atomic.Int64
}

func (g *gateExporter) ExportSpans(ctx context.Context, spans []*trace.Span) error {
	g.calls.Add(1)
	select {
	case <-g.open:
		return nil
	case <-ctx.Done():
		return &FailureError{Reason: FailureTimeout, Err: ctx.Err()}
	}
}

func (g *gateExporter) Shutdown(ctx context.Context) error { return nil }
