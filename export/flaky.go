package export

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// FlakyExporter 是可编排故障的进程内导出器，用于模拟导出端不可用、
// 超时、明确拒绝以及之后恢复，并记录调用历史便于断言。
type FlakyExporter struct {
	mu sync.Mutex
	// policy 决定第 n 次（从 1 开始）调用的结果；返回 (reason, ok)。
	// ok=true 表示成功。
	policy func(call int64) (FailureReason, bool)
	// delay 为每次调用的固定延迟，配合短超时可制造 timeout。
	delay time.Duration

	calls     atomic.Int64
	historyMu sync.Mutex
	history   []FlakyCall
	// 可选地在成功时转发给底层导出器，便于恢复后校验内容不丢。
	inner Exporter

	shutdown atomic.Bool
}

// FlakyCall 记录一次导出调用的结论。
type FlakyCall struct {
	Seq    int64
	Count  int
	OK     bool
	Reason FailureReason
	At     time.Time
}

// FlakyConfig 配置故障导出器。
type FlakyConfig struct {
	Policy func(call int64) (FailureReason, bool)
	Delay  time.Duration
	Inner  Exporter
}

// NewFlakyExporter 创建可编排故障的导出器。
func NewFlakyExporter(cfg FlakyConfig) *FlakyExporter {
	f := &FlakyExporter{policy: cfg.Policy, delay: cfg.Delay, inner: cfg.Inner}
	if f.policy == nil {
		f.policy = func(int64) (FailureReason, bool) { return "", true }
	}
	return f
}

// ExportSpans 按策略返回可区分原因的失败或成功。
func (f *FlakyExporter) ExportSpans(ctx context.Context, spans []*trace.Span) error {
	seq := f.calls.Add(1)
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			f.record(seq, len(spans), false, FailureTimeout)
			return &FailureError{Reason: FailureTimeout, Err: ctx.Err()}
		}
	}
	if f.shutdown.Load() {
		f.record(seq, len(spans), false, FailureShutdown)
		return &FailureError{Reason: FailureShutdown, Err: errExporterShutdown}
	}
	if err := ctx.Err(); err != nil {
		f.record(seq, len(spans), false, FailureTimeout)
		return &FailureError{Reason: FailureTimeout, Err: err}
	}
	reason, ok := f.policy(seq)
	if !ok {
		f.record(seq, len(spans), false, reason)
		return &FailureError{Reason: reason, Err: errors.New("flaky exporter injected failure: " + string(reason))}
	}
	f.record(seq, len(spans), true, "")
	if f.inner != nil {
		return f.inner.ExportSpans(ctx, spans)
	}
	return nil
}

// Shutdown 标记关闭并透传底层导出器。
func (f *FlakyExporter) Shutdown(ctx context.Context) error {
	f.shutdown.Store(true)
	if f.inner != nil {
		return f.inner.Shutdown(ctx)
	}
	return nil
}

// Calls 返回调用次数。
func (f *FlakyExporter) Calls() int64 { return f.calls.Load() }

// History 返回调用历史快照。
func (f *FlakyExporter) History() []FlakyCall {
	f.historyMu.Lock()
	defer f.historyMu.Unlock()
	out := make([]FlakyCall, len(f.history))
	copy(out, f.history)
	return out
}

func (f *FlakyExporter) record(seq int64, count int, ok bool, reason FailureReason) {
	f.historyMu.Lock()
	defer f.historyMu.Unlock()
	f.history = append(f.history, FlakyCall{Seq: seq, Count: count, OK: ok, Reason: reason, At: time.Now()})
}
