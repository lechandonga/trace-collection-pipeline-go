package tracing

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ExportFailureReason 以可区分的原因说明导出失败类别。
type ExportFailureReason string

const (
	ExportFailureNone        ExportFailureReason = ""
	ExportFailureUnavailable ExportFailureReason = "unavailable" // 暂时不可用（可重试，如网络中断）
	ExportFailureRejected    ExportFailureReason = "rejected"    // 永久拒绝（不可重试，如数据非法）
	ExportFailureTooLarge    ExportFailureReason = "too_large"   // 批次过大且无法拆分到合规
	ExportFailureStopped     ExportFailureReason = "stopped"     // 导出器已关闭
)

// ExportError 携带可区分原因与可重试语义。
// 语义约定：
//   - Retryable=true（如 Unavailable）：处理器必须保留数据稍后重试，不得丢失；
//   - Retryable=false（如 Rejected）：处理器丢弃该批并计入诊断，不得无限重试。
type ExportError struct {
	Reason    ExportFailureReason
	Retryable bool
	Err       error
}

func (e *ExportError) Error() string {
	if e.Err == nil {
		return string(e.Reason)
	}
	return string(e.Reason) + ": " + e.Err.Error()
}

func (e *ExportError) Unwrap() error { return e.Err }

// AsExportError 从错误链提取 *ExportError。
func AsExportError(err error) (*ExportError, bool) {
	var ee *ExportError
	if errors.As(err, &ee) {
		return ee, true
	}
	return nil, false
}

// ExportState 是导出器可查询的状态。
type ExportState int

const (
	ExportStateUnknown ExportState = iota
	ExportStateHealthy
	ExportStateDegraded // 连续失败、正在重试
	ExportStateStopped
)

func (s ExportState) String() string {
	switch s {
	case ExportStateHealthy:
		return "healthy"
	case ExportStateDegraded:
		return "degraded"
	case ExportStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Exporter 把一批不可变跨度发送到后端（本实现均为进程内模拟）。
// 返回 nil 表示成功；返回 *ExportError 表达失败类别；其他错误一律按可重试处理。
type Exporter interface {
	ExportSpans(ctx context.Context, batch []SpanData) error
	Shutdown(ctx context.Context) error
}

// StateReader 由可查询状态的导出器实现。
type StateReader interface {
	State() ExportState
	ConsecutiveFailures() int64
	ExportedCount() int64
	LastFailure() (ExportFailureReason, bool)
}

// ControllableExporter 是测试用故障注入后端：
// 可切换可用/不可用、永久拒绝、批次过大等故障，同时记录成功导出的数据。
type ControllableExporter struct {
	available atomic.Bool
	rejectAll atomic.Bool
	rejectRsn atomic.Value // ExportFailureReason
	maxBatch  atomic.Int64 // >0 时超过即返回 TooLarge（不可重试）
	failCount atomic.Int64
	exportedN atomic.Int64
	stopped   atomic.Bool

	mu       sync.Mutex
	exported []SpanData
	lastRsn  ExportFailureReason
	hasLast  bool
}

// NewControllableExporter 创建默认可用的测试导出器。
func NewControllableExporter() *ControllableExporter {
	e := &ControllableExporter{}
	e.available.Store(true)
	return e
}

func (e *ControllableExporter) ExportSpans(ctx context.Context, batch []SpanData) error {
	if e.stopped.Load() {
		e.recordFailure(ExportFailureStopped)
		return &ExportError{Reason: ExportFailureStopped, Retryable: false, Err: errors.New("exporter shut down")}
	}
	if e.rejectAll.Load() {
		rsn, _ := e.rejectRsn.Load().(ExportFailureReason)
		if rsn == "" {
			rsn = ExportFailureRejected
		}
		e.recordFailure(rsn)
		return &ExportError{Reason: rsn, Retryable: false, Err: errors.New("batch permanently rejected")}
	}
	if mb := e.maxBatch.Load(); mb > 0 && int64(len(batch)) > mb {
		e.recordFailure(ExportFailureTooLarge)
		return &ExportError{Reason: ExportFailureTooLarge, Retryable: false, Err: errors.New("batch exceeds exporter limit")}
	}
	if !e.available.Load() {
		e.recordFailure(ExportFailureUnavailable)
		return &ExportError{Reason: ExportFailureUnavailable, Retryable: true, Err: errors.New("backend unavailable")}
	}

	e.mu.Lock()
	e.exported = append(e.exported, batch...)
	e.mu.Unlock()
	e.exportedN.Add(int64(len(batch)))
	e.failCount.Store(0)
	return nil
}

func (e *ControllableExporter) recordFailure(r ExportFailureReason) {
	e.failCount.Add(1)
	e.mu.Lock()
	e.lastRsn = r
	e.hasLast = true
	e.mu.Unlock()
}

func (e *ControllableExporter) Shutdown(ctx context.Context) error {
	e.stopped.Store(true)
	return nil
}

// SetAvailable 切换后端可用性（模拟网络故障/恢复）。
func (e *ControllableExporter) SetAvailable(ok bool) { e.available.Store(ok) }

// SetRejectAll 让导出器永久拒绝所有批次（模拟数据非法）。
func (e *ControllableExporter) SetRejectAll(reason ExportFailureReason) {
	e.rejectRsn.Store(reason)
	e.rejectAll.Store(true)
}

// SetMaxBatch 设置后端可接受的最大批量，超限返回 TooLarge。
func (e *ControllableExporter) SetMaxBatch(n int) { e.maxBatch.Store(int64(n)) }

func (e *ControllableExporter) State() ExportState {
	if e.stopped.Load() {
		return ExportStateStopped
	}
	if e.failCount.Load() > 0 {
		return ExportStateDegraded
	}
	return ExportStateHealthy
}

func (e *ControllableExporter) ConsecutiveFailures() int64 { return e.failCount.Load() }
func (e *ControllableExporter) ExportedCount() int64       { return e.exportedN.Load() }

func (e *ControllableExporter) LastFailure() (ExportFailureReason, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastRsn, e.hasLast
}

// Exported 返回成功导出的跨度快照副本（供测试断言）。
func (e *ControllableExporter) Exported() []SpanData {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SpanData, len(e.exported))
	copy(out, e.exported)
	return out
}
