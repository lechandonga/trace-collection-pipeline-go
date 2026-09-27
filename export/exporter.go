package export

import (
	"context"
	"fmt"
	"time"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// FailureReason 可区分的导出失败原因。
type FailureReason string

// 导出失败原因取值。
const (
	// FailureTimeout 单次导出超过截止时间。
	FailureTimeout FailureReason = "timeout"
	// FailureUnavailable 导出端不可用（连接失败 / 5xx 等）。
	FailureUnavailable FailureReason = "unavailable"
	// FailureRejected 导出端明确拒绝（不可重试，如格式非法）。
	FailureRejected FailureReason = "rejected"
	// FailureQueueFull 导出侧有界队列已满。
	FailureQueueFull FailureReason = "queue_full"
	// FailureShutdown 导出器已关闭。
	FailureShutdown FailureReason = "shutdown"
)

// Failure 记录一次导出失败及其可区分原因。
type Failure struct {
	Time   time.Time
	Reason FailureReason
	Err    error
	Count  int // 涉及的跨度数量
}

// Exporter 是导出端抽象。实现必须在不可用时快速返回带原因的错误，
// 不得长时间阻塞调用方。
type Exporter interface {
	ExportSpans(ctx context.Context, spans []*trace.Span) error
	Shutdown(ctx context.Context) error
}

// FailureError 让导出错误携带结构化失败原因，便于调用方按原因区分处理。
type FailureError struct {
	Reason FailureReason
	Err    error
}

// Error 实现 error。
func (e *FailureError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("export failed (%s): %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("export failed (%s)", e.Reason)
}

// Unwrap 支持 errors.Is/As 解包。
func (e *FailureError) Unwrap() error { return e.Err }

// Retryable 判断该原因是否允许重试。
// 明确拒绝与关闭属于终态失败；超时/不可用/队列满允许重试。
func (r FailureReason) Retryable() bool {
	switch r {
	case FailureTimeout, FailureUnavailable, FailureQueueFull:
		return true
	default:
		return false
	}
}

// AsFailureError 尝试把错误解析为 *FailureError。
func AsFailureError(err error) (*FailureError, bool) {
	if fe, ok := err.(*FailureError); ok {
		return fe, true
	}
	return nil, false
}
