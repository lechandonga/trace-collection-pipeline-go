package trace

// DropReason 说明一个跨度被丢弃的可区分原因。
type DropReason string

// 采集/背压层面的丢弃原因。
const (
	// DropReasonUnsampled 该链路在采样策略层面未被采样。
	DropReasonUnsampled DropReason = "unsampled"
	// DropReasonQueueFull 有界队列已满，触发背压丢弃。
	DropReasonQueueFull DropReason = "queue_full"
	// DropReasonSinkClosed 采集端已关闭，拒绝新跨度。
	DropReasonSinkClosed DropReason = "sink_closed"
)

// DroppedSpan 记录一次丢弃，用于可解释诊断。
type DroppedSpan struct {
	TraceID TraceID
	SpanID  SpanID
	Reason  DropReason
	Detail  string
}

// SpanSink 接收已结束的跨度。Tracer 通过它与采集管线解耦。
type SpanSink interface {
	// Accept 接收一个已结束跨度；返回 true 表示被接受。
	Accept(span *Span) bool
	// RecordDrop 显式记录一次丢弃（携带原因）。
	RecordDrop(reason DropReason, span *Span, detail string)
	// Shutdown 关闭采集端。
	Shutdown() error
}
