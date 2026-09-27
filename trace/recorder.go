package trace

import "sync"

// Recorder 是最简单的进程内 SpanSink：聚合所有已结束跨度，
// 供测试按 TraceID 构建链路报告并做一致性断言。
type Recorder struct {
	mu      sync.Mutex
	spans   []*Span
	drops   []DroppedSpan
	closed  bool
}

// NewRecorder 创建聚合记录器。
func NewRecorder() *Recorder { return &Recorder{} }

// Accept 记录一个已结束跨度（始终接受，容量无界，仅用于测试）。
func (r *Recorder) Accept(s *Span) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.spans = append(r.spans, s)
	return true
}

// RecordDrop 记录一次带原因的丢弃。
func (r *Recorder) RecordDrop(reason DropReason, s *Span, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drops = append(r.drops, DroppedSpan{
		TraceID: s.TraceID, SpanID: s.SpanID, Reason: reason, Detail: detail,
	})
}

// Shutdown 关闭记录器。
func (r *Recorder) Shutdown() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

// Spans 返回已采集跨度快照。
func (r *Recorder) Spans() []*Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Span, len(r.spans))
	copy(out, r.spans)
	return out
}

// SpansOf 返回指定链路的已采集跨度。
func (r *Recorder) SpansOf(id TraceID) []*Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*Span
	for _, s := range r.spans {
		if s.TraceID == id {
			out = append(out, s)
		}
	}
	return out
}

// Drops 返回全部丢弃记录。
func (r *Recorder) Drops() []DroppedSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]DroppedSpan, len(r.drops))
	copy(out, r.drops)
	return out
}
