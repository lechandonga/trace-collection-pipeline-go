package trace

import "time"

// SpanReport 是报告中单个跨度的自洽视图。
type SpanReport struct {
	SpanID        SpanID
	ParentID      SpanID
	Service       string
	Name          string
	Kind          SpanKind
	StartTime     time.Time
	EndTime       time.Time
	Duration      time.Duration
	StatusCode    SpanStatusCode
	StatusMessage string
	Attributes    map[string]string
	Events        []Event
	// EndKind 说明跨度是如何结束的，保证结论可解释。
	EndKind SpanEndKind
	// Children 为子跨度 ID（按开始时间排序）。
	Children []SpanID
}

// SpanEndKind 描述跨度的结束方式。
type SpanEndKind string

// 跨度结束方式取值。
const (
	EndNormal      SpanEndKind = "normal"
	EndDuplicate   SpanEndKind = "duplicate_end"
	EndMissing     SpanEndKind = "missing_end_force_closed"
)

// TraceReport 是一条链路的完整、自洽结论。
type TraceReport struct {
	TraceID TraceID
	Sampled bool
	Roots   []SpanID
	Spans   map[SpanID]*SpanReport
	// OrphanIDs 是父跨度缺失（未采集到）的跨度 ID。
	OrphanIDs []SpanID
	// CycleDetected 标记是否检测到父子环（正常不应出现）。
	CycleDetected bool
}

// BuildReport 基于已结束跨度集合构建链路报告。
// 入参为同一条 TraceID 下采集到的全部跨度。
func BuildReport(traceID TraceID, sampled bool, spans []*Span) (*TraceReport, error) { panic("not implemented") }

// String 返回面向日志的可读摘要，包含判定依据。
func (r *TraceReport) String() string { panic("not implemented") }
