package trace

import (
	"sync"
	"sync/atomic"
	"time"
)

// SpanKind 描述跨度在一次调用中的角色。
type SpanKind uint8

// 支持的跨度类型。
const (
	KindInternal SpanKind = iota
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

// String 返回跨度类型名称。
func (k SpanKind) String() string { panic("not implemented") }

// SpanStatusCode 是跨度结论状态。
type SpanStatusCode uint8

// 跨度结论状态取值。
const (
	StatusUnset SpanStatusCode = iota
	StatusOK
	StatusError
)

// Event 是跨度上的带时间戳事件。
type Event struct {
	Time       time.Time
	Name       string
	Attributes map[string]string
}

// Span 表示一次调用操作。
// 字段对同包采集器可读；并发安全由互斥锁与 sync.Once 保证。
type Span struct {
	TraceID       TraceID
	SpanID        SpanID
	ParentID      SpanID
	Service       string
	Name          string
	Kind          SpanKind
	StartTime     time.Time
	EndTime       time.Time
	StatusCode    SpanStatusCode
	StatusMessage string
	Attributes    map[string]string
	Sampled       bool

	mu      sync.Mutex
	events  []Event
	endOnce sync.Once
	ended   atomic.Bool
}

// End 结束跨度。重复调用是幂等的：仅第一次生效，其余被记为重复结束。
func (s *Span) End(ts ...time.Time) { panic("not implemented") }

// Ended 返回跨度是否已结束。
func (s *Span) Ended() bool { panic("not implemented") }

// Duration 返回跨度持续时间；未结束时第二返回值为 false。
func (s *Span) Duration() (time.Duration, bool) { panic("not implemented") }

// SetStatus 设置跨度结论状态。
func (s *Span) SetStatus(code SpanStatusCode, message string) { panic("not implemented") }

// SetAttribute 写入一个属性。
func (s *Span) SetAttribute(key, value string) { panic("not implemented") }

// AddEvent 追加一个带时间戳的事件。
func (s *Span) AddEvent(name string, attrs map[string]string) { panic("not implemented") }

// Events 返回事件快照。
func (s *Span) Events() []Event { panic("not implemented") }
