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

var kindNames = [...]string{
	"internal", "server", "client", "producer", "consumer",
}

// String 返回跨度类型名称。
func (k SpanKind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "unknown"
}

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
// 并发安全：事件/属性/状态由 mu 保护；结束由 sync.Once 保证幂等。
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

	mu            sync.Mutex
	events        []Event
	endOnce       sync.Once
	ended         atomic.Bool
	endKind       SpanEndKind
	duplicateEnds int
	// onEnd 在首次成功结束时调用一次（由 Tracer 注入，用于采集交接）。
	onEnd func(*Span)
}

// End 结束跨度。重复调用是幂等的：仅第一次生效，其余被记为重复结束，
// 因此无论调用方结束多少次，跨度都只会被下游采集一次，不会重复归属。
func (s *Span) End(ts ...time.Time) {
	first := false
	s.endOnce.Do(func() {
		first = true
		s.mu.Lock()
		endAt := time.Now()
		if len(ts) > 0 && !ts[0].IsZero() {
			endAt = ts[0]
		}
		if endAt.Before(s.StartTime) {
			endAt = s.StartTime // 自洽性：结束时间不早于开始时间
		}
		s.EndTime = endAt
		if s.endKind == "" {
			s.endKind = EndNormal
		}
		s.mu.Unlock()
		s.ended.Store(true)
		if s.onEnd != nil {
			s.onEnd(s)
		}
	})
	if !first {
		// 首次结束之外的调用：仅计数，绝不产生第二个跨度。
		// 跨度自身的结束方式仍为 normal；重复信息通过计数独立可查。
		s.mu.Lock()
		s.duplicateEnds++
		s.mu.Unlock()
	}
}

// forceClose 由采集器在跨度缺失结束（超时未 End）时调用。
// 语义与 End 互斥：若并发 End 先发生，则本次回收不产生第二个跨度。
func (s *Span) forceClose(at time.Time, hook func(*Span)) bool {
	claimed := false
	s.endOnce.Do(func() {
		claimed = true
		s.mu.Lock()
		if at.Before(s.StartTime) {
			at = s.StartTime
		}
		s.EndTime = at
		s.endKind = EndMissing
		if s.StatusCode == StatusUnset {
			s.StatusCode = StatusError
			s.StatusMessage = "span missing explicit end, force-closed by sweeper"
		}
		s.mu.Unlock()
		s.ended.Store(true)
		if hook != nil {
			hook(s)
		}
	})
	return claimed
}

// Ended 返回跨度是否已结束。
func (s *Span) Ended() bool { return s.ended.Load() }

// EndKind 返回结束方式（normal / duplicate_end / missing_end_force_closed）。
func (s *Span) EndKind() SpanEndKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endKind
}

// DuplicateEndCount 返回首次结束之后又收到的 End 调用次数。
func (s *Span) DuplicateEndCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duplicateEnds
}

// Duration 返回跨度持续时间；未结束时第二返回值为 false。
func (s *Span) Duration() (time.Duration, bool) {
	if !s.ended.Load() {
		return 0, false
	}
	return s.EndTime.Sub(s.StartTime), true
}

// SetStatus 设置跨度结论状态。结束后不可再改，保证结论稳定。
func (s *Span) SetStatus(code SpanStatusCode, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended.Load() {
		return
	}
	s.StatusCode = code
	s.StatusMessage = message
}

// SetAttribute 写入一个属性。
func (s *Span) SetAttribute(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Attributes == nil {
		s.Attributes = map[string]string{}
	}
	s.Attributes[key] = value
}

// AddEvent 追加一个带时间戳的事件。
func (s *Span) AddEvent(name string, attrs map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev := Event{Time: time.Now(), Name: name, Attributes: cloneAttrs(attrs)}
	s.events = append(s.events, ev)
}

// Events 返回事件快照。
func (s *Span) Events() []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

func cloneAttrs(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
