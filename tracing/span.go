package tracing

import (
	"context"
	"sync"
	"time"
)

// SpanKind 描述跨度在一次调用中的角色。
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	SpanKindServer
	SpanKindClient
)

func (k SpanKind) String() string {
	switch k {
	case SpanKindServer:
		return "server"
	case SpanKindClient:
		return "client"
	default:
		return "internal"
	}
}

// SpanData 是跨度结束后不可变的快照，供采样、缓冲、导出使用。
// 任何持有方都不得修改其 map/slice（构造时已做深拷贝）。
type SpanData struct {
	TraceID      TraceID
	SpanID       SpanID
	ParentSpanID SpanID // 全零表示根跨度
	Name         string
	Kind         SpanKind
	Service      string
	StartTime    time.Time
	EndTime      time.Time
	Attributes   map[string]string
	Status       Status
	Events       []Event
}

// Status 描述跨度结论。
type Status struct {
	Code    StatusCode
	Message string
}

// StatusCode 为结论状态码。
type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

func (c StatusCode) String() string {
	switch c {
	case StatusOK:
		return "ok"
	case StatusError:
		return "error"
	default:
		return "unset"
	}
}

// Event 为跨度上的带时间戳日志点。
type Event struct {
	Time       time.Time
	Name       string
	Attributes map[string]string
}

// HasParent 报告该跨度是否有父跨度。
func (s SpanData) HasParent() bool {
	var zero SpanID
	return s.ParentSpanID != zero
}

// Duration 返回跨度耗时；未结束（EndTime 为零值）时返回 0。
func (s SpanData) Duration() time.Duration {
	if s.EndTime.IsZero() || s.StartTime.IsZero() {
		return 0
	}
	return s.EndTime.Sub(s.StartTime)
}

// Span 是进行中的跨度句柄，由 Tracer 创建，可被多个 goroutine 并发使用。
// 不变量：
//   - End() 幂等：重复调用只有第一次生效，跨度只会被归属与入队一次；
//   - 结束后所有修改方法静默失效，不会产生悬挂的"半结束"状态；
//   - 父子关系在 Start 时仅凭 ID 确定，因此父子以任意顺序结束都不影响链路结构。
type Span struct {
	mu      sync.Mutex
	tracer  *Tracer
	data    SpanData
	sampled bool
	ended   bool
}

// End 结束跨度。重复调用安全：仅第一次会生成快照并提交处理器。
func (s *Span) End(opts ...SpanEndOption) {
	cfg := endConfig{endTime: time.Now()}
	for _, o := range opts {
		o.applyEnd(&cfg)
	}

	s.mu.Lock()
	if s.ended {
		// 在 span 自己的锁内识别重复结束，保证并发 End 也能被准确计数。
		s.tracer.noteDoubleEnd()
		s.mu.Unlock()
		return
	}
	s.ended = true
	if !cfg.endTime.IsZero() {
		s.data.EndTime = cfg.endTime
	}
	if s.data.EndTime.IsZero() {
		s.data.EndTime = time.Now()
	}
	if s.data.EndTime.Before(s.data.StartTime) {
		// 乱序/测试时钟导致结束早于开始时，归并为零时长，保证结论自洽。
		s.data.EndTime = s.data.StartTime
	}
	sampled := s.sampled
	snap := s.lockedSnapshot()
	s.mu.Unlock()

	s.tracer.onSpanEnd(s, snap, sampled)
}

// SetAttributes 设置属性，参数为交替的 key, value；奇数个参数时末位值为空串。
func (s *Span) SetAttributes(kv ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || !s.sampled {
		return
	}
	for i := 0; i < len(kv); i += 2 {
		v := ""
		if i+1 < len(kv) {
			v = kv[i+1]
		}
		s.data.Attributes[kv[i]] = v
	}
}

// AddEvent 追加带时间戳事件。
func (s *Span) AddEvent(name string, kv ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || !s.sampled {
		return
	}
	ev := Event{Time: time.Now(), Name: name, Attributes: make(map[string]string)}
	for i := 0; i < len(kv); i += 2 {
		v := ""
		if i+1 < len(kv) {
			v = kv[i+1]
		}
		ev.Attributes[kv[i]] = v
	}
	s.data.Events = append(s.data.Events, ev)
}

// SetStatus 设置结论；一旦标记为 Error 不会被后续调用降级。
func (s *Span) SetStatus(code StatusCode, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || !s.sampled {
		return
	}
	if s.data.Status.Code == StatusError && code != StatusError {
		return
	}
	s.data.Status = Status{Code: code, Message: msg}
}

// RecordError 记录异常事件并把状态置为 Error。
func (s *Span) RecordError(err error) {
	if err == nil {
		return
	}
	s.AddEvent("exception", "exception.message", err.Error())
	s.SetStatus(StatusError, err.Error())
}

// SpanContext 返回可跨 goroutine / 跨组件安全传播的不可变上下文。
func (s *Span) SpanContext() SpanContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return SpanContext{TraceID: s.data.TraceID, SpanID: s.data.SpanID, Sampled: s.sampled}
}

// IsRecording 报告跨度是否仍在采集（存在、未结束且已采样）。
func (s *Span) IsRecording() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.ended && s.sampled
}

// lockedSnapshot 调用方须持锁。深拷贝 map/slice。
func (s *Span) lockedSnapshot() SpanData {
	d := s.data
	d.Attributes = make(map[string]string, len(s.data.Attributes))
	for k, v := range s.data.Attributes {
		d.Attributes[k] = v
	}
	if len(s.data.Events) > 0 {
		d.Events = make([]Event, len(s.data.Events))
		for i, ev := range s.data.Events {
			attrs := make(map[string]string, len(ev.Attributes))
			for k, v := range ev.Attributes {
				attrs[k] = v
			}
			d.Events[i] = Event{Time: ev.Time, Name: ev.Name, Attributes: attrs}
		}
	}
	return d
}

// SpanEndOption 控制结束行为。
type SpanEndOption interface{ applyEnd(*endConfig) }

type endConfig struct {
	endTime time.Time
}

type endTimeOption time.Time

func (o endTimeOption) applyEnd(c *endConfig) { c.endTime = time.Time(o) }

// WithEndTime 指定结束时间（主要用于测试乱序场景）。
func WithEndTime(t time.Time) SpanEndOption { return endTimeOption(t) }

// spanContextKey 用于在 context.Context 中存取当前跨度。
type spanContextKey struct{}

// ContextWithSpan 把跨度存入 context。
func ContextWithSpan(ctx context.Context, sp *Span) context.Context {
	return context.WithValue(ctx, spanContextKey{}, sp)
}

// SpanFromContext 从 context 取出当前跨度，不存在返回 nil。
func SpanFromContext(ctx context.Context) *Span {
	if sp, ok := ctx.Value(spanContextKey{}).(*Span); ok {
		return sp
	}
	return nil
}
