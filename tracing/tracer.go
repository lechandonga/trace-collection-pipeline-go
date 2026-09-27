package tracing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// SpanSink 接收已结束的跨度快照。Tracer 不直接依赖具体处理器。
type SpanSink interface {
	// EnqueueFinished 提交一个已结束、已采样的跨度。
	// siblings 为该跨度所属 trace 在本进程内已启动的跨度总数（含自身），
	// 供处理器判断"整条 trace 是否已全部结束"，从而只放行完整链路。
	// 返回 accepted=false 时 reason 给出可区分原因（队列满等）。
	EnqueueFinished(data SpanData, siblings int) (accepted bool, reason DropReason)
}

// TracerStats 是 tracer 自身的诊断计数。
type TracerStats struct {
	Started        int64
	Ended          int64
	DoubleEnded    int64 // End() 被重复调用的次数（第 2 次及以后）
	Abandoned      int64 // 未显式 End、由 CloseAbandoned 回收的跨度
	SamplerDropped int64 // 采样规则拒绝的"链路"数（按根计）
}

// Tracer 负责创建跨度、维护父子关系与采样结论。
// 它只记录 ID 之间的关系，不缓存跨度内容，因此父子任意顺序结束都成立。
type Tracer struct {
	service string
	sampler Sampler
	sink    SpanSink
	idGen   IDGenerator
	now     func() time.Time

	mu         sync.Mutex
	active     map[SpanID]*Span // 进行中跨度注册表（弱引用式登记，用于回收缺失结束）
	traceSpans map[TraceID]int  // 每条 trace 在本进程已启动的跨度总数
	closed     bool
	stats      TracerStats
}

// NewTracer 创建一个 Tracer。service 作为服务名属性。
func NewTracer(service string, sampler Sampler, sink SpanSink) *Tracer {
	return &Tracer{
		service:    service,
		sampler:    sampler,
		sink:       sink,
		idGen:      RandomIDGenerator{},
		now:        time.Now,
		active:     make(map[SpanID]*Span),
		traceSpans: make(map[TraceID]int),
	}
}

// withIDGenerator 内部/测试使用，固定 ID 来源。
func (t *Tracer) withIDGenerator(g IDGenerator) *Tracer {
	t.idGen = g
	return t
}

// withClock 内部/测试使用，固定时间来源。
func (t *Tracer) withClock(f func() time.Time) *Tracer {
	t.now = f
	return t
}

// Start 在 ctx 中的当前跨度下开启子跨度，并把新跨度放入返回的 ctx。
// 若 ctx 不含跨度（或父 SpanContext 无效），则开启一条新链路的根跨度。
// 跨度创建后立即登记父子 ID；后续与父跨度何时结束完全无关，可跨 goroutine 传递。
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanStartOption) (context.Context, *Span) {
	var remote *SpanContext
	if parent := SpanFromContext(ctx); parent != nil {
		sc := parent.SpanContext()
		if sc.IsValid() {
			remote = &sc
		}
	}
	return t.startWithParent(ctx, name, remote, opts...)
}

// StartWithRemoteParent 在"从传播载体提取出的上游上下文"下开启服务端子跨度。
// 当 remote 为 nil 或无效时等同于 Start（开启新的根跨度）。
// 父跨度本身不在本进程内，因此只依据 ID 建立归属，不需要父 Span 对象存在——
// 这正是跨组件/跨协程场景下父子乱序结束依然自洽的关键。
func (t *Tracer) StartWithRemoteParent(ctx context.Context, name string, remote SpanContext, opts ...SpanStartOption) (context.Context, *Span) {
	if !remote.IsValid() {
		return t.startWithParent(ctx, name, nil, opts...)
	}
	r := remote
	return t.startWithParent(ctx, name, &r, opts...)
}

func (t *Tracer) startWithParent(ctx context.Context, name string, remote *SpanContext, opts ...SpanStartOption) (context.Context, *Span) {
	cfg := startConfig{kind: SpanKindInternal}
	for _, o := range opts {
		o.applyStart(&cfg)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		// 关闭后返回不记录的空跨度，调用方代码路径无需感知。
		sp := &Span{tracer: t, sampled: false, ended: true}
		return ContextWithSpan(ctx, sp), sp
	}

	var parentSC SpanContext
	hasParent := remote != nil && remote.IsValid()
	parentSampled := false
	if hasParent {
		parentSC = *remote
		parentSampled = parentSC.Sampled
	}

	traceID := t.idGen.NewTraceID()
	if hasParent {
		traceID = parentSC.TraceID
	}
	spanID := t.idGen.NewSpanID()

	// 头采样（head-based）：在链路入口做一次判定，结论沿传播关系"粘住"，
	// 同一条 trace 内所有跨度采样结论必然一致，杜绝局部丢弃破坏链路完整性。
	sampled := true
	var dropReason DropReason
	if hasParent {
		sampled = parentSampled
	} else {
		res := t.sampler.ShouldSample(SamplingParameters{
			TraceID:       traceID,
			Name:          name,
			HasParent:     false,
			ParentSampled: false,
		})
		sampled = res.Decision == DecisionRecordAndSample
		if !sampled {
			dropReason = res.Reason
			if dropReason == DropReasonNone {
				dropReason = DropReasonSampler
			}
			atomic.AddInt64(&t.stats.SamplerDropped, 1)
		}
	}

	data := SpanData{
		TraceID:    traceID,
		SpanID:     spanID,
		Name:       name,
		Kind:       cfg.kind,
		Service:    t.service,
		StartTime:  t.now(),
		Attributes: make(map[string]string),
	}
	if hasParent {
		data.ParentSpanID = parentSC.SpanID
	}
	sp := &Span{tracer: t, data: data, sampled: sampled}

	atomic.AddInt64(&t.stats.Started, 1)
	t.active[spanID] = sp
	t.traceSpans[traceID]++
	t.mu.Unlock()

	_ = dropReason // 采样拒绝通过 IsRecording/计数体现，不创建任何待处理数据
	return ContextWithSpan(ctx, sp), sp
}

// onSpanEnd 由 Span.End 调用。保证每条跨度至多入队一次（幂等性在 Span 内保证）。
func (t *Tracer) onSpanEnd(sp *Span, data SpanData, sampled bool) {
	t.mu.Lock()
	delete(t.active, data.SpanID)
	siblings := t.traceSpans[data.TraceID]
	atomic.AddInt64(&t.stats.Ended, 1)
	sink := t.sink
	t.mu.Unlock()

	if !sampled {
		return
	}
	if sink != nil {
		sink.EnqueueFinished(data, siblings)
	}
}

// noteDoubleEnd 由 Span.End 在 span 锁内调用，识别第 2 次及以后的结束。
func (t *Tracer) noteDoubleEnd() {
	atomic.AddInt64(&t.stats.DoubleEnded, 1)
}

// abandonSpan 在关闭阶段回收缺失结束的跨度：补一个明确的结束结论后照常入队，
// 使链路不出现"悬挂未结束"状态，且该事件可在诊断中区分。
func (t *Tracer) abandonSpan(sp *Span) {
	sp.mu.Lock()
	if sp.ended {
		sp.mu.Unlock()
		return
	}
	sp.ended = true
	end := t.now()
	sp.data.EndTime = end
	if sp.data.Events == nil {
		sp.data.Events = []Event{}
	}
	sp.data.Events = append(sp.data.Events, Event{
		Time:       end,
		Name:       "span.abandoned",
		Attributes: map[string]string{"reason": "missing_end_at_shutdown"},
	})
	if sp.data.Status.Code == StatusUnset {
		sp.data.Status = Status{Code: StatusError, Message: "span abandoned: missing end"}
	}
	sampled := sp.sampled
	snap := sp.lockedSnapshot()
	sp.mu.Unlock()

	t.mu.Lock()
	delete(t.active, snap.SpanID)
	siblings := t.traceSpans[snap.TraceID]
	atomic.AddInt64(&t.stats.Abandoned, 1)
	atomic.AddInt64(&t.stats.Ended, 1)
	sink := t.sink
	t.mu.Unlock()

	if sampled && sink != nil {
		sink.EnqueueFinished(snap, siblings)
	}
}

// CloseAbandoned 回收当前所有未结束跨度（模拟进程退出前的兜底）。
// 返回回收数量。回收后这些跨度再调用 End 也不会产生重复数据。
func (t *Tracer) CloseAbandoned() int {
	t.mu.Lock()
	pending := make([]*Span, 0, len(t.active))
	for _, sp := range t.active {
		pending = append(pending, sp)
	}
	t.mu.Unlock()

	for _, sp := range pending {
		t.abandonSpan(sp)
	}
	return len(pending)
}

// ActiveCount 返回尚未结束的跨度数量。
func (t *Tracer) ActiveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active)
}

// Stats 返回诊断计数快照。
func (t *Tracer) Stats() TracerStats {
	return TracerStats{
		Started:        atomic.LoadInt64(&t.stats.Started),
		Ended:          atomic.LoadInt64(&t.stats.Ended),
		DoubleEnded:    atomic.LoadInt64(&t.stats.DoubleEnded),
		Abandoned:      atomic.LoadInt64(&t.stats.Abandoned),
		SamplerDropped: atomic.LoadInt64(&t.stats.SamplerDropped),
	}
}

// Shutdown 停止接收新跨度并回收缺失结束的跨度。
// 之后通常应继续调用处理器 Shutdown 排空导出。
func (t *Tracer) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.CloseAbandoned()
	return nil
}

// SpanStartOption 控制跨度创建行为。
type SpanStartOption interface{ applyStart(*startConfig) }

type startConfig struct {
	kind SpanKind
}

// WithSpanKind 设置跨度类型。
func WithSpanKind(k SpanKind) SpanStartOption { return kindOption(k) }

type kindOption SpanKind

func (o kindOption) applyStart(c *startConfig) { c.kind = SpanKind(o) }
