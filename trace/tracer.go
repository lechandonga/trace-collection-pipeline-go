package trace

import (
	"context"
	"sync"
	"time"
)

// TracerConfig 是 Tracer 的配置。
type TracerConfig struct {
	Service string
	Sampler Sampler
	Sink    SpanSink
	Now     func() time.Time
	// MaxSpanAge 超过该时长仍未结束的跨度视为缺失结束。
	MaxSpanAge time.Duration
	// SweepInterval 后台扫描未结束跨度的周期；0 表示不启用后台扫描。
	SweepInterval time.Duration
}

// Tracer 创建跨度并在进程内维护进行中的跨度索引。
type Tracer struct {
	cfg    TracerConfig
	idGen  func() TraceID
	spanID func() SpanID

	mu     sync.Mutex
	active map[SpanID]*Span
	closed bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewTracer 创建一个采集器。
func NewTracer(cfg TracerConfig) *Tracer {
	if cfg.Service == "" {
		cfg.Service = "unknown-service"
	}
	if cfg.Sampler == nil {
		cfg.Sampler = AlwaysSample()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxSpanAge <= 0 {
		cfg.MaxSpanAge = 30 * time.Second
	}
	t := &Tracer{
		cfg:    cfg,
		idGen:  NewTraceID,
		spanID: NewSpanID,
		active: make(map[SpanID]*Span),
		stopCh: make(chan struct{}),
	}
	if cfg.SweepInterval > 0 {
		t.wg.Add(1)
		go t.sweepLoop(cfg.SweepInterval)
	}
	return t
}

type ctxKey struct{}

// ContextWithSpan 把跨度放入上下文，支持跨协程传递
// （把返回的 ctx 交给新 goroutine 即可延续父子关系）。
func ContextWithSpan(ctx context.Context, span *Span) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxKey{}, span)
}

// SpanFromContext 从上下文取出当前跨度；不存在时返回 nil。
func SpanFromContext(ctx context.Context) *Span {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKey{}).(*Span)
	return s
}

// SpanOption 在创建跨度时补充信息。
type SpanOption func(*Span)

// WithKind 指定跨度类型。
func WithKind(kind SpanKind) SpanOption {
	return func(s *Span) { s.Kind = kind }
}

// WithAttributes 指定初始属性。
func WithAttributes(attrs map[string]string) SpanOption {
	return func(s *Span) {
		if len(attrs) > 0 {
			s.Attributes = cloneAttrs(attrs)
		}
	}
}

// Start 创建并开始一个跨度。
//
// 归属规则（解决乱序/跨协程问题的关键）：
//   - 父子关系在跨度“创建时”由当时上下文中的父跨度决定，之后不再变化，
//     因此父跨度晚于子跨度结束、或两者运行在不同协程，都不会造成
//     错归属、重复归属或悬挂；
//   - 采样判定对根跨度只做一次，子跨度仅继承父链标志，全链路一致。
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	now := t.cfg.Now()
	parent := SpanFromContext(ctx)

	span := &Span{
		SpanID:    t.spanID(),
		Service:   t.cfg.Service,
		Name:      name,
		Kind:      KindInternal,
		StartTime: now,
		Attributes: map[string]string{},
	}
	for _, o := range opts {
		o(span)
	}

	var parentSC SpanContext
	if parent != nil {
		parentSC = SpanContext{
			TraceID: parent.TraceID,
			SpanID:  parent.SpanID,
			Sampled: parent.Sampled,
		}
		span.TraceID = parent.TraceID
		span.ParentID = parent.SpanID
		span.Sampled = parent.Sampled
	} else {
		span.TraceID = t.idGen()
	}

	// 采样：根跨度由采样器决定；子跨度的采样器调用必然返回继承结论。
	res := t.cfg.Sampler.ShouldSample(parentSC, t.cfg.Service, name, span.Kind)
	if parent == nil {
		span.Sampled = res.Decision == DecisionRecordAndSample
	}

	// 采集交接钩子只负责把“终态跨度”交给 sink 一次。
	span.onEnd = t.deliver

	t.mu.Lock()
	if t.closed {
		// 关闭后新建的跨度不进入活跃索引，但其 End 仍可安全调用；
		// 若它属于已采样链路，仍尝试交给 sink，由 sink 决定拒收并记原因。
		t.mu.Unlock()
		if span.Sampled {
			t.deliver(span)
		} else {
			t.recordDropUnsampled(span, string(res.Reason))
		}
		return ContextWithSpan(ctx, span), span
	}
	t.active[span.SpanID] = span
	t.mu.Unlock()

	if !span.Sampled {
		// 未采样链路：不占活跃索引之外的额外资源，丢弃理由可区分。
		t.recordDropUnsampled(span, string(res.Reason))
	}
	return ContextWithSpan(ctx, span), span
}

// deliver 在跨度首次进入终态（正常 End 或 sweeper 强制回收）时调用一次。
func (t *Tracer) deliver(s *Span) {
	t.mu.Lock()
	_, isActive := t.active[s.SpanID]
	delete(t.active, s.SpanID)
	closed := t.closed
	t.mu.Unlock()
	_ = isActive

	if !s.Sampled {
		// 未采样链路在 Start 时已登记原因；这里不再二次记录。
		return
	}
	if t.cfg.Sink == nil {
		return
	}
	if !t.cfg.Sink.Accept(s) {
		// 背压（队列满）等拒绝原因由 Sink 自身记录；此处无需补充。
		return
	}
	if closed {
		// 已关闭 sink 仍接受的情况由 sink 语义负责，这里不做假设。
	}
}

func (t *Tracer) recordDropUnsampled(s *Span, reason string) {
	if t.cfg.Sink != nil {
		t.cfg.Sink.RecordDrop(DropReasonUnsampled, s, reason)
	}
}

// SweepStale 扫描并回收超过 MaxSpanAge 仍未结束的跨度。
// 回收以“缺失结束”终态进入下游，不会留下悬挂未结束状态。
// 主要用于测试中的确定性判定；后台扫描复用同一逻辑。
// 返回本次被回收的跨度数。
func (t *Tracer) SweepStale(now time.Time) int {
	t.mu.Lock()
	stale := make([]*Span, 0)
	for _, s := range t.active {
		if !s.Ended() && now.Sub(s.StartTime) >= t.cfg.MaxSpanAge {
			stale = append(stale, s)
		}
	}
	t.mu.Unlock()

	for _, s := range stale {
		// forceClose 与并发 End 竞争时只有一方生效（sync.Once），
		// 保证跨度恰好进入下游一次。
		if s.forceClose(now, t.deliver) {
			// hook 已在 forceClose 内通过 deliver 交接。
		}
	}
	return len(stale)
}

func (t *Tracer) sweepLoop(interval time.Duration) {
	defer t.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stopCh:
			return
		case at := <-ticker.C:
			t.SweepStale(at)
		}
	}
}

// ActiveCount 返回尚未结束的活跃跨度数。
func (t *Tracer) ActiveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active)
}

// Shutdown 停止采集器：停止后台扫描，回收全部残留跨度（缺失结束语义），
// 再关闭下游 sink。重复调用返回 nil。
func (t *Tracer) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.stopCh)
	pending := make([]*Span, 0, len(t.active))
	for _, s := range t.active {
		pending = append(pending, s)
	}
	t.active = make(map[SpanID]*Span)
	t.mu.Unlock()

	t.wg.Wait()

	// 残留跨度全部按“缺失结束”强制收口，保证无悬挂。
	for _, s := range pending {
		s.forceClose(t.cfg.Now(), t.deliver)
	}

	if t.cfg.Sink != nil {
		return t.cfg.Sink.Shutdown()
	}
	return nil
}
