package trace

import (
	"context"
	"sync"
	"time"
)

// TracerConfig 是 Tracer 的配置。
type TracerConfig struct {
	Service  string
	Sampler  Sampler
	Sink     SpanSink
	Now      func() time.Time
	// MaxSpanAge 超过该时长仍未结束的跨度视为缺失结束。
	MaxSpanAge time.Duration
	// SweepInterval 扫描未结束跨度的周期；0 表示不启用后台扫描。
	SweepInterval time.Duration
}

// Tracer 创建跨度并在进程内维护进行中的跨度索引。
type Tracer struct {
	cfg TracerConfig

	mu      sync.Mutex
	active  map[SpanID]*Span
	closed  bool
}

// NewTracer 创建一个采集器。
func NewTracer(cfg TracerConfig) *Tracer { panic("not implemented") }

type ctxKey struct{}

// ContextWithSpan 把跨度放入上下文，支持跨协程传递。
func ContextWithSpan(ctx context.Context, span *Span) context.Context { panic("not implemented") }

// SpanFromContext 从上下文取出当前跨度。
func SpanFromContext(ctx context.Context) *Span { panic("not implemented") }

// SpanOption 在创建跨度时补充信息。
type SpanOption func(*Span)

// WithKind 指定跨度类型。
func WithKind(kind SpanKind) SpanOption { panic("not implemented") }

// WithAttributes 指定初始属性。
func WithAttributes(attrs map[string]string) SpanOption { panic("not implemented") }

// Start 创建并开始一个跨度。
// 若 ctx 中携带父跨度，则新跨度归属同一调用链并挂到父节点下；
// 结束顺序不影响父子归属（归属在创建时确定）。
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	panic("not implemented")
}

// SweepStale 强制扫描并回收超过 MaxSpanAge 仍未结束的跨度。
// 主要用于测试中的确定性判定；后台扫描也会使用同一逻辑。
// 返回被回收（以缺失结束状态进入下游）的跨度数。
func (t *Tracer) SweepStale(now time.Time) int { panic("not implemented") }

// ActiveCount 返回尚未结束的活跃跨度数。
func (t *Tracer) ActiveCount() int { panic("not implemented") }

// Shutdown 停止采集器：先扫描一次残留跨度，再关闭下游。
func (t *Tracer) Shutdown(ctx context.Context) error { panic("not implemented") }
