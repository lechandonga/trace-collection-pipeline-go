package tracing

import (
	"context"
	"errors"
	"sync"
)

// CallError 用于在流量中模拟一次故障调用。
var CallError = errors.New("simulated downstream failure")

// Component 模拟微服务系统中的一个组件。组件之间不共享内存状态，
// 只通过 TextMapCarrier（模拟 RPC header / MQ 属性）传递追踪上下文。
type Component struct {
	name      string
	tracer    *Tracer
	prop      *TextMapPropagator
	mu        sync.Mutex
	processed int
}

// NewComponent 创建一个绑定到 tracer 的模拟组件。
func NewComponent(name string, tr *Tracer) *Component {
	return &Component{name: name, tracer: tr, prop: NewTextMapPropagator()}
}

// Inject 把跨度上下文写入出站载体，供下一个组件提取。
func (c *Component) Inject(sp *Span, carrier TextMapCarrier) error {
	return c.prop.Inject(sp.SpanContext(), carrier)
}

// Extract 从入站载体提取上游上下文；无上游时返回零值（由调用方决定开新链路）。
func (c *Component) Extract(carrier TextMapCarrier) (SpanContext, error) {
	return c.prop.Extract(carrier)
}

// Entry 是组件处理一次调用的入口：
// carrier 为入站载体（nil 表示这是一条新链路的入口）；
// work 在 server 跨度内执行，返回时跨度自动结束。
// 可在 work 内通过 CallRemote 触发下游组件调用。
func (c *Component) Entry(ctx context.Context, carrier TextMapCarrier, name string, work ServerWork) (err error) {
	var remote SpanContext
	if carrier != nil {
		remote, err = c.Extract(carrier)
		if err != nil {
			return err
		}
	}
	srvCtx, sp := c.tracer.StartWithRemoteParent(ctx, c.name+"/"+name, remote, WithSpanKind(SpanKindServer))
	sp.SetAttributes("component", c.name)
	defer func() {
		if err != nil {
			sp.RecordError(err)
		}
		sp.End()
		c.mu.Lock()
		c.processed++
		c.mu.Unlock()
	}()
	if work == nil {
		return nil
	}
	return work(srvCtx)
}

// ServerWork 是组件在 server 跨度内执行的业务逻辑。
type ServerWork func(ctx context.Context) error

// CallRemote 模拟组件向 downstream 发起一次 RPC：
// 在当前跨度下开 client 跨度 -> 注入载体 -> 交给下游 Entry 处理 -> 结束 client 跨度。
// downstreamWork 为下游在自己 server 跨度内执行的逻辑（可为 nil；可在其中继续向下调用）。
// 整个过程父、子跨度位于不同"组件"，可并发、可乱序结束。
func (c *Component) CallRemote(ctx context.Context, op string, downstream *Component, fail bool, downstreamWork ServerWork) error {
	cliCtx, cli := c.tracer.Start(ctx, c.name+"/call/"+op, WithSpanKind(SpanKindClient))
	carrier := NewMapCarrier()
	if err := c.Inject(cli, carrier); err != nil {
		cli.End()
		return err
	}
	work := downstreamWork
	if fail {
		work = func(ctx context.Context) error { return CallError }
	}
	err := downstream.Entry(cliCtx, carrier, op, work)
	if err != nil {
		cli.RecordError(err)
	}
	cli.End()
	return err
}

// Processed 返回该组件累计处理的请求数。
func (c *Component) Processed() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processed
}
