package tracing

import (
	"context"
	"time"
)

// PipelineConfig 描述整条采集管线的配置。
type PipelineConfig struct {
	Service   string          // 服务名，写入每个跨度的 service 属性
	Sampler   Sampler         // 缺省 AlwaysSample
	Processor ProcessorConfig // 队列/批量/背压/重试参数
}

// Pipeline 组合 Tracer、处理器与导出器，提供统一生命周期。
// 典型用法：
//
//	pl := tracing.NewPipeline(cfg, exp)
//	pl.Start(ctx)
//	defer pl.Shutdown(context.Background())
//	ctx, sp := pl.Tracer().Start(ctx, "op")
//	defer sp.End()
type Pipeline struct {
	tracer    *Tracer
	processor *BatchSpanProcessor
	exporter  Exporter
}

// NewPipeline 按配置组装管线（不启动后台循环，需调用 Start）。
func NewPipeline(cfg PipelineConfig, exp Exporter) *Pipeline {
	if cfg.Sampler == nil {
		cfg.Sampler = &AlwaysSample{}
	}
	proc := NewBatchSpanProcessor(exp, cfg.Processor)
	tr := NewTracer(cfg.Service, cfg.Sampler, proc)
	return &Pipeline{tracer: tr, processor: proc, exporter: exp}
}

func (p *Pipeline) Tracer() *Tracer                { return p.tracer }
func (p *Pipeline) Processor() *BatchSpanProcessor { return p.processor }
func (p *Pipeline) Exporter() Exporter             { return p.exporter }

// Start 启动后台导出循环。
func (p *Pipeline) Start(ctx context.Context) error {
	return p.processor.Start(ctx)
}

// Shutdown 确定性关闭顺序：
//  1. Tracer 停止接收新跨度并把缺失 End 的跨度标记为 abandoned 回收（不悬挂）；
//  2. 处理器停止接取、中断重试退避、把队列全部排空导出（受 ctx 限制）；
//  3. 导出器关闭。
func (p *Pipeline) Shutdown(ctx context.Context) error {
	if err := p.tracer.Shutdown(ctx); err != nil {
		return err
	}
	return p.processor.Shutdown(ctx)
}

// ShutdownWithTimeout 是 Shutdown 的便捷封装。
func (p *Pipeline) ShutdownWithTimeout(d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return p.Shutdown(ctx)
}
