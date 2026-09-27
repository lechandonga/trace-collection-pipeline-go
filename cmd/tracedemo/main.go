// 命令 tracedemo 在单进程内生成测试流量，演示：
//  1. 多组件通过文本载体传播追踪上下文（A -> B -> C）；
//  2. 并发、父子乱序结束、重复结束、缺失结束下链路结构自洽；
//  3. 采样与有界背压（整条 trace 降载，原因可区分）；
//  4. 导出端故障不阻塞调用、原因可区分、恢复后确定续传。
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/trace-collection-pipeline-go/tracing"
)

func main() {
	ctx := context.Background()

	// 场景 1：跨组件传播 + 链路结构校验
	demoCrossComponent(ctx)

	// 场景 2：并发乱序 / 重复结束 / 缺失结束
	demoSpanLifecycle(ctx)

	// 场景 3：采样与背压
	demoBackpressure(ctx)

	// 场景 4：导出故障与恢复
	demoExportFailure(ctx)
}

func traceSizes(data []tracing.SpanData) map[string]int {
	m := map[string]int{}
	for _, d := range data {
		m[fmt.Sprintf("%x", d.TraceID[:8])]++
	}
	return m
}

func demoCrossComponent(ctx context.Context) {
	fmt.Println("================ 场景1: 跨组件传播 A->B->C ================")
	exp := tracing.NewControllableExporter()
	pl := tracing.NewPipeline(tracing.PipelineConfig{
		Service:   "mesh",
		Sampler:   &tracing.AlwaysSample{},
		Processor: tracing.DefaultProcessorConfig(),
	}, exp)
	pl.Start(ctx)
	tr := pl.Tracer()

	a := tracing.NewComponent("gateway", tr)
	b := tracing.NewComponent("orders", tr)
	c := tracing.NewComponent("payments", tr)

	err := a.Entry(ctx, nil, "POST /checkout", func(ctx context.Context) error {
		return a.CallRemote(ctx, "CreateOrder", b, false, func(ctx context.Context) error {
			return b.CallRemote(ctx, "Charge", c, false, nil)
		})
	})
	fmt.Printf("输入: 一次 checkout 请求穿过 3 个组件; 返回错误=%v\n", err)
	pl.ShutdownWithTimeout(2 * time.Second)

	data := exp.Exported()
	for tid, n := range traceSizes(data) {
		fmt.Printf("结论: trace=%s 完整导出 %d 个跨度\n", tid, n)
	}
	for _, d := range data {
		parent := "-"
		if d.HasParent() {
			parent = fmt.Sprintf("%x", d.ParentSpanID[6:])
		}
		fmt.Printf("  span=%x parent=%s service=%s name=%q status=%s\n",
			d.SpanID[6:], parent, d.Service, d.Name, d.Status.Code)
	}
}

func demoSpanLifecycle(ctx context.Context) {
	fmt.Println("================ 场景2: 并发乱序/重复/缺失结束 ================")
	exp := tracing.NewControllableExporter()
	pl := tracing.NewPipeline(tracing.PipelineConfig{
		Service:   "svc",
		Processor: tracing.DefaultProcessorConfig(),
	}, exp)
	pl.Start(ctx)
	tr := pl.Tracer()

	rootCtx, root := tr.Start(ctx, "root")
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, child := tr.Start(rootCtx, fmt.Sprintf("child-%d", i))
			child.End() // 子先于父结束
		}(i)
	}
	wg.Wait()

	// 重复结束
	_, dup := tr.Start(ctx, "will-double-end")
	dup.End()
	dup.End()

	// 缺失结束
	_, leak := tr.Start(rootCtx, "will-not-end")

	root.End()
	fmt.Printf("输入: 1父+5子(子先结束)、1跨度End两次、1跨度从不End; 关闭前活动跨度=%d\n",
		tr.ActiveCount())

	pl.ShutdownWithTimeout(2 * time.Second)
	st := tr.Stats()
	data := exp.Exported()
	fmt.Printf("结论: 导出跨度=%d; tracer统计 Started=%d Ended=%d DoubleEnded=%d Abandoned=%d\n",
		len(data), st.Started, st.Ended, st.DoubleEnded, st.Abandoned)
	for _, d := range data {
		if d.Name == "will-not-end" {
			fmt.Printf("  缺失结束跨度被回收: name=%q status=%s 事件数=%d\n",
				d.Name, d.Status.Code, len(d.Events))
		}
	}
	_ = leak
}

func demoBackpressure(ctx context.Context) {
	fmt.Println("================ 场景3: 突发流量 + 有界背压 ================")
	exp := tracing.NewControllableExporter()
	cfg := tracing.DefaultProcessorConfig()
	cfg.MaxQueueSize = 8
	cfg.MaxExportBatchSize = 4
	cfg.BatchTimeout = 50 * time.Millisecond
	pl := tracing.NewPipeline(tracing.PipelineConfig{
		Service:   "burst",
		Sampler:   tracing.NewRatioSampler(0.5), // 50% 入口采样
		Processor: cfg,
	}, exp)
	pl.Start(ctx)
	tr := pl.Tracer()

	const n = 2000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx2, root := tr.Start(ctx, "request")
			_, child := tr.Start(ctx2, "db")
			child.End()
			root.End()
		}()
	}
	wg.Wait()
	pl.ShutdownWithTimeout(3 * time.Second)

	c := pl.Processor().Stats()
	data := exp.Exported()
	sizes := traceSizes(data)
	fmt.Printf("输入: %d 条双跨度trace, 采样率0.5, 队列容量=%d\n", n, cfg.MaxQueueSize)
	fmt.Printf("采样器拒绝链路数=%d\n", tr.Stats().SamplerDropped)
	fmt.Printf("背压结论: 导出跨度=%d (完整trace=%d), queueFull=%d traceShed=%d\n",
		len(data), len(sizes), c.QueueFull, c.TraceShed)
	partial := 0
	for _, sz := range sizes {
		if sz != 2 {
			partial++
		}
	}
	fmt.Printf("结论: 残缺链路=%d (必须为0); 诊断原因示例:\n", partial)
	for i, d := range pl.Processor().Diagnostics() {
		if i >= 3 {
			break
		}
		fmt.Printf("  reason=%s trace=%x span=%x detail=%q\n", d.Reason, d.TraceID[:4], d.SpanID[6:], d.Detail)
	}
}

func demoExportFailure(ctx context.Context) {
	fmt.Println("================ 场景4: 导出故障不阻塞 + 恢复续传 ================")
	exp := tracing.NewControllableExporter()
	cfg := tracing.DefaultProcessorConfig()
	cfg.MaxQueueSize = 512
	cfg.BatchTimeout = 20 * time.Millisecond
	cfg.RetryInitial = 5 * time.Millisecond
	pl := tracing.NewPipeline(tracing.PipelineConfig{Service: "svc", Processor: cfg}, exp)
	pl.Start(ctx)
	tr := pl.Tracer()

	exp.SetAvailable(false)
	start := time.Now()
	for i := 0; i < 50; i++ {
		_, sp := tr.Start(ctx, fmt.Sprintf("op-%d", i))
		sp.End()
	}
	fmt.Printf("输入: 后端不可用时结束 50 个跨度, 耗时=%v (不应被阻塞)\n", time.Since(start).Round(time.Millisecond))
	time.Sleep(30 * time.Millisecond)
	rsn, _ := exp.LastFailure()
	fmt.Printf("故障期: 状态=%s 连续失败=%d 最近原因=%v 重试批次=%d\n",
		exp.State(), exp.ConsecutiveFailures(), rsn, pl.Processor().Stats().ExportRetries)

	exp.SetAvailable(true)
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("恢复后: 导出跨度=%d 状态=%s\n", exp.ExportedCount(), exp.State())
	pl.ShutdownWithTimeout(2 * time.Second)
	fmt.Printf("关闭后最终导出=%d (ExportLost=%d)\n",
		exp.ExportedCount(), pl.Processor().Stats().ExportLost)
}
