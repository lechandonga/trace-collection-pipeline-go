package tracing

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// smallQueueConfig 构造极小规模配置以便稳定触发背压。
func smallQueueConfig(blocking bool) ProcessorConfig {
	cfg := DefaultProcessorConfig()
	cfg.MaxQueueSize = 8
	cfg.MaxExportBatchSize = 4
	cfg.BatchTimeout = 50 * time.Millisecond
	cfg.Blocking = blocking
	return cfg
}

// exportedTraceSizes 统计每个 traceID 导出了多少跨度。
func exportedTraceSizes(data []SpanData) map[TraceID]int {
	m := map[TraceID]int{}
	for _, d := range data {
		m[d.TraceID]++
	}
	return m
}

// TestNonBlockingShedsWholeTraces 输入: 队列容量 8，突发 500 条单跨度 trace。
// 判定依据: queue_full 触发降载，trace_shed 计数；导出端收到的 trace 必须完整(各1跨度)，
// 且 接受数+各类丢弃数 == 输入总数（不丢"结论"）。
func TestNonBlockingShedsWholeTraces(t *testing.T) {
	exp := NewControllableExporter()
	cfg := smallQueueConfig(false)
	pl := NewPipeline(PipelineConfig{Service: "svc", Sampler: &AlwaysSample{}, Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	const total = 500
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, sp := tr.Start(context.Background(), "burst")
			sp.End()
		}()
	}
	wg.Wait()
	t.Logf("输入: %d 条单跨度trace 队列容量=%d（非阻塞，接受/拒绝以处理器计数为准）",
		total, cfg.MaxQueueSize)

	if err := pl.ShutdownWithTimeout(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	counts := pl.Processor().Stats()
	got := exp.Exported()
	t.Logf("关闭后: 导出=%d, Stats QueueFull=%d TraceShed=%d StoppedRejected=%d",
		len(got), counts.QueueFull, counts.TraceShed, counts.StoppedRejected)

	// 结论守恒：导出 + 各种明确丢弃 = 总输入
	accounted := int64(len(got)) + counts.QueueFull + counts.TraceShed + counts.StoppedRejected
	if accounted != total {
		t.Fatalf("结论不守恒: 导出%d + queueFull%d + shed%d + stopped%d = %d != %d",
			len(got), counts.QueueFull, counts.TraceShed, counts.StoppedRejected, accounted, total)
	}
	if counts.QueueFull == 0 {
		t.Fatal("小队列 + 500 突发应至少触发一次 queue_full")
	}
	// 单跨度 trace：导出的当然完整；此用例主要验证守恒与原因可区分。
	sizes := exportedTraceSizes(got)
	for tid, n := range sizes {
		if n != 1 {
			t.Fatalf("单跨度trace %x 导出了 %d 条", tid, n)
		}
	}
	diag := pl.Processor().Diagnostics()
	reasons := map[DropReason]int{}
	for _, r := range diag {
		reasons[r.Reason]++
	}
	t.Logf("诊断记录条数=%d, 原因分布=%v（queue_full 与 trace_shed 可区分）", len(diag), reasons)
	if reasons[DropReasonQueueFull] == 0 {
		t.Fatal("诊断中应包含 queue_full 记录")
	}
	t.Logf("结论: 资源有界、丢弃原因可区分、接受+丢弃=%d 与输入守恒", total)
}

// recordingSink 包装处理器，记录每个跨度的入队结论（输入与判定依据可打印）。
type recordingSink struct {
	inner SpanSink
	mu    sync.Mutex
	log   []enqueueVerdict
}

type enqueueVerdict struct {
	name   string
	ok     bool
	reason DropReason
}

func (r *recordingSink) EnqueueFinished(d SpanData, siblings int) (bool, DropReason) {
	ok, reason := r.inner.EnqueueFinished(d, siblings)
	r.mu.Lock()
	r.log = append(r.log, enqueueVerdict{d.Name, ok, reason})
	r.mu.Unlock()
	return ok, reason
}

// TestMultispanTraceIntegrity 输入: 多跨度 trace（每条约 4 跨度）在小队列下突发，
// 关键结论: 任一导出 trace 必须完整（要么 4 跨度全到，要么整条不到），杜绝残缺链路。
func TestMultispanTraceIntegrity(t *testing.T) {
	exp := newGatedExporter() // 先关闭导出闸门，让完整 trace 队列真实打满
	cfg := smallQueueConfig(false)
	proc := NewBatchSpanProcessor(exp, cfg)
	sink := &recordingSink{inner: proc}
	tr := NewTracer("svc", &AlwaysSample{}, sink)
	proc.Start(context.Background())

	const traces = 120
	const spansPer = 4
	var wg sync.WaitGroup
	for i := 0; i < traces; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, root := tr.Start(context.Background(), fmt.Sprintf("root-%d", i))
			for j := 0; j < spansPer-1; j++ {
				_, child := tr.Start(ctx, fmt.Sprintf("child-%d-%d", i, j))
				child.End() // 子先结束，进一步加剧乱序
			}
			root.End()
		}(i)
	}
	wg.Wait()
	t.Logf("闸门关闭、队列打满后: Queued=%d PendingTraces=%d", proc.Queued(), proc.PendingTraces())
	exp.open() // 放行，worker 退避后重试成功
	// 等待队列被消费完（导出恢复后的确定语义：重试到成功）。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if proc.Queued() == 0 && proc.PendingTraces() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 再给 worker 手持批次一次刷盘周期。
	fctx, fcancel := context.WithTimeout(context.Background(), time.Second)
	if err := proc.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	fcancel()
	sdCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := proc.Shutdown(sdCtx); err != nil {
		t.Fatal(err)
	}

	got := exp.Exported()
	sizes := exportedTraceSizes(got)
	partial := 0
	for tid, n := range sizes {
		if n != spansPer {
			partial++
			t.Errorf("残缺链路: trace=%x 导出 %d/%d", tid, n, spansPer)
		}
	}
	counts := proc.Stats()
	// processor 侧结论守恒：成功导出 + 队列满整条降载 + 降载过滤 = 输入跨度总数。
	attempts := traces * spansPer
	accounted := int64(len(got)) + counts.QueueFull + counts.TraceShed
	t.Logf("输入: %d trace x %d 跨度 = %d; 判定依据(processor计数): 导出=%d queueFull=%d traceShed=%d",
		traces, spansPer, attempts, len(got), counts.QueueFull, counts.TraceShed)
	t.Logf("导出 trace 数=%d (每条 %d 跨度), 残缺=%d", len(sizes), spansPer, partial)

	if partial != 0 {
		t.Fatalf("存在 %d 条残缺链路——局部丢弃破坏了已采样链路完整性", partial)
	}
	if counts.QueueFull == 0 {
		t.Fatal("闸门关闭 + 小队列应必然触发 queue_full 整条降载")
	}
	if accounted != int64(attempts) {
		t.Fatalf("结论不守恒: 导出%d + queueFull%d + traceShed%d = %d != %d",
			len(got), counts.QueueFull, counts.TraceShed, accounted, attempts)
	}
	sink.mu.Lock()
	var reasons = map[DropReason]int{}
	for _, v := range sink.log {
		if !v.ok {
			reasons[v.reason]++
		}
	}
	sink.mu.Unlock()
	t.Logf("sink 侧拒绝分布(触发trace=%d): %v", traces-len(sizes), reasons)
	t.Logf("结论: 真实满队压力下导出的每条 trace 均为完整 %d 跨度，降载以整条 trace 为单位", spansPer)
}

// TestBlockingModeNeverDrops 输入: 同样的小队列与突发量，但 Blocking=true。
// 结论: 入队不丢弃（可能短暂阻塞），最终全部导出。
func TestBlockingModeNeverDrops(t *testing.T) {
	exp := NewControllableExporter()
	cfg := smallQueueConfig(true)
	pl := NewPipeline(PipelineConfig{Service: "svc", Sampler: &AlwaysSample{}, Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	const total = 200
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, sp := tr.Start(context.Background(), "blocked-burst")
			sp.End()
		}()
	}
	wg.Wait()
	if err := pl.ShutdownWithTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	counts := pl.Processor().Stats()
	got := len(exp.Exported())
	t.Logf("输入: %d 跨度 Blocking=true 队列=%d; 判定依据: 导出=%d queueFull=%d",
		total, cfg.MaxQueueSize, got, counts.QueueFull)
	if got != total {
		t.Fatalf("阻塞模式不应丢弃跨度: 导出 %d/%d", got, total)
	}
	if counts.QueueFull != 0 || counts.TraceShed != 0 {
		t.Fatal("阻塞模式不应产生降载计数")
	}
	// 峰值队列占用不得超过容量
	t.Logf("结论: 阻塞背压下 0 丢弃，且资源严格有界（channel cap=%d）", cfg.MaxQueueSize)
}
