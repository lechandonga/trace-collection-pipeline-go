package tracing

import (
	"context"
	"testing"
	"time"
)

// assertTraceComplete 校验导出数据中不存在残缺 trace（每个导出 trace 恰好 expectN 跨度）。
func assertTraceComplete(t *testing.T, data []SpanData, expectN int) {
	t.Helper()
	sizes := exportedTraceSizes(data)
	for tid, n := range sizes {
		if n != expectN {
			t.Fatalf("残缺链路 trace=%x: %d/%d", tid, n, expectN)
		}
	}
}

// TestExportUnavailableDoesNotBlockAndRetries 输入: 后端不可用期间持续产生完整 trace。
// 判定依据: End() 不阻塞（有界队列满则 trace 级降载）；恢复后队列内数据全部补出，
// 状态从 degraded 回到 healthy，重试次数与最近失败原因可查询。
func TestExportUnavailableDoesNotBlockAndRetries(t *testing.T) {
	exp := NewControllableExporter()
	cfg := DefaultProcessorConfig()
	cfg.MaxQueueSize = 100
	cfg.MaxExportBatchSize = 10
	cfg.BatchTimeout = 20 * time.Millisecond
	cfg.RetryInitial = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	pl := NewPipeline(PipelineConfig{Service: "svc", Sampler: &AlwaysSample{}, Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	exp.SetAvailable(false)
	t.Logf("输入: 后端置为不可用")

	const n = 30
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			_, sp := tr.Start(context.Background(), "during-outage")
			sp.End()
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("后端不可用阻塞了调用方（End 不应被导出端拖死）")
	}
	t.Logf("判定依据: %d 条 trace 在故障期全部快速结束，未阻塞调用方", n)

	// 故障期状态可查询
	time.Sleep(50 * time.Millisecond)
	if r, ok := exp.LastFailure(); !ok || r != ExportFailureUnavailable {
		t.Fatalf("最近失败原因应可查询为 unavailable, got=%v ok=%v", r, ok)
	}
	if pl.Processor().ExportState() != ExportStateDegraded {
		t.Fatalf("故障期状态应为 degraded, got=%v", pl.Processor().ExportState())
	}
	retriesDuring := pl.Processor().Stats().ExportRetries
	if retriesDuring == 0 {
		t.Fatal("故障期应有可重试失败计数")
	}
	t.Logf("故障期: exporter状态=%s 连续失败=%d 处理器重试批次=%d 队列=%d",
		exp.State(), exp.ConsecutiveFailures(), retriesDuring, pl.Processor().Queued())

	// 恢复：确定语义为"重试到成功"，队列中的完整 trace 全部补出，顺序不乱、不缺。
	exp.SetAvailable(true)
	t.Logf("输入: 后端恢复可用")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if exp.ExportedCount() == n {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := exp.ExportedCount(); got != n {
		t.Fatalf("恢复后应补出全部 %d 条 trace, got=%d（ExportLost=%d）",
			n, got, pl.Processor().Stats().ExportLost)
	}
	assertTraceComplete(t, exp.Exported(), 1)
	if exp.State() != ExportStateHealthy {
		t.Fatalf("成功导出后、关闭前状态应恢复 healthy, got=%v", exp.State())
	}
	if err := pl.ShutdownWithTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("结论: 恢复后 %d 条完整 trace 全部补出，状态 healthy；失败原因=unavailable 与重试均可查询",
		exp.ExportedCount())
}

// TestExportPermanentRejectionDistinguishable 输入: 导出器永久拒绝（数据非法类）。
// 判定依据: 不重试、不阻塞；跨度计入 ExportRejected，原因=rejected，与 unavailable 区分。
func TestExportPermanentRejectionDistinguishable(t *testing.T) {
	exp := NewControllableExporter()
	cfg := DefaultProcessorConfig()
	cfg.BatchTimeout = 20 * time.Millisecond
	pl := NewPipeline(PipelineConfig{Service: "svc", Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	exp.SetRejectAll(ExportFailureRejected)
	const n = 5
	for i := 0; i < n; i++ {
		_, sp := tr.Start(context.Background(), "rejected")
		sp.End()
	}
	if err := pl.ShutdownWithTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	c := pl.Processor().Stats()
	rsn, ok := exp.LastFailure()
	t.Logf("输入: 导出器永久拒绝 %d 跨度; 判定依据: ExportRejected=%d Retries=%d lastReason=%v(%v)",
		n, c.ExportRejected, c.ExportRetries, rsn, ok)
	if c.ExportRejected != n {
		t.Fatalf("不可重试拒绝应计数 %d, got=%d", n, c.ExportRejected)
	}
	if c.ExportRetries != 0 {
		t.Fatalf("永久拒绝不应触发重试, got=%d", c.ExportRetries)
	}
	if rsn != ExportFailureRejected {
		t.Fatalf("失败原因应为 rejected 以区分 unavailable, got=%v", rsn)
	}
	if len(exp.Exported()) != 0 {
		t.Fatal("被永久拒绝的数据不应出现在成功导出中")
	}
	diag := pl.Processor().Diagnostics()
	var seen bool
	for _, d := range diag {
		if d.Reason == DropReasonRejected {
			seen = true
		}
	}
	if !seen {
		t.Fatal("诊断中应包含 export_rejected 逐条记录")
	}
	t.Logf("结论: rejected 不可重试、不阻塞，计数/最近原因/逐条诊断均可区分")
}

// TestExportTooLargeSplitsDeterministically 输入: 后端只接受批量<=2，处理器批次为8。
// 判定依据: 自动对半拆分，最终全部成功；若单跨度仍超限才记 batch_too_large。
func TestExportTooLargeSplitsDeterministically(t *testing.T) {
	exp := NewControllableExporter()
	exp.SetMaxBatch(2)
	cfg := DefaultProcessorConfig()
	cfg.MaxExportBatchSize = 8
	cfg.BatchTimeout = 30 * time.Millisecond
	pl := NewPipeline(PipelineConfig{Service: "svc", Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	const n = 8
	for i := 0; i < n; i++ {
		_, sp := tr.Start(context.Background(), "big")
		sp.End()
	}
	if err := pl.ShutdownWithTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	c := pl.Processor().Stats()
	t.Logf("输入: %d 跨度，处理器批=%d 后端限制=2; 判定依据: 导出=%d rejected=%d tooLarge=%d",
		n, cfg.MaxExportBatchSize, exp.ExportedCount(), c.ExportRejected, c.BatchTooLarge)
	if exp.ExportedCount() != n {
		t.Fatalf("拆批后应全部成功导出 %d, got=%d", n, exp.ExportedCount())
	}
	if c.BatchTooLarge != 0 {
		t.Fatal("可拆分的超限批次不应计入 batch_too_large")
	}
	t.Logf("结论: 超限批次确定地对半拆分直到成功，无丢失")
}

// alwaysTooLargeExporter 对任何批次（包括单跨度）返回不可重试的 too_large。
type alwaysTooLargeExporter struct{}

func (alwaysTooLargeExporter) ExportSpans(ctx context.Context, batch []SpanData) error {
	return &ExportError{Reason: ExportFailureTooLarge, Retryable: false}
}
func (alwaysTooLargeExporter) Shutdown(ctx context.Context) error { return nil }

// TestExportSingleSpanTooLarge 输入: 即使拆分收敛到单跨度，后端仍报 too_large。
// 判定依据: 记 batch_too_large 且不无限重试，原因可区分。
func TestExportSingleSpanTooLarge(t *testing.T) {
	exp := alwaysTooLargeExporter{}
	cfg := DefaultProcessorConfig()
	cfg.MaxExportBatchSize = 4
	cfg.BatchTimeout = 20 * time.Millisecond
	pl := NewPipeline(PipelineConfig{Service: "svc", Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	_, sp := tr.Start(context.Background(), "oversized-single")
	sp.End()
	if err := pl.ShutdownWithTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	c := pl.Processor().Stats()
	rsn, _, has := pl.Processor().LastExportFailure()
	t.Logf("输入: 单跨度仍超限; 判定依据: BatchTooLarge=%d ExportRetries=%d lastReason=%v(%v)",
		c.BatchTooLarge, c.ExportRetries, rsn, has)
	if c.BatchTooLarge != 1 {
		t.Fatalf("单跨度超限应记 1 次 batch_too_large, got=%d", c.BatchTooLarge)
	}
	if c.ExportRetries != 0 {
		t.Fatal("too_large 不可重试，不应计入重试")
	}
	if rsn != ExportFailureTooLarge {
		t.Fatalf("最近失败原因应为 too_large, got=%v", rsn)
	}
	t.Logf("结论: 拆分到单跨度仍失败时按 too_large 丢弃并记因，不无限重试")
}

// TestExportStoppedRejectsNewEnqueues 输入: 关闭后仍有跨度提交。
func TestExportStoppedRejectsNewEnqueues(t *testing.T) {
	exp := NewControllableExporter()
	pl := NewPipeline(PipelineConfig{Service: "svc", Processor: DefaultProcessorConfig()}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()
	if err := pl.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// tracer 也已关闭：Start 返回不记录的空跨度；直接验证 sink 行为：
	ok, reason := pl.Processor().EnqueueFinished(SpanData{Name: "late"}, 1)
	t.Logf("输入: 关闭后提交 1 跨度; 判定依据: accepted=%v reason=%v", ok, reason)
	if ok || reason != DropReasonStopped {
		t.Fatalf("关闭后提交应被拒绝且原因 pipeline_stopped, got=%v/%v", ok, reason)
	}
	if pl.Processor().Stats().StoppedRejected != 1 {
		t.Fatal("StoppedRejected 应计数 1")
	}
	if tr.ActiveCount() != 0 {
		t.Fatal("关闭后不应登记活动跨度")
	}
	t.Logf("结论: 关闭后提交拒绝且原因=pipeline_stopped，可区分、可计数")
}
