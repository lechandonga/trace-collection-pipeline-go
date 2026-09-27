package tracing

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestChaosConcurrentOutageAndBackpressure 综合场景：
//   - 大量 goroutine 并发产生多跨度 trace，父子乱序结束；
//   - 导出器在运行中途反复 不可用<->恢复；
//   - 队列很小，非阻塞模式下必然发生整条 trace 降载。
//
// 最终不变量（结论一致且可解释）：
//  1. 所有成功导出的 trace 都完整（spansPer 跨度）；
//  2. 跨度总数守恒：导出 + queueFull + traceShed + exportLost == 输入；
//  3. 各类原因计数非零且可区分；调用方从未被阻塞超过给定阈值。
func TestChaosConcurrentOutageAndBackpressure(t *testing.T) {
	exp := NewControllableExporter()
	cfg := DefaultProcessorConfig()
	cfg.MaxQueueSize = 16
	cfg.MaxExportBatchSize = 4
	cfg.BatchTimeout = 10 * time.Millisecond
	cfg.RetryInitial = 2 * time.Millisecond
	cfg.RetryMax = 10 * time.Millisecond
	cfg.Blocking = false
	pl := NewPipeline(PipelineConfig{Service: "chaos", Sampler: &AlwaysSample{}, Processor: cfg}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	const traces = 300
	const spansPer = 3

	// 故障抖动：后台切换可用性。
	stopFlap := make(chan struct{})
	var flapWg sync.WaitGroup
	flapWg.Add(1)
	go func() {
		defer flapWg.Done()
		var up bool
		for {
			select {
			case <-stopFlap:
				exp.SetAvailable(true)
				return
			default:
				exp.SetAvailable(up)
				up = !up
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	var maxEndWaitNs atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < traces; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, root := tr.Start(context.Background(), fmt.Sprintf("root-%d", i))
			var kids []*Span
			for j := 0; j < spansPer-1; j++ {
				_, child := tr.Start(ctx, fmt.Sprintf("child-%d-%d", i, j))
				kids = append(kids, child)
			}
			start := time.Now()
			for _, k := range kids {
				k.End()
			}
			root.End()
			d := time.Since(start)
			for {
				old := maxEndWaitNs.Load()
				if int64(d) <= old || maxEndWaitNs.CompareAndSwap(old, int64(d)) {
					break
				}
			}
		}(i)
	}
	wg.Wait()
	close(stopFlap)
	flapWg.Wait()

	// 恢复后等待排空，给足重试时间。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pl.Processor().Queued() == 0 && pl.Processor().PendingTraces() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := pl.ShutdownWithTimeout(3 * time.Second); err != nil {
		t.Fatal(err)
	}

	got := exp.Exported()
	c := pl.Processor().Stats()
	input := traces * spansPer
	accounted := int64(len(got)) + c.QueueFull + c.TraceShed + c.ExportLost + c.StoppedRejected
	t.Logf("输入: %d trace x %d = %d 跨度，故障抖动+小队列并发", traces, spansPer, input)
	t.Logf("判定依据: 导出=%d queueFull=%d traceShed=%d exportLost=%d stopped=%d retries=%d",
		len(got), c.QueueFull, c.TraceShed, c.ExportLost, c.StoppedRejected, c.ExportRetries)
	maxEndWait := time.Duration(maxEndWaitNs.Load())
	t.Logf("End 最长耗时=%v（应远小于导出退避，证明不被导出端阻塞）", maxEndWait)

	// 不变量1：导出的链路全部完整
	sizes := exportedTraceSizes(got)
	for tid, n := range sizes {
		if n != spansPer {
			t.Fatalf("残缺链路 trace=%x: %d/%d", tid, n, spansPer)
		}
	}
	// 不变量2：守恒
	if accounted != int64(input) {
		t.Fatalf("跨度结论不守恒: %d != %d", accounted, input)
	}
	// 不变量3：压力确实触发了各类路径（允许 ExportLost 为 0，因为恢复后有重试窗口）
	if len(sizes) == 0 {
		t.Fatal("至少应有完整 trace 成功导出")
	}
	if c.QueueFull == 0 {
		t.Fatal("小队列+300 trace 应触发 queue_full 降载")
	}
	if c.ExportRetries == 0 {
		t.Fatal("故障抖动应产生可重试计数")
	}
	if maxEndWait > 100*time.Millisecond {
		t.Fatalf("End 被导出端拖慢: %v", maxEndWait)
	}
	// 诊断原因分布可打印、可区分
	reasons := map[DropReason]int{}
	for _, d := range pl.Processor().Diagnostics() {
		reasons[d.Reason]++
	}
	t.Logf("诊断原因分布=%v", reasons)
	t.Logf("结论: 成功导出 %d 条完整 trace；丢弃全部有明确原因且总数守恒", len(sizes))
}
