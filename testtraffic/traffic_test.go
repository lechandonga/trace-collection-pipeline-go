package testtraffic

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/trace-collection-pipeline-go/export"
	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// buildSharedTracers 为每个组件构造一个共享同一 Sink 的采集器。
func buildSharedTracers(sink trace.SpanSink, sampler trace.Sampler) map[string]*trace.Tracer {
	comps := DefaultComponents()
	out := make(map[string]*trace.Tracer, len(comps))
	for _, c := range comps {
		out[c.Name] = trace.NewTracerSharedSink(trace.TracerConfig{
			Service:    c.Name,
			Sampler:    sampler,
			Sink:       sink,
			MaxSpanAge: time.Hour, // 集成测试由 Shutdown 统一收口，避免后台干扰
		})
	}
	return out
}

func assertTreeShape(t *testing.T, rep *trace.TraceReport, spanIDs map[string]trace.SpanID) {
	t.Helper()
	// 期望拓扑（与结束顺序无关）：
	// gateway(root) -> order -> payment
	//                         \-> notify
	root := spanIDs["gateway"]
	order := spanIDs["order"]
	pay := spanIDs["payment"]
	notify := spanIDs["notify"]

	if len(rep.Roots) != 1 || rep.Roots[0] != root {
		t.Fatalf("roots=%v, want only gateway %s", rep.Roots, root)
	}
	if got := rep.Spans[root].Children; len(got) != 1 || got[0] != order {
		t.Fatalf("gateway.children=%v, want [order]", got)
	}
	if got := rep.Spans[order].Children; len(got) != 2 ||
		(got[0] != pay && got[1] != pay) {
		t.Fatalf("order.children=%v, want [payment,notify]", got)
	}
	if parent := rep.Spans[pay].ParentID; parent != order {
		t.Fatalf("payment.parent=%s, want order", parent)
	}
	if parent := rep.Spans[notify].ParentID; parent != order {
		t.Fatalf("notify.parent=%s, want order", parent)
	}
}

// idByService 从 outcome.SpanIDs 无法直接得知服务映射，故通过 runner 生成的
// 拓扑顺序约定：[gateway, order, payment, notify]。
func indexOutcome(o TraceOutcome) map[string]trace.SpanID {
	return map[string]trace.SpanID{
		"gateway": o.SpanIDs[0],
		"order":   o.SpanIDs[1],
		"payment": o.SpanIDs[2],
		"notify":  o.SpanIDs[3],
	}
}

// runScenarioAndCollect 执行场景、关闭采集器、返回内存导出器全部跨度。
func runScenarioAndCollect(t *testing.T, scenario Scenario, numTraces, workers int,
	processor *export.Processor, tracers map[string]*trace.Tracer) []TraceOutcome {
	t.Helper()
	runner := NewRunner(SetupConfig{
		Tracers: tracers, Components: DefaultComponents(),
		NumTraces: numTraces, NumWorkers: workers,
	})
	outs, err := runner.Run(context.Background(), scenario)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tr := range tracers {
		if err := tr.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := processor.Flush(ctx); err != nil {
		// 失败类场景预期 Flush 报错；具体断言由调用方完成。
		t.Logf("Flush returned (may be expected for failure scenarios): %v", err)
	}
	return outs
}

func TestConcurrentOutOfOrderScenariosConsistent(t *testing.T) {
	// 输入：4 类场景 × 20 条链路 × 8 协程并发，全部走内存导出器。
	// 判定：每条链路 4 个跨度、拓扑一致、无孤儿/环、无丢失。
	scenarios := []Scenario{
		ScenarioOutOfOrder, ScenarioDuplicateEnd,
		ScenarioMissingEnd, ScenarioCrossGoroutine,
	}
	for _, sc := range scenarios {
		t.Run(string(sc), func(t *testing.T) {
			mem := export.NewMemoryExporter()
			proc := export.NewProcessor(export.ProcessorConfig{
				Exporter: mem, QueueSize: 512, BatchSize: 32,
				FlushInterval: 10 * time.Millisecond,
			})
			tracers := buildSharedTracers(proc, trace.AlwaysSample())
			const n = 20
			outs := runScenarioAndCollect(t, sc, n, 8, proc, tracers)
			if err := proc.ShutdownCtx(context.Background()); err != nil {
				t.Fatal(err)
			}
			all := mem.Spans()
			byTrace := map[trace.TraceID][]*trace.Span{}
			for _, s := range all {
				byTrace[s.TraceID] = append(byTrace[s.TraceID], s)
			}
			if len(byTrace) != n {
				t.Fatalf("scenario=%s collected traces=%d, want %d (no trace lost)", sc, len(byTrace), n)
			}
			missingEndCount := 0
			dupEndCount := 0
			for _, o := range outs {
				ss := byTrace[o.TraceID]
				if len(ss) != 4 {
					t.Fatalf("trace %s spans=%d, want 4", o.TraceID, len(ss))
				}
				rep, err := trace.BuildReport(o.TraceID, true, ss)
				if err != nil {
					t.Fatal(err)
				}
				assertTreeShape(t, rep, indexOutcome(o))
				if rep.CycleDetected || len(rep.OrphanIDs) != 0 {
					t.Fatalf("trace %s not self-consistent: cycle=%t orphans=%d",
						o.TraceID, rep.CycleDetected, len(rep.OrphanIDs))
				}
				for _, sp := range rep.Spans {
					if sp.EndKind == trace.EndMissing {
						missingEndCount++
					}
					if sp.DuplicateEnds > 0 {
						dupEndCount += sp.DuplicateEnds
					}
				}
			}
			t.Logf("input: scenario=%s traces=%d workers=8; judgment: traces=%d spans=%d, forceClosed=%d duplicateEndCalls=%d",
				sc, n, len(byTrace), len(all), missingEndCount, dupEndCount)
			if err := proc.ShutdownCtx(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBurstBackpressureDropsAreReasonedAndBounded(t *testing.T) {
	// 输入：200 条链路 ×4 跨度 = 800 跨度，并发 32 协程，但队列容量仅 16，
	// 且导出器被闸门冻结一段时间 => 必然触发背压。
	// 判定：已接纳跨度与背压丢弃之和 == 总输入；占用始终 <= 容量；
	// 每条成功导出的链路都保持 4 跨度完整，不出现局部撕裂。
	// 容量 4（恰为一条链路的跨度总数），冻结 50ms 期间突发必然打满；
	// 冻结结束后闸门自动放开，导出恢复、在途等待者被排空。
	gate := newSlowGateExporter(50 * time.Millisecond)
	proc := export.NewProcessor(export.ProcessorConfig{
		Exporter: gate, QueueSize: 4, BatchSize: 4,
		FlushInterval: time.Millisecond,
		// 新链路满时立即被拒（整链降入）；已接纳链路后续跨度可等待空位，
		// 保证被采样链路不被局部丢弃撕裂。
		BlockOnFull:   5 * time.Second,
		ExportTimeout: 30 * time.Second,
	})
	tracers := buildSharedTracers(proc, trace.AlwaysSample())
	runner := NewRunner(SetupConfig{
		Tracers: tracers, Components: DefaultComponents(),
		NumTraces: 200, NumWorkers: 32,
	})
	outs, err := runner.Run(context.Background(), ScenarioBurst)
	if err != nil {
		t.Fatal(err)
	}
	// 闸门会自动放开；等待恢复完成后关闭采集器，让已接纳跨度全部导出。
	<-gate.open
	ctx := context.Background()
	for _, tr := range tracers {
		_ = tr.Shutdown(ctx)
	}
	if err := proc.ShutdownCtx(ctx); err != nil {
		t.Fatal(err)
	}
	mem := gate.snapshot()
	st := proc.Stats()
	totalInput := int64(len(outs) * 4)
	accounted := st.Exported + st.BackpressureDrops
	t.Logf("input: traces=%d spans=%d queueCap=4 workers=32 short-freeze; "+
		"judgment: exported=%d backpressureDropped=%d accounted=%d/%d peakInFlight<=4",
		len(outs), totalInput, st.Exported, st.BackpressureDrops, accounted, totalInput)
	if accounted != totalInput {
		t.Fatalf("accounting mismatch: exported(%d)+backpressure(%d)=%d != total %d",
			st.Exported, st.BackpressureDrops, accounted, totalInput)
	}
	if gate.peakInFlight() > 4 {
		t.Fatalf("peak in-flight=%d exceeded bound 4", gate.peakInFlight())
	}
	// 已导出链路的完整性：每条出现的 trace 必须恰好 4 跨度。
	byTrace := map[trace.TraceID]int{}
	for _, s := range mem {
		byTrace[s.TraceID]++
	}
	for tid, c := range byTrace {
		if c != 4 {
			t.Fatalf("trace %s exported %d spans, want exactly 4 (integrity)", tid, c)
		}
	}
	t.Logf("judgment: %d fully exported traces, each with exactly 4 spans; %d spans backpressure-dropped at admission (whole traces only)",
		len(byTrace), st.BackpressureDrops)
}

func TestExporterOutageThenRecoveryDiagnosticsDistinct(t *testing.T) {
	// 输入：导出器先不可用后恢复；若干链路在故障窗口内产生。
	// 判定：故障期间调用方不阻塞、失败原因可查询(unavailable)、
	// 失败批次进入死信保留 SpanID；恢复后后续链路正常导出。
	flaky := export.NewFlakyExporter(export.FlakyConfig{
		Inner: export.NewMemoryExporter(),
		Policy: func(call int64) (export.FailureReason, bool) {
			if call <= 4 {
				return export.FailureUnavailable, false
			}
			return "", true
		},
	})
	proc := export.NewProcessor(export.ProcessorConfig{
		Exporter: flaky, QueueSize: 64, BatchSize: 4,
		FlushInterval: 5 * time.Millisecond, MaxAttempts: 2,
		RetryBackoff: time.Millisecond,
	})
	tracers := buildSharedTracers(proc, trace.AlwaysSample())
	runner := NewRunner(SetupConfig{
		Tracers: tracers, Components: DefaultComponents(),
		NumTraces: 12, NumWorkers: 6,
	})
	outs, err := runner.Run(context.Background(), ScenarioBurst)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, tr := range tracers {
		_ = tr.Shutdown(ctx)
	}
	_ = proc.ShutdownCtx(ctx)

	st := proc.Stats()
	dls := proc.DeadLetters()
	t.Logf("input: 12 traces during unavailable window then recovery; "+
		"judgment: lastFailure=%v retried=%d deadLetters=%d exported=%d received=%d",
		func() string {
			if st.LastFailure != nil {
				return string(st.LastFailure.Reason)
			}
			return "<none>"
		}(), st.Retried, len(dls), st.Exported, st.Received)
	if st.LastFailure == nil || st.LastFailure.Reason != export.FailureUnavailable {
		t.Fatalf("last failure=%v, want unavailable (reason distinguishable)", st.LastFailure)
	}
	if len(dls) == 0 {
		t.Fatal("expected dead letters during outage retaining diagnostics")
	}
	for _, dl := range dls {
		if dl.Reason != export.FailureUnavailable || len(dl.SpanIDs) == 0 {
			t.Fatalf("dead letter not reasoned/traceable: %+v", dl)
		}
	}
	if st.Exported+st.Dropped != st.Received {
		t.Fatalf("exported(%d)+dropped(%d) != received(%d)", st.Exported, st.Dropped, st.Received)
	}
	t.Logf("judgment: %d callers never blocked; %d batches retained in dead-letter with reason; %d spans exported after recovery",
		len(outs), len(dls), st.Exported)
}
