package export

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

func makeSpan(traceID trace.TraceID) *trace.Span {
	return &trace.Span{
		TraceID: traceID, SpanID: trace.NewSpanID(),
		Service: "svc", Name: "n",
		StartTime: time.Unix(0, 0),
		EndTime:   time.Unix(1, 0),
		Sampled:   true,
	}
}

func TestProcessorAcceptNonBlockingWhenFull(t *testing.T) {
	// 输入：QueueSize=4、BatchSize=1，导出闸门关闭使消费冻结；
	// 占用按“在队 + 导出在途”统一计数，因此提交 4 个后管线恰好满。
	// 判定：随后提交立即拒绝、不阻塞调用方，背压原因可查、资源有界。
	gate := &gateExporter{open: make(chan struct{})}
	p := NewProcessor(ProcessorConfig{
		Exporter: gate, QueueSize: 4, BatchSize: 1,
		FlushInterval: time.Millisecond, BlockOnFull: 0,
		ExportTimeout: 10 * time.Second,
	})
	tid := trace.NewTraceID()
	// 填满：第 4 个 Accept 完成时（无论是否已被 worker 取去导出）
	// 占用都恰为 4，不会超过容量，故这 4 次一定成功且不等待。
	for i := 0; i < 4; i++ {
		if !p.Accept(makeSpan(tid)) {
			t.Fatalf("fill Accept #%d unexpectedly rejected", i+1)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && p.Stats().InFlight != 4 {
		time.Sleep(time.Millisecond)
	}
	if got := p.Stats().InFlight; got != 4 {
		t.Fatalf("in-flight after fill=%d, want 4", got)
	}
	for i := 0; i < 10; i++ {
		if p.Accept(makeSpan(tid)) {
			t.Fatalf("over-cap Accept #%d must be rejected", i+1)
		}
	}
	if got := p.Stats().BackpressureDrops; got != 10 {
		t.Fatalf("backpressureDrops=%d, want 10", got)
	}
	close(gate.open)
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Log("input: 4 fills then 10 over-cap submits (frozen exporter); judgment: 4 admitted, 10/10 rejected immediately, caller never blocked")
}

func TestProcessorInFlightTracePreferredOverNewTrace(t *testing.T) {
	// 输入：QueueSize=4、BatchSize=1，闸门关闭使消费冻结；恰好 4 个
	// trace A 跨度占满“队列+在导”名额，trace B 是全新链路。
	// 判定：B 立即被背压拒绝（不等待、不局部撕裂）；放行闸门后，在途
	// 链路 A 等待中的后续跨度得以入队，且 A 全部完整导出。
	gate := &gateExporter{open: make(chan struct{})}
	p := NewProcessor(ProcessorConfig{
		Exporter: gate, QueueSize: 4, BatchSize: 1,
		FlushInterval: time.Millisecond, BlockOnFull: 2 * time.Second,
		ExportTimeout: 10 * time.Second,
	})
	traceA := trace.NewTraceID()
	traceB := trace.NewTraceID()

	for i := 0; i < 4; i++ {
		if !p.Accept(makeSpan(traceA)) {
			t.Fatalf("trace A fill #%d rejected", i+1)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && p.Stats().InFlight != 4 {
		time.Sleep(time.Millisecond)
	}

	// 新链路 B：立即拒绝，绝不阻塞。
	done := make(chan bool, 1)
	go func() { done <- p.Accept(makeSpan(traceB)) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("new trace must be rejected when pipeline full")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Accept blocked caller for a new trace (must be non-blocking)")
	}

	// 在途链路 A 的第五个跨度：有界等待。确认在等待后放行闸门。
	accepted := make(chan bool, 1)
	go func() { accepted <- p.Accept(makeSpan(traceA)) }()
	time.Sleep(30 * time.Millisecond)
	close(gate.open) // worker 排空，等待者获得名额
	select {
	case ok := <-accepted:
		if !ok {
			t.Fatal("in-flight trace A span should be admitted after bounded wait")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight wait never resolved after opening gate")
	}
	if p.Stats().BackpressureDrops < 1 {
		t.Fatalf("backpressureDrops=%d, want >=1 (new-trace rejection)", p.Stats().BackpressureDrops)
	}
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exported := p.Stats().Exported; exported != 5 {
		t.Fatalf("exported=%d, want 5 (all in-flight trace A spans, integrity preserved)", exported)
	}
	if dl := len(p.DeadLetters()); dl != 0 {
		t.Fatalf("deadLetters=%d, want 0 for trace A", dl)
	}
	t.Log("input: full pipeline, new trace B vs in-flight trace A; judgment: B rejected(queue_full), all 5 A spans exported, no partial loss")
}

func TestProcessorRetryableFailureRetriesThenSucceeds(t *testing.T) {
	// 输入：导出器前 2 次不可用，第 3 次成功。
	// 判定：发生重试、最终成功、统计 exported 正确、状态恢复 available=true。
	mem := NewMemoryExporter()
	var mu sync.Mutex
	flaky := NewFlakyExporter(FlakyConfig{
		Inner: mem,
		Policy: func(call int64) (FailureReason, bool) {
			mu.Lock()
			defer mu.Unlock()
			if call <= 2 {
				return FailureUnavailable, false
			}
			return "", true
		},
	})
	p := NewProcessor(ProcessorConfig{
		Exporter: flaky, QueueSize: 8, BatchSize: 4,
		FlushInterval: time.Hour, MaxAttempts: 5, RetryBackoff: time.Millisecond,
	})
	for i := 0; i < 4; i++ {
		p.Accept(makeSpan(trace.NewTraceID()))
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("flush after retries: %v", err)
	}
	st := p.Stats()
	if st.Exported != 4 || st.Retried < 2 || !st.Available {
		t.Fatalf("stats exported=%d retried=%d available=%t, want 4/>=2/true",
			st.Exported, st.Retried, st.Available)
	}
	if st.LastFailure != nil && st.LastFailure.Reason != FailureUnavailable {
		t.Fatalf("last failure reason=%q, want unavailable", st.LastFailure.Reason)
	}
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: exporter unavailable x2 then ok; judgment: retried=%d exported=4 available=true reason=unavailable", st.Retried)
}

func TestProcessorNonRetryableFailureToDeadLetter(t *testing.T) {
	// 输入：导出器始终明确拒绝（rejected，不可重试）。
	// 判定：不重试，批次进入死信，原因可区分、SpanID 可追溯。
	flaky := NewFlakyExporter(FlakyConfig{
		Policy: func(int64) (FailureReason, bool) { return FailureRejected, false },
	})
	p := NewProcessor(ProcessorConfig{
		Exporter: flaky, QueueSize: 8, BatchSize: 3,
		FlushInterval: time.Hour, MaxAttempts: 5, RetryBackoff: time.Millisecond,
		DeadLetterCap: 4,
	})
	var ids []trace.SpanID
	for i := 0; i < 3; i++ {
		s := makeSpan(trace.NewTraceID())
		ids = append(ids, s.SpanID)
		p.Accept(s)
	}
	err := p.Flush(context.Background())
	var fe *FailureError
	if !errors.As(err, &fe) || fe.Reason != FailureRejected {
		t.Fatalf("err=%v, want FailureError{rejected}", err)
	}
	if flaky.Calls() != 1 {
		t.Fatalf("exporter calls=%d, want 1 (non-retryable must not retry)", flaky.Calls())
	}
	dls := p.DeadLetters()
	if len(dls) != 1 || dls[0].Reason != FailureRejected || len(dls[0].SpanIDs) != 3 {
		t.Fatalf("dead letters=%+v, want 1 rejected entry with 3 span ids", dls)
	}
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Logf("input: exporter hard-rejects batch; judgment: calls=1 deadLetter reason=%q spanCount=3", dls[0].Reason)
}

func TestProcessorRetryExhaustedThenRecoveryContinues(t *testing.T) {
	// 输入：前 3 次导出“尝试”不可用，之后恢复。MaxAttempts=2 =>
	// 第一批耗尽尝试 1、2 后进入死信；第二批尝试 3 失败、尝试 4 成功。
	// 判定：失败批次以不可用原因保留死信；恢复后后续批次按确定语义继续，
	// 导出端不可用期间不阻塞 Accept，状态先 unavailable 后 available。
	mem := NewMemoryExporter()
	flaky := NewFlakyExporter(FlakyConfig{
		Inner: mem,
		Policy: func(call int64) (FailureReason, bool) {
			if call <= 3 {
				return FailureUnavailable, false
			}
			return "", true
		},
	})
	p := NewProcessor(ProcessorConfig{
		Exporter: flaky, QueueSize: 16, BatchSize: 1,
		FlushInterval: time.Hour, MaxAttempts: 2, RetryBackoff: time.Millisecond,
	})
	batch1 := makeSpan(trace.NewTraceID())
	p.Accept(batch1)
	if err := p.Flush(context.Background()); err == nil {
		t.Fatal("first batch must fail after exhausting attempts")
	}
	st := p.Stats()
	if st.Available || len(p.DeadLetters()) != 1 {
		t.Fatalf("during outage available=%t deadLetters=%d, want false/1",
			st.Available, len(p.DeadLetters()))
	}
	if dls := p.DeadLetters(); dls[0].Reason != FailureUnavailable {
		t.Fatalf("dead letter reason=%q, want unavailable", dls[0].Reason)
	}
	batch2 := makeSpan(trace.NewTraceID())
	p.Accept(batch2)
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("second batch should recover on its second attempt: %v", err)
	}
	if n := len(mem.Spans()); n != 1 || mem.Spans()[0].SpanID != batch2.SpanID {
		t.Fatalf("exported after recovery=%v, want only batch2", mem.Spans())
	}
	if len(p.DeadLetters()) != 1 {
		t.Fatalf("dead letters=%d, want 1 (only exhausted batch)", len(p.DeadLetters()))
	}
	if !p.Stats().Available {
		t.Fatal("pipeline should be available again after a successful export")
	}
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Log("input: attempts 1-3 unavailable, then recovery; judgment: batch1 -> dead letter(unavailable), batch2 exported, pipeline continued")
}

func TestProcessorTimeoutIsClassifiedAndRetryable(t *testing.T) {
	// 输入：导出器每次睡眠超过 ExportTimeout。
	flaky := NewFlakyExporter(FlakyConfig{
		Delay: 200 * time.Millisecond,
		Policy: func(int64) (FailureReason, bool) { return "", true },
	})
	p := NewProcessor(ProcessorConfig{
		Exporter: flaky, QueueSize: 4, BatchSize: 2,
		FlushInterval: time.Hour, MaxAttempts: 2,
		ExportTimeout: 20 * time.Millisecond, RetryBackoff: time.Millisecond,
	})
	p.Accept(makeSpan(trace.NewTraceID()))
	p.Accept(makeSpan(trace.NewTraceID()))
	err := p.Flush(context.Background())
	var fe *FailureError
	if !errors.As(err, &fe) || fe.Reason != FailureTimeout {
		t.Fatalf("err=%v, want timeout", err)
	}
	if !FailureTimeout.Retryable() || FailureRejected.Retryable() {
		t.Fatal("retryability classification wrong")
	}
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Log("input: exporter slower than deadline; judgment: failure reason=timeout, classified retryable")
}

func TestProcessorDeadLetterBounded(t *testing.T) {
	// 输入：持续 rejected，死信上限 3。判定：内存有界，仅保留最新 3 条。
	flaky := NewFlakyExporter(FlakyConfig{
		Policy: func(int64) (FailureReason, bool) { return FailureRejected, false },
	})
	p := NewProcessor(ProcessorConfig{
		Exporter: flaky, QueueSize: 64, BatchSize: 1,
		FlushInterval: time.Hour, DeadLetterCap: 3,
	})
	for i := 0; i < 10; i++ {
		p.Accept(makeSpan(trace.NewTraceID()))
		// BatchSize=1：每次 Flush 恰好消费一个跨度为一批。
		_ = p.Flush(context.Background())
	}
	if got := len(p.DeadLetters()); got != 3 {
		t.Fatalf("dead letters=%d, want bounded 3", got)
	}
	if err := p.ShutdownCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Log("input: 10 rejected batches, cap=3; judgment: dead letters bounded at 3")
}

func TestFailureReasonsAreDistinct(t *testing.T) {
	all := []FailureReason{FailureTimeout, FailureUnavailable, FailureRejected, FailureQueueFull, FailureShutdown}
	seen := map[FailureReason]bool{}
	for _, r := range all {
		if seen[r] {
			t.Fatalf("duplicate reason %q", r)
		}
		seen[r] = true
		if r.Retryable() != (r == FailureTimeout || r == FailureUnavailable || r == FailureQueueFull) {
			t.Fatalf("retryable classification mismatch for %q", r)
		}
	}
	t.Logf("input: all failure reasons; judgment: %d distinct reasons, retryability matrix defined (%v)", len(all), all)
}

func ExampleFailureError() {
	err := &FailureError{Reason: FailureUnavailable, Err: fmt.Errorf("connection refused")}
	fmt.Println(err.Error())
	// Output: export failed (unavailable): connection refused
}
