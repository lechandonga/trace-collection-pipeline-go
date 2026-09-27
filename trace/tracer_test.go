package trace

import (
	"context"
	"sync"
	"testing"
	"time"
)

func newTestTracer(clock *Clock, sink SpanSink) *Tracer {
	return NewTracer(TracerConfig{
		Service:    "svc",
		Sampler:    AlwaysSample(),
		Sink:       sink,
		Now:        clock.Now,
		MaxSpanAge: 10 * time.Second,
	})
}

func TestTracerOutOfOrderDoesNotAffectTree(t *testing.T) {
	// 输入：root -> (a -> a1, b)；结束顺序 a1,b,root,a（完全乱序）。
	clock := NewClock(time.Unix(1000, 0))
	rec := NewRecorder()
	tr := newTestTracer(clock, rec)
	ctx := context.Background()

	ctx, root := tr.Start(ctx, "root")
	ctxA, a := tr.Start(ctx, "a")
	_, a1 := tr.Start(ctxA, "a1")
	_, b := tr.Start(ctx, "b")

	a1.End(clock.Now())
	b.End(clock.Now())
	root.End(clock.Now())
	a.End(clock.Now()) // 父 a 最后结束

	if tr.ActiveCount() != 0 {
		t.Fatalf("active=%d, want 0 (no dangling)", tr.ActiveCount())
	}
	rep, err := BuildReport(root.TraceID, true, rec.Spans())
	if err != nil {
		t.Fatal(err)
	}
	// 判定依据：与结束顺序无关，父子归属必须与创建时一致。
	if len(rep.Roots) != 1 || rep.Roots[0] != root.SpanID {
		t.Fatalf("roots=%v, want only %s", rep.Roots, root.SpanID)
	}
	if len(rep.Spans[a.SpanID].Children) != 1 || rep.Spans[a.SpanID].Children[0] != a1.SpanID {
		t.Fatalf("a.children=%v, want [a1]", rep.Spans[a.SpanID].Children)
	}
	if len(rep.Spans[root.SpanID].Children) != 2 {
		t.Fatalf("root.children=%v, want [a,b]", rep.Spans[root.SpanID].Children)
	}
	if len(rec.Spans()) != 4 {
		t.Fatalf("recorded=%d, want 4 (no span lost)", len(rec.Spans()))
	}
	t.Logf("input: 4 spans ended in order a1,b,root,a; judgment: tree matches creation order, recorded=4\n%s", rep.String())
}

func TestTracerCrossGoroutineParentEndsFirst(t *testing.T) {
	// 输入：子跨度在新协程中创建后异步结束，父先结束。
	clock := NewClock(time.Unix(2000, 0))
	rec := NewRecorder()
	tr := newTestTracer(clock, rec)
	ctx, parent := tr.Start(context.Background(), "parent")

	var wg sync.WaitGroup
	for g := 0; g < 30; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, child := tr.Start(ctx, "child")
			child.End(clock.Now())
		}()
	}
	parent.End(clock.Now()) // 父先于跨协程子结束
	wg.Wait()

	rep, err := BuildReport(parent.TraceID, true, rec.Spans())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Spans[parent.SpanID].Children) != 30 {
		t.Fatalf("parent children=%d, want 30 (cross-goroutine spans must not be lost or mis-parented)",
			len(rep.Spans[parent.SpanID].Children))
	}
	if tr.ActiveCount() != 0 {
		t.Fatalf("active=%d, want 0", tr.ActiveCount())
	}
	t.Logf("input: 30 children in goroutines, parent ended first; judgment: children=%d no dangling",
		len(rep.Spans[parent.SpanID].Children))
}

func TestTracerSweepStaleForceCloses(t *testing.T) {
	// 输入：一个跨度从不调用 End，逻辑时钟推进超过 MaxSpanAge。
	clock := NewClock(time.Unix(3000, 0))
	rec := NewRecorder()
	tr := newTestTracer(clock, rec)
	_, s := tr.Start(context.Background(), "never-ended")

	if n := tr.SweepStale(clock.Now()); n != 0 {
		t.Fatalf("premature sweep=%d, want 0", n)
	}
	if tr.ActiveCount() != 1 {
		t.Fatalf("active before expiry=%d, want 1", tr.ActiveCount())
	}
	at := clock.Advance(11 * time.Second)
	if n := tr.SweepStale(at); n != 1 {
		t.Fatalf("sweep after expiry=%d, want 1", n)
	}
	// 判定依据：跨度以 missing_end 终态恰好进入下游一次，无悬挂。
	if tr.ActiveCount() != 0 {
		t.Fatalf("active after sweep=%d, want 0", tr.ActiveCount())
	}
	got := rec.Spans()
	if len(got) != 1 || got[0].EndKind() != EndMissing {
		t.Fatalf("recorded=%v, want exactly 1 missing_end span", got)
	}
	// sweep 后再 End 必须幂等，不得产生第二条。
	s.End(clock.Now())
	if len(rec.Spans()) != 1 {
		t.Fatalf("span recorded twice after sweep+end: %d", len(rec.Spans()))
	}
	t.Logf("input: span without End, clock advanced past MaxSpanAge; judgment: force-closed once as %s",
		got[0].EndKind())
}

func TestTracerShutdownForceClosesResidual(t *testing.T) {
	clock := NewClock(time.Unix(4000, 0))
	rec := NewRecorder()
	tr := newTestTracer(clock, rec)
	tr.Start(context.Background(), "residual")
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tr.ActiveCount() != 0 {
		t.Fatalf("active after shutdown=%d, want 0", tr.ActiveCount())
	}
	if len(rec.Spans()) != 1 || rec.Spans()[0].EndKind() != EndMissing {
		t.Fatalf("residual span not force-closed on shutdown: %v", rec.Spans())
	}
	t.Log("input: un-ended span at Shutdown; judgment: force-closed, no dangling state")
}

func TestTracerUnsampledTraceDropsAllWithReason(t *testing.T) {
	// 输入：Fraction=0 的采样器 => 根及全部子跨度未采样。
	clock := NewClock(time.Unix(5000, 0))
	rec := NewRecorder()
	tr := NewTracer(TracerConfig{
		Service: "svc", Now: clock.Now, MaxSpanAge: time.Second,
		Sampler: NewSampler(SamplerConfig{Fraction: 0}), Sink: rec,
	})
	ctx, root := tr.Start(context.Background(), "root")
	_, child := tr.Start(ctx, "child")
	child.End()
	root.End()

	if len(rec.Spans()) != 0 {
		t.Fatalf("unsampled spans recorded: %d", len(rec.Spans()))
	}
	drops := rec.Drops()
	if len(drops) != 2 {
		t.Fatalf("drops=%d, want 2", len(drops))
	}
	// 判定依据：根跨度因概率门限被丢（probability_filter）；
	// 子跨度严格继承根结论（root_trace_dropped）。两类理由都可区分。
	reasons := map[string]int{}
	for _, d := range drops {
		if d.Reason != DropReasonUnsampled {
			t.Fatalf("drop category=%q, want unsampled", d.Reason)
		}
		reasons[d.Detail]++
	}
	if reasons[string(DropReasonProbability)] != 1 || reasons[string(DropReasonRootDrop)] != 1 {
		t.Fatalf("drop reasons=%v, want 1 probability_filter + 1 root_trace_dropped", reasons)
	}
	t.Logf("input: fraction=0 root+child; judgment: 0 sampled; root reason=probability_filter, child reason=root_trace_dropped")
}
