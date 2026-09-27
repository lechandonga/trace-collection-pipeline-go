package trace

import (
	"sync"
	"testing"
	"time"
)

func TestSpanEndIdempotent(t *testing.T) {
	// 输入：1 个跨度，连续 End 3 次（含一次显式时间戳）。
	delivered := 0
	s := &Span{
		TraceID:   NewTraceID(),
		SpanID:    NewSpanID(),
		StartTime: time.Unix(100, 0),
	}
	s.onEnd = func(*Span) { delivered++ }

	base := time.Unix(101, 0)
	s.End(base)
	s.End(base.Add(time.Second)) // 重复：不得改变结论
	s.End()                      // 重复：不得再次交接

	// 判定依据：End 仅首次生效 => onEnd 恰好 1 次、重复计数为 2、
	// 结束时间锁定为第一次传入的时间。
	if got := delivered; got != 1 {
		t.Fatalf("deliver hook called %d times, want exactly 1", got)
	}
	if got := s.DuplicateEndCount(); got != 2 {
		t.Fatalf("duplicateEnds=%d, want 2", got)
	}
	if !s.EndTime.Equal(base) {
		t.Fatalf("end time changed by duplicate end: got %v want %v", s.EndTime, base)
	}
	if !s.Ended() {
		t.Fatalf("span should be ended")
	}
	if s.EndKind() != EndNormal {
		t.Fatalf("end kind=%q, want normal (duplicate must not pollute conclusion)", s.EndKind())
	}
	t.Logf("input: 3 End calls on one span; judgment: hook=%d duplicateEnds=%d endTime=%s kind=%s",
		delivered, s.DuplicateEndCount(), s.EndTime, s.EndKind())
}

func TestSpanEndBeforeStartClamped(t *testing.T) {
	// 输入：结束时间早于开始时间（乱序时钟）。判定：持续时间自洽为 0。
	s := &Span{StartTime: time.Unix(200, 0)}
	s.End(time.Unix(199, 0))
	if d, ok := s.Duration(); !ok || d != 0 {
		t.Fatalf("duration=%v ok=%v, want 0/true (end clamped to start)", d, ok)
	}
	t.Logf("input: end<start; judgment: duration clamped to %v", s.EndTime.Sub(s.StartTime))
}

func TestSpanSetStatusAfterEndRejected(t *testing.T) {
	s := &Span{StartTime: time.Unix(0, 0)}
	s.SetStatus(StatusError, "boom")
	s.End(time.Unix(1, 0))
	s.SetStatus(StatusOK, "too late") // 结束后改写必须无效
	if s.StatusCode != StatusError || s.StatusMessage != "boom" {
		t.Fatalf("status mutated after end: %d %q", s.StatusCode, s.StatusMessage)
	}
	t.Logf("input: SetStatus after End; judgment: status stays error=%q", s.StatusMessage)
}

func TestSpanForceCloseRacesWithEnd(t *testing.T) {
	// 输入：50 个跨度，每个都让正常 End 与 sweeper forceClose 并发竞争。
	// 判定：每个跨度恰好交接一次，要么 normal 要么 missing，绝不重复/悬挂。
	for i := 0; i < 50; i++ {
		var wg sync.WaitGroup
		hooks := make(chan string, 2)
		s := &Span{TraceID: NewTraceID(), SpanID: NewSpanID(), StartTime: time.Unix(0, 0)}
		s.onEnd = func(sp *Span) { hooks <- "end" }
		wg.Add(2)
		go func() { defer wg.Done(); s.End(time.Unix(1, 0)) }()
		go func() {
			defer wg.Done()
			s.forceClose(time.Unix(2, 0), func(sp *Span) { hooks <- "force" })
		}()
		wg.Wait()
		close(hooks)
		n := 0
		for range hooks {
			n++
		}
		if n != 1 {
			t.Fatalf("case %d: span delivered %d times, want exactly 1", i, n)
		}
		if !s.Ended() {
			t.Fatalf("case %d: span left un-ended (dangling)", i)
		}
	}
	t.Log("input: 50 spans with concurrent End vs forceClose; judgment: each delivered exactly once, none dangling")
}

func TestSpanEventsAndAttributesSafe(t *testing.T) {
	s := &Span{StartTime: time.Unix(0, 0), Attributes: map[string]string{}}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); s.SetAttribute("k", "v") }()
		go func() { defer wg.Done(); s.AddEvent("e", nil) }()
	}
	wg.Wait()
	if len(s.Events()) != 20 {
		t.Fatalf("events=%d, want 20", len(s.Events()))
	}
	t.Log("input: 20 goroutines writing attrs/events; judgment: no panic, events=20")
}
