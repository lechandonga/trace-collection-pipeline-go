package trace

import (
	"strings"
	"testing"
	"time"
)

func mkSpan(tid TraceID, id, parent SpanID, startOffsetSec int64) *Span {
	s := &Span{
		TraceID: tid, SpanID: id, ParentID: parent,
		Service: "svc", Name: "n", StartTime: time.Unix(startOffsetSec, 0),
		Attributes: map[string]string{},
	}
	s.End(time.Unix(startOffsetSec+1, 0))
	return s
}

func TestBuildReportOrphanSpanNotLost(t *testing.T) {
	// 输入：两个跨度引用了同一个未被采集到的父 ID（父缺失）。
	tid := NewTraceID()
	missing := NewSpanID()
	s1 := mkSpan(tid, NewSpanID(), missing, 2)
	s2 := mkSpan(tid, NewSpanID(), missing, 1)
	rep, err := BuildReport(tid, true, []*Span{s1, s2})
	if err != nil {
		t.Fatal(err)
	}
	// 判定依据：孤儿不得丢失，被当作虚拟根呈现；按开始时间确定性排序。
	if len(rep.OrphanIDs) != 2 {
		t.Fatalf("orphans=%v, want 2", rep.OrphanIDs)
	}
	if rep.Roots[0] != s2.SpanID || rep.Roots[1] != s1.SpanID {
		t.Fatalf("roots order=%v, want sorted by start time [s2,s1]", rep.Roots)
	}
	t.Logf("input: 2 spans with absent parent, starts=%d,%d; judgment: orphans=2, deterministic root order",
		1, 2)
}

func TestBuildReportDuplicateSpanIDCollapsed(t *testing.T) {
	// 输入：同一 SpanID 指针被错误地放入两次（模拟重复归属防御）。
	tid := NewTraceID()
	id := NewSpanID()
	s := mkSpan(tid, id, SpanID{}, 1)
	rep, err := BuildReport(tid, true, []*Span{s, s})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Spans) != 1 {
		t.Fatalf("spans=%d, want 1 (duplicate id must collapse)", len(rep.Spans))
	}
	t.Log("input: same span instance listed twice; judgment: collapsed to 1, no double attribution")
}

func TestBuildReportCycleDetected(t *testing.T) {
	// 输入：手工构造 a->b->a 的父子环（传播协议正常不会产生，须可识别）。
	tid := NewTraceID()
	a := NewSpanID()
	b := NewSpanID()
	sa := mkSpan(tid, a, b, 1) // a 的父是 b
	sb := mkSpan(tid, b, a, 2) // b 的父是 a
	rep, err := BuildReport(tid, true, []*Span{sa, sb})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.CycleDetected {
		t.Fatal("cycle a<->b must be detected")
	}
	t.Log("input: synthetic parent cycle a<->b; judgment: CycleDetected=true")
}

func TestBuildReportRejectsForeignTrace(t *testing.T) {
	tid := NewTraceID()
	other := NewTraceID()
	s := mkSpan(tid, NewSpanID(), SpanID{}, 1)
	foreign := mkSpan(other, NewSpanID(), SpanID{}, 1)
	rep, err := BuildReport(tid, true, []*Span{s, foreign})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Spans) != 1 {
		t.Fatalf("foreign trace span included: %d", len(rep.Spans))
	}
	t.Log("input: spans from 2 different traces; judgment: only matching trace included")
}

func TestBuildReportSummaryContainsEvidence(t *testing.T) {
	tid := NewTraceID()
	root := mkSpan(tid, NewSpanID(), SpanID{}, 0)
	child := &Span{
		TraceID: tid, SpanID: NewSpanID(), ParentID: root.SpanID,
		Service: "svc", Name: "n", StartTime: time.Unix(1, 0),
		Attributes: map[string]string{},
	}
	child.forceClose(time.Unix(3, 0), nil)
	rep, err := BuildReport(tid, true, []*Span{child, root})
	if err != nil {
		t.Fatal(err)
	}
	out := rep.String()
	for _, want := range []string{"sampled=true", "spans=2", "missing_end_force_closed", "n"} {
		if !strings.Contains(out, want) {
			t.Fatalf("summary missing %q:\n%s", want, out)
		}
	}
	t.Logf("input: root + force-closed child; judgment summary:\n%s", out)
}
