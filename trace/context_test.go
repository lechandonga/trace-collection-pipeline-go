package trace

import (
	"errors"
	"testing"
)

func TestPropagatorRoundTrip(t *testing.T) {
	// 输入：一个带采样标志与 baggage 的上下文，注入后提取。
	tid := NewTraceID()
	sid := NewSpanID()
	orig := SpanContext{
		TraceID: tid, SpanID: sid, Sampled: true,
		Baggage: map[string]string{"user": "alice", "x-odd": "a b,c"},
	}
	carrier := TextMapCarrier{}
	DefaultPropagator().Inject(orig, carrier)

	t.Logf("input trace=%s; wire: %s | %s", tid.String(), carrier[headerTraceParent], carrier[headerBaggage])

	got, err := DefaultPropagator().Extract(carrier)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	// 判定依据：trace/span ID、采样标志逐位一致 => 跨组件结论一致。
	if got.TraceID != orig.TraceID || got.SpanID != orig.SpanID || got.Sampled != orig.Sampled {
		t.Fatalf("round trip mismatch: %+v vs %+v", got, orig)
	}
	if got.Baggage["user"] != "alice" || got.Baggage["x-odd"] != "a b,c" {
		t.Fatalf("baggage corrupted: %+v", got.Baggage)
	}
	t.Logf("judgment: IDs equal=%t sampled preserved=%t baggage keys=%d",
		got.TraceID == orig.TraceID, got.Sampled, len(got.Baggage))
}

func TestPropagatorSampledFlagStable(t *testing.T) {
	// 输入：sampled=false 的链路。判定：传播后仍为 false，下游不会误采。
	sc := SpanContext{TraceID: NewTraceID(), SpanID: NewSpanID(), Sampled: false}
	c := TextMapCarrier{}
	InjectContext(sc, c)
	got, err := ExtractContext(c)
	if err != nil {
		t.Fatal(err)
	}
	if got.Sampled {
		t.Fatal("sampled=false must survive propagation but got true")
	}
	if flags := c[headerTraceParent][len(c[headerTraceParent])-2:]; flags != "00" {
		t.Fatalf("flag nibble=%s, want 00", flags)
	}
	t.Logf("input: sampled=false; judgment: extracted sampled=%t flag=%s", got.Sampled,
		c[headerTraceParent][len(c[headerTraceParent])-2:])
}

func TestPropagatorExtractAbsentAndMalformed(t *testing.T) {
	// 输入 A：空载体 => 零上下文 + nil（全新根调用）。
	if sc, err := ExtractContext(TextMapCarrier{}); err != nil || sc.IsValid() {
		t.Fatalf("absent carrier: sc=%v err=%v, want zero/nil", sc, err)
	}
	// 输入 B：格式非法 => ErrInvalidContext（可区分于“无上下文”）。
	bad := TextMapCarrier{headerTraceParent: "99-zz-too-short"}
	if _, err := ExtractContext(bad); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("malformed carrier err=%v, want ErrInvalidContext", err)
	}
	t.Log("input: empty vs malformed carrier; judgment: nil vs ErrInvalidContext distinguished")
}

func TestPropagatorInjectInvalidNoop(t *testing.T) {
	c := TextMapCarrier{}
	InjectContext(SpanContext{}, c)
	if len(c) != 0 {
		t.Fatalf("invalid context must not be injected, got %v", c)
	}
	t.Log("input: zero SpanContext; judgment: carrier untouched")
}
