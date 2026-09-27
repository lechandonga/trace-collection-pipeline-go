package tracing

import "testing"

func TestPropagationRoundTrip(t *testing.T) {
	gen := newFixedIDGenerator()
	want := SpanContext{TraceID: gen.NewTraceID(), SpanID: gen.NewSpanID(), Sampled: true}
	prop := NewTextMapPropagator()
	carrier := NewMapCarrier()
	if err := prop.Inject(want, carrier); err != nil {
		t.Fatalf("Inject input=%v err=%v", want, err)
	}
	t.Logf("输入 SpanContext=%+v; 判定依据: 载体内容=%v", want, carrier.values)
	got, err := prop.Extract(carrier)
	if err != nil {
		t.Fatalf("Extract err=%v", err)
	}
	if got != want {
		t.Fatalf("结论不一致: got=%+v want=%+v", got, want)
	}
	t.Logf("结论: 往返一致 trace=%x span=%x sampled=%v", got.TraceID, got.SpanID, got.Sampled)
}

func TestPropagationMissingMeansNoUpstream(t *testing.T) {
	prop := NewTextMapPropagator()
	got, err := prop.Extract(NewMapCarrier())
	t.Logf("输入: 空载体; 判定依据: err=%v", err)
	if err != nil {
		t.Fatalf("空载体应视为无上游而非错误, got err=%v", err)
	}
	if got.IsValid() {
		t.Fatalf("空载体应提取出无效上下文, got=%+v", got)
	}
	t.Logf("结论: 无上游，调用方应开启根跨度")
}

func TestPropagationMalformedDistinguishable(t *testing.T) {
	prop := NewTextMapPropagator()
	cases := []map[string]string{
		{"x-trace-traceid": "zz", "x-trace-spanid": "0000000000000001"},
		{"x-trace-traceid": "00000000000000000000000000000001", "x-trace-spanid": "bad"},
		{"x-trace-traceid": "00000000000000000000000000000001", "x-trace-spanid": "0000000000000001", "x-trace-sampled": "maybe"},
	}
	for i, in := range cases {
		_, err := prop.Extract(FromMap(in))
		t.Logf("用例%d 输入=%v; 判定依据: err=%v", i, in, err)
		if err != ErrMalformedContext {
			t.Fatalf("用例%d: 应得到 ErrMalformedContext, got=%v", i, err)
		}
	}
	t.Logf("结论: 字段缺失(无上游) 与 字段非法(ErrMalformedContext) 原因可区分")
}

func TestInjectInvalidRejected(t *testing.T) {
	prop := NewTextMapPropagator()
	err := prop.Inject(SpanContext{}, NewMapCarrier())
	if err != ErrMalformedContext {
		t.Fatalf("注入无效上下文应被拒绝, got=%v", err)
	}
	t.Logf("结论: 零值上下文不允许注入，防止传播垃圾数据")
}

func TestRatioSamplerTraceConsistency(t *testing.T) {
	// 输入: 同一条 TraceID 的 100 次子判定（模拟一条 trace 上的大量跨度/乱序）
	// 判定依据: 哈希输入仅含 TraceID，结论必须全部相同。
	s := NewRatioSampler(0.5)
	tid := RandomIDGenerator{}.NewTraceID()
	first := s.ShouldSample(SamplingParameters{TraceID: tid})
	for i := 0; i < 100; i++ {
		got := s.ShouldSample(SamplingParameters{TraceID: tid})
		if got != first {
			t.Fatalf("同一 trace 采样结论分歧: first=%v got=%v @%d", first, got, i)
		}
	}
	t.Logf("输入: traceID=%x ratio=0.5 重复100次; 结论: 全部=%v (reason=%v)", tid, first.Decision, first.Reason)

	// ratio 边界
	if NewRatioSampler(0).ShouldSample(SamplingParameters{TraceID: tid}).Decision != DecisionDrop {
		t.Fatal("ratio=0 应全丢弃")
	}
	if NewRatioSampler(1).ShouldSample(SamplingParameters{TraceID: tid}).Decision != DecisionRecordAndSample {
		t.Fatal("ratio=1 应全采样")
	}
	t.Logf("结论: ratio=0/1 边界确定")
}

func TestChildInheritsSamplingDecision(t *testing.T) {
	s := NewRatioSampler(0.0) // 根必拒
	tid := RandomIDGenerator{}.NewTraceID()
	root := s.ShouldSample(SamplingParameters{TraceID: tid})
	child := s.ShouldSample(SamplingParameters{TraceID: tid, HasParent: true, ParentSampled: root.Decision == DecisionRecordAndSample})
	t.Logf("输入: 根判定=%v; 判定依据: 子跨度必须继承; 子判定=%v", root.Decision, child.Decision)
	if child.Decision != DecisionDrop {
		t.Fatal("父未采样时子必须不采样，否则会出现孤立子跨度")
	}
}
