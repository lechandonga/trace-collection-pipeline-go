package trace

import "testing"

func TestSamplerInheritedFlagNeverReconsidered(t *testing.T) {
	// 输入：任意一个子跨度，父上下文 sampled=false（哪怕 Fraction=1.0）。
	s := NewSampler(SamplerConfig{Fraction: 1.0})
	parent := SpanContext{TraceID: NewTraceID(), SpanID: NewSpanID(), Sampled: false}
	res := s.ShouldSample(parent, "payment", "charge", KindClient)
	// 判定依据：子跨度不得被重新采样，且丢弃理由是“根链路未采样”而不是概率。
	if res.Decision != DecisionDrop || res.Reason != DropReasonRootDrop {
		t.Fatalf("child of dropped trace got decision=%d reason=%q, want drop/root_trace_dropped",
			res.Decision, res.Reason)
	}

	// 对称地，sampled=true 的父链路即使概率为 0 也必须保持采样（完整性）。
	s0 := NewSampler(SamplerConfig{Fraction: 0})
	parentOn := SpanContext{TraceID: NewTraceID(), SpanID: NewSpanID(), Sampled: true}
	res = s0.ShouldSample(parentOn, "payment", "charge", KindClient)
	if res.Decision != DecisionRecordAndSample || res.Reason != SampleReasonInherited {
		t.Fatalf("child of sampled trace got %d/%q, want sample/inherited_sampled_flag",
			res.Decision, res.Reason)
	}
	t.Log("input: child spans under sampled true/false parents with conflicting fractions; " +
		"judgment: inherited flag always wins, reasons distinguished")
}

func TestSamplerRootRuleBeforeProbability(t *testing.T) {
	// 输入：Fraction=0，但规则强制命中 service=payment 的根跨度。
	s := NewSampler(SamplerConfig{
		Fraction: 0,
		Rule:     func(service, name string) bool { return service == "payment" },
	})
	r1 := s.ShouldSample(SpanContext{}, "payment", "charge", KindServer)
	r2 := s.ShouldSample(SpanContext{}, "order", "create", KindServer)
	if r1.Decision != DecisionRecordAndSample || r1.Reason != SampleReasonRuleMatch {
		t.Fatalf("rule-matched root got %d/%q", r1.Decision, r1.Reason)
	}
	if r2.Decision != DecisionDrop || r2.Reason != DropReasonProbability {
		t.Fatalf("non-matched root at fraction=0 got %d/%q, want drop/probability_filter",
			r2.Decision, r2.Reason)
	}
	t.Logf("input: fraction=0 + payment rule; judgment: payment=>%q order=>%q",
		r1.Reason, r2.Reason)
}

func TestSamplerProbabilityBoundaries(t *testing.T) {
	SetSamplingRollForTest(42)
	// Fraction=1 恒采；Fraction=0 恒不采；负数钳为 0。
	if r := NewSampler(SamplerConfig{Fraction: 1}).ShouldSample(SpanContext{}, "a", "b", KindInternal); r.Decision != DecisionRecordAndSample {
		t.Fatalf("fraction=1 root dropped: %v", r)
	}
	if r := NewSampler(SamplerConfig{Fraction: 0}).ShouldSample(SpanContext{}, "a", "b", KindInternal); r.Decision != DecisionDrop {
		t.Fatalf("fraction=0 root sampled: %v", r)
	}
	if r := NewSampler(SamplerConfig{Fraction: -3}).ShouldSample(SpanContext{}, "a", "b", KindInternal); r.Decision != DecisionDrop {
		t.Fatalf("negative fraction should clamp to 0 but root sampled: %v", r)
	}
	t.Log("input: fraction=1 / 0 / negative; judgment: sample / drop / drop")
}
