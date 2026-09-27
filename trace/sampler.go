package trace

// SamplingDecision 是采样判定结果。
type SamplingDecision uint8

// 采样判定取值。
const (
	// DecisionDrop 直接不采样该链路。
	DecisionDrop SamplingDecision = iota
	// DecisionRecordAndSample 采样并记录。
	DecisionRecordAndSample
)

// String 返回判定名称。
func (d SamplingDecision) String() string {
	switch d {
	case DecisionRecordAndSample:
		return "record_and_sample"
	default:
		return "drop"
	}
}

// SamplingResult 是采样器对一次新建跨度的判定。
type SamplingResult struct {
	Decision SamplingDecision
	Reason   SampleReason
}

// SampleReason 说明采样/丢弃的可区分理由。
type SampleReason string

// 采样相关理由。
const (
	SampleReasonRootSampled SampleReason = "root_trace_sampled"
	SampleReasonInherited   SampleReason = "inherited_sampled_flag"
	SampleReasonRuleMatch   SampleReason = "rule_match"
)

// 丢弃相关理由（采样策略层面）。
const (
	DropReasonProbability SampleReason = "probability_filter"
	DropReasonRootDrop    SampleReason = "root_trace_dropped"
)

// Sampler 对新建跨度做采样判定。
// 参数：父上下文（可能无效）、服务名、跨度名、跨度类型。
type Sampler interface {
	ShouldSample(parent SpanContext, service, name string, kind SpanKind) SamplingResult
	Description() string
}

// SamplerConfig 配置组合采样器。
type SamplerConfig struct {
	// Fraction 根链路采样概率，范围 [0,1]；默认 1.0。
	Fraction float64
	// Rule 为非空时，服务/跨度名命中的根链路强制采样。
	Rule func(service, name string) bool
}

// compositeSampler 组合规则匹配与概率采样；子跨度继承父链采样标志。
type compositeSampler struct {
	cfg SamplerConfig
}

// NewSampler 创建组合采样器。
func NewSampler(cfg SamplerConfig) Sampler {
	if cfg.Fraction <= 0 || cfg.Fraction > 1 {
		cfg.Fraction = 1
	}
	return &compositeSampler{cfg: cfg}
}

// alwaysOn 是始终采样的简单采样器。
type alwaysOn struct{}

// AlwaysSample 返回始终采样的采样器。
func AlwaysSample() Sampler { return alwaysOn{} }

func (alwaysOn) ShouldSample(SpanContext, string, string, SpanKind) SamplingResult {
	return SamplingResult{Decision: DecisionRecordAndSample, Reason: SampleReasonRootSampled}
}

func (alwaysOn) Description() string { return "AlwaysSample" }

func (s *compositeSampler) Description() string { return "CompositeSampler" }

// ShouldSample 的关键不变量：
//  1. 携带有效父上下文时，采样标志只能被继承，绝不重新随机 —— 这样同一条
//     链路无论在多少个组件/协程中展开，结论都一致，已采样链路不会在
//     下游被局部丢弃而破坏完整性；
//  2. 仅对根跨度应用规则与概率判定，且规则优先于概率，丢弃理由可区分。
func (s *compositeSampler) ShouldSample(parent SpanContext, service, name string, kind SpanKind) SamplingResult {
	if parent.IsValid() {
		if parent.Sampled {
			return SamplingResult{Decision: DecisionRecordAndSample, Reason: SampleReasonInherited}
		}
		return SamplingResult{Decision: DecisionDrop, Reason: DropReasonRootDrop}
	}

	if s.cfg.Rule != nil && s.cfg.Rule(service, name) {
		return SamplingResult{Decision: DecisionRecordAndSample, Reason: SampleReasonRuleMatch}
	}
	// 以 traceID 无关的哈希式判定难以在无 ID 时进行，这里改用确定性
	// 浮点比较；概率门限的真正随机来源由调用方通过 Fraction 控制。
	if rollSampling(s.cfg.Fraction) {
		return SamplingResult{Decision: DecisionRecordAndSample, Reason: SampleReasonRootSampled}
	}
	return SamplingResult{Decision: DecisionDrop, Reason: DropReasonProbability}
}
