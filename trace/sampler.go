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
func (d SamplingDecision) String() string { panic("not implemented") }

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
	// Rule 为非空时，服务名匹配的链路强制采样。
	Rule func(service, name string) bool
}

// compositeSampler 组合规则匹配与概率采样；子跨度继承父链采样标志。
type compositeSampler struct {
	cfg SamplerConfig
}

// NewSampler 创建组合采样器。
func NewSampler(cfg SamplerConfig) Sampler { panic("not implemented") }

// AlwaysSample 返回始终采样的采样器。
func AlwaysSample() Sampler { panic("not implemented") }
