package tracing

import "hash/fnv"

// Decision 表示采样判定结果类别。
type Decision int

const (
	// DecisionRecordAndSample 记录并导出。
	DecisionRecordAndSample Decision = iota
	// DecisionDrop 不记录。
	DecisionDrop
)

// DropReason 给出丢弃/降采样的可区分原因，便于诊断与日志判读。
type DropReason string

const (
	DropReasonNone          DropReason = ""
	DropReasonSampler       DropReason = "sampler_reject"   // 采样器规则拒绝
	DropReasonQueueFull     DropReason = "queue_full"       // 处理器队列已满（阻塞模式以外的背压丢弃）
	DropReasonTraceShed     DropReason = "trace_shed"       // 为保证已采样链路完整，整条 trace 被降载
	DropReasonBatchTooLarge DropReason = "batch_too_large"  // 批次无法拆分到合规
	DropReasonExportLost    DropReason = "export_lost"      // 关闭时后端仍不可用，未能导出
	DropReasonRejected      DropReason = "export_rejected"  // 导出端永久拒绝（不可重试）
	DropReasonStopped       DropReason = "pipeline_stopped" // 管线已关闭后仍有提交
)

// SamplingResult 是一次采样判定的结论。
type SamplingResult struct {
	Decision Decision
	Reason   DropReason
}

// Sampler 根据根/子跨度信息决定是否采样。
type Sampler interface {
	ShouldSample(params SamplingParameters) SamplingResult
	Description() string
}

// SamplingParameters 为采样输入。
type SamplingParameters struct {
	TraceID       TraceID
	Name          string
	HasParent     bool
	ParentSampled bool
}

// AlwaysSample 采样所有链路。
type AlwaysSample struct{}

func (s *AlwaysSample) ShouldSample(_ SamplingParameters) SamplingResult {
	return SamplingResult{Decision: DecisionRecordAndSample}
}

func (s *AlwaysSample) Description() string { return "AlwaysSample" }

// NeverSample 拒绝所有链路（用于测试采样丢弃路径）。
type NeverSample struct{}

func (s *NeverSample) ShouldSample(_ SamplingParameters) SamplingResult {
	return SamplingResult{Decision: DecisionDrop, Reason: DropReasonSampler}
}

func (s *NeverSample) Description() string { return "NeverSample" }

// RatioSampler 按比例采样。
//
// 确定性语义：哈希输入只有 TraceID，因此同一条 trace 的任意跨度（任意 goroutine、
// 任意结束顺序）得到的结论必然相同。采样点在链路入口（根跨度），结论随后随
// SpanContext 传播（见 Tracer），子跨度直接继承。
//
// ratio=0 表示全不采样，ratio>=1 表示全采样。
type RatioSampler struct {
	threshold uint64 // hash < threshold 即采样
	ratio     float64
}

// NewRatioSampler 创建比例采样器，越界 ratio 会被截断到 [0,1]。
func NewRatioSampler(ratio float64) *RatioSampler {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	var threshold uint64
	if ratio >= 1 {
		threshold = ^uint64(0)
	} else if ratio > 0 {
		// float64 能精确表示 ^uint64(0) 以下的 scale 吗？用直接乘法，边界可接受。
		threshold = uint64(ratio * float64(^uint64(0)))
	}
	return &RatioSampler{threshold: threshold, ratio: ratio}
}

func (s *RatioSampler) ShouldSample(p SamplingParameters) SamplingResult {
	// 已有上游结论时必须继承，避免同一 trace 出现分歧。
	if p.HasParent {
		if p.ParentSampled {
			return SamplingResult{Decision: DecisionRecordAndSample}
		}
		return SamplingResult{Decision: DecisionDrop, Reason: DropReasonSampler}
	}
	h := fnv.New64a()
	h.Write(p.TraceID[:])
	sum := h.Sum64()
	if s.ratio >= 1 || sum < s.threshold {
		return SamplingResult{Decision: DecisionRecordAndSample}
	}
	return SamplingResult{Decision: DecisionDrop, Reason: DropReasonSampler}
}

func (s *RatioSampler) Description() string { return "RatioSampler" }
