package testtraffic

import (
	"context"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// Scenario 标识本地模拟的测试场景。
type Scenario string

// 支持的测试场景。
const (
	ScenarioOutOfOrder   Scenario = "out_of_order"
	ScenarioDuplicateEnd Scenario = "duplicate_end"
	ScenarioMissingEnd   Scenario = "missing_end"
	ScenarioCrossGoroutine Scenario = "cross_goroutine"
	ScenarioBurst        Scenario = "burst_backpressure"
)

// Component 描述一个被模拟的协作组件。
type Component struct {
	Name string
}

// SetupConfig 配置一次本地测试流量。
type SetupConfig struct {
	Tracer *trace.Tracer
	// Components 为参与协作的组件列表（如 gateway/order/payment/notify）。
	Components []Component
	// NumTraces 生成的调用链数量。
	NumTraces int
	// NumWorkers 并发协程数。
	NumWorkers int
	// Seed 随机种子，保证流量可复现。
	Seed int64
}

// TraceOutcome 是单条调用链的执行结果记录。
type TraceOutcome struct {
	TraceID  trace.TraceID
	Scenario Scenario
	SpanIDs  []trace.SpanID
	Note     string
}

// Runner 生成并执行本地多组件测试流量。
type Runner struct {
	cfg SetupConfig
}

// NewRunner 创建流量模拟器。
func NewRunner(cfg SetupConfig) *Runner { panic("not implemented") }

// Run 生成并执行指定场景的测试流量。
func (r *Runner) Run(ctx context.Context, scenario Scenario) ([]TraceOutcome, error) {
	panic("not implemented")
}

// Components 是便捷的默认组件拓扑。
func DefaultComponents() []Component { panic("not implemented") }
