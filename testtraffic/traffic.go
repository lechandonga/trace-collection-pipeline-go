package testtraffic

import (
	"context"
	"fmt"
	"sync"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// Scenario 标识本地模拟的测试场景。
type Scenario string

// 支持的测试场景。
const (
	ScenarioOutOfOrder     Scenario = "out_of_order"
	ScenarioDuplicateEnd   Scenario = "duplicate_end"
	ScenarioMissingEnd     Scenario = "missing_end"
	ScenarioCrossGoroutine Scenario = "cross_goroutine"
	ScenarioBurst          Scenario = "burst_backpressure"
)

// Component 描述一个被模拟的协作组件。
type Component struct {
	Name string
}

// DefaultComponents 是默认拓扑：网关 -> 订单 -> (支付, 通知)。
func DefaultComponents() []Component {
	return []Component{
		{Name: "gateway"},
		{Name: "order"},
		{Name: "payment"},
		{Name: "notify"},
	}
}

// SetupConfig 配置一次本地测试流量。
type SetupConfig struct {
	// Tracers 按组件名提供各自的采集器（共享同一 Sink 与采样结论）。
	Tracers map[string]*trace.Tracer
	// Components 为参与协作的组件列表。
	Components []Component
	// NumTraces 生成的调用链数量。
	NumTraces int
	// NumWorkers 并发协程数。
	NumWorkers int
}

// TraceOutcome 是单条调用链的执行结果记录。
type TraceOutcome struct {
	Index    int
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
func NewRunner(cfg SetupConfig) *Runner {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 4
	}
	if len(cfg.Components) == 0 {
		cfg.Components = DefaultComponents()
	}
	return &Runner{cfg: cfg}
}

func (r *Runner) tracer(name string) *trace.Tracer {
	if t, ok := r.cfg.Tracers[name]; ok && t != nil {
		return t
	}
	// 容错：未显式配置的组件复用第一个可用 tracer。
	for _, c := range r.cfg.Components {
		if t := r.cfg.Tracers[c.Name]; t != nil {
			return t
		}
	}
	return nil
}

// hop 表示跨组件传播一跳：把当前跨度注入载体，再由对端提取建链。
// 返回对端新跨度。
func (r *Runner) hop(ctx context.Context, from, to, spanName string) (context.Context, *trace.Span) {
	parent := trace.SpanFromContext(ctx)
	carrier := trace.TextMapCarrier{}
	trace.InjectContext(trace.SpanContext{
		TraceID: parent.TraceID,
		SpanID:  parent.SpanID,
		Sampled: parent.Sampled,
	}, carrier)
	remote, err := trace.ExtractContext(carrier)
	if err != nil {
		panic(fmt.Sprintf("internal: inject/extract round trip failed: %v", err))
	}
	return r.tracer(to).StartWithParent(ctx, remote, spanName)
}

// Run 生成并执行指定场景的测试流量，返回每条链路的执行结果。
func (r *Runner) Run(ctx context.Context, scenario Scenario) ([]TraceOutcome, error) {
	if r.cfg.NumTraces <= 0 {
		r.cfg.NumTraces = 1
	}
	outcomes := make([]TraceOutcome, r.cfg.NumTraces)
	jobs := make(chan int, r.cfg.NumTraces)
	for i := 0; i < r.cfg.NumTraces; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	for w := 0; w < r.cfg.NumWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				outcomes[i] = r.runOne(ctx, i, scenario)
			}
		}()
	}
	wg.Wait()
	return outcomes, nil
}

// runOne 生成一条跨 4 组件的调用链，拓扑：
// gateway(GET /checkout) -> order(create) -> payment(charge)
//                                   \-----> notify(email)
//
// 各场景仅改变跨度的结束编排，父子归属在创建时已固定，
// 因此无论怎么乱序/重复/缺失，结构结论都必须一致。
func (r *Runner) runOne(ctx context.Context, idx int, scenario Scenario) TraceOutcome {
	gw := r.tracer("gateway")
	ctx, root := gw.Start(ctx, "GET /checkout", trace.WithKind(trace.KindServer))
	root.SetAttribute("trace_index", fmt.Sprintf("%d", idx))

	ctxOrder, orderSpan := r.hop(ctx, "gateway", "order", "order.create")
	_, paySpan := r.hop(ctxOrder, "order", "payment", "payment.charge")
	_, notifySpan := r.hop(ctxOrder, "order", "notify", "notify.email")

	ids := []trace.SpanID{root.SpanID, orderSpan.SpanID, paySpan.SpanID, notifySpan.SpanID}
	out := TraceOutcome{Index: idx, TraceID: root.TraceID, Scenario: scenario, SpanIDs: ids}

	switch scenario {
	case ScenarioOutOfOrder:
		// 叶子先结束、根最后结束：典型的父晚于子结束乱序。
		paySpan.End()
		notifySpan.End()
		orderSpan.End()
		root.End()
		out.Note = "children ended before parent (out-of-order)"

	case ScenarioDuplicateEnd:
		paySpan.End()
		paySpan.End() // 重复结束：必须幂等，只采集一次
		paySpan.End() // 再来一次，仍然幂等
		notifySpan.End()
		orderSpan.End()
		root.End()
		out.Note = "payment span ended 3 times, duplicates must be idempotent"

	case ScenarioMissingEnd:
		// payment 故意不结束，交由 sweeper/Shutdown 强制收口。
		notifySpan.End()
		orderSpan.End()
		root.End()
		out.Note = "payment span never ended; sweeper/Shutdown must force-close it"

	case ScenarioCrossGoroutine:
		// 把携带父跨度的 ctx 传给新协程，归属仍挂在 order 下；
		// 主协程先结束 order，子协程后结束 payment（跨协程 + 乱序）。
		done := make(chan struct{})
		go func() {
			defer close(done)
			paySpan.SetAttribute("executed_by", "worker-goroutine")
			paySpan.End()
		}()
		notifySpan.End()
		orderSpan.End() // 父先于跨协程子结束
		<-done
		root.End()
		out.Note = "payment ran in a separate goroutine; parent ended first"

	case ScenarioBurst:
		// 高并发小队列场景由 NumTraces/NumWorkers 放大；这里正常结束。
		paySpan.End()
		notifySpan.End()
		orderSpan.End()
		root.End()
		out.Note = "burst traffic through bounded queue"

	default:
		paySpan.End()
		notifySpan.End()
		orderSpan.End()
		root.End()
		out.Note = "normal ordering"
	}
	return out
}
