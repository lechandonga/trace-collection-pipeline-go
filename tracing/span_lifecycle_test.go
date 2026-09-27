package tracing

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// buildTree 由已导出跨度重建链路结构，用于断言"无丢失、无重复归属、无悬挂"。
type treeStats struct {
	spans    map[SpanID]SpanData
	children map[SpanID][]SpanID
	roots    []SpanData
	traces   map[TraceID]map[SpanID]bool
	missingP []SpanData // 父 ID 非空但找不到父跨度
}

func analyze(data []SpanData) treeStats {
	st := treeStats{
		spans:    map[SpanID]SpanData{},
		children: map[SpanID][]SpanID{},
		traces:   map[TraceID]map[SpanID]bool{},
	}
	for _, d := range data {
		if _, dup := st.spans[d.SpanID]; dup {
			panic(fmt.Sprintf("duplicate span exported: %x", d.SpanID))
		}
		st.spans[d.SpanID] = d
		if st.traces[d.TraceID] == nil {
			st.traces[d.TraceID] = map[SpanID]bool{}
		}
		st.traces[d.TraceID][d.SpanID] = true
	}
	for _, d := range data {
		if !d.HasParent() {
			st.roots = append(st.roots, d)
			continue
		}
		if _, ok := st.spans[d.ParentSpanID]; !ok {
			st.missingP = append(st.missingP, d)
		} else {
			st.children[d.ParentSpanID] = append(st.children[d.ParentSpanID], d.SpanID)
		}
	}
	return st
}

// TestOutOfOrderEndsAcrossGoroutines 输入: 父跨度启动后，子跨度在不同 goroutine
// 中先结束、父后结束；多个子调用并发。结论必须与顺序结束完全一致。
func TestOutOfOrderEndsAcrossGoroutines(t *testing.T) {
	exp := NewControllableExporter()
	pl := NewPipeline(PipelineConfig{
		Service:   "svc",
		Sampler:   &AlwaysSample{},
		Processor: DefaultProcessorConfig(),
	}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	ctx, parent := tr.Start(context.Background(), "parent")
	const children = 50
	var wg sync.WaitGroup
	for i := 0; i < children; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			// 跨 goroutine 传递 ctx（只携带不可变 SpanContext 语义）。
			_, child := tr.Start(ctx, fmt.Sprintf("child-%d", i))
			child.SetAttributes("i", fmt.Sprint(i))
			child.End() // 子先于父结束
		}()
	}
	wg.Wait()
	parent.End() // 父最后结束

	if err := pl.ShutdownWithTimeout(2_000_000_000); err != nil {
		t.Fatal(err)
	}
	got := exp.Exported()
	t.Logf("输入: 1 父 + %d 子, 子全部先结束; 判定依据: 实际导出 %d 条", children, len(got))

	st := analyze(got)
	if len(st.spans) != children+1 {
		t.Fatalf("跨度丢失或重复: 期望 %d 得到 %d", children+1, len(st.spans))
	}
	if len(st.roots) != 1 {
		t.Fatalf("根跨度数应=1, got=%d", len(st.roots))
	}
	if len(st.missingP) != 0 {
		t.Fatalf("存在悬挂归属: %d 条子跨度找不到父", len(st.missingP))
	}
	if len(st.children[parent.SpanContext().SpanID]) != children {
		t.Fatalf("父的直接子跨度数应为 %d, got=%d", children, len(st.children[parent.SpanContext().SpanID]))
	}
	t.Logf("结论: 父子乱序+跨协程下链路自洽，根=%x 子跨度=%d", st.roots[0].SpanID, len(st.children[parent.SpanContext().SpanID]))
}

// TestDuplicateEndIsIdempotent 输入: 对同一跨度并发多次 End。
func TestDuplicateEndIsIdempotent(t *testing.T) {
	exp := NewControllableExporter()
	pl := NewPipeline(PipelineConfig{Service: "svc", Processor: DefaultProcessorConfig()}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	_, sp := tr.Start(context.Background(), "dup")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sp.End() }()
	}
	wg.Wait()
	pl.ShutdownWithTimeout(2_000_000_000)

	got := exp.Exported()
	stats := tr.Stats()
	t.Logf("输入: 1 个跨度 End x20; 判定依据: 导出=%d tracer统计 Ended=%d DoubleEnded=%d",
		len(got), stats.Ended, stats.DoubleEnded)
	if len(got) != 1 {
		t.Fatalf("重复结束导致重复归属: 导出 %d 条", len(got))
	}
	if stats.Ended != 1 {
		t.Fatalf("Ended 计数应为1, got=%d", stats.Ended)
	}
	if stats.DoubleEnded == 0 {
		t.Fatalf("重复 End 应被识别并计数")
	}
	t.Logf("结论: End 幂等，无重复导出；重复调用次数可查询=%d", stats.DoubleEnded)
}

// TestMissingEndIsReclaimed 输入: 一个跨度从未调用 End（泄漏/故障）。
func TestMissingEndIsReclaimed(t *testing.T) {
	exp := NewControllableExporter()
	pl := NewPipeline(PipelineConfig{Service: "svc", Processor: DefaultProcessorConfig()}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	ctx, root := tr.Start(context.Background(), "root")
	_, leaked := tr.Start(ctx, "leaked") // 故意不 End
	_, ok := tr.Start(ctx, "ok")
	ok.End()
	root.End()

	if tr.ActiveCount() != 1 {
		t.Fatalf("关闭前应有 1 个未结束跨度, got=%d", tr.ActiveCount())
	}
	t.Logf("输入: 3 个跨度其中 1 个缺失 End; 判定依据: 关闭前 ActiveCount=%d", tr.ActiveCount())

	if err := pl.ShutdownWithTimeout(2_000_000_000); err != nil {
		t.Fatal(err)
	}
	stats := tr.Stats()
	got := exp.Exported()
	st := analyze(got)
	t.Logf("判定依据: 导出=%d, Abandoned=%d, 悬挂父缺失=%d", len(got), stats.Abandoned, len(st.missingP))

	if stats.Abandoned != 1 {
		t.Fatalf("应回收 1 个缺失结束跨度, got=%d", stats.Abandoned)
	}
	if len(got) != 3 {
		t.Fatalf("回收后跨度不应丢失: 期望3 got=%d", len(got))
	}
	if len(st.missingP) != 0 {
		t.Fatal("回收跨度后链路不应出现悬挂归属")
	}
	var leakData SpanData
	for _, d := range got {
		if d.Name == "leaked" {
			leakData = d
		}
	}
	found := false
	for _, ev := range leakData.Events {
		if ev.Name == "span.abandoned" {
			found = true
		}
	}
	if !found {
		t.Fatal("被回收跨度应带 span.abandoned 诊断事件，原因可区分")
	}
	if leakData.Status.Code != StatusError {
		t.Fatal("被回收跨度结论应为 Error，明确表达缺失结束")
	}
	// 回收后再 End 不得产生重复
	leaked.End()
	if len(exp.Exported()) != 3 {
		t.Fatal("回收后再次 End 产生了重复数据")
	}
	t.Logf("结论: 缺失结束被确定性回收为 Error+abandoned 事件，无悬挂、无重复")
}

// TestCrossComponentPropagationTree 输入: 三个"组件"通过载体接力传播，
// 模拟 A->B->C 的完整链路。
func TestCrossComponentPropagationTree(t *testing.T) {
	exp := NewControllableExporter()
	pl := NewPipeline(PipelineConfig{Service: "mesh", Processor: DefaultProcessorConfig()}, exp)
	pl.Start(context.Background())
	tr := pl.Tracer()

	a := NewComponent("A", tr)
	b := NewComponent("B", tr)
	c := NewComponent("C", tr)

	// 链路1: A -> B -> C 成功（5 个跨度: A server, A client, B server, B client, C server）
	err := a.Entry(context.Background(), nil, "request", func(ctx context.Context) error {
		return a.CallRemote(ctx, "toB", b, false, func(ctx context.Context) error {
			return b.CallRemote(ctx, "toC", c, false, nil)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	// 链路2: A -> B 失败（3 个跨度: A server, A client, B server=error）
	err = a.Entry(context.Background(), nil, "request2", func(ctx context.Context) error {
		return a.CallRemote(ctx, "toBfail", b, true, nil)
	})
	if err != CallError {
		t.Fatalf("预期模拟失败, got=%v", err)
	}
	pl.ShutdownWithTimeout(2_000_000_000)

	got := exp.Exported()
	st := analyze(got)
	t.Logf("输入: 2 条入口链路 (1 条 A->B->C 成功, 1 条 A->B 失败); 判定依据: 导出=%d traces=%d 悬挂=%d",
		len(got), len(st.traces), len(st.missingP))
	if len(got) != 8 {
		t.Fatalf("导出跨度数应为 5+3=8, got=%d", len(got))
	}
	if len(st.traces) != 2 {
		t.Fatalf("应形成 2 条独立 trace, got=%d", len(st.traces))
	}
	sizes := map[int]int{}
	for _, spans := range st.traces {
		sizes[len(spans)]++
	}
	if sizes[5] != 1 || sizes[3] != 1 {
		t.Fatalf("链路规模应分别为 5 和 3, got=%v", sizes)
	}
	if len(st.missingP) != 0 {
		t.Fatal("跨组件传播后不得有悬挂跨度")
	}
	var errCount int
	for _, d := range got {
		if d.Status.Code == StatusError {
			errCount++
		}
	}
	if errCount != 3 { // A server(整请求失败) + A client + B server
		t.Fatalf("失败链路上应有 3 个 error 状态跨度(A server, A client, B server), got=%d", errCount)
	}
	t.Logf("结论: 跨组件传播结构完整(5/3)，失败只改状态(3 个 error)不破坏归属")
}
