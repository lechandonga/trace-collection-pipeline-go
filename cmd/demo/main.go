package main

import (
	"context"
	"fmt"
	"time"

	"github.com/lechandonga/trace-collection-pipeline-go/export"
	traffic "github.com/lechandonga/trace-collection-pipeline-go/testtraffic"
	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

func main() {
	ctx := context.Background()

	// 1) 进程内导出管线：小容量队列以演示背压；底层为内存导出器。
	mem := export.NewMemoryExporter()
	proc := export.NewProcessor(export.ProcessorConfig{
		Exporter:      mem,
		QueueSize:     256,
		BatchSize:     16,
		FlushInterval: 200 * time.Millisecond,
		ExportTimeout: time.Second,
		MaxAttempts:   3,
		RetryBackoff:  5 * time.Millisecond,
		DeadLetterCap: 32,
		BlockOnFull:   2 * time.Millisecond,
	})

	// 2) 每个组件一个 Tracer，共享同一 Sink 与采样器。
	comps := traffic.DefaultComponents()
	tracers := make(map[string]*trace.Tracer, len(comps))
	for _, c := range comps {
		tracers[c.Name] = trace.NewTracerSharedSink(trace.TracerConfig{
			Service:    c.Name,
			Sampler:    trace.NewSampler(trace.SamplerConfig{Fraction: 1}),
			Sink:       proc,
			MaxSpanAge: 5 * time.Second,
		})
	}

	// 3) 生成乱序 / 重复结束 / 缺失结束 / 跨协程四类测试流量。
	scenarios := []traffic.Scenario{
		traffic.ScenarioOutOfOrder,
		traffic.ScenarioDuplicateEnd,
		traffic.ScenarioMissingEnd,
		traffic.ScenarioCrossGoroutine,
	}
	runner := traffic.NewRunner(traffic.SetupConfig{
		Tracers: tracers, Components: comps, NumTraces: 3, NumWorkers: 4,
	})
	for _, sc := range scenarios {
		outs, err := runner.Run(ctx, sc)
		if err != nil {
			panic(err)
		}
		fmt.Printf("scenario=%-17s traces=%d\n", sc, len(outs))
	}

	// 4) Shutdown 会把未显式结束的跨度按“缺失结束”强制收口，无悬挂；
	//    收口产生的跨度在此之后才入队，故先关闭 tracer，再统一 Flush。
	for _, t := range tracers {
		_ = t.Shutdown(ctx)
	}
	if err := proc.Flush(ctx); err != nil {
		fmt.Println("flush error:", err)
	}

	// 5) 打印一条缺失结束链路的自洽报告作为结论示例。
	spans := mem.Spans()
	byTrace := map[trace.TraceID][]*trace.Span{}
	for _, s := range spans {
		byTrace[s.TraceID] = append(byTrace[s.TraceID], s)
	}
	printed := 0
	for id, ss := range byTrace {
		hasMissing := false
		for _, s := range ss {
			if s.EndKind() == trace.EndMissing {
				hasMissing = true
			}
		}
		if !hasMissing {
			continue
		}
		rep, err := trace.BuildReport(id, true, ss)
		if err != nil {
			panic(err)
		}
		fmt.Println("\n== 缺失结束链路的自洽报告 ==")
		fmt.Print(rep.String())
		printed++
		if printed == 1 {
			break
		}
	}

	st := proc.Stats()
	fmt.Printf("\nexporter stats: received=%d exported=%d inQueue=%d deadLetters=%d available=%t\n",
		st.Received, st.Exported, st.InQueue, st.DeadLetters, st.Available)
	_ = proc.Shutdown()
}
