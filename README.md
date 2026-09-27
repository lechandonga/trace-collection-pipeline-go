# trace-collection-pipeline-go

一套可直接在**单进程内**运行的调用链（trace）追踪采集与导出组件。用本地生成的
测试流量模拟多个组件（网关 / 订单 / 支付）协作，目标是在**并发、乱序结束、
重复结束、缺失结束、跨协程/跨组件传递、突发容量超限、导出端故障**等场景下，
链路结论始终：

- **一致**：不丢跨度、不重复归属、不出现悬挂未结束状态；成功导出的 trace 必完整；
- **可解释**：每一种丢弃/失败都有可区分的原因（枚举）、计数与逐条诊断记录；
- **有界**：突发流量下内存占用存在确定上限，调用方永不被故障的导出端阻塞。

无第三方依赖，仅使用标准库。

## 快速开始

```go
exp := tracing.NewControllableExporter()              // 进程内模拟后端（可注入故障）
pl := tracing.NewPipeline(tracing.PipelineConfig{
    Service:   "checkout",
    Sampler:   tracing.NewRatioSampler(0.5),          // 或 &tracing.AlwaysSample{}
    Processor: tracing.DefaultProcessorConfig(),
}, exp)
pl.Start(context.Background())
defer pl.ShutdownWithTimeout(5 * time.Second)

ctx, sp := pl.Tracer().Start(context.Background(), "create-order")
defer sp.End()
// ctx 传给下游函数/协程，继续 Start 即可形成父子关系
```

运行演示程序：

```bash
go run ./cmd/tracedemo
```

运行测试（含输入与判定依据日志）：

```bash
go test -race -count=1 ./...
go test -v -run TestChaos ./tracing        # 并发+故障+背压综合场景
```

## 包结构

| 文件 | 职责 |
| --- | --- |
| `propagation.go` | `TraceID/SpanID/SpanContext`、文本传播协议、载体注入/提取 |
| `idgen.go` | 随机 ID 生成器（及测试用固定生成器） |
| `span.go` | 跨度状态机：并发安全、`End` 幂等、状态/事件/属性、快照深拷贝 |
| `tracer.go` | 创建跨度、登记父子 ID、头采样结论继承、活动注册表、缺失结束回收 |
| `sampler.go` | `AlwaysSample/NeverSample/RatioSampler`，丢弃原因枚举 |
| `processor.go` | trace 完整度缓冲、有界队列、整条 trace 背压降载、批处理与重试、诊断环 |
| `exporter.go` | `Exporter` 接口、可区分失败原因、可查询状态、故障注入后端 |
| `pipeline.go` | 统一组装与确定性关闭顺序 |
| `traffic.go` | `Component`：用文本载体模拟跨组件 RPC 的测试流量 |

## 1. 传播协议

跨度上下文通过 `TextMapCarrier`（模拟 HTTP header / MQ 属性）在"组件边界"传播，
键名固定为三个，均为小写十六进制：

| 键 | 值 |
| --- | --- |
| `x-trace-traceid` | 32 个 hex 字符（128 bit TraceID） |
| `x-trace-spanid`  | 16 个 hex 字符（64 bit SpanID，即上游父跨度 ID） |
| `x-trace-sampled` | `1` 采样 / `0` 不采样 |

提取语义严格区分两种情况，避免静默吞错：

- 载体中**两个 ID 键都缺失** ⇒ 视为"无上游"，返回零值 `SpanContext` 且
  **不报错**，由本组件开启新的根跨度；
- ID 键存在但**长度/字符非法、全零、sampled 非 `0/1`** ⇒ 返回
  `ErrMalformedContext`，调用方可据此拒绝请求或打点告警。

注入一个无效（零值）上下文同样返回 `ErrMalformedContext`，不传播垃圾数据。

跨组件入口用 `Tracer.StartWithRemoteParent(ctx, name, remoteSpanContext, ...)`：
父子关系在子跨度**创建时**仅凭 ID 登记，不依赖父 Span 对象是否存在、是否已结束，
因此父子位于不同组件/协程、以任意顺序结束都成立。

## 2. 跨度生命周期与结构自洽

- **并发安全**：`Span` 内部互斥，属性/事件/状态可跨 goroutine 写入；
  `SpanContext()` 返回不可变值，可安全跨协程、跨组件传递。
- **乱序结束**：处理器按 trace 分组缓冲，只有当一条 trace 的**所有跨度都结束**
  （tracer 在每个跨度创建时登记 trace 内跨度总数 `siblings`）时，整条 trace
  才作为不可分割单元进入导出队列。父先于子、子先于父结束，结论完全一致。
- **重复结束**：`End()` 幂等，只有第一次生效；后续调用计入
  `TracerStats.DoubleEnded`，跨度不会被归属或入队两次。
- **缺失结束**：`Tracer.Shutdown`（被 `Pipeline.Shutdown` 调用）回收所有未结束
  跨度——补上结束时间、追加 `span.abandoned`（reason=`missing_end_at_shutdown`）
  事件、状态置为 `Error` 后照常导出，计入 `TracerStats.Abandoned`。
  回收之后再 `End()` 不会产生重复数据。链路中不会有悬挂未结束跨度。
- **不可变快照**：`SpanData` 入队前深拷贝 map/slice，后续修改不影响已采集数据。

重建链路时可用的不变量（测试中的 `analyze` 已断言）：每个 spanID 恰好出现一次、
每个非根 span 的 parentSpanID 都能找到对应跨度、每条 trace 有且仅有一个根。

## 3. 采样规则

采用**头采样（head-based）+ 结论粘滞**：

1. 采样判定只在**根跨度**（无上游）执行一次；
2. `RatioSampler` 用 FNV-1a 哈希 **TraceID**（与 goroutine、结束顺序、跨度名无关），
   `hash64(traceID) / 2^64 < ratio` 即采样。同一条 trace 上任意跨度、任意并发度
   得到的结论必然相同；
3. 结论写入 `x-trace-sampled` 随上下文传播，子跨度（本地或远程）直接继承；
   父未采样则子一律不采样，杜绝孤立子跨度。

采样被拒绝的链路**不创建任何待处理数据**，拒绝数按根计入
`TracerStats.SamplerDropped`，原因 `sampler_reject`。

## 4. 背压与容量规则

数据流：

```
Span.End ──▶ tracer 计数/回收 ──▶ processor.pending（按 trace 分组缓冲）
                                   │  trace 的最后一个跨度到达
                                   ▼
                     queue（有界，容量 MaxQueueSize，单位=完整 trace）
                                   ▼
                         worker 攒批（≤ MaxExportBatchSize）
                                   ▼
                              Exporter
```

**完整性优先的降载单位是整条 trace，而不是单个跨度。** 队列满时：

| 模式 | 行为 |
| --- | --- |
| `Blocking=false`（默认） | 当前整条 trace 被**降载（shed）**：该 trace 全部跨度不导出，标记其 TraceID；同 trace 的后续跨度以 `trace_shed` 拒绝；队列与 worker 待发批次中的同 trace 跨度在发送前过滤。后端永远不会收到残缺链路。 |
| `Blocking=true` | 结束动作阻塞直到队列有空位或管线关闭（`pipeline_stopped`），已采样链路不丢弃，用调用方延迟换取完整性。 |

缓冲有界性：队列容量固定为 `MaxQueueSize`；降载标记集合在每次批次刷出后
清理已无在途批次的 trace，并以 `MaxQueueSize` 为硬上限 FIFO 淘汰（此时被淘汰
trace 的在途数据必然已处理完，不会放出残缺链路）；诊断记录是容量
`DiagCapacity` 的环形缓冲。内存占用与突发总量无关，只与容量配置和
当前在途 trace 数相关。

丢弃/降载原因（`DropReason`，计数见 `Processor().Stats()`）：

| 原因 | 含义 |
| --- | --- |
| `sampler_reject` | 采样规则在入口拒绝（tracer 侧计数） |
| `queue_full` | 队列满，触发整条 trace 降载（按跨度计数） |
| `trace_shed` | 该跨度属于已被降载的 trace（后续到达/在队列中被过滤） |
| `batch_too_large` | 拆到单跨度后端仍不接受 |
| `export_rejected` | 导出端永久拒绝（不可重试） |
| `export_lost` | 关闭排空时后端仍不可用，ctx 到期仍未导出 |
| `pipeline_stopped` | 关闭后仍有提交 |

每次丢弃同时写一条 `DropRecord`（时间、原因、TraceID、SpanID、跨度名、细节），
通过 `Processor().Diagnostics()` 读取——回答"哪条链路的哪个跨度、为什么被丢"。

**守恒断言**（测试中验证）：成功导出数 + 各原因丢弃数 == 输入跨度总数。

## 5. 导出失败语义

`Exporter.ExportSpans` 返回 `*ExportError` 表达失败类别；非 `ExportError` 的
普通 error 一律按"可重试"处理。

| 原因（`ExportFailureReason`） | 可重试 | 处理器行为 |
| --- | --- | --- |
| `unavailable` | 是 | 保留数据，指数退避（`RetryInitial`→翻倍→`RetryMax` 封顶）重试；恢复后确定地补出全部数据。退避期间响应关闭/ctx 取消。 |
| `rejected` | 否 | 丢弃该批，逐跨度计 `export_rejected`，不重试。 |
| `too_large` | 否（先拆批） | 批次 >1 跨度时先对半拆分递归重试，确定收敛到单跨度；单跨度仍超限才丢弃并计 `batch_too_large`。 |
| `stopped` | 否 | 导出器已关闭。 |

**不阻塞调用方**：导出与重试只发生在后台 worker；`Span.End` 的代价与后端状态
无关（默认模式下最多一次非阻塞 channel 入队）。

**恢复语义是确定的**：故障期间数据留在内存缓冲中按原顺序重试；`unavailable`
恢复后全部补出（测试用例断言导出数与状态 `degraded → healthy`）。

状态可查询（`ControllableExporter` 同时实现 `StateReader`）：

- `State()`：`healthy / degraded / stopped`；
- `ConsecutiveFailures()`、`ExportedCount()`、`LastFailure()`；
- 处理器侧：`ExportState()`、`LastExportFailure()`、`Stats()`、`Diagnostics()`。

关闭语义（`Pipeline.Shutdown(ctx)`）：

1. tracer 停止接收新跨度并把缺失 End 的跨度回收为 `abandoned`；
2. worker 放行 pending、排空完整 trace 队列，重试受 `ctx` 限制；
   后端持续不可用到 ctx 到期的跨度计入 `export_lost` 并逐条诊断，不静默丢失；
3. 关闭 exporter。之后提交返回 `pipeline_stopped`。

## 6. 配置项

`ProcessorConfig`（默认值见 `DefaultProcessorConfig`）：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `MaxQueueSize` | `2048` | 完整 trace 待导出队列上限，背压阈值 |
| `MaxExportBatchSize` | `512` | 单次导出最大跨度数 |
| `BatchTimeout` | `1s` | 最长攒批等待 |
| `Blocking` | `false` | 队列满时阻塞（保完整性、牺牲延迟）或立即整条 trace 降载 |
| `RetryInitial` | `50ms` | 可重试失败初始退避 |
| `RetryMax` | `2s` | 退避上限（指数翻倍封顶） |
| `DiagCapacity` | `256` | 丢弃/失败诊断环形记录条数 |

`Sampler`：`&AlwaysSample{}`、`&NeverSample{}`、`NewRatioSampler(ratio)`
（ratio 自动截断到 `[0,1]`，仅以 TraceID 为哈希输入）。

## 7. 测试场景对照

| 测试文件 | 覆盖场景 |
| --- | --- |
| `propagation_test.go` | 传播往返一致；缺字段=无上游 vs 非法字段=错误可区分；采样同 trace 一致、子继承父 |
| `span_lifecycle_test.go` | 50 子跨协程先于父结束；20 次并发 End 幂等；缺失 End 回收为 abandoned；三组件链路与错误状态 |
| `backpressure_test.go` | 500 突发守恒；闸门关闭真实打满队列后多跨度 trace 仍 0 残缺；阻塞模式 0 丢弃 |
| `exporter_test.go` | 故障期不阻塞+恢复补出+状态查询；永久拒绝不重试；超限拆批；单跨度超限；关闭后拒绝 |
| `chaos_test.go` | 300 trace 并发 + 可用性抖动 + 小队列同时发生：完整性、守恒、原因可区分、End 不被拖慢 |

所有单测均打印**输入**与**判定依据**（数量、原因分布、状态、守恒等式），
失败断言指向具体不变量。
