# 调用链追踪采集与导出组件 — 设计说明

一套可**直接在进程内运行**的调用链（trace/span）采集与导出组件。本地测试流量
模拟多个组件（gateway → order → payment/notify）协作，在并发、乱序、容量超限
与导出端故障下，链路结论保持一致且可解释。

- `trace`：ID、上下文传播、Span、采样器、采集器（Tracer）、链路报告
- `export`：有界批处理导出管线（Processor）、内存/可编排故障导出器
- `testtraffic`：多组件本地测试流量（乱序/重复结束/缺失结束/跨协程/突发）
- `cmd/demo`：进程内可运行演示

运行：

```bash
go run ./cmd/demo
go test ./... -race
```

---

## 1. 传播协议（跨组件上下文）

采用与 W3C Trace Context 兼容的 `traceparent` 风格文本协议，载体为字符串键值
（`trace.TextMapCarrier`，可对应 HTTP Header / RPC Metadata）。

```
traceparent: 00-<32hex TraceID>-<16hex SpanID>-<2hex flags>
trace-baggage: k1=v1,k2=v2          # 可选，键值经 URL query 转义，键排序后编码
```

- 版本固定 `00`；`flags` 最低位为**采样标志**。
- 注入 `Inject` / 提取 `Extract` 对称；`Extract` 对三种情况给出可区分结论：
  - 载体无 `traceparent` → 零值上下文 + `nil`（视为全新根调用）；
  - 格式非法 → `trace.ErrInvalidContext`（区别于“无上下文”）；
  - 正常 → 还原 TraceID/SpanID/采样标志/Baggage。
- 组件 B 提取到 A 的上下文后，用 `Tracer.StartWithParent` 把自己的服务端跨度
  挂到 A 的链路下（见 `testtraffic` 的 `hop`）。

**关键不变量：采样标志随链路逐跳传播、永不被下游重新随机。** 上游一旦采样，
所有下游组件结论一致，从协议层面保证“已采样链路完整性”。

---

## 2. 跨度生命周期与结构自洽

### 2.1 父子归属在“创建时”确定

`Tracer.Start` 从 `context.Context` 取当前跨度作为父，父子关系一旦建立即固定。
因此：

- **父晚于子结束（乱序）**：不影响归属，报告按 ParentID 重建树；
- **跨协程传递**：把携带父跨度的 ctx 交给新 goroutine 即可延续归属，
  即使父先结束、子在别的协程后结束也不会错归属或悬挂；
- 报告 `BuildReport` 不依赖结束顺序：先建索引→按 ParentID 链接→按开始时间
  确定性排序。父跨度未被采集到时记为 **Orphan（孤儿，虚拟根）**，不丢失；
  并附带父子环检测（`CycleDetected`）。

### 2.2 结束语义：幂等、不重复、不悬挂

`Span.End` 由 `sync.Once` 保护：

- **重复结束**幂等：只有第一次生效，跨度恰好交接下游一次；额外调用计入
  `DuplicateEnds`，但跨度自身结论仍为 `normal`（重复信息独立可查，不污染结论）。
- **缺失结束**：超过 `MaxSpanAge` 未结束的跨度由 sweeper（或 `Shutdown`）
  `forceClose` 强制收口，终态为 `missing_end_force_closed`，状态置为 error 并
  附带说明。`forceClose` 与并发 `End` 竞争时仍由 `sync.Once` 保证恰好交接一次。
- 结束时间早于开始时间会被钳制到开始时间；结束后 `SetStatus` 无效，保证结论稳定。

跨度结束方式在报告中显式呈现：`normal` / `missing_end_force_closed`
（重复结束次数单列 `DuplicateEnds`）。

---

## 3. 采样规则

`trace.NewSampler(SamplerConfig{Fraction, Rule})`：

1. **子跨度只继承**：父上下文有效时，严格沿用父链采样标志，返回理由
   `inherited_sampled_flag`（采样）或 `root_trace_dropped`（丢弃）。
2. **仅根跨度做判定**，规则优先于概率：
   - `Rule` 命中 → 采样，理由 `rule_match`；
   - 否则按 `Fraction∈[0,1]` 概率：采样 `root_trace_sampled`，
     丢弃 `probability_filter`；
   - `Fraction<0` 钳为 0，`>1` 钳为 1。
3. 未采样跨度不进入导出管线，但每次丢弃都通过 `SpanSink.RecordDrop` 以
   **可区分理由**登记（类别 `unsampled`，明细为上述具体理由）。

> 概率源可用 `trace.SetSamplingRollForTest(seed)` 注入，保证测试确定性。

---

## 4. 背压与有界资源

导出管线 `export.Processor` 是有界的，资源占用 = **队列在队 + 已出队正在导出
（leased）**，统一以 `QueueSize` 为硬上界；已出队批次在到达终态（成功或死信）
前持续占用名额。

突发流量下的准入规则：

- **新链路**（该 TraceID 尚无线缆被接纳）在管线满时**立即拒绝、绝不阻塞调用方**，
  记背压丢弃 `queue_full`。等价于在突发入口对**整条链路**做降入，而不是撕裂在途链路。
- **已接纳链路的后续跨度**：管线满时等待名额，上限 `BlockOnFull`
  （默认取 `ExportTimeout`；显式负值表示不等待的激进模式）。等待期间调用方被
  “背压传导”而不是被丢弃；导出恢复、名额释放后继续入队。由此保证**被采样链路
  不会因局部丢弃而缺跨度**。
- 状态可查询：`ProcessorStats` 提供 `InQueue / Leased / InFlight / Received /
  Exported / BackpressureDrops / UnsampledDrops / Retried / DeadLetters /
  LastFailure / Available`，并有恒等式
  `Exported + Dropped = Received`、`InFlight ≤ QueueSize`。

死信缓冲区同样有界（`DeadLetterCap`，超出覆盖最旧条目），故障再多也不膨胀内存。

---

## 5. 导出失败语义

底层只需实现 `export.Exporter`；失败用 `export.FailureError{Reason, Err}` 携带
**可区分原因**：

| Reason | 含义 | 可重试 |
|---|---|---|
| `timeout` | 单次导出超过 `ExportTimeout` | 是 |
| `unavailable` | 导出端不可用（连接失败/5xx 等） | 是 |
| `queue_full` | 导出端容量满 | 是 |
| `rejected` | 明确拒绝（格式非法等终态错误） | 否 |
| `shutdown` | 导出器已关闭 | 否 |

处理规则：

- **不阻塞调用方**：`Accept` 立即返回；导出只发生在后台 worker / `Flush` /
  `Shutdown`。导出端不可用时调用路径不受影响。
- **重试**：可重试原因按 `MaxAttempts`（含首次）重试，线性退避 `RetryBackoff`，
  每次尝试都受 `ExportTimeout` 约束，退避可被关闭/取消打断。
- **不丢关键诊断**：重试耗尽或不可重试错误 → 批次进入**死信**，保留
  TraceID/SpanID 列表、原因、时间、错误明细（`DeadLetters()` 可查）。
  批次一经取出即不回队，避免与后续批次混批或重复导出。
- **恢复后确定语义继续**：成功导出即把 `Available` 置回 true；失败只影响当批，
  后续批次照常按相同规则处理。`Stats().LastFailure` 始终反映最近一次原因。
- **优雅关闭**：`ShutdownCtx` 停止接收→等待在导批次终态→排空队列做最后导出
  →关闭底层导出器；残留/在途批次同样受超时与死信保护。

`export` 内置两个进程内导出器便于测试：

- `MemoryExporter`：保留成功导出的跨度，供最终内容断言；
- `FlakyExporter`：按“第 n 次调用”编排 `timeout/unavailable/rejected/恢复`，
  并记录完整调用历史。

---

## 6. 配置项一览

### `trace.TracerConfig`

| 字段 | 说明 | 默认 |
|---|---|---|
| `Service` | 组件/服务名 | `unknown-service` |
| `Sampler` | 采样器 | `AlwaysSample` |
| `Sink` | 已结束跨度的下游（通常为 `*export.Processor`） | 无（仅采集） |
| `Now` | 可注入时钟（测试用 `trace.Clock`） | `time.Now` |
| `MaxSpanAge` | 未结束跨度判定为缺失结束的阈值 | 30s |
| `SweepInterval` | 后台扫描周期，`0` 关闭后台扫描 | 关闭 |
| `CloseSinkOnShutdown` | 是否随自身 Shutdown 级联关闭 Sink | `true` |

多组件共享同一管线时，用 `trace.NewTracerSharedSink` 构造（不级联关闭），
由管线拥有方统一 `Processor.ShutdownCtx`。

### `export.ProcessorConfig`

| 字段 | 说明 | 默认 |
|---|---|---|
| `Exporter` | 底层导出端 | 必填 |
| `QueueSize` | 有界容量（在队+在导硬上界） | 1024 |
| `BatchSize` | 单批最大跨度/立即导出阈值 | 64 |
| `FlushInterval` | 定时导出周期 | 1s |
| `ExportTimeout` | 单次导出尝试截止时间 | 2s |
| `MaxAttempts` | 每批最大尝试次数（含首次） | 3 |
| `RetryBackoff` | 重试线性退避基数 | 10ms |
| `DeadLetterCap` | 死信条目保留上限 | 128 |
| `BlockOnFull` | 在途链路满时的有界等待上界；`0`=默认取 `ExportTimeout`；负值=不等待（激进模式） | `ExportTimeout` |

---

## 7. 一致性结论如何被验证

- 单测日志均打印**输入**与**判定依据**（见各 `*_test.go` 的 `t.Logf`）。
- 并发乱序：4 类场景 × 多链路 × 多协程，逐条校验拓扑、跨度数、无孤儿/环/丢失。
- 容量超限：突发 800 跨度 + 小容量 + 冻结导出，校验
  `Exported + BackpressureDrops = 输入总数`、`InFlight ≤ 容量`、
  每条成功导出链路恰好完整（4 跨度）。
- 导出失败：故障窗口内 `LastFailure.Reason=unavailable` 可区分、死信保留
  SpanID、恢复后继续导出且计数守恒。
