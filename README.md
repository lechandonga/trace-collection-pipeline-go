# trace-collection-pipeline-go

进程内可直接运行的调用链追踪采集与导出组件：跨组件上下文传播、乱序/重复/缺失
结束下的链路自洽、采样与有界背压、导出失败可区分处理与恢复后确定续传。

## 快速开始

```bash
go run ./cmd/demo        # 进程内演示：乱序/重复/缺失结束/跨协程 + 报告与状态
go test ./... -race      # 覆盖并发乱序、容量超限、导出失败（日志含输入与判定依据）
```

## 包结构

- `trace/`：ID、传播协议、Span（幂等结束/缺失收口）、采样器、Tracer、链路报告
- `export/`：有界批处理 Processor（背压/重试/死信/状态查询）、内存与故障模拟导出器
- `testtraffic/`：gateway→order→payment/notify 多组件本地测试流量
- `cmd/demo/`：可运行演示
- `docs/design.md`：传播协议、采样与背压规则、导出失败语义、全部配置项说明
