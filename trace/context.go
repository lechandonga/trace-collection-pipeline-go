package trace

// SpanContext 是跨组件传播的追踪上下文。
type SpanContext struct {
	TraceID  TraceID
	SpanID   SpanID
	ParentID SpanID
	// Sampled 表示当前链路是否已被采样；该标志沿传播链保持一致，
	// 用于保证已采样链路的完整性。
	Sampled bool
	// Baggage 随上下文跨组件传播的业务附带信息。
	Baggage map[string]string
}

// IsValid 判断上下文是否携带有效 TraceID/SpanID。
func (c SpanContext) IsValid() bool { panic("not implemented") }

// TextMapCarrier 是跨进程传播所用的字符串键值载体
// （例如 HTTP Header / RPC Metadata）。
type TextMapCarrier map[string]string

// Propagator 定义上下文注入/提取协议。
type Propagator interface {
	Inject(sc SpanContext, carrier TextMapCarrier)
	Extract(carrier TextMapCarrier) (SpanContext, error)
	Fields() []string
}

// textMapPropagator 是默认的私有传播器实现。
type textMapPropagator struct{}

// DefaultPropagator 返回默认传播器（traceparent 风格协议）。
func DefaultPropagator() Propagator { panic("not implemented") }

// InjectContext 使用默认传播器注入上下文。
func InjectContext(sc SpanContext, carrier TextMapCarrier) { panic("not implemented") }

// ExtractContext 使用默认传播器提取上下文。
func ExtractContext(carrier TextMapCarrier) (SpanContext, error) { panic("not implemented") }
