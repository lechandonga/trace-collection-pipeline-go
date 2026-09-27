package trace

// TraceID 标识一条调用链（16 字节）。
type TraceID [16]byte

// SpanID 标识链路上的一个跨度（8 字节）。
type SpanID [8]byte

// NewTraceID 生成一个新的 TraceID。
func NewTraceID() TraceID { panic("not implemented") }

// NewSpanID 生成一个新的 SpanID。
func NewSpanID() SpanID { panic("not implemented") }

// IsValid 判断 TraceID 是否非全零。
func (id TraceID) IsValid() bool { panic("not implemented") }

// IsValid 判断 SpanID 是否非全零。
func (id SpanID) IsValid() bool { panic("not implemented") }

// String 返回小写十六进制表示。
func (id TraceID) String() string { panic("not implemented") }

// String 返回小写十六进制表示。
func (id SpanID) String() string { panic("not implemented") }

// ParseTraceID 解析 32 位十六进制字符串。
func ParseTraceID(s string) (TraceID, error) { panic("not implemented") }

// ParseSpanID 解析 16 位十六进制字符串。
func ParseSpanID(s string) (SpanID, error) { panic("not implemented") }
