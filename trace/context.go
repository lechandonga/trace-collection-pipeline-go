package trace

import (
	"errors"
	"net/url"
	"sort"
	"strings"
)

// SpanContext 是跨组件传播的追踪上下文。
type SpanContext struct {
	TraceID  TraceID
	SpanID   SpanID
	ParentID SpanID
	// Sampled 表示当前链路是否已被采样；该标志沿传播链保持一致，
	// 用于保证已采样链路的完整性（上游一旦采样，下游不得局部丢弃）。
	Sampled bool
	// Baggage 随上下文跨组件传播的业务附带信息。
	Baggage map[string]string
}

// IsValid 判断上下文是否携带有效 TraceID/SpanID。
func (c SpanContext) IsValid() bool {
	return c.TraceID.IsValid() && c.SpanID.IsValid()
}

// TextMapCarrier 是跨进程传播所用的字符串键值载体
// （例如 HTTP Header / RPC Metadata）。
type TextMapCarrier map[string]string

// 传播协议使用的载体键与版本常量。
const (
	headerTraceParent = "traceparent"
	headerBaggage     = "trace-baggage"
	protocolVersion   = "00"
	flagSampled       = 0x01
)

// ErrInvalidContext 表示载体中的追踪上下文格式非法。
var ErrInvalidContext = errors.New("invalid trace context in carrier")

// Propagator 定义上下文注入/提取协议。
type Propagator interface {
	Inject(sc SpanContext, carrier TextMapCarrier)
	Extract(carrier TextMapCarrier) (SpanContext, error)
	Fields() []string
}

// textMapPropagator 是默认的私有传播器实现。
//
// 线格式（traceparent 风格，与 W3C Trace Context 兼容）：
//
//	traceparent: 00-<32hex traceID>-<16hex spanID>-<2hex flags>
//	trace-baggage: k1=v1,k2=v2          （键值经 query 转义，可省略）
//
// flags 的最低位为采样标志：它随链路传播保持不变，
// 这样所有下游组件对“该链路是否采样”得出一致结论。
type textMapPropagator struct{}

// DefaultPropagator 返回默认传播器（traceparent 风格协议）。
func DefaultPropagator() Propagator { return textMapPropagator{} }

// Fields 返回传播器写入载体的全部键名。
func (textMapPropagator) Fields() []string {
	return []string{headerTraceParent, headerBaggage}
}

// Inject 把上下文写入载体。
func (p textMapPropagator) Inject(sc SpanContext, carrier TextMapCarrier) {
	if carrier == nil || !sc.IsValid() {
		return
	}
	var flags byte
	if sc.Sampled {
		flags |= flagSampled
	}
	carrier[headerTraceParent] = protocolVersion + "-" +
		sc.TraceID.String() + "-" +
		sc.SpanID.String() + "-" +
		toHex2(flags)
	if len(sc.Baggage) > 0 {
		carrier[headerBaggage] = encodeBaggage(sc.Baggage)
	}
}

// Extract 从载体提取上下文。载体中不存在 traceparent 时返回零值上下文与
// nil（视为一次全新的根调用）；格式非法时返回 ErrInvalidContext。
func (p textMapPropagator) Extract(carrier TextMapCarrier) (SpanContext, error) {
	if carrier == nil {
		return SpanContext{}, nil
	}
	raw, ok := carrier[headerTraceParent]
	if !ok || strings.TrimSpace(raw) == "" {
		return SpanContext{}, nil
	}
	parts := strings.Split(raw, "-")
	if len(parts) != 4 || parts[0] != protocolVersion || len(parts[3]) != 2 {
		return SpanContext{}, ErrInvalidContext
	}
	tid, err := ParseTraceID(parts[1])
	if err != nil {
		return SpanContext{}, errors.Join(ErrInvalidContext, err)
	}
	sid, err := ParseSpanID(parts[2])
	if err != nil {
		return SpanContext{}, errors.Join(ErrInvalidContext, err)
	}
	flags, err := fromHex2(parts[3])
	if err != nil {
		return SpanContext{}, errors.Join(ErrInvalidContext, err)
	}
	sc := SpanContext{
		TraceID: tid,
		SpanID:  sid,
		Sampled: flags&flagSampled != 0,
	}
	if b := carrier[headerBaggage]; b != "" {
		sc.Baggage = decodeBaggage(b)
	}
	return sc, nil
}

// InjectContext 使用默认传播器注入上下文。
func InjectContext(sc SpanContext, carrier TextMapCarrier) {
	DefaultPropagator().Inject(sc, carrier)
}

// ExtractContext 使用默认传播器提取上下文。
func ExtractContext(carrier TextMapCarrier) (SpanContext, error) {
	return DefaultPropagator().Extract(carrier)
}

// --- 小工具（确定性、无第三方依赖） ---

func toHex2(b byte) string {
	const hexChars = "0123456789abcdef"
	return string([]byte{hexChars[b>>4], hexChars[b&0x0f]})
}

func fromHex2(s string) (byte, error) {
	hi, ok1 := hexNibble(s[0])
	lo, ok2 := hexNibble(s[1])
	if !ok1 || !ok2 {
		return 0, ErrInvalidContext
	}
	return hi<<4 | lo, nil
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func encodeBaggage(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys) // 确定性顺序，便于比对与日志解释
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(m[k]))
	}
	return strings.Join(parts, ",")
}

func decodeBaggage(s string) map[string]string {
	out := map[string]string{}
	for _, item := range strings.Split(s, ",") {
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			continue // 容忍个别坏项，不影响上下文提取
		}
		k, err1 := url.QueryUnescape(kv[0])
		v, err2 := url.QueryUnescape(kv[1])
		if err1 == nil && err2 == nil {
			out[k] = v
		}
	}
	return out
}
