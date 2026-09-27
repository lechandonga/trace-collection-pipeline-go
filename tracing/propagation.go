package tracing

import "errors"

// TraceID 标识一条调用链，全链路唯一（128 位）。
type TraceID [16]byte

// SpanID 标识链路中的一个跨度（64 位）。
type SpanID [8]byte

// SpanContext 携带跨组件传播所需的最小上下文。
type SpanContext struct {
	TraceID TraceID
	SpanID  SpanID
	Sampled bool
}

// IsValid 判断 SpanContext 是否可用于建立父子关系。
func (sc SpanContext) IsValid() bool {
	return sc.TraceID != TraceID{} && sc.SpanID != SpanID{}
}

// Propagator 负责跨度上下文在"组件边界"（这里用文本载体模拟）上的注入与提取。
//
// 传播协议（文本，3 个键）：
//
//	<prefix>traceid : 32 个十六进制字符
//	<prefix>spanid  : 16 个十六进制字符
//	<prefix>sampled : "1" 或 "0"
//
// 提取时只要载体缺少合法 traceid/spanid，即视为"无上游上下文"
// （返回零值 SpanContext 且不报错），由调用方开启新的根跨度；
// 仅在字段存在但格式非法时返回 ErrMalformedContext，便于区分故障原因。
type Propagator interface {
	Inject(sc SpanContext, carrier TextMapCarrier) error
	Extract(carrier TextMapCarrier) (SpanContext, error)
}

// TextMapCarrier 模拟跨组件传输载体（如 HTTP header / 消息队列属性）。
type TextMapCarrier interface {
	Set(key, value string)
	Get(key string) string
	Keys() []string
}

// MapCarrier 是 TextMapCarrier 的内存实现，用于本地测试流量。
type MapCarrier struct {
	values map[string]string
}

// NewMapCarrier 创建空的内存载体。
func NewMapCarrier() *MapCarrier {
	return &MapCarrier{values: make(map[string]string)}
}

// FromMap 用已有键值对构造载体（模拟"收到"一次远程调用）。
func FromMap(m map[string]string) *MapCarrier {
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return &MapCarrier{values: cp}
}

func (c *MapCarrier) Set(key, value string) { c.values[key] = value }

func (c *MapCarrier) Get(key string) string { return c.values[key] }

func (c *MapCarrier) Keys() []string {
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	return keys
}

// ErrMalformedContext 表示载体中存在格式非法的上下文字段。
var ErrMalformedContext = errors.New("malformed propagated span context")

const (
	defaultPrefix = "x-trace-"
	keyTraceID    = "traceid"
	keySpanID     = "spanid"
	keySampled    = "sampled"
)

// TextMapPropagator 是基于固定前缀键的传播器，零值可用。
type TextMapPropagator struct {
	prefix string
}

// NewTextMapPropagator 创建默认前缀（x-trace-）的传播器。
func NewTextMapPropagator() *TextMapPropagator {
	return &TextMapPropagator{prefix: defaultPrefix}
}

func (p *TextMapPropagator) k(suffix string) string {
	if p.prefix == "" {
		return defaultPrefix + suffix
	}
	return p.prefix + suffix
}

// Inject 把上下文写入载体。零值上下文返回 ErrMalformedContext 而不是静默传播垃圾数据。
func (p *TextMapPropagator) Inject(sc SpanContext, carrier TextMapCarrier) error {
	if carrier == nil {
		return errors.New("nil carrier")
	}
	if !sc.IsValid() {
		return ErrMalformedContext
	}
	carrier.Set(p.k(keyTraceID), hexEncode(sc.TraceID[:]))
	carrier.Set(p.k(keySpanID), hexEncode(sc.SpanID[:]))
	if sc.Sampled {
		carrier.Set(p.k(keySampled), "1")
	} else {
		carrier.Set(p.k(keySampled), "0")
	}
	return nil
}

// Extract 从载体读取上下文。缺字段 = 无上游（零值 + nil）；字段坏 = ErrMalformedContext。
func (p *TextMapPropagator) Extract(carrier TextMapCarrier) (SpanContext, error) {
	if carrier == nil {
		return SpanContext{}, errors.New("nil carrier")
	}
	rawTrace := carrier.Get(p.k(keyTraceID))
	rawSpan := carrier.Get(p.k(keySpanID))
	if rawTrace == "" && rawSpan == "" {
		return SpanContext{}, nil // 无上游上下文
	}
	var sc SpanContext
	tid, err := decodeTraceID(rawTrace)
	if err != nil {
		return SpanContext{}, ErrMalformedContext
	}
	sid, err := decodeSpanID(rawSpan)
	if err != nil {
		return SpanContext{}, ErrMalformedContext
	}
	var zeroTrace TraceID
	var zeroSpan SpanID
	if tid == zeroTrace || sid == zeroSpan {
		return SpanContext{}, ErrMalformedContext
	}
	sc.TraceID = tid
	sc.SpanID = sid
	switch carrier.Get(p.k(keySampled)) {
	case "1":
		sc.Sampled = true
	case "0", "":
		sc.Sampled = false
	default:
		return SpanContext{}, ErrMalformedContext
	}
	return sc, nil
}

func hexEncode(b []byte) string {
	const h = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = h[v>>4]
		out[i*2+1] = h[v&0x0f]
	}
	return string(out)
}

func decodeTraceID(s string) (TraceID, error) {
	var out TraceID
	if err := decodeHexInto(s, out[:]); err != nil {
		return TraceID{}, err
	}
	return out, nil
}

func decodeSpanID(s string) (SpanID, error) {
	var out SpanID
	if err := decodeHexInto(s, out[:]); err != nil {
		return SpanID{}, err
	}
	return out, nil
}

func decodeHexInto(s string, out []byte) error {
	n := len(out)
	if len(s) != n*2 {
		return ErrMalformedContext
	}
	for i := 0; i < n; i++ {
		hi, ok1 := fromHexChar(s[i*2])
		lo, ok2 := fromHexChar(s[i*2+1])
		if !ok1 || !ok2 {
			return ErrMalformedContext
		}
		out[i] = hi<<4 | lo
	}
	return nil
}

func fromHexChar(c byte) (byte, bool) {
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
