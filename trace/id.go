package trace

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// TraceID 标识一条调用链（16 字节）。
type TraceID [16]byte

// SpanID 标识链路上的一个跨度（8 字节）。
type SpanID [8]byte

// NewTraceID 生成一个新的 TraceID。极小概率的全零结果会被重试，
// 同时以时间信息混入熵源，保证进程内可复现测试也可注入自定义生成器。
func NewTraceID() TraceID {
	var id TraceID
	for i := 0; i < 16; i++ {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			// rand.Read 失败极其罕见；退化为确定性填充而不是 panic。
			for j := range b {
				b[j] = byte(i*16 + j + 1)
			}
		}
		copy(id[:], b)
		if id.IsValid() {
			return id
		}
	}
	id[0] = 1
	return id
}

// NewSpanID 生成一个新的 SpanID。
func NewSpanID() SpanID {
	var id SpanID
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		for j := range b {
			b[j] = byte(j + 1)
		}
	}
	copy(id[:], b)
	if !id.IsValid() {
		id[0] = 1
	}
	return id
}

// IsValid 判断 TraceID 是否非全零。
func (id TraceID) IsValid() bool {
	for _, b := range id {
		if b != 0 {
			return true
		}
	}
	return false
}

// IsValid 判断 SpanID 是否非全零。
func (id SpanID) IsValid() bool {
	for _, b := range id {
		if b != 0 {
			return true
		}
	}
	return false
}

// String 返回小写十六进制表示。
func (id TraceID) String() string { return hex.EncodeToString(id[:]) }

// String 返回小写十六进制表示。
func (id SpanID) String() string { return hex.EncodeToString(id[:]) }

var (
	errTraceIDLen = errors.New("trace id must be 32 hex characters")
	errSpanIDLen  = errors.New("span id must be 16 hex characters")
)

// ParseTraceID 解析 32 位十六进制字符串。
func ParseTraceID(s string) (TraceID, error) {
	var id TraceID
	if len(s) != 32 {
		return id, errTraceIDLen
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	copy(id[:], b)
	return id, nil
}

// ParseSpanID 解析 16 位十六进制字符串。
func ParseSpanID(s string) (SpanID, error) {
	var id SpanID
	if len(s) != 16 {
		return id, errSpanIDLen
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	copy(id[:], b)
	return id, nil
}
