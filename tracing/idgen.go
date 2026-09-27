package tracing

import "crypto/rand"

// IDGenerator 生成 TraceID / SpanID。抽成接口便于测试中固定种子。
type IDGenerator interface {
	NewTraceID() TraceID
	NewSpanID() SpanID
}

// RandomIDGenerator 用 crypto/rand 生成随机 ID，零值即可用。
type RandomIDGenerator struct{}

func (RandomIDGenerator) NewTraceID() TraceID {
	var id TraceID
	_, _ = rand.Read(id[:])
	return id
}

func (RandomIDGenerator) NewSpanID() SpanID {
	var id SpanID
	_, _ = rand.Read(id[:])
	return id
}

// fixedIDGenerator 供测试使用：按计数器生成可预测的非零 ID。
type fixedIDGenerator struct {
	traceSeed byte
	spanSeq   byte
}

func newFixedIDGenerator() *fixedIDGenerator {
	return &fixedIDGenerator{traceSeed: 0xA0, spanSeq: 0x10}
}

func (g *fixedIDGenerator) NewTraceID() TraceID {
	var id TraceID
	id[0] = g.traceSeed
	id[15] = g.traceSeed
	g.traceSeed++
	return id
}

func (g *fixedIDGenerator) NewSpanID() SpanID {
	var id SpanID
	id[0] = 0x00
	id[7] = g.spanSeq
	g.spanSeq++
	return id
}
