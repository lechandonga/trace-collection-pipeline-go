package trace

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SpanReport 是报告中单个跨度的自洽视图。
type SpanReport struct {
	SpanID        SpanID
	ParentID      SpanID
	Service       string
	Name          string
	Kind          SpanKind
	StartTime     time.Time
	EndTime       time.Time
	Duration      time.Duration
	StatusCode    SpanStatusCode
	StatusMessage string
	Attributes    map[string]string
	Events        []Event
	// EndKind 说明跨度是如何结束的，保证结论可解释。
	EndKind SpanEndKind
	// DuplicateEnds 为首次结束后又收到的 End 调用次数。
	DuplicateEnds int
	// Children 为子跨度 ID（按开始时间排序）。
	Children []SpanID
}

// SpanEndKind 描述跨度的结束方式。
type SpanEndKind string

// 跨度结束方式取值。
const (
	EndNormal    SpanEndKind = "normal"
	EndDuplicate SpanEndKind = "duplicate_end"
	EndMissing   SpanEndKind = "missing_end_force_closed"
)

// TraceReport 是一条链路的完整、自洽结论。
type TraceReport struct {
	TraceID TraceID
	Sampled bool
	Roots   []SpanID
	Spans   map[SpanID]*SpanReport
	// OrphanIDs 是父跨度缺失（未采集到）的跨度 ID。
	OrphanIDs []SpanID
	// CycleDetected 标记是否检测到父子环（正常传播不应出现）。
	CycleDetected bool
}

// BuildReport 基于已结束跨度集合构建链路报告。
//
// 构造过程不依赖跨度结束顺序：先建索引，再按 ParentID 链接；
// 父缺失的跨度归入 OrphanIDs 并作为虚拟根展示，绝不丢弃。
// 同一 SpanID 多次出现（理论上由 End 幂等保证不会发生）按“后到忽略”处理
// 并可在此处显式计数。
func BuildReport(traceID TraceID, sampled bool, spans []*Span) (*TraceReport, error) {
	if !traceID.IsValid() {
		return nil, errors.New("build report: invalid trace id")
	}
	r := &TraceReport{
		TraceID: traceID,
		Sampled: sampled,
		Spans:   make(map[SpanID]*SpanReport, len(spans)),
	}

	byStart := make([]*Span, 0, len(spans))
	for _, s := range spans {
		if s == nil || s.TraceID != traceID {
			continue
		}
		if _, exists := r.Spans[s.SpanID]; exists {
			// 重复归属防护：同一 ID 只归并一次。
			continue
		}
		r.Spans[s.SpanID] = toReport(s)
		byStart = append(byStart, s)
	}
	// 按开始时间稳定排序，使结论与输出确定性可复现。
	sort.SliceStable(byStart, func(i, j int) bool {
		return byStart[i].StartTime.Before(byStart[j].StartTime)
	})

	// 链接父子；父不在集合中 => 孤儿（父跨度缺失）。
	for _, s := range byStart {
		if s.ParentID.IsValid() {
			if parent, ok := r.Spans[s.ParentID]; ok {
				parent.Children = append(parent.Children, s.SpanID)
			} else {
				r.OrphanIDs = append(r.OrphanIDs, s.SpanID)
				r.Roots = append(r.Roots, s.SpanID)
			}
		} else {
			r.Roots = append(r.Roots, s.SpanID)
		}
	}
	r.CycleDetected = detectCycle(r)
	return r, nil
}

func toReport(s *Span) *SpanReport {
	dur, _ := s.Duration()
	endKind := s.EndKind()
	if endKind == "" {
		endKind = EndNormal
	}
	rep := &SpanReport{
		SpanID:        s.SpanID,
		ParentID:      s.ParentID,
		Service:       s.Service,
		Name:          s.Name,
		Kind:          s.Kind,
		StartTime:     s.StartTime,
		EndTime:       s.EndTime,
		Duration:      dur,
		StatusCode:    s.StatusCode,
		StatusMessage: s.StatusMessage,
		Attributes:    cloneAttrs(s.Attributes),
		Events:        s.Events(),
		EndKind:       endKind,
		DuplicateEnds: s.DuplicateEndCount(),
	}
	return rep
}

// detectCycle 用着色 DFS 检测父子环；返回 true 表示结构不自洽。
func detectCycle(r *TraceReport) bool {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[SpanID]int, len(r.Spans))
	var visit func(id SpanID) bool
	visit = func(id SpanID) bool {
		switch color[id] {
		case gray:
			return true
		case black:
			return false
		}
		color[id] = gray
		for _, ch := range r.Spans[id].Children {
			if _, ok := r.Spans[ch]; !ok {
				continue
			}
			if visit(ch) {
				return true
			}
		}
		color[id] = black
		return false
	}
	for id := range r.Spans {
		if color[id] == white && visit(id) {
			return true
		}
	}
	return false
}

// String 返回面向日志的可读摘要，包含输入规模与判定依据。
func (r *TraceReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "trace=%s sampled=%t spans=%d roots=%d orphans=%d cycle=%t\n",
		r.TraceID.String(), r.Sampled, len(r.Spans), len(r.Roots), len(r.OrphanIDs), r.CycleDetected)

	var walk func(id SpanID, depth int)
	walk = func(id SpanID, depth int) {
		s, ok := r.Spans[id]
		if !ok {
			return
		}
		fmt.Fprintf(&b, "%s- [%s/%s] %s kind=%s dur=%s status=%d end=%s",
			strings.Repeat("  ", depth),
			s.Service, s.SpanID.String()[:8], s.Name, s.Kind.String(),
			s.Duration, s.StatusCode, s.EndKind)
		if s.DuplicateEnds > 0 {
			fmt.Fprintf(&b, " duplicateEnds=%d", s.DuplicateEnds)
		}
		if s.StatusMessage != "" {
			fmt.Fprintf(&b, " msg=%q", s.StatusMessage)
		}
		b.WriteByte('\n')
		for _, ch := range s.Children {
			walk(ch, depth+1)
		}
	}
	printed := make(map[SpanID]bool)
	for _, root := range r.Roots {
		var printTree func(id SpanID, depth int)
		printTree = func(id SpanID, depth int) {
			if printed[id] {
				return
			}
			printed[id] = true
			walk(id, depth)
		}
		printTree(root, 0)
	}
	return b.String()
}
