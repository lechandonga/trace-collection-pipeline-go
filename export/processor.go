package export

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	trace "github.com/lechandonga/trace-collection-pipeline-go/trace"
)

// ProcessorConfig 配置批处理导出器。
type ProcessorConfig struct {
	Exporter Exporter
	// QueueSize 有界等待队列容量（资源占用上界）。
	QueueSize int
	// BatchSize 单次导出的最大跨度数量，也是触发立即导出的阈值。
	BatchSize int
	// FlushInterval 定时导出周期。
	FlushInterval time.Duration
	// ExportTimeout 单次导出尝试的截止时间。
	ExportTimeout time.Duration
	// MaxAttempts 每批最大尝试次数（含首次）；不可重试错误立即放弃。
	MaxAttempts int
	// RetryBackoff 重试基础退避时长（按尝试次数线性增长）。
	RetryBackoff time.Duration
	// DeadLetterCap 死信诊断信息保留条数上限（有界）。
	DeadLetterCap int
	// BlockOnFull 队列满时，对“已进入队列的链路”允许的短暂等待上界；
	// 0 表示不等待。作用是让突发只在入口处整链降入，而不撕裂在途链路。
	BlockOnFull time.Duration
	Now         func() time.Time
}

// ProcessorStats 是 Processor 的可查询状态快照。
type ProcessorStats struct {
	Received          int64
	Exported          int64
	Dropped           int64
	UnsampledDrops    int64
	BackpressureDrops int64
	Retried           int64
	InQueue           int
	Leased            int // 已出队、正在导出中的跨度数
	InFlight          int // InQueue + Leased：真实资源占用（不超过 QueueSize）
	AdmittedTraces    int
	DeadLetters       int
	LastFailure       *Failure
	Available         bool
}

// deadLetterEntry 保留导出彻底失败批次的关键诊断信息。
type deadLetterEntry struct {
	Time    time.Time
	Reason  FailureReason
	TraceID trace.TraceID
	SpanIDs []trace.SpanID
	Detail  string
}

// DeadLetterView 是死信条目的只读视图。
type DeadLetterView struct {
	Time    time.Time
	Reason  FailureReason
	TraceID trace.TraceID
	SpanIDs []trace.SpanID
	Detail  string
}

// Processor 是有界、带重试与状态查询的批处理导出管线。
// 它实现 trace.SpanSink：非阻塞（或极短有界等待）接收，后台批量导出。
type Processor struct {
	cfg ProcessorConfig

	mu          sync.Mutex
	queue       []*trace.Span
	leased      int // 已出队、正在导出（尚未终态）的跨度数
	traceRefs   map[trace.TraceID]int // 每条链路在“队列 + 导出在途”中的跨度总数
	deadLetters []deadLetterEntry
	lastFailure *Failure
	available   bool
	closed      bool
	notFull     *sync.Cond
	exportMu    sync.Mutex // 串行化实际导出尝试

	flushSignal chan struct{}
	stopCh      chan struct{}

	received          atomic.Int64
	exported          atomic.Int64
	dropped           atomic.Int64
	unsampledDrops    atomic.Int64
	backpressureDrops atomic.Int64
	retried           atomic.Int64

	wg sync.WaitGroup
}

// NewProcessor 创建批处理导出管线并启动后台工作协程。
func NewProcessor(cfg ProcessorConfig) *Processor {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.ExportTimeout <= 0 {
		cfg.ExportTimeout = 2 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 10 * time.Millisecond
	}
	if cfg.DeadLetterCap <= 0 {
		cfg.DeadLetterCap = 128
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	p := &Processor{
		cfg:         cfg,
		queue:       make([]*trace.Span, 0, cfg.QueueSize),
		traceRefs:   make(map[trace.TraceID]int),
		flushSignal: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	p.notFull = sync.NewCond(&p.mu)
	p.wg.Add(1)
	go p.worker()
	return p
}

// Accept 非阻塞接收一个已结束跨度（实现 trace.SpanSink）。
//
// 背压语义（保证在途已采样链路不被局部丢弃撕裂）：
//   - 队列满时，“尚未进入队列的新链路”立即被拒绝，由调用方记录
//     queue_full 原因 —— 等价于在突发入口对整条链路做降入，结论一致；
//   - 已经有跨度在队列中的链路，其后续跨度享受最多 BlockOnFull 的
//     有界等待（绝不无限阻塞调用方），等待超时同样以 queue_full 拒绝；
//   - 无论接受或拒绝都不影响其它链路。
func (p *Processor) Accept(span *trace.Span) bool {
	if span == nil {
		return false
	}
	p.received.Add(1)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.recordFailureLocked(FailureShutdown, errors.New("processor closed"), 1)
		p.dropped.Add(1)
		return false
	}
	_, inFlight := p.traceRefs[span.TraceID]
	for len(p.queue)+p.leased >= p.cfg.QueueSize {
		if !inFlight || p.cfg.BlockOnFull <= 0 {
			p.mu.Unlock()
			p.backpressureDrops.Add(1)
			p.dropped.Add(1)
			return false
		}
		// 在途链路：最多等待 BlockOnFull，绝不无限阻塞调用方。
		if !p.waitRoom(p.cfg.BlockOnFull) {
			p.mu.Unlock()
			p.backpressureDrops.Add(1)
			p.dropped.Add(1)
			return false
		}
	}
	p.queue = append(p.queue, span)
	p.traceRefs[span.TraceID]++
	shouldSignal := len(p.queue) >= p.cfg.BatchSize
	p.mu.Unlock()

	if shouldSignal {
		select {
		case p.flushSignal <- struct{}{}:
		default:
		}
	}
	return true
}

// waitRoom 在持有 p.mu 的情况下等待“真实占用”低于容量或管线关闭，
// 最多等待 timeout；返回 true 表示出现空位，false 表示超时/关闭。
// 唤醒源是批次终态（releaseBatchFinalized）与 Shutdown 的 Broadcast。
func (p *Processor) waitRoom(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	wake := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			p.notFull.Broadcast() // 超时后唤醒等待者重新检查
		case <-wake:
		}
	}()
	for len(p.queue)+p.leased >= p.cfg.QueueSize {
		if p.closed {
			close(wake)
			return false
		}
		p.notFull.Wait()
		select {
		case <-timer.C:
			close(wake)
			return len(p.queue)+p.leased < p.cfg.QueueSize
		default:
		}
	}
	close(wake)
	return true
}

// RecordDrop 实现 trace.SpanSink：登记在更早环节发生的丢弃及其原因。
func (p *Processor) RecordDrop(reason trace.DropReason, span *trace.Span, detail string) {
	switch reason {
	case trace.DropReasonUnsampled:
		p.unsampledDrops.Add(1)
	case trace.DropReasonQueueFull:
		p.backpressureDrops.Add(1)
		p.dropped.Add(1)
	default:
		p.dropped.Add(1)
	}
}

// Shutdown 实现 trace.SpanSink：优雅关闭。
// 停止接收新跨度，排空队列并做最后导出，再关闭底层导出器。
func (p *Processor) Shutdown() error {
	return p.ShutdownCtx(context.Background())
}

// ShutdownCtx 携带截止时间关闭管线。
func (p *Processor) ShutdownCtx(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.stopCh)
	p.notFull.Broadcast()
	p.mu.Unlock()

	p.wg.Wait()

	// 工作协程退出后队列归本协程独占：尽力排空。
	var lastErr error
	for {
		p.mu.Lock()
		batch := p.takeBatchLocked()
		p.mu.Unlock()
		if len(batch) == 0 {
			break
		}
		if err := p.exportBatch(ctx, batch); err != nil {
			lastErr = err // 死信已记录；继续排空，最后返回最后一个错误
		}
		p.releaseBatchFinalized(batch)
	}
	if err := p.cfg.Exporter.Shutdown(ctx); err != nil {
		return err
	}
	return lastErr
}

// Flush 立即导出当前队列中的全部跨度（按 BatchSize 分批）。
func (p *Processor) Flush(ctx context.Context) error {
	for {
		p.mu.Lock()
		batch := p.takeBatchLocked()
		p.mu.Unlock()
		if len(batch) == 0 {
			return nil
		}
		err := p.exportBatch(ctx, batch)
		p.releaseBatchFinalized(batch)
		if err != nil {
			return err
		}
	}
}

// takeBatchLocked 从队首取出至多 BatchSize 个跨度并计入 leased；
// 引用计数保持不变（这些跨度仍代表已被本管线接纳的资源）。
// 调用时必须持有 p.mu。
func (p *Processor) takeBatchLocked() []*trace.Span {
	if len(p.queue) == 0 {
		return nil
	}
	n := p.cfg.BatchSize
	if n > len(p.queue) {
		n = len(p.queue)
	}
	batch := append([]*trace.Span(nil), p.queue[:n]...)
	p.queue = append(p.queue[:0], p.queue[n:]...)
	p.leased += n
	return batch
}

// releaseBatchFinalized 在批次到达终态（成功或死信）后释放其占用：
// 减少 leased 与对应链路引用计数，并唤醒等待空位的提交者。
func (p *Processor) releaseBatchFinalized(batch []*trace.Span) {
	p.mu.Lock()
	p.leased -= len(batch)
	if p.leased < 0 {
		p.leased = 0
	}
	for _, s := range batch {
		p.decRef(s.TraceID)
	}
	p.mu.Unlock()
	p.notFull.Broadcast()
}

// Stats 返回可查询状态快照。
func (p *Processor) Stats() ProcessorStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ProcessorStats{
		Received:          p.received.Load(),
		Exported:          p.exported.Load(),
		Dropped:           p.dropped.Load(),
		UnsampledDrops:    p.unsampledDrops.Load(),
		BackpressureDrops: p.backpressureDrops.Load(),
		Retried:           p.retried.Load(),
		InQueue:           len(p.queue),
		Leased:            p.leased,
		InFlight:          len(p.queue) + p.leased,
		AdmittedTraces:    len(p.traceRefs),
		DeadLetters:       len(p.deadLetters),
		LastFailure:       p.lastFailure,
		Available:         p.available,
	}
}

// DeadLetters 返回死信诊断快照（有界保留）。
func (p *Processor) DeadLetters() []DeadLetterView {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]DeadLetterView, 0, len(p.deadLetters))
	for _, e := range p.deadLetters {
		out = append(out, DeadLetterView{
			Time: e.Time, Reason: e.Reason, TraceID: e.TraceID,
			SpanIDs: append([]trace.SpanID(nil), e.SpanIDs...), Detail: e.Detail,
		})
	}
	return out
}

func (p *Processor) worker() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.flushSignal:
		case <-ticker.C:
		}
		p.exportQueuedBatches()
	}
}

// exportQueuedBatches 导出当前队列；每批至多 BatchSize，直到队列空。
// 单个批次失败（已入死信）不影响继续处理后续批次。
func (p *Processor) exportQueuedBatches() {
	for {
		p.mu.Lock()
		batch := p.takeBatchLocked()
		p.mu.Unlock()
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ExportTimeout*time.Duration(p.cfg.MaxAttempts))
		err := p.exportBatch(ctx, batch)
		cancel()
		p.releaseBatchFinalized(batch)
		if err != nil {
			return // 避免在持续故障期空转；等待下一个 tick/flush 信号
		}
	}
}

// exportBatch 执行一批的导出，含可区分失败处理与重试。
//
// 语义：
//   - timeout/unavailable/queue_full 为可重试原因，按 MaxAttempts 重试，
//     采用可被 ctx 取消的线性退避；期间状态可通过 Stats 查询；
//   - rejected 为终态失败，立即进入死信，不浪费重试；
//   - 重试耗尽同样进入死信，保留 TraceID/SpanID/原因，不丢失关键诊断；
//   - 批次一经取出即不回队：失败只入死信，避免与后续批次混批或重复导出；
//   - 任一批次结果都不影响后续批次：成功即恢复 Available=true，后续按
//     相同确定语义继续。
func (p *Processor) exportBatch(ctx context.Context, batch []*trace.Span) error {
	if len(batch) == 0 {
		return nil
	}
	p.exportMu.Lock()
	defer p.exportMu.Unlock()

	var lastErr error
	for attempt := 1; attempt <= p.cfg.MaxAttempts; attempt++ {
		// 每次尝试都在 ExportTimeout 内完成；父 ctx 的截止时间仍优先。
		attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.ExportTimeout)
		err := p.cfg.Exporter.ExportSpans(attemptCtx, batch)
		cancel()
		if err == nil {
			p.exported.Add(int64(len(batch)))
			p.mu.Lock()
			p.available = true
			p.mu.Unlock()
			return nil
		}
		lastErr = err
		reason := classifyError(err)
		p.recordFailure(reason, err, len(batch))
		if !reason.Retryable() || attempt == p.cfg.MaxAttempts {
			p.toDeadLetter(batch, reason, err)
			p.dropped.Add(int64(len(batch)))
			return err
		}
		p.retried.Add(1)
		// 线性退避，可被关闭/取消打断。
		wait := time.Duration(attempt) * p.cfg.RetryBackoff
		select {
		case <-ctx.Done():
			p.toDeadLetter(batch, FailureTimeout, ctx.Err())
			p.dropped.Add(int64(len(batch)))
			return ctx.Err()
		case <-p.stopCh:
			p.toDeadLetter(batch, reason, err)
			p.dropped.Add(int64(len(batch)))
			return err
		case <-time.After(wait):
		}
	}
	return lastErr
}

func classifyError(err error) FailureReason {
	var fe *FailureError
	if errors.As(err, &fe) {
		return fe.Reason
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout
	}
	return FailureUnavailable
}

func (p *Processor) recordFailure(reason FailureReason, err error, count int) {
	p.recordFailureLocked(reason, err, count)
}

// 调用约定：本方法自带加锁。
func (p *Processor) recordFailureLocked(reason FailureReason, err error, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastFailure = &Failure{Time: p.cfg.Now(), Reason: reason, Err: err, Count: count}
	p.available = false
}

func (p *Processor) toDeadLetter(batch []*trace.Span, reason FailureReason, cause error) {
	ids := make([]trace.SpanID, 0, len(batch))
	var tid trace.TraceID
	for _, s := range batch {
		ids = append(ids, s.SpanID)
		if !tid.IsValid() {
			tid = s.TraceID
		}
	}
	detail := ""
	if cause != nil {
		detail = cause.Error()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := deadLetterEntry{
		Time: p.cfg.Now(), Reason: reason, TraceID: tid, SpanIDs: ids, Detail: detail,
	}
	if len(p.deadLetters) >= p.cfg.DeadLetterCap {
		// 有界保留：丢弃最旧条目（环形覆盖语义）。
		p.deadLetters = append(p.deadLetters[1:], entry)
	} else {
		p.deadLetters = append(p.deadLetters, entry)
	}
}

// decRef 减少链路在队引用计数，调用时持有 p.mu。
func (p *Processor) decRef(id trace.TraceID) {
	if n := p.traceRefs[id] - 1; n > 0 {
		p.traceRefs[id] = n
	} else {
		delete(p.traceRefs, id)
	}
}
