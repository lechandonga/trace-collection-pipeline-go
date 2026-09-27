package tracing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ProcessorConfig 配置批处理处理器的容量与批量行为。
type ProcessorConfig struct {
	// MaxQueueSize 为"已完整、待导出的 trace"队列上限（背压阈值）。
	MaxQueueSize int
	// MaxExportBatchSize 为单次导出最大跨度数（多条小 trace 会合并到一批）。
	MaxExportBatchSize int
	// BatchTimeout 为最长攒批等待。
	BatchTimeout time.Duration
	// Blocking=true：trace 队列满时阻塞调用方直至有空位/关闭，不丢已采样链路；
	// Blocking=false（默认）：队列满时整条 trace 立即降载并记因。
	Blocking bool
	// RetryInitial/RetryMax 控制可重试导出失败的指数退避。
	RetryInitial time.Duration
	RetryMax     time.Duration
	// DiagCapacity 为丢弃/失败诊断记录环容量。
	DiagCapacity int
}

// DefaultProcessorConfig 返回默认配置。
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		MaxQueueSize:       2048,
		MaxExportBatchSize: 512,
		BatchTimeout:       1 * time.Second,
		Blocking:           false,
		RetryInitial:       50 * time.Millisecond,
		RetryMax:           2 * time.Second,
		DiagCapacity:       256,
	}
}

// DropCounts 是丢弃/失败计数快照（按可区分原因分列）。
type DropCounts struct {
	QueueFull       int64 // 队列满时被整条降载的跨度数
	TraceShed       int64 // 属于已降载 trace 的后续跨度，或在队列/批次中被滤除的跨度
	ExportRejected  int64 // 导出端永久拒绝（不可重试）的跨度数
	BatchTooLarge   int64 // 单跨度批次仍超出后端限制
	ExportLost      int64 // 关闭时后端仍不可用，未能导出的跨度数
	StoppedRejected int64 // 管线已停止后提交被拒的跨度数
	ExportRetries   int64 // 可重试导出失败的批次次数
}

// DropRecord 是一条丢弃/失败诊断，回答"哪个链路、哪个跨度、为什么"。
type DropRecord struct {
	Time    time.Time
	Reason  DropReason
	TraceID TraceID
	SpanID  SpanID
	Name    string
	Detail  string
}

type atomicCounters struct {
	queueFull       atomic.Int64
	traceShed       atomic.Int64
	exportRejected  atomic.Int64
	batchTooLarge   atomic.Int64
	exportLost      atomic.Int64
	stoppedRejected atomic.Int64
	exportRetries   atomic.Int64
}

func (c *atomicCounters) snapshot() DropCounts {
	return DropCounts{
		QueueFull:       c.queueFull.Load(),
		TraceShed:       c.traceShed.Load(),
		ExportRejected:  c.exportRejected.Load(),
		BatchTooLarge:   c.batchTooLarge.Load(),
		ExportLost:      c.exportLost.Load(),
		StoppedRejected: c.stoppedRejected.Load(),
		ExportRetries:   c.exportRetries.Load(),
	}
}

// traceBatch 是一条"已全部结束"的 trace 的全部跨度，作为不可分割的导出单元。
type traceBatch struct {
	traceID TraceID
	spans   []SpanData
}

// traceBuffer 在 trace 完成前暂存其跨度。total 为该 trace 应有的跨度总数。
type traceBuffer struct {
	total int
	spans []SpanData
}

// BatchSpanProcessor 后台攒批、应用背压并驱动导出器。
//
// 完整性设计：跨度按 trace 分组缓冲，只有当某条 trace 的所有跨度都结束
// （正常 End，或在关闭时被 Tracer 以 abandoned 回收）时，该 trace 才作为
// 不可分割的整体进入待导出队列。因此：
//   - 父子乱序结束：trace 在"最后一个跨度结束"时才放行，顺序无关；
//   - 队列满：降载单位是整条 trace，后端永远不会收到残缺链路；
//   - 已缓冲的 trace 被降载：缓冲与待发批次中的同 trace 跨度一律滤除。
type BatchSpanProcessor struct {
	cfg ProcessorConfig
	exp Exporter

	queue    chan traceBatch
	stop     chan struct{}
	flushCh  chan flushReq
	stopOnce sync.Once
	wg       sync.WaitGroup

	doneCtx     context.Context
	cancelDone  context.CancelFunc
	shutdownCtx context.Context // Shutdown 时设置，排空导出用它限制重试

	mu      sync.Mutex
	stopped bool
	pending map[TraceID]*traceBuffer // 未完成 trace 的跨度缓冲
	shed    map[TraceID]struct{}     // 已整条降载的 trace
	queuedN map[TraceID]int          // 各 trace 在 queue 中的批次数（正常为 0/1）

	cts atomicCounters

	diagMu sync.Mutex
	diag   []DropRecord

	lastFailMu  sync.Mutex
	lastFailRsn ExportFailureReason
	lastFailAt  time.Time
}

// NewBatchSpanProcessor 创建批处理器并规整配置。
func NewBatchSpanProcessor(exp Exporter, cfg ProcessorConfig) *BatchSpanProcessor {
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = 2048
	}
	if cfg.MaxExportBatchSize <= 0 || cfg.MaxExportBatchSize > cfg.MaxQueueSize {
		cfg.MaxExportBatchSize = 512
		if cfg.MaxExportBatchSize > cfg.MaxQueueSize {
			cfg.MaxExportBatchSize = cfg.MaxQueueSize
		}
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = time.Second
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = 50 * time.Millisecond
	}
	if cfg.RetryMax <= 0 {
		cfg.RetryMax = 2 * time.Second
	}
	if cfg.DiagCapacity <= 0 {
		cfg.DiagCapacity = 256
	}
	p := &BatchSpanProcessor{
		cfg:     cfg,
		exp:     exp,
		queue:   make(chan traceBatch, cfg.MaxQueueSize),
		stop:    make(chan struct{}),
		flushCh: make(chan flushReq, 1),
		pending: make(map[TraceID]*traceBuffer),
		shed:    make(map[TraceID]struct{}),
		queuedN: make(map[TraceID]int),
	}
	p.doneCtx, p.cancelDone = context.WithCancel(context.Background())
	p.shutdownCtx = p.doneCtx
	return p
}

// EnqueueFinished 实现 SpanSink。完整度由 siblings（trace 跨度总数）保证：
// 跨度先进入对应 trace 的缓冲；缓冲凑齐后整条 trace 才尝试进入有界队列。
func (p *BatchSpanProcessor) EnqueueFinished(data SpanData, siblings int) (bool, DropReason) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		p.cts.stoppedRejected.Add(1)
		p.recordDiag(DropReasonStopped, data, "enqueue after shutdown")
		return false, DropReasonStopped
	}
	if _, bad := p.shed[data.TraceID]; bad {
		p.mu.Unlock()
		p.cts.traceShed.Add(1)
		p.recordDiag(DropReasonTraceShed, data, "span belongs to an already shed trace")
		return false, DropReasonTraceShed
	}

	buf := p.pending[data.TraceID]
	if buf == nil {
		buf = &traceBuffer{total: siblings}
		p.pending[data.TraceID] = buf
	}
	if siblings > buf.total {
		buf.total = siblings
	}
	buf.spans = append(buf.spans, data)

	if len(buf.spans) < buf.total {
		// trace 尚未全部结束：继续缓冲，调用方视为已接受（内存中有界于在途 trace）。
		p.mu.Unlock()
		return true, ""
	}
	// trace 完整：从 pending 取出，准备作为整体放行。
	complete := buf.spans
	delete(p.pending, data.TraceID)
	blocking := p.cfg.Blocking
	p.mu.Unlock()

	tb := traceBatch{traceID: data.TraceID, spans: complete}
	if blocking {
		select {
		case p.queue <- tb:
			p.accountQueued(data.TraceID, 1)
			return true, ""
		case <-p.stop:
			p.cts.stoppedRejected.Add(int64(len(complete)))
			for _, sd := range complete {
				p.recordDiag(DropReasonStopped, sd, "blocked enqueue interrupted by shutdown")
			}
			return false, DropReasonStopped
		}
	}

	select {
	case p.queue <- tb:
		p.accountQueued(data.TraceID, 1)
		return true, ""
	default:
		// 队列满：整条 trace 降载。杜绝局部丢弃造成残缺链路。
		p.mu.Lock()
		p.shed[data.TraceID] = struct{}{}
		p.mu.Unlock()
		p.cts.queueFull.Add(int64(len(complete)))
		for _, sd := range complete {
			p.recordDiag(DropReasonQueueFull, sd, "queue full: whole trace shed to preserve integrity of other traces")
		}
		return false, DropReasonQueueFull
	}
}

func (p *BatchSpanProcessor) accountQueued(t TraceID, delta int) {
	p.mu.Lock()
	p.queuedN[t] += delta
	p.mu.Unlock()
}

// Start 启动后台导出循环。
func (p *BatchSpanProcessor) Start(ctx context.Context) error {
	p.wg.Add(1)
	go p.run()
	return nil
}

// flushReq 是外部 -> worker 的排空请求。done 在排空完成后关闭。
type flushReq struct {
	ctx  context.Context
	done chan struct{}
}

func (p *BatchSpanProcessor) run() {
	defer p.wg.Done()

	batch := make([]SpanData, 0, p.cfg.MaxExportBatchSize)
	timer := time.NewTimer(p.cfg.BatchTimeout)
	defer timer.Stop()

	exportBatch := func(ctx context.Context) {
		batch = p.filterShed(batch)
		p.exportOne(ctx, batch)
		batch = batch[:0]
		p.gcShedAfterFlush()
		timer.Reset(p.cfg.BatchTimeout)
	}
	admitBatch := func(tb traceBatch) {
		p.accountQueued(tb.traceID, -1)
		if p.isShed(tb.traceID) {
			p.cts.traceShed.Add(int64(len(tb.spans)))
			for _, sd := range tb.spans {
				p.recordDiag(DropReasonTraceShed, sd, "complete trace was queued when it got shed")
			}
			return
		}
		batch = append(batch, tb.spans...)
		if len(batch) >= p.cfg.MaxExportBatchSize {
			exportBatch(p.doneCtx)
		}
	}
	// drainAll 收走所有已完整 trace 并导出；关闭时把尚未完整的 pending 也强制放行
	// （Pipeline 关闭前 Tracer 已回收缺失结束跨度，故常规路径 pending 为空）。
	drainAll := func(ctx context.Context, flushPending bool) {
		if flushPending {
			p.mu.Lock()
			var forced []traceBatch
			for id, buf := range p.pending {
				forced = append(forced, traceBatch{traceID: id, spans: buf.spans})
				delete(p.pending, id)
			}
			p.mu.Unlock()
			for _, tb := range forced {
				for _, sd := range tb.spans {
					p.recordDiag(DropReasonTraceShed, sd, "incomplete trace forced out at processor shutdown")
				}
				admitBatch(tb)
			}
		}
		for {
			select {
			case tb := <-p.queue:
				admitBatch(tb)
				if len(batch) >= p.cfg.MaxExportBatchSize {
					batch = p.filterShed(batch)
					p.exportOne(ctx, batch)
					batch = batch[:0]
				}
			default:
				if len(batch) > 0 {
					batch = p.filterShed(batch)
					p.exportOne(ctx, batch)
					batch = batch[:0]
				}
				p.gcShedAfterFlush()
				return
			}
		}
	}

	for {
		select {
		case tb := <-p.queue:
			admitBatch(tb)
		case <-timer.C:
			if len(batch) > 0 {
				exportBatch(p.doneCtx)
			} else {
				timer.Reset(p.cfg.BatchTimeout)
			}
		case req := <-p.flushCh:
			drainAll(req.ctx, false)
			close(req.done)
		case <-p.stop:
			drainAll(p.shutdownCtx, true)
			return
		}
	}
}

// filterShed 发送前滤除已降载 trace 的跨度，并计入 trace_shed。
func (p *BatchSpanProcessor) filterShed(batch []SpanData) []SpanData {
	kept := batch[:0]
	var shedSpans []SpanData
	p.mu.Lock()
	for _, sd := range batch {
		if _, bad := p.shed[sd.TraceID]; bad {
			shedSpans = append(shedSpans, sd)
			continue
		}
		kept = append(kept, sd)
	}
	p.mu.Unlock()
	for _, sd := range shedSpans {
		p.cts.traceShed.Add(1)
		p.recordDiag(DropReasonTraceShed, sd, "span filtered from outgoing batch")
	}
	return kept
}

func (p *BatchSpanProcessor) isShed(t TraceID) bool {
	p.mu.Lock()
	_, ok := p.shed[t]
	p.mu.Unlock()
	return ok
}

// gcShedAfterFlush 清理已无在途批次的降载标记，集合大小受 MaxQueueSize 上限约束。
func (p *BatchSpanProcessor) gcShedAfterFlush() {
	p.mu.Lock()
	for t, n := range p.queuedN {
		if n <= 0 {
			delete(p.queuedN, t)
			delete(p.shed, t)
		}
	}
	if len(p.shed) > p.cfg.MaxQueueSize {
		excess := len(p.shed) - p.cfg.MaxQueueSize
		for t := range p.shed {
			delete(p.shed, t)
			excess--
			if excess == 0 {
				break
			}
		}
	}
	p.mu.Unlock()
}

// exportOne 发送一批跨度并按失败语义处理：
//   - 可重试（unavailable 等）：指数退避重试，数据保留不丢；
//   - too_large：对半拆分重试，确定地收敛到单跨度；
//   - 其他不可重试：丢弃并按原因计数，不无限重试。
//
// ctx 取消时未发出的跨度计入 ExportLost。
func (p *BatchSpanProcessor) exportOne(parentCtx context.Context, batch []SpanData) {
	if len(batch) == 0 {
		return
	}
	backoff := p.cfg.RetryInitial
	for {
		err := p.exp.ExportSpans(parentCtx, batch)
		if err == nil {
			return
		}
		ee, ok := AsExportError(err)
		if !ok {
			ee = &ExportError{Reason: ExportFailureUnavailable, Retryable: true, Err: err}
		}
		p.noteFailure(ee.Reason)

		if ee.Reason == ExportFailureTooLarge && len(batch) > 1 {
			mid := len(batch) / 2
			p.exportOne(parentCtx, batch[:mid])
			p.exportOne(parentCtx, batch[mid:])
			return
		}

		if !ee.Retryable {
			for _, sd := range batch {
				if ee.Reason == ExportFailureTooLarge {
					p.cts.batchTooLarge.Add(1)
					p.recordDiag(DropReasonBatchTooLarge, sd, "single-span batch still too large")
				} else {
					p.cts.exportRejected.Add(1)
					p.recordDiag(DropReasonRejected, sd, "exporter permanently rejected: "+string(ee.Reason))
				}
			}
			return
		}

		// 可重试：等待退避，期间响应关闭/取消。
		p.cts.exportRetries.Add(1)
		select {
		case <-parentCtx.Done():
			for _, sd := range batch {
				p.cts.exportLost.Add(1)
				p.recordDiag(DropReasonExportLost, sd, "exporter unavailable when ctx ended: "+ee.Error())
			}
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > p.cfg.RetryMax {
			backoff = p.cfg.RetryMax
		}
	}
}

func (p *BatchSpanProcessor) noteFailure(r ExportFailureReason) {
	p.lastFailMu.Lock()
	p.lastFailRsn = r
	p.lastFailAt = time.Now()
	p.lastFailMu.Unlock()
}

// Flush 请求 worker 排空并导出当前全部"已完整"的 trace。
// 重试受 ctx 限制；ctx 结束时仍未发出的跨度计入 ExportLost 并有逐条诊断。
func (p *BatchSpanProcessor) Flush(ctx context.Context) error {
	req := flushReq{ctx: ctx, done: make(chan struct{})}
	select {
	case p.flushCh <- req:
		<-req.done
		return nil
	case <-p.stop:
		return nil
	}
}

// Shutdown 顺序：停止接收 -> 中断重试退避 -> worker 放行 pending 并排空队列
// （重试受 ctx 限制）-> 关闭导出器。后端持续不可用时未导出跨度计入 ExportLost。
func (p *BatchSpanProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.shutdownCtx = ctx
	p.stopOnce.Do(func() { close(p.stop) })
	p.mu.Unlock()

	p.cancelDone()
	p.wg.Wait()
	return p.exp.Shutdown(ctx)
}

// Stats 返回丢弃/失败计数快照。
func (p *BatchSpanProcessor) Stats() DropCounts { return p.cts.snapshot() }

// PendingTraces 返回尚未凑齐的 trace 缓冲数量（可观测性/测试）。
func (p *BatchSpanProcessor) PendingTraces() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}

// Diagnostics 返回诊断记录副本（有界环形，最多 DiagCapacity 条）。
func (p *BatchSpanProcessor) Diagnostics() []DropRecord {
	p.diagMu.Lock()
	defer p.diagMu.Unlock()
	out := make([]DropRecord, len(p.diag))
	copy(out, p.diag)
	return out
}

func (p *BatchSpanProcessor) recordDiag(reason DropReason, sd SpanData, detail string) {
	p.diagMu.Lock()
	defer p.diagMu.Unlock()
	p.appendDiagLocked(reason, sd, detail)
}

// recordDiagLocked 已移除：诊断写入不得与 p.mu 嵌套（避免锁序反转）。

func (p *BatchSpanProcessor) appendDiagLocked(reason DropReason, sd SpanData, detail string) {
	p.diag = append(p.diag, DropRecord{
		Time:    time.Now(),
		Reason:  reason,
		TraceID: sd.TraceID,
		SpanID:  sd.SpanID,
		Name:    sd.Name,
		Detail:  detail,
	})
	if len(p.diag) > p.cfg.DiagCapacity {
		p.diag = p.diag[len(p.diag)-p.cfg.DiagCapacity:]
	}
}

// ExportState 综合导出器状态给出管线导出状态。
func (p *BatchSpanProcessor) ExportState() ExportState {
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return ExportStateStopped
	}
	if sr, ok := p.exp.(StateReader); ok {
		return sr.State()
	}
	return ExportStateUnknown
}

// LastExportFailure 返回最近一次导出失败类别与时间。
func (p *BatchSpanProcessor) LastExportFailure() (ExportFailureReason, time.Time, bool) {
	p.lastFailMu.Lock()
	defer p.lastFailMu.Unlock()
	return p.lastFailRsn, p.lastFailAt, !p.lastFailAt.IsZero()
}

// Queued 返回当前完整 trace 队列中的批次数。
func (p *BatchSpanProcessor) Queued() int { return len(p.queue) }
