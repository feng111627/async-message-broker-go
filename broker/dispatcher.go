package broker

import (
	"time"
)

// run 是每个 (topic, group) 唯一的串行调度循环。
// 该循环是所有队列状态（ready/retry/inflight/consumers）的唯一修改者，
// 因此“同组同一条消息同一时间只投给一个消费者”由模型天然保证，无需额外加锁竞争。
func (gs *groupState) run() {
	defer close(gs.done)
	ticker := time.NewTicker(gs.cfg.SweepInterval)
	defer ticker.Stop()

	rr := 0 // 消费者轮询起点
	for {
		gs.promizeRetry()
		gs.tryDeliver(&rr)

		// 计算下一次需要醒来的时间：取最近的退避到期与可见性超时。
		nextWake := gs.cfg.SweepInterval
		if d := gs.nextRetryWake(); d > 0 && d < nextWake {
			nextWake = d
		}
		if d := gs.nextTimeoutIn(); d > 0 && d < nextWake {
			nextWake = d
		}
		timer := time.NewTimer(nextWake)

		select {
		case <-gs.stop:
			timer.Stop()
			return
		case req := <-gs.enqueueReq:
			timer.Stop()
			gs.handleEnqueue(req)
		case req := <-gs.settleReq:
			timer.Stop()
			gs.handleSettle(req)
		case req := <-gs.registerReq:
			timer.Stop()
			gs.handleRegister(req)
			gs.signalSpace()
		case req := <-gs.heartbeatReq:
			timer.Stop()
			gs.handleHeartbeat(req)
		case req := <-gs.reserveReq:
			timer.Stop()
			gs.handleReserve(req)
		case req := <-gs.statsReq:
			timer.Stop()
			req.res <- statsResp{
				ready:    len(gs.ready),
				retry:    len(gs.retry),
				inflight: len(gs.inflight),
				capacity: gs.capacity,
			}
		case req := <-gs.forceDrain:
			timer.Stop()
			gs.handleForceDrain()
			close(req.res)
		case <-timer.C:
			timer.Stop()
			gs.reclaimExpired(time.Now())
			// 周期性扫描：退避到期提升 + 超时回收 + 分发
		}
	}
}

// handleEnqueue 处理入队请求。容量按 ready+retry+inflight 的消息总数计算。
func (gs *groupState) handleEnqueue(req enqueueReq) {
	if gs.capacity > 0 && gs.inSystemCount() >= gs.capacity {
		req.res <- enqueueResp{accepted: false, err: ErrQueueFull}
		return
	}
	gs.ready = append(gs.ready, req.e)
	req.res <- enqueueResp{accepted: true}
	gs.logger("enqueue topic=%s group=%s id=%d depth=%d",
		gs.topic, gs.group, req.e.id, gs.inSystemCount())
}

// handleRegister 注册或移除消费者。
func (gs *groupState) handleRegister(req registerReq) {
	if req.removed {
		if _, ok := gs.consumers[req.c.id]; ok {
			delete(gs.consumers, req.c.id)
			gs.reclaimFromConsumer(req.c.id, "consumer closed")
		}
	} else {
		gs.consumers[req.c.id] = req.c
		gs.logger("consumer registered topic=%s group=%s consumer=%s total=%d",
			gs.topic, gs.group, req.c.id, len(gs.consumers))
	}
	close(req.done)
}

// handleSettle 处理消费者的显式 Ack/Nack。leaseGen 不匹配说明这是过期投递，
// （消息已被超时回收/重投），结算被明确拒绝且不产生任何计数。
func (gs *groupState) handleSettle(req settleReq) {
	e, ok := gs.inflight[req.messageID]
	if !ok {
		// 已终态（acked/dead）或未知投递：幂等拒绝。
		req.res <- settleResp{err: ErrUnknownDelivery}
		return
	}
	if e.leaseGen != req.leaseGen {
		req.res <- settleResp{err: ErrUnknownDelivery}
		return
	}
	delete(gs.inflight, e.id)
	gs.metrics.Inflight.Add(-1)
	gs.signalSpace()

	if req.ack {
		e.status = StatusAcked
		e.settled = true
		gs.metrics.Acked.Add(1)
		gs.logger("ACK topic=%s group=%s id=%d consumer=%s attempts=%d",
			gs.topic, gs.group, e.id, e.holderID, e.attempts)
		req.res <- settleResp{}
		return
	}

	// Nack：显式否认按一次失败尝试处理，与超时回收走同一套重试/死信规则。
	e.lastError = req.errMsg
	gs.retryOrDead(e, "nack")
	req.res <- settleResp{}
}

// retryOrDead 决定一条失败消息是退避重试还是进入死信。
// trigger 为日志/统计用的触发来源（nack / timeout / heartbeat）。
func (gs *groupState) retryOrDead(e *entry, trigger string) {
	if e.attempts > gs.cfg.MaxRetries {
		gs.toDLQ(e, DLQReasonMaxRetries, trigger)
		return
	}
	e.status = StatusRetry
	delay := time.Duration(0)
	if gs.cfg.Backoff != nil {
		delay = gs.cfg.Backoff.Next(e.attempts)
	}
	e.notBefore = time.Now().Add(delay)
	if e.holderID != "" {
		e.lastHolder = e.holderID // 记住上一持有者，重投时优先避开
	}
	e.holderID = ""
	gs.retry = append(gs.retry, e)
	gs.metrics.Retried.Add(1)
	gs.logger("RETRY schedule topic=%s group=%s id=%d attempts=%d backoff=%v trigger=%s lastErr=%q",
		gs.topic, gs.group, e.id, e.attempts, delay, trigger, e.lastError)
}

// toDLQ 把消息置入死信并落终态。同一条消息只会进一次（settled 保证）。
func (gs *groupState) toDLQ(e *entry, reason, trigger string) {
	e.status = StatusDead
	e.settled = true
	dl := &DeadLetter{
		Message:    e.msg,
		Topic:      e.topic,
		Group:      gs.group,
		Reason:     reason,
		LastError:  e.lastError,
		Attempts:   e.attempts,
		EnqueuedAt: e.enqueuedAt,
		DeadAt:     time.Now(),
		_messageID: e.id,
	}
	gs.dlq.add(dl)
	gs.metrics.DeadLettered.Add(1)
	gs.logger("DLQ topic=%s group=%s id=%d reason=%s trigger=%s attempts=%d lastErr=%q",
		gs.topic, gs.group, e.id, reason, trigger, e.attempts, e.lastError)
}

// promizeRetry 把退避到期的消息移回 ready。
func (gs *groupState) promizeRetry() {
	if len(gs.retry) == 0 {
		return
	}
	now := time.Now()
	kept := gs.retry[:0]
	for _, e := range gs.retry {
		if !now.Before(e.notBefore) {
			e.status = StatusPending
			gs.ready = append(gs.ready, e)
		} else {
			kept = append(kept, e)
		}
	}
	gs.retry = kept
}

// reclaimExpired 回收可见性超时/心跳超时的在途消息。
func (gs *groupState) reclaimExpired(now time.Time) {
	for id, e := range gs.inflight {
		timedOut := !now.Before(e.deadline)
		heartbeatLost := gs.cfg.HeartbeatTimeout > 0 &&
			!e.lastBeat.IsZero() &&
			now.Sub(e.lastBeat) > gs.cfg.HeartbeatTimeout
		if !timedOut && !heartbeatLost {
			continue
		}
		delete(gs.inflight, id)
		gs.metrics.Inflight.Add(-1)
		trigger := "visibility_timeout"
		if heartbeatLost {
			trigger = "heartbeat_lost"
		}
		gs.logger("RECLAIM topic=%s group=%s id=%d from=%s trigger=%s attempts=%d",
			gs.topic, gs.group, e.id, e.holderID, trigger, e.attempts)
		e.lastError = trigger
		if e.attempts > gs.cfg.MaxRetries {
			gs.toDLQ(e, DLQReasonTimeout, trigger)
		} else {
			gs.retryOrDead(e, trigger)
			// retryOrDead 已把它放进 retry 并按退避等待，避免掉线者立刻又抢到。
		}
		gs.signalSpace()
	}
}

// reclaimFromConsumer 在消费者显式关闭时立即回收它持有的全部消息（不等超时）。
// 消息会被标记为退避后尽快重投，优先落到组内其他存活消费者。
func (gs *groupState) reclaimFromConsumer(consumerID, reason string) {
	for id, e := range gs.inflight {
		if e.holderID != consumerID {
			continue
		}
		delete(gs.inflight, id)
		gs.metrics.Inflight.Add(-1)
		e.lastError = reason
		gs.logger("RECLAIM topic=%s group=%s id=%d from=%s trigger=%s attempts=%d",
			gs.topic, gs.group, e.id, consumerID, reason, e.attempts)
		if e.attempts > gs.cfg.MaxRetries {
			gs.toDLQ(e, DLQReasonTimeout, reason)
		} else {
			// 显式掉线：退避设为 0（若配置了 Backoff 仍有短暂延迟，防止抖动）。
			gs.retryOrDead(e, reason)
		}
	}
	gs.signalSpace()
}

// tryDeliver 把 ready 中的消息投递给当前空闲（通道可写）的消费者。
func (gs *groupState) tryDeliver(rr *int) {
	if len(gs.ready) == 0 || len(gs.consumers) == 0 {
		return
	}
	ids := make([]string, 0, len(gs.consumers))
	for id := range gs.consumers {
		ids = append(ids, id)
	}
	// 简单排序保证轮询稳定。
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}

	remaining := gs.ready[:0]
	idx := *rr % len(ids)
	for _, e := range gs.ready {
		delivered := false
		// 候选顺序：从轮询点开始；对重投消息（存在上一持有者）先只尝试“其他消费者”，
		// 一圈都没有可投递的，再允许投回原持有者。这样消费者掉线/崩溃后，
		// 消息会优先被组内其他存活消费者接管；没有其他消费者时才会回到原持有者。
		avoid := e.lastHolder
		hasAlternative := false
		for _, cid := range ids {
			if cid != avoid {
				hasAlternative = true
				break
			}
		}
		for pass := 0; pass < 2 && !delivered; pass++ {
			for n := 0; n < len(ids); n++ {
				cid := ids[idx%len(ids)]
				idx++
				if pass == 0 && hasAlternative && cid == avoid {
					continue
				}
				c := gs.consumers[cid]
				if c.isClosed() {
					continue
				}
				d := gs.makeDelivery(e, c)
				select {
				case c.deliveries <- d:
					e.status = StatusInflight
					e.holderID = c.id
					e.attempts++
					e.leaseGen++
					e.deadline = time.Now().Add(gs.cfg.VisibilityTimeout)
					e.lastBeat = time.Now()
					gs.inflight[e.id] = e
					gs.metrics.Inflight.Add(1)
					if e.attempts > 1 {
						gs.metrics.Redelivered.Add(1)
					}
					gs.logger("DELIVER topic=%s group=%s id=%d consumer=%s attempt=%d",
						gs.topic, gs.group, e.id, c.id, e.attempts)
					delivered = true
				default:
					// 通道满 = 该消费者有未读消息，视为忙，尝试下一个。
				}
				if delivered {
					break
				}
			}
		}
		if !delivered {
			remaining = append(remaining, e)
		}
	}
	gs.ready = remaining
	*rr = idx
}

func (gs *groupState) makeDelivery(e *entry, c *Consumer) *Delivery {
	dno := gs.brokerRef.nextDeliveryNo()
	d := &Delivery{
		ID:           dno,
		Message:      e.msg,
		Topic:        gs.topic,
		Group:        gs.group,
		DeliveryNo:   dno,
		Attempt:      e.attempts + 1,
		ReceivedAt:   time.Now(),
		Deadline:     time.Now().Add(gs.cfg.VisibilityTimeout),
		ConsumerID:   c.id,
		_messageID:   e.id,
		_deliveryKey: e.leaseGen + 1,
	}
	d.Ack = func() error { return c.settle(e.id, d._deliveryKey, true, "") }
	d.Nack = func(err error) error {
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		return c.settle(e.id, d._deliveryKey, false, msg)
	}
	return d
}

func (gs *groupState) inSystemCount() int {
	n := len(gs.ready) + len(gs.retry) + len(gs.inflight)
	for _, r := range gs.reserved {
		n += r
	}
	return n
}

func (gs *groupState) nextRetryWake() time.Duration {
	var soonest time.Time
	for _, e := range gs.retry {
		if soonest.IsZero() || e.notBefore.Before(soonest) {
			soonest = e.notBefore
		}
	}
	if soonest.IsZero() {
		return 0
	}
	d := time.Until(soonest)
	if d < 0 {
		return 0
	}
	return d
}

func (gs *groupState) nextTimeoutIn() time.Duration {
	var soonest time.Time
	now := time.Now()
	for _, e := range gs.inflight {
		if soonest.IsZero() || e.deadline.Before(soonest) {
			soonest = e.deadline
		}
	}
	if soonest.IsZero() {
		return 0
	}
	d := soonest.Sub(now)
	if d < 0 {
		return 0
	}
	return d
}

// signalSpace 非阻塞地通知阻塞中的 Publish 队列出现了空位。
func (gs *groupState) signalSpace() {
	if gs.capacity <= 0 || gs.inSystemCount() < gs.capacity {
		select {
		case gs.space <- struct{}{}:
		default:
		}
	}
}

type heartbeatReq struct {
	id  string
	at  time.Time
	res chan error
}

// handleHeartbeat 更新该消费者所有在途消息的最近心跳时间。
func (gs *groupState) handleHeartbeat(req heartbeatReq) {
	n := 0
	for _, e := range gs.inflight {
		if e.holderID == req.id {
			e.lastBeat = req.at
			n++
		}
	}
	req.res <- nil
	gs.logger("heartbeat group=%s consumer=%s inflight=%d", gs.group, req.id, n)
}

type statsReq struct {
	res chan statsResp
}

type statsResp struct {
	ready, retry, inflight, capacity int
}

type forceDrainReq struct {
	res chan struct{}
}

// handleForceDrain 在关闭超时兜底时，把所有在途消息标记为掉线回收：
// 未超重试上限的进入退避队列（留在内存，状态明确），超限的进入死信。
// 这样不会有消息停留在“投出去但无主”的半处理状态。
func (gs *groupState) handleForceDrain() {
	if len(gs.inflight) == 0 {
		return
	}
	ids := make([]int64, 0, len(gs.inflight))
	for id := range gs.inflight {
		ids = append(ids, id)
	}
	for _, id := range ids {
		e := gs.inflight[id]
		delete(gs.inflight, id)
		gs.metrics.Inflight.Add(-1)
		e.lastError = "broker shutdown forced drain"
		if e.attempts > gs.cfg.MaxRetries {
			gs.toDLQ(e, DLQReasonTimeout, e.lastError)
		} else {
			gs.retryOrDead(e, e.lastError)
		}
	}
	gs.signalSpace()
}

// snapshotConsumers 返回当前消费者列表（关闭流程用，锁外调用安全：run 已停止或事件串行）。
func (gs *groupState) snapshotConsumers() []*Consumer {
	out := make([]*Consumer, 0, len(gs.consumers))
	for _, c := range gs.consumers {
		out = append(out, c)
	}
	return out
}

// reserveReq 是多组原子发布的两阶段协议：
// commit=false 且有容量 -> 在该组预留一个空位（占用容量），返回 ticket；
// commit=false 且容量不足 -> 返回 full=true，不预留；
// commit=true  -> 把 entry 放入 ready 并释放对应预留；
// cancel=true  -> 放弃预留（其他组满或整体失败时回滚本已预留的空位）。
type reserveReq struct {
	ticket int64
	e      *entry
	commit bool
	cancel bool
	res    chan reserveResp
}

type reserveResp struct {
	ok   bool
	full bool
}

func (gs *groupState) handleReserve(req reserveReq) {
	switch {
	case req.cancel:
		if n, ok := gs.reserved[req.ticket]; ok {
			delete(gs.reserved, req.ticket)
			_ = n
			gs.signalSpace()
		}
		req.res <- reserveResp{ok: true}
	case req.commit:
		if _, ok := gs.reserved[req.ticket]; ok {
			delete(gs.reserved, req.ticket)
			gs.ready = append(gs.ready, req.e)
			gs.logger("enqueue topic=%s group=%s id=%d depth=%d (atomic fanout commit)",
				gs.topic, gs.group, req.e.id, gs.inSystemCount())
		}
		req.res <- reserveResp{ok: true}
	default:
		if gs.capacity > 0 && gs.inSystemCount() >= gs.capacity {
			req.res <- reserveResp{ok: false, full: true}
			return
		}
		gs.reserved[req.ticket] = 1
		req.res <- reserveResp{ok: true}
	}
}
