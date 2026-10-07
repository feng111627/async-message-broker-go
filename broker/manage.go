package broker

import (
	"context"
	"time"
)

// Subscribe 在消费组 group 内创建一个消费者，订阅 topic。
// 同组多个消费者按竞争方式消费：每条消息同一时间只投递给其中一个。
// 不同组各自独立消费同一条消息（组间广播）。
func (b *Broker) Subscribe(group, topic, consumerID string) (*Consumer, error) {
	if group == "" || topic == "" || consumerID == "" {
		return nil, ErrInvalidMessage
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.closing {
		return nil, ErrBrokerClosed
	}
	gs := b.groupLocked(topic, group, true)
	c := &Consumer{
		id:         consumerID,
		group:      group,
		topic:      topic,
		broker:     b,
		state:      gs,
		deliveries: make(chan *Delivery, b.cfg.DeliveryBuffer),
	}
	done := make(chan struct{})
	req := registerReq{c: c, done: done}
	gs.registerReq <- req
	<-done
	return c, nil
}

// Metrics 返回指标快照。
func (b *Broker) Metrics() Snapshot { return b.metrics.snapshot() }

// QueueStats 返回某个 (topic, group) 的积压/在途/容量，便于观察背压。
func (b *Broker) QueueStats(topic, group string) (ready, inflight, capacity int, err error) {
	b.mu.Lock()
	gs := b.groups[groupKey(topic, group)]
	b.mu.Unlock()
	if gs == nil {
		return 0, 0, 0, ErrNoSubscription
	}
	// 通过一次入队通道的旁路请求读取，保证与调度状态一致。
	req := statsReq{res: make(chan statsResp, 1)}
	select {
	case gs.statsReq <- req:
		r := <-req.res
		return r.ready, r.inflight, r.capacity, nil
	case <-gs.done:
		return 0, 0, gs.capacity, nil
	}
}

// Close 优雅关闭：
//  1. 标记 closing，拒绝一切新的 Publish/Subscribe（返回 ErrBrokerClosed）；
//  2. 等待每个组的在途消息结算（Ack 完成 / 失败重试 / 进入死信）；
//  3. 若 ctx 在结算完成前到期，则把仍在途的消息重排后停止调度，不出现半处理状态；
//  4. 停止所有调度 goroutine，关闭后 Metrics/DLQ 仍可读。
func (b *Broker) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closing = true
		groups := make([]*groupState, 0, len(b.groups))
		for _, gs := range b.groups {
			groups = append(groups, gs)
		}
		b.mu.Unlock()
		b.logger("shutdown begin: groups=%d, draining inflight messages", len(groups))
		close(b.doneCh)
		b.shutdownGroups(groups, ctx)
		b.mu.Lock()
		b.closed = true
		b.mu.Unlock()
		b.logger("shutdown complete: %s", b.Metrics())
	})
	return nil
}

// shutdownGroups 等所有组在途清空；ctx 超时则强制重排后停止。
func (b *Broker) shutdownGroups(groups []*groupState, ctx context.Context) {
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()

	for {
		allIdle := true
		anyAlive := false
		for _, gs := range groups {
			select {
			case <-gs.done:
				continue
			default:
			}
			anyAlive = true
			req := statsReq{res: make(chan statsResp, 1)}
			select {
			case gs.statsReq <- req:
				r := <-req.res
				// 优雅关闭只等待“在途消息”结算完毕；
				// 尚未投递/退避中的消息保留在队列内存中，状态可解释，不会半处理。
				if r.inflight > 0 {
					allIdle = false
				}
			case <-gs.done:
			}
		}
		if !anyAlive || allIdle {
			break
		}
		select {
		case <-ctx.Done():
			b.logger("shutdown deadline exceeded, force-requeuing remaining inflight messages")
			for _, gs := range groups {
				req := forceDrainReq{res: make(chan struct{})}
				select {
				case gs.forceDrain <- req:
					<-req.res
				case <-gs.done:
				}
			}
			break
		case <-poll.C:
		}
	}

	// 所有消息收尾后再停止调度器，保证不会把“已分发但未结算”的消息丢在半空。
	for _, gs := range groups {
		select {
		case gs.stop <- struct{}{}:
			<-gs.done
		case <-gs.done:
		}
		// 关闭所有尚未关闭的消费者通道，让 Receive 返回。
		for _, c := range gs.snapshotConsumers() {
			c.closedMu.Lock()
			if !c.closedChDone {
				c.closedChDone = true
				close(c.deliveries)
			}
			c.closed = true
			c.closedMu.Unlock()
		}
	}
}
