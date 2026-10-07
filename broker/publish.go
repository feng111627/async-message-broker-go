package broker

import "context"

// Publish 发布一条消息到 topic。
//
// 语义：消息会为“当前订阅该 topic 的每个消费组”各保存一份独立副本，
// 不同组之间互不影响（组间广播、组内竞争）。
//
// 原子性：一次发布要么被所有订阅组接受，要么整体失败，绝不会只写进部分组。
//
// 背压（每组队列长度受 Config.QueueCapacity 约束，含 ready/retry/inflight/已预留）：
//   - block=false：任一组队列满，立即返回 ErrQueueFull，可被调用方明确感知；
//   - block=true：阻塞等待所有组都有空位，直到空位出现、ctx 取消或 broker 关闭。
//
// 发布时若没有任何订阅者，返回 ErrNoSubscription，绝不静默丢消息。
func (b *Broker) Publish(ctx context.Context, topic string, msg *Message, block bool) (int64, error) {
	if msg == nil || msg.Payload == nil {
		return 0, ErrInvalidMessage
	}
	b.mu.Lock()
	if b.closed || b.closing {
		b.mu.Unlock()
		return 0, ErrBrokerClosed
	}
	var targets []*groupState
	for _, gs := range b.groups {
		if gs.topic == topic {
			targets = append(targets, gs)
		}
	}
	if len(targets) == 0 {
		b.mu.Unlock()
		return 0, ErrNoSubscription
	}
	b.mu.Unlock()

	var firstID int64
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		b.mu.Lock()
		if b.closed || b.closing {
			b.mu.Unlock()
			return 0, ErrBrokerClosed
		}
		ticket := b.nextMessageID() // 复用 ID 序列作为本次发布的唯一票据
		if firstID == 0 {
			firstID = ticket
		}
		entries := make([]*entry, len(targets))
		for i := range targets {
			id := b.nextMessageID()
			if i == 0 {
				firstID = id
			}
			mcopy := &Message{
				Key:        msg.Key,
				Payload:    append([]byte(nil), msg.Payload...),
				Headers:    cloneHeaders(msg.Headers),
				EnqueuedAt: msg.EnqueuedAt,
			}
			if mcopy.EnqueuedAt.IsZero() {
				mcopy.EnqueuedAt = b.now()
			}
			entries[i] = &entry{id: id, topic: topic, msg: mcopy, status: StatusPending, enqueuedAt: mcopy.EnqueuedAt}
		}
		b.mu.Unlock()

		// 阶段 1：向所有目标组“预留空位”。任一组满 -> 取消全部预留 -> 等待或拒绝。
		reserved := make([]bool, len(targets))
		anyFull := false
		for i, gs := range targets {
			req := reserveReq{ticket: ticket, res: make(chan reserveResp, 1)}
			select {
			case gs.reserveReq <- req:
				r := <-req.res
				if r.ok && !r.full {
					reserved[i] = true
				}
				if r.full {
					anyFull = true
				}
			case <-gs.done:
				b.cancelReservations(ticket, targets, reserved)
				return 0, ErrBrokerClosed
			case <-ctx.Done():
				b.cancelReservations(ticket, targets, reserved)
				return 0, ctx.Err()
			}
		}

		if anyFull {
			b.cancelReservations(ticket, targets, reserved)
			if !block {
				b.metrics.PublishFull.Add(1)
				b.logger("publish REJECTED queue full topic=%s", topic)
				return 0, ErrQueueFull
			}
			b.metrics.BlockedWaits.Add(1)
			b.logger("publish BLOCKED topic=%s, waiting for space", topic)
			if !b.waitAnySpace(ctx, targets) {
				return 0, ctx.Err()
			}
			continue
		}

		// 阶段 2：全部预留成功 -> 逐组提交（把消息放入 ready 并释放预留）。
		for i, gs := range targets {
			req := reserveReq{ticket: ticket, e: entries[i], commit: true, res: make(chan reserveResp, 1)}
			select {
			case gs.reserveReq <- req:
				<-req.res
			case <-gs.done:
				return 0, ErrBrokerClosed
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		b.metrics.Published.Add(1)
		b.logger("publish accepted topic=%s id=%d groups=%d block=%v attempt=%d",
			topic, firstID, len(targets), block, attempt)
		return firstID, nil
	}
}

// cancelReservations 回滚已成功预留的组。
func (b *Broker) cancelReservations(ticket int64, targets []*groupState, reserved []bool) {
	for i, gs := range targets {
		if !reserved[i] {
			continue
		}
		req := reserveReq{ticket: ticket, cancel: true, res: make(chan reserveResp, 1)}
		select {
		case gs.reserveReq <- req:
			<-req.res
		case <-gs.done:
		}
	}
}

// waitAnySpace 阻塞直到任一目标组报告空位、ctx 结束或调度器退出。
func (b *Broker) waitAnySpace(ctx context.Context, targets []*groupState) bool {
	for _, gs := range targets {
		select {
		case <-gs.space:
			return true
		default:
		}
	}
	select {
	case <-targets[0].space:
		return true
	case <-ctx.Done():
		return false
	case <-targets[0].done:
		return false
	}
}

func cloneHeaders(h map[string]string) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}
