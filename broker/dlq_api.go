package broker

import (
	"context"
	"time"
)

// ListDeadLetters 列出死信，可按 topic/group 过滤（空串=不过滤）。
func (b *Broker) ListDeadLetters(topic, group string) []*DeadLetter {
	return b.dlq.List(topic, group)
}

// DeadLetterCount 返回死信总数。
func (b *Broker) DeadLetterCount() int { return b.dlq.Len() }

// RequeueDeadLetter 把指定消息 ID 的死信重新投回其原来的 (topic, group)。
//
// 语义：
//   - 死信从 DLQ 移除，作为“全新一次投递”重新入队，attempts 归零；
//     因此重新投递后即使再次失败，也会按完整的重试周期处理，
//     成功/死信终态计数只在新生命周期结算时累加一次，不会与历史重复。
//   - 若原消费组已无人订阅，返回 ErrNoSubscription。
//   - 支持与 Publish 相同的 block 背压参数。
func (b *Broker) RequeueDeadLetter(ctx context.Context, messageID int64, block bool) error {
	dl := b.dlq.remove(messageID)
	if dl == nil {
		return ErrUnknownDelivery
	}
	b.mu.Lock()
	var target *groupState
	for _, gs := range b.groups {
		if gs.topic == dl.Topic && gs.group == dl.Group {
			target = gs
			break
		}
	}
	if target == nil {
		// 没有订阅者了：放回 DLQ，保持状态不丢失。
		b.dlq.add(dl)
		b.mu.Unlock()
		return ErrNoSubscription
	}
	id := b.nextMessageID()
	b.mu.Unlock()

	msg := &Message{
		Key:        dl.Message.Key,
		Payload:    append([]byte(nil), dl.Message.Payload...),
		Headers:    cloneHeaders(dl.Message.Headers),
		EnqueuedAt: time.Now(),
	}
	e := &entry{id: id, topic: dl.Topic, msg: msg, status: StatusPending, enqueuedAt: msg.EnqueuedAt}

	for {
		req := enqueueReq{gs: target, e: e, res: make(chan enqueueResp, 1)}
		select {
		case target.enqueueReq <- req:
		case <-target.done:
			return ErrBrokerClosed
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case resp := <-req.res:
			if resp.accepted {
				b.metrics.Requeued.Add(1)
				b.logger("DLQ REQUEUE topic=%s group=%s oldID=%d newID=%d reason_was=%s attempts_was=%d",
					dl.Topic, dl.Group, dl._messageID, id, dl.Reason, dl.Attempts)
				return nil
			}
			if resp.err == ErrQueueFull {
				if !block {
					b.metrics.PublishFull.Add(1)
					return ErrQueueFull
				}
				b.metrics.BlockedWaits.Add(1)
				select {
				case <-target.space:
					continue
				case <-ctx.Done():
					return ctx.Err()
				case <-target.done:
					return ErrBrokerClosed
				}
			}
			return resp.err
		case <-ctx.Done():
			return ctx.Err()
		case <-target.done:
			return ErrBrokerClosed
		}
	}
}
