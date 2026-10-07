package broker

import (
	"context"
	"sync/atomic"
)

// DeadLetters 返回死信队列的快照（拷贝切片，元素本身只读使用）。
func (b *Broker) DeadLetters() []*DeadLetter {
	b.dlqMu.Lock()
	defer b.dlqMu.Unlock()
	out := make([]*DeadLetter, len(b.dlq))
	copy(out, b.dlq)
	return out
}

// RequeueDeadLetter 按死信在快照中的下标把消息重新投回原消费组。
// 重试计数清零（按一条全新的消息重新计算尝试次数）；只有成功重新入队后才移除死信，
// 入队失败（如背压拒绝）时死信保留，调用方可重试本操作。
// 底层消息 ID 不变，业务侧可基于 MessageID 做幂等；重新投递只走正常的
// delivered/acked 计数，不会重复累加历史死信计数。
func (b *Broker) RequeueDeadLetter(ctx context.Context, index int) error {
	b.dlqMu.Lock()
	if index < 0 || index >= len(b.dlq) {
		b.dlqMu.Unlock()
		return ErrDLQMessageGone
	}
	dl := b.dlq[index]
	b.dlqMu.Unlock()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBrokerClosed
	}
	g := b.groups[dl.Group]
	if g == nil {
		g = b.ensureGroupLocked(dl.Group)
	}
	b.mu.Unlock()

	// enqueue 以 attempt=1 重新入组队列；队列满时按背压策略拒绝/阻塞。
	if err := g.enqueue(ctx, dl.Message, true); err != nil {
		return err
	}

	b.dlqMu.Lock()
	// 重新按下标定位，避免与并发的 Purge/Requeue 错位。
	pos := -1
	for i, x := range b.dlq {
		if x == dl {
			pos = i
			break
		}
	}
	if pos >= 0 {
		b.dlq = append(b.dlq[:pos], b.dlq[pos+1:]...)
	}
	b.dlqMu.Unlock()

	atomic.AddInt64(&b.stats.RequeueDLQ, 1)
	return nil
}

// PurgeDeadLetter 删除指定下标的死信。
func (b *Broker) PurgeDeadLetter(index int) error {
	b.dlqMu.Lock()
	defer b.dlqMu.Unlock()
	if index < 0 || index >= len(b.dlq) {
		return ErrDLQMessageGone
	}
	b.dlq = append(b.dlq[:index], b.dlq[index+1:]...)
	return nil
}
