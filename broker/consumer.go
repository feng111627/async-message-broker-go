package broker

import "sync/atomic"

// SubscribeOptions 控制单次订阅行为。
type SubscribeOptions struct {
	// ConsumerID 消费者实例标识；为空时自动生成。
	ConsumerID string
	// Buffer 该消费者本地投递通道的缓冲大小，0 使用默认值 1。
	// 缓冲即该消费者未被业务取走时的在途上限，配合 AckTimeout 使用。
	Buffer int
}

// Subscribe 在指定消费组中注册一个消费者，返回投递通道与消费者 ID。
// 组内一条消息同一时刻只会派发给一个消费者。
func (b *Broker) Subscribe(group string, opts SubscribeOptions) (<-chan *Delivery, string, error) {
	if group == "" {
		return nil, "", ErrInvalidArgument
	}
	buf := opts.Buffer
	if buf <= 0 {
		buf = 1
	}
	id := opts.ConsumerID
	if id == "" {
		id = "consumer-" + newID()
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, "", ErrBrokerClosed
	}
	g := b.ensureGroupLocked(group)
	b.mu.Unlock()

	c := &consumer{
		id:      id,
		group:   g,
		deliver: make(chan *Delivery, buf),
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, "", ErrBrokerClosed
	}
	for _, m := range g.members {
		if m.id == id {
			g.mu.Unlock()
			return nil, "", ErrInvalidArgument
		}
	}
	g.members = append(g.members, c)
	g.mu.Unlock()
	g.kick()
	return c.deliver, id, nil
}

// Unsubscribe 注销消费者：其在途消息立即交还消费组（不计失败重试），
// 通道随后关闭，派发循环会把这些消息投给组内其他消费者。
func (b *Broker) Unsubscribe(group, consumerID string) error {
	b.mu.Lock()
	g := b.groupLocked(group)
	b.mu.Unlock()
	if g == nil {
		return ErrConsumerGone
	}

	g.mu.Lock()
	var target *consumer
	kept := g.members[:0]
	for _, c := range g.members {
		if c.id == consumerID {
			target = c
			continue
		}
		kept = append(kept, c)
	}
	if target == nil {
		g.members = kept
		g.mu.Unlock()
		return ErrConsumerGone
	}
	g.members = kept

	// 通道里尚未被业务取走的投递也属于"未确认"，全部回收。
	reclaimed := []*readyMsg(nil)
	close(target.deliver)
	target.closed = true
	for did, e := range g.inflight {
		if e.consumer == target {
			delete(g.inflight, did)
			reclaimed = append(reclaimed, &readyMsg{msg: e.msg, attempt: e.attempt})
		}
	}
	g.mu.Unlock()

	n := int64(len(reclaimed))
	if n > 0 {
		atomic.AddInt64(&b.stats.Inflight, -n)
		atomic.AddInt64(&b.stats.Reclaimed, n)
		g.readdFront(reclaimed)
	}
	g.kick()
	return nil
}

// readdFront 把回收的消息放回组队列队首。这些消息此前已被系统接受，
// 因此不受容量背压限制，也不会因为重新排队而丢失。
func (g *consumerGroup) readdFront(msgs []*readyMsg) {
	g.mu.Lock()
	g.ready = append(msgs, g.ready...)
	g.mu.Unlock()
	g.kick()
}
