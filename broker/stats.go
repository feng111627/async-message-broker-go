package broker

import "sync/atomic"

// Stats 是 broker 的可观测计数快照。所有计数均按"每条消息每个语义事件"
// 只累加一次，重试与重投不会造成重复计数。
type Stats struct {
	Published    int64
	Delivered    int64
	Acked        int64
	Retried      int64
	DeadLettered int64
	Reclaimed    int64 // 消费者掉线 / 确认超时触发的重投次数
	RequeueDLQ   int64
	Inflight     int64
	QueueDepth   int64
}

func (s *Stats) snapshot() Stats {
	out := Stats{}
	out.Published = atomic.LoadInt64(&s.Published)
	out.Delivered = atomic.LoadInt64(&s.Delivered)
	out.Acked = atomic.LoadInt64(&s.Acked)
	out.Retried = atomic.LoadInt64(&s.Retried)
	out.DeadLettered = atomic.LoadInt64(&s.DeadLettered)
	out.Reclaimed = atomic.LoadInt64(&s.Reclaimed)
	out.RequeueDLQ = atomic.LoadInt64(&s.RequeueDLQ)
	out.Inflight = atomic.LoadInt64(&s.Inflight)
	out.QueueDepth = atomic.LoadInt64(&s.QueueDepth)
	return out
}
