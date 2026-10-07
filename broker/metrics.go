package broker

import (
	"fmt"
	"sync/atomic"
)

// Metrics 是 broker 运行期的可解释计数。所有计数均以单条消息的单次终态为准，
// 重试/重投不会导致成功、死信等终态计数重复累加。
type Metrics struct {
	Published    atomic.Int64 // 成功发布的消息数
	PublishFull  atomic.Int64 // 因队列满被明确拒绝的次数（非阻塞模式）
	BlockedWaits atomic.Int64 // 阻塞模式下因队列满进入等待的次数
	Acked        atomic.Int64 // 显式确认成功的消息数
	Redelivered  atomic.Int64 // 重新投递的次数（过程计数，可与终态计数并存）
	Retried      atomic.Int64 // 进入退避等待的消息次数（过程计数）
	DeadLettered atomic.Int64 // 进入死信的消息数
	Requeued     atomic.Int64 // 从死信重新投回的消息数
	Inflight     atomic.Int64 // 当前在途（已领取未确认）消息数
}

// Snapshot 是某一时刻 Metrics 的普通值快照。
type Snapshot struct {
	Published    int64
	PublishFull  int64
	BlockedWaits int64
	Acked        int64
	Redelivered  int64
	Retried      int64
	DeadLettered int64
	Requeued     int64
	Inflight     int64
}

func (m *Metrics) snapshot() Snapshot {
	return Snapshot{
		Published:    m.Published.Load(),
		PublishFull:  m.PublishFull.Load(),
		BlockedWaits: m.BlockedWaits.Load(),
		Acked:        m.Acked.Load(),
		Redelivered:  m.Redelivered.Load(),
		Retried:      m.Retried.Load(),
		DeadLettered: m.DeadLettered.Load(),
		Requeued:     m.Requeued.Load(),
		Inflight:     m.Inflight.Load(),
	}
}

// String 输出便于阅读和测试日志断言的指标快照。
func (s Snapshot) String() string {
	return fmt.Sprintf(
		"published=%d acked=%d retried=%d redelivered=%d dead=%d requeued=%d rejected_full=%d blocked_waits=%d inflight=%d",
		s.Published, s.Acked, s.Retried, s.Redelivered, s.DeadLettered,
		s.Requeued, s.PublishFull, s.BlockedWaits, s.Inflight,
	)
}
