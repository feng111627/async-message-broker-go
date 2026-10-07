package broker

import (
	"sync/atomic"
	"time"
)

// Message 是生产者投递的一条消息。
type Message struct {
	ID      string
	Topic   string
	Payload []byte

	// MaxAttempts 非零时覆盖全局配置，取值 <0 表示无限重试。
	MaxAttempts int

	createdAt time.Time
	// seq 是 broker 内部分配的单调序号，用于排队公平性。
	seq uint64
}

func (m *Message) maxAttempts(global int) int {
	if m.MaxAttempts != 0 {
		return m.MaxAttempts
	}
	return global
}

// Delivery 是消费者收到的一次投递。同一条底层消息在重投时会生成新的 Delivery，
// 但 MessageID 与 Attempt 保持同一语义；结算状态只属于本次 Delivery，
// 因此重复 Ack/Nack 会被明确拒绝，不会重复计数。
type Delivery struct {
	Message   *Message
	MessageID string
	Group     string
	Consumer  string
	Attempt   int

	ch   *Broker
	did  deliveryID
	done atomic.Bool
}

// Ack 显式确认：消息处理成功。重复确认返回 ErrAlreadySettled。
func (d *Delivery) Ack() error {
	if d == nil || d.ch == nil {
		return ErrInvalidArgument
	}
	if !d.done.CompareAndSwap(false, true) {
		return ErrAlreadySettled
	}
	return d.ch.settle(d.did, true, nil)
}

// Nack 显式否认：消息处理失败。是否重试、如何退避由 broker 策略决定。
func (d *Delivery) Nack(reason error) error {
	if d == nil || d.ch == nil {
		return ErrInvalidArgument
	}
	if !d.done.CompareAndSwap(false, true) {
		return ErrAlreadySettled
	}
	return d.ch.settle(d.did, false, reason)
}

// DeadLetter 是进入死信队列的消息及其失败上下文。
type DeadLetter struct {
	Message      *Message
	Group        string
	Reason       string
	Attempts     int
	FailedAt     time.Time
	LastConsumer string
}
