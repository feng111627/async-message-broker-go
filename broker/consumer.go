package broker

import (
	"context"
	"errors"
	"sync"
)

// Consumer 是某个消费组内的一个消费者实例。
type Consumer struct {
	id     string
	group  string
	topic  string
	broker *Broker
	state  *groupState

	deliveries chan *Delivery

	closeOnce    sync.Once
	closedMu     sync.RWMutex
	closed       bool
	closedChDone bool
}

// Deliveries 返回只读投递通道。通道关闭表示消费者已退出。
func (c *Consumer) Deliveries() <-chan *Delivery { return c.deliveries }

// Receive 阻塞等待下一条消息；ctx 结束或消费者/broker 关闭时返回错误。
func (c *Consumer) Receive(ctx context.Context) (*Delivery, error) {
	if c.isClosed() {
		return nil, ErrConsumerClosed
	}
	select {
	case d, ok := <-c.deliveries:
		if !ok {
			return nil, ErrConsumerClosed
		}
		return d, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Heartbeat 汇报存活。
// 当配置了 HeartbeatTimeout 时，持有消息期间应周期性调用；
// 超时未心跳且仍持有消息，消息会被回收重投给组内其他消费者。
func (c *Consumer) Heartbeat() error {
	if c.isClosed() {
		return ErrConsumerClosed
	}
	// 心跳通过 register/settle 同类串行通道会过重，这里直接投递一个轻量请求。
	req := heartbeatReq{id: c.id, at: c.broker.now(), res: make(chan error, 1)}
	select {
	case c.state.heartbeatReq <- req:
		select {
		case err := <-req.res:
			return err
		case <-c.state.done:
			return ErrConsumerClosed
		}
	case <-c.state.done:
		return ErrConsumerClosed
	}
}

// settle 提交 Ack/Nack，带租约代号；过期租约（已被超时回收）会被拒绝且不计数。
func (c *Consumer) settle(messageID int64, leaseGen int64, ack bool, errMsg string) error {
	if c.isClosed() {
		// 消费者已关闭时，消息已由 Close 回收，旧 Delivery 的确认必须失效。
		return ErrUnknownDelivery
	}
	req := settleReq{
		messageID: messageID,
		leaseGen:  leaseGen,
		ack:       ack,
		errMsg:    errMsg,
		res:       make(chan settleResp, 1),
	}
	select {
	case c.state.settleReq <- req:
		select {
		case r := <-req.res:
			return r.err
		case <-c.state.done:
			return ErrConsumerClosed
		}
	case <-c.state.done:
		return ErrConsumerClosed
	}
}

// Close 注销消费者：调度器立即把它持有的未确认消息重投给组内其他消费者，
// 然后关闭投递通道。重复调用是幂等的。
func (c *Consumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closedMu.Lock()
		c.closed = true
		c.closedMu.Unlock()

		done := make(chan struct{})
		req := registerReq{c: c, removed: true, done: done}
		select {
		case c.state.registerReq <- req:
			<-done
		case <-c.state.done:
			// 调度器已退出，broker 关闭流程会统一收尾。
		}
		c.closedMu.Lock()
		if !c.closedChDone {
			c.closedChDone = true
			close(c.deliveries)
		}
		c.closedMu.Unlock()
	})
	return err
}

func (c *Consumer) isClosed() bool {
	c.closedMu.RLock()
	defer c.closedMu.RUnlock()
	return c.closed
}

// ID 返回消费者 ID。
func (c *Consumer) ID() string { return c.id }

var _ = errors.New
