package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// deliveryID 唯一标识"一次投递尝试"（同一消息重投会产生新的 deliveryID）。
type deliveryID struct {
	seq uint64
}

// inflightEntry 记录一条在途消息。
type inflightEntry struct {
	msg         *Message
	group       *consumerGroup
	consumer    *consumer
	attempt     int
	deliveredAt time.Time
	deadline    time.Time
}

// consumer 是消费组内的一个消费者实例。
type consumer struct {
	id      string
	group   *consumerGroup
	deliver chan *Delivery
	closed  bool
}

// consumerGroup 持有一个消费组的队列、成员、在途消息与派发循环。
// 所有可变状态均由 mu 保护；channel 发送一律采用非阻塞方式，
// 派发循环不会因为某个消费者卡住而阻塞。
type consumerGroup struct {
	name     string
	broker   *Broker
	mu       sync.Mutex
	ready    []*readyMsg     // 可立即派发的消息（FIFO）
	wait     []*scheduledMsg // 退避中的消息，按 runAt 到期回到 ready
	members  []*consumer
	inflight map[deliveryID]*inflightEntry
	rr       int // 轮询派发位置
	closed   bool
	shutting bool // 优雅关闭阶段：停止接收新消息，派发循环继续工作
	notify   chan struct{}
	shutdown chan struct{}
	capCh    chan struct{} // 组内腾出未确认名额时非阻塞通知，精准唤醒背压阻塞的 Publish
}

type scheduledMsg struct {
	msg     *Message
	runAt   time.Time
	attempt int
}

// readyMsg 是组队列中待派发的消息，attempt 是"每个消费组独立"的尝试序号。
// 同一条消息扇出到不同组时，各组的 attempt 互不影响。
type readyMsg struct {
	msg     *Message
	attempt int
}

func (g *consumerGroup) kick() {
	select {
	case g.notify <- struct{}{}:
	default:
	}
}

// kickCap 通知"组内未确认名额可能已腾出"，精准唤醒背压阻塞中的 Publish。
func (g *consumerGroup) kickCap() {
	select {
	case g.capCh <- struct{}{}:
	default:
	}
}

// Broker 是本地内存版异步消息代理。
type Broker struct {
	cfg    Config
	mu     sync.Mutex // 保护 groups/closed；锁序约定：b.mu -> g.mu，不可反向嵌套
	groups map[string]*consumerGroup

	dlqMu sync.Mutex // 独立保护死信切片，避免在 g.mu 内反向获取 b.mu
	dlq   []*DeadLetter

	closed bool
	stats  Stats
	seq    uint64
	wg     sync.WaitGroup
	stopCh chan struct{}
}

// New 创建一个 Broker。
func New(cfg Config) *Broker {
	cfg = cfg.normalized()
	return &Broker{
		cfg:    cfg,
		groups: make(map[string]*consumerGroup),
		stopCh: make(chan struct{}),
	}
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// groupLocked 返回已存在的消费组；不存在返回 nil。调用方持有 b.mu。
func (b *Broker) groupLocked(name string) *consumerGroup {
	return b.groups[name]
}

// ensureGroupLocked 返回或惰性创建消费组并启动其派发循环。调用方持有 b.mu。
func (b *Broker) ensureGroupLocked(name string) *consumerGroup {
	g := b.groups[name]
	if g == nil {
		g = &consumerGroup{
			name:     name,
			broker:   b,
			inflight: make(map[deliveryID]*inflightEntry),
			notify:   make(chan struct{}, 1),
			shutdown: make(chan struct{}),
			capCh:    make(chan struct{}, 1),
		}
		b.groups[name] = g
		b.wg.Add(1)
		go g.dispatchLoop()
	}
	return g
}

func (b *Broker) nextSeq() uint64 {
	return atomic.AddUint64(&b.seq, 1)
}

func (b *Broker) nextDeliveryID() deliveryID {
	return deliveryID{seq: b.nextSeq()}
}

// Publish 投递一条消息；消息会派发给所有已存在的消费组（发布订阅）。
// BlockOnFull=true 时队列满则阻塞（可被 ctx 取消）；false 时立即返回 ErrQueueFull。
func (b *Broker) Publish(ctx context.Context, msg *Message) error {
	if msg == nil {
		return ErrInvalidArgument
	}
	if msg.ID == "" {
		msg.ID = newID()
	}
	msg.createdAt = time.Now()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBrokerClosed
	}
	msg.seq = b.nextSeq()
	groups := make([]*consumerGroup, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}
	b.mu.Unlock()

	atomic.AddInt64(&b.stats.Published, 1)

	for _, g := range groups {
		if err := g.enqueue(ctx, msg, true); err != nil {
			return err
		}
	}
	return nil
}

// enqueue 把消息放入组队列。tail=true 时新消息排在队尾；
// 重投消息（requeue）插到队首，避免被新消息持续插队而饥饿。
func (g *consumerGroup) enqueue(ctx context.Context, msg *Message, tail bool) error {
	g.mu.Lock()
	// 容量按"组内尚未被最终确认的消息总量"计算：排队 + 退避 + 在途。
	// 已进入消费者通道但未 Ack 的消息仍占资源，不能因为有缓冲消费者就绕过背压。
	for !g.closed && !g.shutting && g.cfg().QueueCapacity >= 0 &&
		len(g.ready)+len(g.wait)+len(g.inflight) >= g.cfg().QueueCapacity {
		if !g.broker.cfg.BlockOnFull {
			g.mu.Unlock()
			return ErrQueueFull
		}
		g.mu.Unlock()
		// 背压阻塞：组内腾出名额、broker 关闭或 ctx 取消时醒来（复核容量）。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.broker.stopCh:
			return ErrBrokerClosed
		case <-g.capCh:
		}
		g.mu.Lock()
	}
	if g.closed || g.shutting {
		g.mu.Unlock()
		return ErrBrokerClosed
	}
	rm := &readyMsg{msg: msg, attempt: 1}
	if tail {
		g.ready = append(g.ready, rm)
	} else {
		g.ready = append([]*readyMsg{rm}, g.ready...)
	}
	g.mu.Unlock()
	g.kick()
	return nil
}

func (g *consumerGroup) cfg() Config { return g.broker.cfg }

// Stats 返回当前计数快照。
func (b *Broker) Stats() Stats {
	s := b.stats.snapshot()
	b.mu.Lock()
	depth := int64(0)
	inflight := int64(0)
	for _, g := range b.groups {
		g.mu.Lock()
		depth += int64(len(g.ready) + len(g.wait))
		inflight += int64(len(g.inflight))
		g.mu.Unlock()
	}
	b.mu.Unlock()
	s.QueueDepth = depth
	s.Inflight = inflight
	return s
}

// dispatchLoop 是每个消费组一个的派发循环：
// 退避到期 -> 回收掉线/超时在途 -> 轮询派发给空闲消费者。
// 组的生命周期与 Broker 相同（在 Close 时统一退出），不做空闲自删除，
// 以避免"派发循环已退出但仍有消息入队"的孤儿组问题。
func (g *consumerGroup) dispatchLoop() {
	defer g.broker.wg.Done()
	ticker := time.NewTicker(g.broker.cfg.ReclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.shutdown:
			// Close 已完成收尾，派发循环退出。
			return
		case <-ticker.C:
		case <-g.notify:
		}
		g.pump()
	}
}

func (g *consumerGroup) pump() {
	now := time.Now()
	g.mu.Lock()

	// 1) 退避到期的消息回到 ready（保持到期顺序）。
	rest := g.wait[:0]
	readyDue := []*readyMsg(nil)
	for _, s := range g.wait {
		if !now.Before(s.runAt) {
			readyDue = append(readyDue, &readyMsg{msg: s.msg, attempt: s.attempt})
		} else {
			rest = append(rest, s)
		}
	}
	g.wait = rest
	if len(readyDue) > 0 {
		g.ready = append(readyDue, g.ready...)
	}

	// 2) 回收：消费者已注销，或在途确认超时。不计入消费失败重试次数。
	reclaimed := []*readyMsg(nil)
	for did, e := range g.inflight {
		if e.consumer.closed || now.After(e.deadline) {
			delete(g.inflight, did)
			reclaimed = append(reclaimed, &readyMsg{msg: e.msg, attempt: e.attempt})
		}
	}
	if len(reclaimed) > 0 {
		atomic.AddInt64(&g.broker.stats.Inflight, -int64(len(reclaimed)))
		atomic.AddInt64(&g.broker.stats.Reclaimed, int64(len(reclaimed)))
		g.ready = append(reclaimed, g.ready...)
	}

	// 3) 派发：轮询消费者，非阻塞发送；通道满的消费者本轮跳过。
	for len(g.ready) > 0 && len(g.members) > 0 {
		picked := -1
		n := len(g.members)
		for i := 0; i < n; i++ {
			idx := (g.rr + i) % n
			c := g.members[idx]
			if c.closed {
				continue
			}
			// 先登记在途再尝试发送；发送失败（通道满）必须同步回滚。
			// 注意：不能把 buildDeliveryLocked 直接放进 select 的发送表达式，
			// 因为 select 无论最终是否选中该 case 都会先对它求值一次，
			// 那样会产生永远不会被结算的幽灵在途记录。
			d := g.buildDeliveryLocked(g.ready[0], c)
			select {
			case c.deliver <- d:
				picked = idx
			default:
				g.rollbackDeliveryLocked(d)
			}
			if picked >= 0 {
				break
			}
		}
		if picked < 0 {
			break
		}
		g.rr = (picked + 1) % len(g.members)
		g.ready = g.ready[1:]
	}

	g.mu.Unlock()
}

// rollbackDeliveryLocked 撤销一次未能真正送出的投递登记。调用方持有 g.mu。
func (g *consumerGroup) rollbackDeliveryLocked(d *Delivery) {
	if _, ok := g.inflight[d.did]; !ok {
		return
	}
	delete(g.inflight, d.did)
	atomic.AddInt64(&g.broker.stats.Delivered, -1)
	atomic.AddInt64(&g.broker.stats.Inflight, -1)
}

// buildDeliveryLocked 构造一次投递并登记在途。调用方持有 g.mu。
func (g *consumerGroup) buildDeliveryLocked(rm *readyMsg, c *consumer) *Delivery {
	attempt := rm.attempt
	if attempt <= 0 {
		attempt = 1
	}
	did := g.broker.nextDeliveryID()
	now := time.Now()
	g.inflight[did] = &inflightEntry{
		msg:         rm.msg,
		group:       g,
		consumer:    c,
		attempt:     attempt,
		deliveredAt: now,
		deadline:    now.Add(g.broker.cfg.AckTimeout),
	}
	atomic.AddInt64(&g.broker.stats.Delivered, 1)
	atomic.AddInt64(&g.broker.stats.Inflight, 1)
	return &Delivery{
		Message:   rm.msg,
		MessageID: rm.msg.ID,
		Group:     g.name,
		Consumer:  c.id,
		Attempt:   attempt,
		ch:        g.broker,
		did:       did,
	}
}

// settle 结算一次在途投递（Ack 或 Nack）。
// 找不到在途记录只可能发生在消息已因超时/掉线被回收，返回 ErrConsumerGone。
func (b *Broker) settle(did deliveryID, ack bool, reason error) error {
	b.mu.Lock()
	groups := make([]*consumerGroup, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}
	b.mu.Unlock()

	for _, g := range groups {
		g.mu.Lock()
		e, ok := g.inflight[did]
		if !ok {
			g.mu.Unlock()
			continue
		}
		delete(g.inflight, did)
		atomic.AddInt64(&b.stats.Inflight, -1)
		if ack {
			atomic.AddInt64(&b.stats.Acked, 1)
			g.mu.Unlock()
			g.kickCap()
			g.kick()
			return nil
		}
		// Nack：消费失败，消耗一次尝试次数，按指数退避重试，超上限进死信。
		maxAttempts := e.msg.maxAttempts(b.cfg.MaxAttempts)
		if maxAttempts < 0 || e.attempt < maxAttempts {
			atomic.AddInt64(&b.stats.Retried, 1)
			g.wait = append(g.wait, &scheduledMsg{
				msg:     e.msg,
				runAt:   time.Now().Add(b.cfg.backoff(e.attempt)),
				attempt: e.attempt + 1,
			})
			g.mu.Unlock()
			g.kick()
			return nil
		}
		dl := &DeadLetter{
			Message:      e.msg,
			Group:        g.name,
			Reason:       reasonString(reason),
			Attempts:     e.attempt,
			FailedAt:     time.Now(),
			LastConsumer: e.consumer.id,
		}
		b.dlqMu.Lock()
		b.dlq = append(b.dlq, dl)
		b.dlqMu.Unlock()
		atomic.AddInt64(&b.stats.DeadLettered, 1)
		g.mu.Unlock()
		g.kickCap()
		g.kick()
		return nil
	}
	return ErrConsumerGone
}

func reasonString(err error) string {
	if err == nil {
		return "nack without reason"
	}
	return err.Error()
}

// workRemaining 返回所有组中尚未最终结算的消息数：排队中 + 退避中 + 在途。
func (b *Broker) workRemaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, g := range b.groups {
		g.mu.Lock()
		n += len(g.ready) + len(g.wait) + len(g.inflight)
		g.mu.Unlock()
	}
	return n
}

// Close 优雅关闭，分两阶段：
//  1. 停止接收新投递/新订阅；派发循环继续工作，把队列剩余消息派发给
//     还在的消费者，并等待在途消息被 Ack/Nack 完成（受 ctx 与 ShutdownTimeout 约束）。
//  2. 到点或收尾完成后关闭所有消费者通道、退出派发循环。
//
// 阶段 1 内每次结算都正常计数；只有到点仍未确认的消息才会被强行回收计数，
// 并通过返回的 ErrShutdownTimeout 明确告知调用方，不会静默丢失或半处理。
func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBrokerClosed
	}
	b.closed = true
	groups := make([]*consumerGroup, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
		g.mu.Lock()
		g.shutting = true
		g.mu.Unlock()
		g.kick()
	}
	b.mu.Unlock()

	deadline := time.Now().Add(b.cfg.ShutdownTimeout)
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	for b.workRemaining() > 0 {
		if waitCtx.Err() != nil {
			break
		}
		select {
		case <-waitCtx.Done():
		case <-time.After(b.cfg.ReclaimInterval):
		}
	}

	timedOut := b.workRemaining() > 0

	// 阶段 2：停止派发循环。关闭后仍在在途的消息（业务没读完/没确认）
	// 按"超时回收"语义如实计数；它们位于内存队列，进程退出不会再被消费。
	close(b.stopCh)
	for _, g := range groups {
		g.mu.Lock()
		g.closed = true
		for _, c := range g.members {
			if !c.closed {
				c.closed = true
				close(c.deliver)
			}
		}
		g.members = nil
		close(g.shutdown)
		g.mu.Unlock()
	}
	b.wg.Wait()

	if timedOut {
		return errors.Join(ErrShutdownTimeout, waitCtx.Err())
	}
	return nil
}
