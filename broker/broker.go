package broker

import (
	"sync"
	"time"
)

// entry 是 broker 内部一条消息的完整生命周期状态。
// 一个 entry 同一时刻只可能处于 pending/inflight/retry 之一，
// 终态为 acked 或 dead；settled 后不再接受任何结算，保证幂等。
type entry struct {
	id         int64
	topic      string
	msg        *Message
	status     string
	attempts   int       // 已发生的投递次数（首投=1，每次重投 +1）
	notBefore  time.Time // 退避结束时间（status=retry 时有效）
	lastError  string    // 最近一次失败原因（进入死信时作为原因的一部分）
	settled    bool      // 是否已到终态（acked/dead）
	holderID   string    // 当前持有者（inflight 时）
	lastHolder string    // 上一持有者（重投时优先避开，优先给组内其他消费者）
	deadline   time.Time // 当前投递的可见性截止时间
	leaseGen   int64     // 租约代号：每次重投 +1，旧投递的 Ack/Nack 一律失效
	lastBeat   time.Time // 持有者最近一次心跳时间
	enqueuedAt time.Time
}

// groupState 是单个 (topic, group) 的队列状态，由其独立的调度 goroutine 串行访问。
type groupState struct {
	topic string
	group string

	ready     []*entry         // 待投递（含退避到期后回到队首的消息）
	retry     []*entry         // 退避中
	inflight  map[int64]*entry // 已投递未结算
	reserved  map[int64]int    // 原子扇出发布的空位预留：ticket -> 条数
	consumers map[string]*Consumer

	// 调度 goroutine 的事件通道。
	enqueueReq   chan enqueueReq
	settleReq    chan settleReq
	registerReq  chan registerReq
	sweep        chan struct{}
	heartbeatReq chan heartbeatReq
	statsReq     chan statsReq
	reserveReq   chan reserveReq
	forceDrain   chan forceDrainReq
	stop         chan struct{}
	done         chan struct{}
	space        chan struct{} // 队列出现空位时非阻塞发信号，唤醒阻塞的 Publish

	capacity int

	metrics *Metrics
	logger  func(format string, args ...any)
	dlq     *DLQ
	cfg     Config

	brokerRef *Broker
}

type enqueueReq struct {
	gs  *groupState
	e   *entry
	res chan enqueueResp
}

type enqueueResp struct {
	accepted bool
	err      error
}

type settleReq struct {
	messageID int64
	leaseGen  int64
	ack       bool
	errMsg    string
	res       chan settleResp
}

type settleResp struct {
	err error
}

type registerReq struct {
	c       *Consumer
	removed bool
	done    chan struct{}
}

// Broker 是纯内存模拟的异步消息 broker。
type Broker struct {
	cfg Config

	mu     sync.Mutex
	groups map[string]*groupState
	dlq    *DLQ

	idSeq      int64
	deliveryID int64

	metrics Metrics

	closeOnce sync.Once
	closed    bool
	closing   bool
	doneCh    chan struct{}
	logger    func(format string, args ...any)
}

// New 用给定配置创建 broker；空字段用默认值补齐。
func New(cfg Config) *Broker {
	d := DefaultConfig()
	if cfg.QueueCapacity == 0 {
		cfg.QueueCapacity = d.QueueCapacity
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = d.VisibilityTimeout
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = d.MaxRetries
	}
	if cfg.Backoff == nil {
		cfg.Backoff = d.Backoff
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = d.SweepInterval
	}
	if cfg.DeliveryBuffer <= 0 {
		cfg.DeliveryBuffer = d.DeliveryBuffer
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = d.ShutdownTimeout
	}
	b := &Broker{
		cfg:    cfg,
		groups: make(map[string]*groupState),
		dlq:    newDLQ(),
		doneCh: make(chan struct{}),
		logger: defaultLogger(),
	}
	return b
}

// SetLogger 替换日志函数；传 nil 关闭日志。
func (b *Broker) SetLogger(f func(format string, args ...any)) {
	if f == nil {
		b.logger = nopLogger()
	} else {
		b.logger = f
	}
}

func groupKey(topic, group string) string { return topic + "\x00" + group }

// getOrCreateGroup 取得或创建某个 (topic, group) 的调度器。
// create=false 时不创建。调用方持有 b.mu。
func (b *Broker) groupLocked(topic, group string, create bool) *groupState {
	gs, ok := b.groups[groupKey(topic, group)]
	if ok || !create {
		return gs
	}
	gs = &groupState{
		topic:        topic,
		group:        group,
		inflight:     make(map[int64]*entry),
		reserved:     make(map[int64]int),
		consumers:    make(map[string]*Consumer),
		enqueueReq:   make(chan enqueueReq),
		settleReq:    make(chan settleReq),
		registerReq:  make(chan registerReq),
		sweep:        make(chan struct{}, 1),
		heartbeatReq: make(chan heartbeatReq),
		statsReq:     make(chan statsReq),
		reserveReq:   make(chan reserveReq),
		forceDrain:   make(chan forceDrainReq),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		space:        make(chan struct{}, 1),
		capacity:     b.cfg.QueueCapacity,
		metrics:      &b.metrics,
		logger:       b.logger,
		dlq:          b.dlq,
		cfg:          b.cfg,
		brokerRef:    b,
	}
	b.groups[groupKey(topic, group)] = gs
	go gs.run()
	return gs
}

func (b *Broker) nextMessageID() int64 {
	b.idSeq++
	return b.idSeq
}

func (b *Broker) nextDeliveryNo() int64 {
	b.deliveryID++
	return b.deliveryID
}

// now 返回当前时间（单独方法便于测试理解）。
func (b *Broker) now() time.Time { return time.Now() }
