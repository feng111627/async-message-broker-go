package broker

import (
	"errors"
	"time"
)

// 投递/操作结果状态。
const (
	StatusAcked    = "acked"    // 消费者显式确认，处理成功
	StatusPending  = "pending"  // 尚未被任何消费者领取
	StatusInflight = "inflight" // 已被领取，等待显式确认
	StatusRetry    = "retry"    // 处理失败/超时，退避等待重新投递
	StatusDead     = "dead"     // 超过重试上限，进入死信
)

// 死信原因。
const (
	DLQReasonMaxRetries = "max_retries_exceeded" // Nack/失败次数超过上限
	DLQReasonTimeout    = "visibility_timeout"   // 多次投递均未确认（超时/掉线）
)

// 常见错误。
var (
	ErrBrokerClosed    = errors.New("broker: broker is closed")
	ErrQueueFull       = errors.New("broker: topic queue is full")
	ErrNoSubscription  = errors.New("broker: no consumer group subscribes to topic")
	ErrInvalidMessage  = errors.New("broker: invalid message payload")
	ErrUnknownDelivery = errors.New("broker: unknown or already settled delivery")
	ErrConsumerClosed  = errors.New("broker: consumer already closed")
	ErrShuttingDown    = errors.New("broker: broker is shutting down")
)

// Message 是生产者发布的原始消息。
type Message struct {
	Key        string            // 可选：用于按 key 路由/解释
	Payload    []byte            // 消息体
	Headers    map[string]string // 可选头
	EnqueuedAt time.Time         // 入队时间（由 broker 填充）
}

// Delivery 是投递给消费者的一条消息，带投递上下文。
type Delivery struct {
	ID           int64             // 投递序号（每次重新投递递增），仅用于日志解释
	Message      *Message          // 原始消息
	Topic        string            // 所属主题
	Group        string            // 所属消费组
	DeliveryNo   int64             // 本条消息第几次投递（1 = 首次）
	Attempt      int               // 当前是第几次消费尝试（含本次）
	ReceivedAt   time.Time         // 本次投递时间
	Deadline     time.Time         // 本次投递的可见性截止时间（超时未确认会重投）
	ConsumerID   string            // 持有本次投递的消费者 ID
	Ack          func() error      // 显式确认：处理成功
	Nack         func(error) error // 显式否认：按策略重试或进死信
	_messageID   int64
	_deliveryKey int64
}

// DeadLetter 是进入死信队列的消息快照，保留可解释的失败原因。
type DeadLetter struct {
	Message    *Message
	Topic      string
	Group      string
	Reason     string // DLQReason*
	LastError  string
	Attempts   int       // 一共被消费/尝试了多少次
	EnqueuedAt time.Time // 最初入队时间
	DeadAt     time.Time // 进入死信的时间
	_messageID int64
}

// Config 是 broker 的配置。
type Config struct {
	// 队列容量（每个 (topic, group) 一条队列，统计 ready+inflight+retry 中的消息数）。
	// 0 或负数表示无界：不会返回 ErrQueueFull，阻塞模式以外永远可写。
	QueueCapacity int
	// 默认满队列策略：true = Publish 默认阻塞等待空位；false = 默认立即返回 ErrQueueFull。
	// 单次 Publish 也可以通过 block 参数覆盖。
	BlockWhenFull bool
	// 消息被领取后多久没确认视为掉线/超时，重新投递给组内其他消费者。
	VisibilityTimeout time.Duration
	// 消费者心跳超时：超过该时长没有心跳且仍持有消息，按掉线处理。
	// 0 表示关闭心跳检查（仅靠 VisibilityTimeout 回收）。
	HeartbeatTimeout time.Duration
	// 最大重试/重投次数：首次投递 + MaxRetries 次重投后仍未成功确认则进死信。
	MaxRetries int
	// 退避策略：根据当前失败尝试次数返回下一次重投前的等待时间。nil 表示不退避。
	Backoff BackoffStrategy
	// 后台调度（超时扫描、退避到期）间隔。
	SweepInterval time.Duration
	// 每个消费者投递通道的缓冲大小（默认 1）。
	DeliveryBuffer int
	// 关闭时最长等待在途消息结算的兜底时长；<=0 表示不兜底（Close 的 ctx 说了算）。
	ShutdownTimeout time.Duration
}

// DefaultConfig 返回一组可直接使用的默认配置。
func DefaultConfig() Config {
	return Config{
		QueueCapacity:     1000,
		BlockWhenFull:     false,
		VisibilityTimeout: 10 * time.Second,
		HeartbeatTimeout:  0,
		MaxRetries:        3,
		Backoff:           ConstantBackoff{Delay: 50 * time.Millisecond},
		SweepInterval:     20 * time.Millisecond,
		DeliveryBuffer:    1,
		ShutdownTimeout:   30 * time.Second,
	}
}

// MessageID 返回该消息在 broker 内的唯一 ID，用于 RequeueDeadLetter。
func (d *DeadLetter) MessageID() int64 { return d._messageID }
