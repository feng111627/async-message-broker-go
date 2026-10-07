package broker

import "time"

// Config 控制 broker 的容量、重试、可见性超时等行为。
// 零值不可直接使用，请用 DefaultConfig() 后再覆盖。
type Config struct {
	// QueueCapacity 每个消费组待投递队列的容量；0 使用默认值，<0 表示无界。
	QueueCapacity int

	// MaxAttempts 一条消息最多被尝试投递的次数（含首次）；<0 表示无限重试。
	MaxAttempts int

	// BackoffInitial / BackoffMax / BackoffMultiplier 指数退避参数。
	BackoffInitial    time.Duration
	BackoffMax        time.Duration
	BackoffMultiplier float64

	// AckTimeout 在途消息多久未确认就被重新投递给组内其他消费者。
	AckTimeout time.Duration
	// ReclaimInterval 扫描超时在途消息的频率。
	ReclaimInterval time.Duration

	// BlockOnFull=true 时 Publish 在队列满时阻塞；false 时立即返回 ErrQueueFull。
	BlockOnFull bool

	// ShutdownTimeout Close 等待在途消息收尾的最长时间。
	ShutdownTimeout time.Duration
}

// DefaultConfig 返回一套可本地运行的默认配置。
func DefaultConfig() Config {
	return Config{
		QueueCapacity:     1024,
		MaxAttempts:       3,
		BackoffInitial:    50 * time.Millisecond,
		BackoffMax:        2 * time.Second,
		BackoffMultiplier: 2,
		AckTimeout:        5 * time.Second,
		ReclaimInterval:   100 * time.Millisecond,
		ShutdownTimeout:   10 * time.Second,
	}
}

func (c Config) normalized() Config {
	out := c
	if out.QueueCapacity == 0 {
		out.QueueCapacity = 1024
	}
	if out.MaxAttempts == 0 {
		out.MaxAttempts = 3
	}
	if out.BackoffInitial <= 0 {
		out.BackoffInitial = 50 * time.Millisecond
	}
	if out.BackoffMax <= 0 {
		out.BackoffMax = 2 * time.Second
	}
	if out.BackoffMultiplier <= 1 {
		out.BackoffMultiplier = 2
	}
	if out.AckTimeout <= 0 {
		out.AckTimeout = 5 * time.Second
	}
	if out.ReclaimInterval <= 0 {
		out.ReclaimInterval = 100 * time.Millisecond
	}
	if out.ShutdownTimeout <= 0 {
		out.ShutdownTimeout = 10 * time.Second
	}
	return out
}

// backoff 返回第 attempt 次失败后、下一次投递前应等待的时长。
// attempt 从 1 开始（即首次投递失败后的退避）。
func (c Config) backoff(attempt int) time.Duration {
	d := c.BackoffInitial
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * c.BackoffMultiplier)
		if d >= c.BackoffMax {
			return c.BackoffMax
		}
	}
	if d > c.BackoffMax {
		d = c.BackoffMax
	}
	return d
}
