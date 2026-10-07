package broker

import (
	"testing"
	"time"
)

// fastConfig 返回一套用于测试的快速配置：退避短、确认超时短。
func fastConfig() Config {
	c := DefaultConfig()
	c.QueueCapacity = 64
	c.MaxAttempts = 3
	c.BackoffInitial = 10 * time.Millisecond
	c.BackoffMax = 40 * time.Millisecond
	c.AckTimeout = 300 * time.Millisecond
	c.ReclaimInterval = 20 * time.Millisecond
	c.ShutdownTimeout = 3 * time.Second
	c.BlockOnFull = false
	return c
}

func waitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}

func mustPublish(t *testing.T, b *Broker, msg *Message) {
	t.Helper()
	if err := b.Publish(testCtx(), msg); err != nil {
		t.Fatalf("publish %s: %v", msg.ID, err)
	}
}

// createGroup 在不订阅消费者的情况下创建消费组（测试用）。
func createGroup(b *Broker, name string) {
	b.mu.Lock()
	b.ensureGroupLocked(name)
	b.mu.Unlock()
}
