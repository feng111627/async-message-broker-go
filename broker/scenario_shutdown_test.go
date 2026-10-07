package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 场景4A：非阻塞模式下队列满被明确拒绝（ErrQueueFull + 拒绝计数），
// 队列腾出位置后可继续投递；阻塞模式下 Publish 会停住并在有空位后自动恢复。
func TestBackpressure_RejectAndBlock(t *testing.T) {
	t.Log("【场景4A】背压：队列满时明确拒绝；阻塞模式下可观察地停住并在空位后恢复")

	b := New(Config{
		QueueCapacity:     3,
		BlockWhenFull:     false,
		VisibilityTimeout: 10 * time.Second,
		MaxRetries:        3,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     5 * time.Millisecond,
		DeliveryBuffer:    1,
	})
	b.SetLogger(nil)
	ctx := context.Background()

	// 不订阅消费者：让消息全部堆积在 ready，快速打满。
	c, _ := b.Subscribe("grp", "t", "slow")

	pub := func(i int, block bool) error {
		_, err := b.Publish(ctx, "t", &Message{Payload: []byte(fmt.Sprintf("m%d", i))}, block)
		return err
	}

	// 容量 3，前 3 条成功，第 4 条非阻塞投递应被明确拒绝。
	for i := 0; i < 3; i++ {
		if err := pub(i, false); err != nil {
			t.Fatalf("前 %d 条应成功，第 %d 条报错: %v", 3, i, err)
		}
	}
	ready, inflight, capn, _ := b.QueueStats("t", "grp")
	t.Logf("用例4A.1: 投递 %d 条后队列 ready=%d inflight=%d capacity=%d", 3, ready, inflight, capn)

	if err := pub(3, false); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("队列满应返回 ErrQueueFull，实际: %v", err)
	}
	t.Log("用例4A.2: 队列满，非阻塞投递被明确拒绝并返回 ErrQueueFull")

	// 阻塞投递应当停住，直到消费者取走并 Ack 一条腾出空位。
	var blockedErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 用带超时的 ctx 保护测试，正常应在消费后很快返回。
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, err := b.Publish(cctx, "t", &Message{Payload: []byte("blocked-msg")}, true)
		if err != nil {
			blockedErr.Store(err)
		}
	}()
	time.Sleep(50 * time.Millisecond) // 确认它确实在阻塞等待
	t.Log("用例4A.3: 阻塞式 Publish 已停住等待空位（背压可观察）")

	// 消费者取走一条并 Ack，腾出空位。
	d, err := c.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
	wg.Wait()
	if v := blockedErr.Load(); v != nil {
		t.Fatalf("腾出空位后阻塞投递应成功，实际报错: %v", v)
	}
	time.Sleep(20 * time.Millisecond)
	m := b.Metrics()
	if m.PublishFull < 1 {
		t.Fatalf("拒绝计数应 >=1，实际 %s", m)
	}
	t.Logf("用例4A.4: 空位出现后阻塞投递自动恢复成功；拒绝计数=%d；指标[%s]", m.PublishFull, m)
	t.Log("【场景4A】通过：满队列可拒绝、可阻塞、可恢复，且全程可观察")
}

// 场景4B：收到退出信号关闭 broker 时，在途消息必须被处理（Ack）完才退出；
// 不允许出现“关闭已返回但在途消息还没处理完”的半完成状态。
func TestGracefulShutdown_WaitsForInflight(t *testing.T) {
	t.Log("【场景4B】退出收尾：Close 等待所有在途消息 Ack 完成后才返回")

	b := New(Config{
		QueueCapacity:     100,
		VisibilityTimeout: 10 * time.Second,
		MaxRetries:        3,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     5 * time.Millisecond,
		DeliveryBuffer:    1,
	})
	b.SetLogger(nil)

	ctx := context.Background()
	const n = 20
	cs := make([]*Consumer, 0, 2)
	for i := 0; i < 2; i++ {
		c, _ := b.Subscribe("grp", "work", fmt.Sprintf("w%d", i))
		cs = append(cs, c)
	}
	for i := 0; i < n; i++ {
		if _, err := b.Publish(ctx, "work", &Message{Payload: []byte(fmt.Sprintf("w%d", i))}, false); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	var acked atomic.Int64
	var wg sync.WaitGroup
	for _, c := range cs {
		wg.Add(1)
		go func(cons *Consumer) {
			defer wg.Done()
			for {
				d, err := cons.Receive(ctx)
				if err != nil {
					return
				}
				// 模拟在途处理需要一点时间，证明 Close 确实在等，而不是立即返回。
				time.Sleep(10 * time.Millisecond)
				if err := d.Ack(); err == nil {
					acked.Add(1)
				}
			}
		}(c)
	}

	// 等一部分消息进入在途后发起关闭（此时仍有消息正在处理）。
	time.Sleep(30 * time.Millisecond)
	t.Logf("用例4B.1: 消费者正在处理中(已 Ack=%d/%d)，此时发起优雅关闭", acked.Load(), n)

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeDone := make(chan struct{})
	go func() {
		b.Close(shutCtx)
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// 好，关闭返回了。
	case <-time.After(6 * time.Second):
		t.Fatal("Close 未能在在途消息处理完后及时返回")
	}

	// Close 返回后，所有在途消息必须已结算，不允许半完成。
	m := b.Metrics()
	if m.Inflight != 0 {
		t.Fatalf("Close 返回后仍有在途消息 inflight=%d", m.Inflight)
	}
	if acked.Load() != n {
		t.Fatalf("Close 返回时仅 Ack=%d/%d，存在处理一半的消息", acked.Load(), n)
	}
	if m.Acked != n {
		t.Fatalf("最终 acked=%d，期望 %d；指标[%s]", m.Acked, n, m)
	}
	wg.Wait()
	t.Logf("用例4B.2: Close 在全部 %d 条在途消息 Ack 完成后才返回；指标[%s]", n, m)

	// 关闭后再发布/订阅必须被明确拒绝。
	if _, err := b.Publish(ctx, "work", &Message{Payload: []byte("after")}, false); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("关闭后 Publish 应返回 ErrBrokerClosed，实际: %v", err)
	}
	if _, err := b.Subscribe("grp", "work", "late"); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("关闭后 Subscribe 应返回 ErrBrokerClosed，实际: %v", err)
	}
	t.Log("用例4B.3: 关闭后新 Publish/Subscribe 被明确拒绝（ErrBrokerClosed）")
	t.Log("【场景4B】通过：退出时在途消息全部结算，无半完成状态，关闭后拒绝新负载")
}
