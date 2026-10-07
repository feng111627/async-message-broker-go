package broker

import (
	"context"
	"testing"
	"time"
)

// 场景3A：消费者显式 Close（优雅掉线），持有的未确认消息立刻重投给组内其他消费者。
func TestConsumerCrashRedelivery_OnClose(t *testing.T) {
	t.Log("【场景3A】消费者优雅掉线(Close)：未 Ack 消息立即重投给存活消费者")

	b := New(Config{
		QueueCapacity:     100,
		VisibilityTimeout: 10 * time.Second,
		MaxRetries:        5,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     5 * time.Millisecond,
	})
	b.SetLogger(nil)
	t.Cleanup(func() { b.Close(context.Background()) })
	ctx := context.Background()

	c1, _ := b.Subscribe("grp", "tasks", "c1")
	c2, _ := b.Subscribe("grp", "tasks", "c2")

	if _, err := b.Publish(ctx, "tasks", &Message{Payload: []byte("job-1")}, false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d1, err := c1.Receive(ctx)
	if err != nil {
		t.Fatalf("c1 receive: %v", err)
	}
	if d1.ConsumerID != "c1" {
		t.Fatalf("首次应由 c1 拿到，实际 %s", d1.ConsumerID)
	}
	t.Log("用例3A.1: c1 拿到 job-1 但不 Ack，直接 Close（模拟优雅退出/掉线）")

	if err := c1.Close(); err != nil {
		t.Fatalf("close c1: %v", err)
	}
	// c1 旧 Delivery 的 Ack 必须失效，防止脑裂式双重确认。
	if err := d1.Ack(); err == nil {
		t.Fatal("掉线消费者旧投递的 Ack 必须失效")
	}
	t.Log("用例3A.2: c1 已关闭，其旧 Ack 已失效，等待 c2 收到重投")

	d2, err := c2.Receive(ctx)
	if err != nil {
		t.Fatalf("c2 重投接收失败: %v", err)
	}
	if string(d2.Message.Payload) != "job-1" {
		t.Fatalf("重投消息内容不对: %s", d2.Message.Payload)
	}
	if d2.ConsumerID != "c2" {
		t.Fatalf("重投应给到存活的 c2，实际 %s", d2.ConsumerID)
	}
	if d2.Attempt != 2 {
		t.Fatalf("重投 attempt=%d，期望 2", d2.Attempt)
	}
	t.Logf("用例3A.3: 存活消费者 c2 收到重投 attempt=%d，执行 Ack", d2.Attempt)
	if err := d2.Ack(); err != nil {
		t.Fatalf("c2 ack: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	m := b.Metrics()
	if m.Acked != 1 || m.Redelivered != 1 || m.Inflight != 0 {
		t.Fatalf("指标异常: %s", m)
	}
	t.Logf("【场景3A】通过：消息未丢，只被重投一次并由存活者确认；指标[%s]", m)
}

// 场景3B：消费者“崩溃”（不调用 Close，直接停止处理），超过可见性超时后
// 未确认消息被回收，重投给组内其他存活消费者。
func TestConsumerCrashRedelivery_OnVisibilityTimeout(t *testing.T) {
	t.Log("【场景3B】消费者崩溃(不 Close)：可见性超时后回收并重投给存活消费者")

	b := New(Config{
		QueueCapacity:     100,
		VisibilityTimeout: 80 * time.Millisecond,
		MaxRetries:        5,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     10 * time.Millisecond,
	})
	b.SetLogger(nil)
	t.Cleanup(func() { b.Close(context.Background()) })
	ctx := context.Background()

	// 先只订阅 crashed，保证首次投递落在它身上。
	crashed, _ := b.Subscribe("grp2", "jobs", "crashed")

	if _, err := b.Publish(ctx, "jobs", &Message{Payload: []byte("job-x")}, false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	d, err := crashed.Receive(ctx)
	if err != nil {
		t.Fatalf("crashed receive: %v", err)
	}
	if d.Attempt != 1 || d.ConsumerID != "crashed" {
		t.Fatalf("首次投递应给 crashed attempt=1，实际 consumer=%s attempt=%d", d.ConsumerID, d.Attempt)
	}
	t.Logf("用例3B.1: crashed 拿到 job-x(attempt=%d) 后模拟崩溃，既不 Ack 也不 Close", d.Attempt)

	// crashed 崩溃后，组内加入另一个存活消费者等待接管。
	alive, _ := b.Subscribe("grp2", "jobs", "alive")

	// alive 应该在可见性超时后拿到重投。
	start := time.Now()
	d2, err := alive.Receive(ctx)
	if err != nil {
		t.Fatalf("alive 未收到超时重投: %v", err)
	}
	elapsed := time.Since(start)
	if string(d2.Message.Payload) != "job-x" {
		t.Fatalf("重投内容不对: %s", d2.Message.Payload)
	}
	if d2.Attempt != 2 {
		t.Fatalf("重投 attempt=%d，期望 2", d2.Attempt)
	}
	if elapsed < 70*time.Millisecond {
		t.Fatalf("重投过早（%v），应在可见性超时之后", elapsed)
	}
	t.Logf("用例3B.2: 等待 %v 后 alive 收到超时重投 attempt=%d", elapsed.Round(time.Millisecond), d2.Attempt)
	if err := d2.Ack(); err != nil {
		t.Fatalf("alive ack: %v", err)
	}

	// crashed 复活后迟到的 Ack 必须无效，不能影响已被 alive 确认的消息。
	if err := d.Ack(); err == nil {
		t.Fatal("超时回收后，原消费者迟到的 Ack 必须失效")
	}
	t.Log("用例3B.3: crashed 迟到的 Ack 已被租约机制拒绝，不会双重结算")

	time.Sleep(30 * time.Millisecond)
	m := b.Metrics()
	if m.Acked != 1 || m.Redelivered != 1 || m.Inflight != 0 || m.DeadLettered != 0 {
		t.Fatalf("指标异常: %s", m)
	}
	t.Logf("【场景3B】通过：超时回收 + 重投 + 旧 Ack 失效；指标[%s]", m)
}

// 场景3C：配置了较短的心跳超时，消费者持有消息后停止心跳，
// 消息在心跳超时后被回收重投（无需等到 VisibilityTimeout）。
func TestConsumerCrashRedelivery_OnHeartbeatLost(t *testing.T) {
	t.Log("【场景3C】心跳丢失：持有消息期间停止心跳，提前于可见性超时被回收重投")

	b := New(Config{
		QueueCapacity:     100,
		VisibilityTimeout: 5 * time.Second, // 很长，主要靠心跳超时触发
		HeartbeatTimeout:  60 * time.Millisecond,
		MaxRetries:        5,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     10 * time.Millisecond,
	})
	b.SetLogger(nil)
	t.Cleanup(func() { b.Close(context.Background()) })
	ctx := context.Background()

	stale, _ := b.Subscribe("grp3", "pings", "stale")
	if _, err := b.Publish(ctx, "pings", &Message{Payload: []byte("ping-1")}, false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	d, err := stale.Receive(ctx)
	if err != nil {
		t.Fatalf("stale receive: %v", err)
	}
	t.Logf("用例3C.1: stale 拿到消息(attempt=%d)后停止心跳（HeartbeatTimeout=60ms）", d.Attempt)

	// 心跳一次确认 API 正常。
	if err := stale.Heartbeat(); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// 之后不再心跳。再加入一个存活且持续心跳的消费者。
	alive, _ := b.Subscribe("grp3", "pings", "alive")
	stopBeat := make(chan struct{})
	go func() {
		tk := time.NewTicker(15 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopBeat:
				return
			case <-tk.C:
				_ = alive.Heartbeat()
			}
		}
	}()

	start := time.Now()
	d2, err := alive.Receive(ctx)
	close(stopBeat)
	if err != nil {
		t.Fatalf("alive 未收到心跳超时重投: %v", err)
	}
	elapsed := time.Since(start)
	if d2.ConsumerID != "alive" || d2.Attempt != 2 {
		t.Fatalf("重投目标/次数不对: consumer=%s attempt=%d", d2.ConsumerID, d2.Attempt)
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("心跳回收耗时 %v 过长，应远早于 5s 的可见性超时", elapsed)
	}
	t.Logf("用例3C.2: %v 后（远早于可见性超时）alive 收到重投 attempt=%d 并 Ack",
		elapsed.Round(time.Millisecond), d2.Attempt)
	if err := d2.Ack(); err != nil {
		t.Fatalf("alive ack: %v", err)
	}
	if err := d.Ack(); err == nil {
		t.Fatal("被心跳回收后，旧持有者迟到的 Ack 必须失效")
	}
	t.Log("【场景3C】通过：心跳超时回收 + 重投 + 旧 Ack 失效")
}
