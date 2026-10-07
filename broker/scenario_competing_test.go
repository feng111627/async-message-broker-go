package broker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 场景1：多个生产者并发投递，同一消费组内多个消费者竞争消费。
// 断言：
//  1. 每条消息恰好被一个消费者拿到且只被 Ack 一次（无重复、无丢失）；
//  2. 所有消息最终都被确认；
//  3. 指标中 published=acked，inflight=0。
func TestConcurrentPublishAndCompetingConsumers(t *testing.T) {
	t.Log("【场景1】并发抢占：多生产者并发投递 + 同组多消费者竞争，验证不丢不重")

	const producers = 8
	const perProducer = 50
	const total = producers * perProducer
	const consumers = 4

	b := New(Config{
		QueueCapacity:     64,
		BlockWhenFull:     true,
		VisibilityTimeout: 5 * time.Second,
		MaxRetries:        2,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     5 * time.Millisecond,
		DeliveryBuffer:    1,
	})
	b.SetLogger(nil)
	t.Cleanup(func() { b.Close(context.Background()) })

	ctx := context.Background()

	cs := make([]*Consumer, 0, consumers)
	for i := 0; i < consumers; i++ {
		c, err := b.Subscribe("order-group", "orders", fmt.Sprintf("consumer-%d", i))
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		cs = append(cs, c)
	}
	t.Logf("用例1.1: 已建立 %d 个同组消费者，准备并发投递 %d 条消息", consumers, total)

	// 消费者侧：记录每条消息被谁拿到、被 Ack 几次。
	var mu sync.Mutex
	holder := make(map[string]string) // payload -> consumerID
	ackCount := make(map[string]int)
	var perConsumer [consumers]int

	var cwg sync.WaitGroup
	for ci, c := range cs {
		cwg.Add(1)
		go func(idx int, cons *Consumer) {
			defer cwg.Done()
			for {
				d, err := cons.Receive(ctx)
				if err != nil {
					return
				}
				key := string(d.Message.Payload)
				mu.Lock()
				if prev, exists := holder[key]; exists && prev != cons.ID() {
					t.Errorf("消息 %s 同时被两个消费者拿到: %s 与 %s", key, prev, cons.ID())
				}
				holder[key] = cons.ID()
				perConsumer[idx]++
				mu.Unlock()
				if err := d.Ack(); err != nil {
					t.Errorf("ack %s: %v", key, err)
				}
				mu.Lock()
				ackCount[key]++
				mu.Unlock()
			}
		}(ci, c)
	}

	// 生产者侧并发投递（队列小，会触发 block 背压）。
	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(pid int) {
			defer pwg.Done()
			for i := 0; i < perProducer; i++ {
				msg := &Message{Payload: []byte(fmt.Sprintf("msg-p%d-%d", pid, i))}
				if _, err := b.Publish(ctx, "orders", msg, true); err != nil {
					t.Errorf("producer %d publish: %v", pid, err)
					return
				}
			}
		}(p)
	}
	pwg.Wait()
	t.Logf("用例1.2: %d 个生产者并发投递完成（容量仅 64，已验证 block 背压不丢消息）", producers)

	// 等待全部 Ack。
	deadline := time.Now().Add(10 * time.Second)
	for {
		m := b.Metrics()
		if m.Acked >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 Ack 超时: %s", b.Metrics())
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, c := range cs {
		c.Close()
	}
	cwg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if len(holder) != total {
		t.Fatalf("被拿到的消息数=%d, 期望 %d（存在丢失或重复）", len(holder), total)
	}
	for k, n := range ackCount {
		if n != 1 {
			t.Fatalf("消息 %s 被 Ack 了 %d 次，期望恰好 1 次", k, n)
		}
	}
	dist := ""
	for i, n := range perConsumer {
		dist += fmt.Sprintf("consumer-%d=%d ", i, n)
	}
	m := b.Metrics()
	if m.Acked != total || m.Inflight != 0 {
		t.Fatalf("终态指标异常: %s", m)
	}
	t.Logf("用例1.3: 结果 全部 %d 条消息恰好 Ack 一次，消费分布[%s]，指标[%s]", total, dist, m)
	t.Log("【场景1】通过：不丢、不重、同组同时只被一个消费者拿到")
}
