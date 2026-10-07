package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testCtx() context.Context { return context.Background() }

// 场景1：多生产者并发投递 + 多消费者按消费组抢占。
// 验证：每条消息在组内恰好被一个消费者拿到并恰好 Ack 一次，不重不漏。
func TestConcurrentProducersAndGroupPreemption(t *testing.T) {
	const (
		producers   = 8
		consumers   = 6
		perProducer = 100
		total       = producers * perProducer
		group       = "orders"
	)
	cfg := fastConfig()
	cfg.QueueCapacity = total * 2
	b := New(cfg)

	var acked int64
	var consumerHits []int64
	consumerHits = make([]int64, consumers)
	var stopConsumers sync.WaitGroup

	for i := 0; i < consumers; i++ {
		ch, id, err := b.Subscribe(group, SubscribeOptions{ConsumerID: fmt.Sprintf("c%d", i), Buffer: 4})
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		stopConsumers.Add(1)
		go func(idx int, ch <-chan *Delivery, id string) {
			defer stopConsumers.Done()
			for d := range ch {
				if d.Consumer != id {
					t.Errorf("delivery consumer mismatch")
				}
				consumerHits[idx]++
				if err := d.Ack(); err != nil {
					t.Errorf("ack: %v", err)
				}
				atomic.AddInt64(&acked, 1)
			}
		}(i, ch, id)
	}

	var pwg sync.WaitGroup
	for p := 0; p < producers; p++ {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for i := 0; i < perProducer; i++ {
				mustPublish(t, b, &Message{
					ID:      fmt.Sprintf("p%d-m%d", p, i),
					Topic:   "orders.created",
					Payload: []byte("x"),
				})
			}
		}(p)
	}
	pwg.Wait()
	t.Logf("[业务:并发抢占] 已投递 %d 条，等待消费组 %s 的 %d 个消费者抢占确认", total, group, consumers)

	if !waitFor(5*time.Second, func() bool { return atomic.LoadInt64(&acked) == total }) {
		t.Fatalf("acked=%d want=%d", atomic.LoadInt64(&acked), total)
	}

	// 所有消费者都应实际抢到过消息（公平性的弱校验）。
	for i, h := range consumerHits {
		if h == 0 {
			t.Fatalf("consumer c%d 从未收到消息，轮询派发可能有饥饿", i)
		}
	}
	hits := make([]int64, consumers)
	copy(hits, consumerHits)
	t.Logf("[结果] 每个消费者抢占条数: %v，合计=%d", hits, total)

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	stopConsumers.Wait()

	s := b.Stats()
	if s.Published != int64(total) {
		t.Fatalf("published=%d want=%d", s.Published, total)
	}
	if s.Acked != int64(total) {
		t.Fatalf("acked stat=%d want=%d", s.Acked, total)
	}
	if s.DeadLettered != 0 || s.Retried != 0 {
		t.Fatalf("不应有重试/死信: retried=%d dlq=%d", s.Retried, s.DeadLettered)
	}
	if s.Inflight != 0 {
		t.Fatalf("关闭后在途应为0, got=%d", s.Inflight)
	}
	t.Logf("[结果] 计数核对 published=%d delivered=%d acked=%d inflight=%d",
		s.Published, s.Delivered, s.Acked, s.Inflight)
}

// 场景1b：不同消费组之间是广播，各自独立确认与计数。
func TestMultipleGroupsEachGetCopy(t *testing.T) {
	b := New(fastConfig())
	var gotA, gotB int64
	run := func(group string, counter *int64) {
		ch, _, err := b.Subscribe(group, SubscribeOptions{Buffer: 8})
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			for d := range ch {
				atomic.AddInt64(counter, 1)
				_ = d.Ack()
			}
		}()
	}
	run("group-a", &gotA)
	run("group-b", &gotB)

	for i := 0; i < 20; i++ {
		mustPublish(t, b, &Message{ID: fmt.Sprintf("m%d", i), Topic: "t"})
	}
	if !waitFor(3*time.Second, func() bool {
		return atomic.LoadInt64(&gotA) == 20 && atomic.LoadInt64(&gotB) == 20
	}) {
		t.Fatalf("gotA=%d gotB=%d", gotA, gotB)
	}
	t.Logf("[业务:组间广播] group-a=%d group-b=%d，各拿一份且互不影响", gotA, gotB)
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
