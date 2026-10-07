package broker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// 场景3：消费者拿到消息后掉线（未 Ack），消息必须被组内其他消费者拿到并确认；
// 掉线重投不消耗失败重试次数，也不应进死信。
func TestConsumerGoneRequeue(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxAttempts = 1 // 若错误地计为消费失败，消息会直接进死信，测试会暴露
	cfg.AckTimeout = 30 * time.Second
	b := New(cfg)

	ch1, id1, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "slow-1", Buffer: 8})
	ch2, id2, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "alive-2", Buffer: 8})

	for i := 0; i < 10; i++ {
		mustPublish(t, b, &Message{ID: fmt.Sprintf("m%d", i), Topic: "t"})
	}

	// 让 slow-1 收走若干消息但全部不确认，模拟"拿到后卡住"。
	var stolen []string
	deadline := time.After(500 * time.Millisecond)
loop:
	for {
		select {
		case d := <-ch1:
			stolen = append(stolen, d.MessageID)
		case <-deadline:
			break loop
		}
	}
	if len(stolen) == 0 {
		t.Fatal("slow-1 没有收到任何消息，测试前提不成立")
	}
	t.Logf("[业务:消费者掉线] slow-1 收走 %d 条但全部未确认: %v", len(stolen), stolen)

	var ackedByAlive int64
	reclaimedIDs := make(chan string, 16)
	done := make(chan struct{})
	go func() {
		for d := range ch2 {
			if d.Consumer != id2 {
				t.Errorf("重投目标消费者=%s want %s", d.Consumer, id2)
			}
			if d.Attempt != 1 {
				t.Errorf("掉线重投 attempt=%d want 1（不消耗重试次数）", d.Attempt)
			}
			if err := d.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
			atomic.AddInt64(&ackedByAlive, 1)
			reclaimedIDs <- d.MessageID
		}
		close(done)
	}()

	// 等待 alive-2 把未被 slow-1 拿走的消息先消费完（共 10-stolen 条）。
	baseline := int64(10 - len(stolen))
	if !waitFor(3*time.Second, func() bool { return atomic.LoadInt64(&ackedByAlive) == baseline }) {
		t.Fatalf("前置消费完成数=%d want %d", ackedByAlive, baseline)
	}
	stolenSet := map[string]bool{}
	for _, id := range stolen {
		stolenSet[id] = true
	}

	// slow-1 掉线：在途消息应立即回收重投给 alive-2。
	if err := b.Unsubscribe("g", id1); err != nil {
		t.Fatal(err)
	}
	t.Logf("[动作] slow-1 已 Unsubscribe，等待 alive-2 接管 %d 条在途消息", len(stolen))

	gotReclaimed := map[string]bool{}
	deadlineCh := time.After(3 * time.Second)
	for len(gotReclaimed) < len(stolen) {
		select {
		case id := <-reclaimedIDs:
			if stolenSet[id] {
				gotReclaimed[id] = true
			}
		case <-deadlineCh:
			t.Fatalf("alive-2 接管确认=%d want %d", len(gotReclaimed), len(stolen))
		}
	}

	s := b.Stats()
	if s.DeadLettered != 0 {
		t.Fatalf("掉线重投不应进死信, dlq=%d", s.DeadLettered)
	}
	if s.Reclaimed != int64(len(stolen)) {
		t.Fatalf("reclaimed=%d want %d", s.Reclaimed, len(stolen))
	}
	if s.Acked != 10 {
		t.Fatalf("10 条消息最终都应被确认, acked=%d", s.Acked)
	}
	t.Logf("[结果] alive-2 共确认 %d 条（其中接管重投 %d 条）；总 acked=%d reclaimed=%d dlq=%d",
		atomic.LoadInt64(&ackedByAlive), len(gotReclaimed), s.Acked, s.Reclaimed, s.DeadLettered)

	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-done
}

// 场景3b：消费者进程"假死"——既不确认也不显式注销。
// 超过 AckTimeout 后消息必须能重新投给组内其他人，不能一直卡着。
func TestAckTimeoutReclaim(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxAttempts = 1
	cfg.AckTimeout = 120 * time.Millisecond
	cfg.ReclaimInterval = 30 * time.Millisecond
	b := New(cfg)

	chZombie, _, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "zombie", Buffer: 1})
	chHealthy, id2, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "healthy", Buffer: 4})

	mustPublish(t, b, &Message{ID: "stuck-1", Topic: "t"})

	// zombie 取走但永不 Ack。
	var zombieGotAt time.Time
	select {
	case d := <-chZombie:
		if d.MessageID != "stuck-1" {
			t.Fatal("got", d.MessageID)
		}
		zombieGotAt = time.Now()
		t.Logf("[业务:确认超时] zombie 取走 stuck-1 后不确认（AckTimeout=120ms）")
	case <-time.After(2 * time.Second):
		t.Fatal("zombie 未收到消息")
	}

	select {
	case d := <-chHealthy:
		if d.MessageID != "stuck-1" || d.Consumer != id2 {
			t.Fatalf("超时重投目标错误: %+v", d)
		}
		if d.Attempt != 1 {
			t.Fatalf("超时重投 attempt=%d want 1", d.Attempt)
		}
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
		t.Logf("[结果] zombie 取走 %v 后 healthy 接管并确认（>= AckTimeout）", time.Since(zombieGotAt).Round(time.Millisecond))
	case <-time.After(3 * time.Second):
		t.Fatal("超时后消息未重投给 healthy")
	}

	s := b.Stats()
	if s.Acked != 1 || s.DeadLettered != 0 || s.Reclaimed < 1 {
		t.Fatalf("计数错误: %+v", s)
	}
	t.Logf("[结果] acked=%d reclaimed=%d dlq=%d", s.Acked, s.Reclaimed, s.DeadLettered)
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景3c：掉线重投期间旧消费者迟到的 Ack 必须失败，不能把消息重复确认。
func TestLateAckAfterReclaimRejected(t *testing.T) {
	cfg := fastConfig()
	cfg.AckTimeout = 100 * time.Millisecond
	cfg.ReclaimInterval = 20 * time.Millisecond
	b := New(cfg)
	ch1, id1, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "c1", Buffer: 1})
	ch2, _, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "c2", Buffer: 4})
	mustPublish(t, b, &Message{ID: "x1", Topic: "t"})
	stale := <-ch1
	// 等消息被超时回收并重新投递给 c2 且被确认。
	var redelivered *Delivery
	select {
	case redelivered = <-ch2:
	case <-time.After(3 * time.Second):
		t.Fatal("未重投")
	}
	if err := redelivered.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := stale.Ack(); !errors.Is(err, ErrConsumerGone) {
		t.Fatalf("旧投递迟到 Ack want ErrConsumerGone, got %v", err)
	}
	if b.Stats().Acked != 1 {
		t.Fatalf("迟到 Ack 不应重复累计, acked=%d", b.Stats().Acked)
	}
	t.Logf("[结果] 旧消费者 %s 的迟到 Ack 被拒绝，acked 仍为 1", id1)
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
