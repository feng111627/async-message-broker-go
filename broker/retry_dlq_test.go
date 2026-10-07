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

// 场景2：消费一直失败 -> 按 MaxAttempts 重试 -> 超过上限进死信，
// 死信里要能看到区分得开的失败原因；重试期间计数精确。
func TestRetryToDeadLetter(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxAttempts = 3
	b := New(cfg)

	ch, cid, err := b.Subscribe("payments", SubscribeOptions{ConsumerID: "worker-1", Buffer: 16})
	if err != nil {
		t.Fatal(err)
	}
	var attemptsSeen []int32
	var mu sync.Mutex
	failReason := errors.New("payment gateway 500")
	go func() {
		for d := range ch {
			mu.Lock()
			attemptsSeen = append(attemptsSeen, int32(d.Attempt))
			mu.Unlock()
			_ = d.Nack(failReason)
		}
	}()

	mustPublish(t, b, &Message{ID: "pay-1", Topic: "payments.charge"})
	t.Logf("[业务:重试到死信] pay-1 投递，消费者 worker-1 始终 Nack，MaxAttempts=3")

	if !waitFor(3*time.Second, func() bool { return b.Stats().DeadLettered == 1 }) {
		t.Fatalf("未进入死信: %+v", b.Stats())
	}

	dlq := b.DeadLetters()
	if len(dlq) != 1 {
		t.Fatalf("dlq len=%d want 1", len(dlq))
	}
	dl := dlq[0]
	if dl.Message.ID != "pay-1" || dl.Group != "payments" || dl.LastConsumer != cid {
		t.Fatalf("死信上下文错误: %+v", dl)
	}
	if dl.Attempts != 3 {
		t.Fatalf("死信 Attempts=%d want 3", dl.Attempts)
	}
	if dl.Reason != failReason.Error() {
		t.Fatalf("死信原因=%q want %q", dl.Reason, failReason.Error())
	}
	mu.Lock()
	got := append([]int32(nil), attemptsSeen...)
	mu.Unlock()
	want := []int32{1, 2, 3}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("投递尝试序号=%v want %v（退避后必须带上递增的 attempt）", got, want)
	}
	s := b.Stats()
	if s.Delivered != 3 || s.Retried != 2 || s.DeadLettered != 1 || s.Acked != 0 || s.Inflight != 0 {
		t.Fatalf("计数错误: %+v", s)
	}
	t.Logf("[结果] 尝试序号=%v，delivered=%d retried=%d dlq=%d 原因=%q",
		got, s.Delivered, s.Retried, s.DeadLettered, dl.Reason)

	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景2b：不同失败原因的死信必须能彼此区分。
func TestDeadLetterReasonsAreDistinguishable(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxAttempts = 1
	b := New(cfg)
	ch, _, _ := b.Subscribe("g", SubscribeOptions{Buffer: 16})
	reasons := []error{
		errors.New("invalid payload: missing amount"),
		errors.New("dependency down: inventory"),
		errors.New("business rule rejected: frozen account"),
	}
	go func() {
		i := int32(0)
		for d := range ch {
			idx := int(atomic.AddInt32(&i, 1)-1) % len(reasons)
			_ = d.Nack(reasons[idx])
		}
	}()
	for i := range reasons {
		mustPublish(t, b, &Message{ID: fmt.Sprintf("m%d", i), Topic: "t"})
	}
	if !waitFor(3*time.Second, func() bool { return b.Stats().DeadLettered == 3 }) {
		t.Fatalf("dlq=%d", b.Stats().DeadLettered)
	}
	dlq := b.DeadLetters()
	seen := map[string]bool{}
	for _, dl := range dlq {
		seen[dl.Reason] = true
	}
	for _, r := range reasons {
		if !seen[r.Error()] {
			t.Fatalf("死信原因缺失: %q，实际: %v", r.Error(), seen)
		}
	}
	t.Logf("[结果] 3 条死信原因可区分: %v", keys(seen))
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// 场景2c：死信重新投回后按全新尝试计数，重放成功不再产生重复死信计数。
func TestRequeueDeadLetter(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxAttempts = 2
	b := New(cfg)
	ch, _, _ := b.Subscribe("g", SubscribeOptions{Buffer: 16})

	var willFail atomic.Bool
	willFail.Store(true)
	var acked atomic.Int32
	go func() {
		for d := range ch {
			if willFail.Load() {
				_ = d.Nack(errors.New("transient"))
				continue
			}
			_ = d.Ack()
			acked.Add(1)
		}
	}()

	mustPublish(t, b, &Message{ID: "m1", Topic: "t"})
	if !waitFor(3*time.Second, func() bool { return b.Stats().DeadLettered == 1 }) {
		t.Fatal("未进死信")
	}
	before := b.Stats()
	if len(b.DeadLetters()) != 1 {
		t.Fatal("dlq len")
	}
	t.Logf("[业务:死信重投] m1 已失败 %d 次进死信（delivered=%d），现在修复后重新投回",
		before.DeadLettered, before.Delivered)

	willFail.Store(false)
	if err := b.RequeueDeadLetter(context.Background(), 0); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(b.DeadLetters()) != 0 {
		t.Fatalf("重投后死信应移除, len=%d", len(b.DeadLetters()))
	}
	if !waitFor(3*time.Second, func() bool { return acked.Load() == 1 }) {
		t.Fatal("重投后未被 Ack")
	}
	s := b.Stats()
	if s.RequeueDLQ != 1 {
		t.Fatalf("requeue stat=%d want 1", s.RequeueDLQ)
	}
	if s.DeadLettered != 1 {
		t.Fatalf("死信计数不应因重投而重复累加: %d", s.DeadLettered)
	}
	if s.Acked != 1 || s.Inflight != 0 {
		t.Fatalf("重投后计数错误: %+v", s)
	}
	// 重投按 attempt=1 重新开始：历史 delivered=2，重投只多 1 次。
	if s.Delivered != 3 {
		t.Fatalf("delivered=%d want 3（历史2次 + 重投1次，attempt 已重置）", s.Delivered)
	}
	t.Logf("[结果] 重投后首次尝试即 Ack：delivered=%d acked=%d dlq=%d requeue=%d",
		s.Delivered, s.Acked, s.DeadLettered, s.RequeueDLQ)

	// 下标越界要明确报错。
	if err := b.RequeueDeadLetter(context.Background(), 0); !errors.Is(err, ErrDLQMessageGone) {
		t.Fatalf("want ErrDLQMessageGone, got %v", err)
	}
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景2d：同一次投递重复 Ack/Nack 只生效一次，计数不累加。
func TestDuplicateSettlementCountsOnce(t *testing.T) {
	b := New(fastConfig())
	ch, _, _ := b.Subscribe("g", SubscribeOptions{Buffer: 4})
	mustPublish(t, b, &Message{ID: "m1", Topic: "t"})
	d := <-ch
	if err := d.Ack(); err != nil {
		t.Fatal(err)
	}
	if err := d.Ack(); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("重复 Ack want ErrAlreadySettled, got %v", err)
	}
	if err := d.Nack(errors.New("late")); !errors.Is(err, ErrAlreadySettled) {
		t.Fatalf("Ack 后 Nack want ErrAlreadySettled, got %v", err)
	}
	s := b.Stats()
	if s.Acked != 1 || s.Delivered != 1 || s.Retried != 0 || s.DeadLettered != 0 {
		t.Fatalf("重复结算导致计数错乱: %+v", s)
	}
	t.Logf("[结果] 重复 Ack/Nack 被拒绝且计数不变: acked=%d delivered=%d", s.Acked, s.Delivered)
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
