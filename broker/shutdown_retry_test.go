package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 场景5c：Close 期间在途消息 Nack 后进入短退避再重投，最终 Ack，
// 收尾必须把这一轮重试也算进去，不能提前退出导致消息半途丢失。
func TestShutdownDrainsRetryDuringDrain(t *testing.T) {
	cfg := fastConfig()
	cfg.MaxAttempts = 5
	cfg.BackoffInitial = 30 * time.Millisecond
	cfg.BackoffMax = 30 * time.Millisecond
	cfg.ShutdownTimeout = 5 * time.Second
	b := New(cfg)
	ch, _, _ := b.Subscribe("g", SubscribeOptions{Buffer: 4})

	var attempts atomic.Int32
	var nacks atomic.Int32
	go func() {
		for d := range ch {
			if d.MessageID == "flaky" && attempts.Add(1) <= 2 {
				nacks.Add(1)
				_ = d.Nack(errors.New("flaky during shutdown"))
				continue
			}
			_ = d.Ack()
		}
	}()

	mustPublish(t, b, &Message{ID: "flaky", Topic: "t"})
	// 让消息先派发出去、并经历一次失败进入退避，再发起 Close。
	if !waitFor(2*time.Second, func() bool { return attempts.Load() >= 1 }) {
		t.Fatal("未产生首次投递")
	}
	t.Logf("[业务:收尾含重试] Close 时 flaky 已尝试 %d 次（Nack %d 次）处于退避，收尾须等它重试到成功",
		attempts.Load(), nacks.Load())

	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("收尾应等待退避重试完成: %v", err)
	}
	s := b.Stats()
	if s.Acked != 1 || s.Inflight != 0 {
		t.Fatalf("状态对不上: %+v", s)
	}
	if attempts.Load() != 3 || nacks.Load() != 2 || s.Retried != 2 {
		t.Fatalf("应收尾中再失败 1 次后成功: attempts=%d nacks=%d retried=%d",
			attempts.Load(), nacks.Load(), s.Retried)
	}
	t.Logf("[结果] 收尾期间完成退避重试并 Ack：共尝试 3 次、Nack 2 次；acked=%d retried=%d inflight=0",
		s.Acked, s.Retried)
}
