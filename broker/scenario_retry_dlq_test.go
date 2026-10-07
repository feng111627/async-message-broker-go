package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 场景2：消费失败 -> 按退避重试 -> 超过上限进入死信（带可区分原因）
//
//	-> 死信重投回队列 -> 处理成功；全程终态计数不重复累加。
func TestRetryToDLQAndRequeue(t *testing.T) {
	t.Log("【场景2】失败重试到死信 + 死信重投 + 计数幂等")

	b := New(Config{
		QueueCapacity:     100,
		VisibilityTimeout: 5 * time.Second,
		MaxRetries:        2, // 首投 + 2 次重投，共 3 次尝试后进死信
		Backoff:           ConstantBackoff{Delay: 5 * time.Millisecond},
		SweepInterval:     2 * time.Millisecond,
		DeliveryBuffer:    1,
	})
	b.SetLogger(nil)
	ctx := context.Background()
	t.Cleanup(func() { b.Close(context.Background()) })

	c, err := b.Subscribe("pay-group", "payments", "w1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	const msgID = "always-fail"
	if _, err := b.Publish(ctx, "payments", &Message{Payload: []byte(msgID)}, false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("用例2.1: 投递一条始终失败的消息，消费者每次都 Nack，预期重试 2 次后进死信")

	var mu sync.Mutex
	failSeen := 0
	// 先消费并 Nack 三次（attempts 1、2、3）。
	deadline := time.Now().Add(3 * time.Second)
	for failSeen < 3 {
		d, rerr := c.Receive(ctx)
		if rerr != nil {
			t.Fatalf("receive: %v", rerr)
		}
		if string(d.Message.Payload) != msgID {
			t.Fatalf("意外消息: %s", d.Message.Payload)
		}
		mu.Lock()
		failSeen++
		attempt := d.Attempt
		mu.Unlock()
		t.Logf("  第 %d 次尝试(attempt=%d) -> Nack(业务错误码)", failSeen, attempt)

		// 针对“同一次投递”重复 Nack：除第一次外都必须失败，且不能重复计数。
		if nerr := d.Nack(errors.New("payment gateway 500")); nerr != nil {
			t.Fatalf("首次 Nack 不应失败: %v", nerr)
		}
		if nerr := d.Nack(errors.New("duplicate nack")); nerr == nil {
			t.Fatal("重复 Nack 必须返回错误，不能重复结算")
		} else {
			t.Logf("  重复 Nack 被正确拒绝: %v（计数不累加）", nerr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 3 次尝试超时，failSeen=%d metrics=%s", failSeen, b.Metrics())
		}
	}

	// 等待进入死信。
	deadline = time.Now().Add(3 * time.Second)
	for b.DeadLetterCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("等待进入死信超时: %s", b.Metrics())
		}
		time.Sleep(5 * time.Millisecond)
	}

	dls := b.ListDeadLetters("payments", "pay-group")
	if len(dls) != 1 {
		t.Fatalf("死信数=%d, 期望 1", len(dls))
	}
	dl := dls[0]
	if dl.Reason != DLQReasonMaxRetries {
		t.Fatalf("死信原因=%q，期望 %q", dl.Reason, DLQReasonMaxRetries)
	}
	if dl.Attempts != 3 {
		t.Fatalf("死信 attempts=%d，期望 3", dl.Attempts)
	}
	if dl.LastError != "payment gateway 500" {
		t.Fatalf("死信失败原因=%q，期望保留最后一次业务错误", dl.LastError)
	}
	m1 := b.Metrics()
	if m1.DeadLettered != 1 || m1.Acked != 0 || m1.Inflight != 0 {
		t.Fatalf("进死信后指标异常: %s", m1)
	}
	t.Logf("用例2.2: 已进死信 reason=%s attempts=%d lastError=%q；指标[%s]",
		dl.Reason, dl.Attempts, dl.LastError, m1)

	// 区分另一种死信原因：可见性超时（场景3 会触发），这里只确认常量可区分。
	if DLQReasonMaxRetries == DLQReasonTimeout {
		t.Fatal("两类死信原因必须可区分")
	}

	// 死信重投：作为全新生命周期，attempts 归零，这次处理成功。
	t.Log("用例2.3: 将该死信重新投回队列，消费者这次处理成功")
	// 让旧消费者退场、换新消费者，证明重投可被组内任意消费者处理。
	if err := c.Close(); err != nil {
		t.Fatalf("close old consumer: %v", err)
	}
	c2, err := b.Subscribe("pay-group", "payments", "w2")
	if err != nil {
		t.Fatalf("subscribe w2: %v", err)
	}
	if err := b.RequeueDeadLetter(ctx, dl.MessageID(), true); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	d2, err := c2.Receive(ctx)
	if err != nil {
		t.Fatalf("receive after requeue: %v", err)
	}
	if d2.Attempt != 1 {
		t.Fatalf("重投后 attempt=%d，期望 1（全新生命周期）", d2.Attempt)
	}
	if err := d2.Ack(); err != nil {
		t.Fatalf("ack after requeue: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	if got := b.DeadLetterCount(); got != 0 {
		t.Fatalf("重投后死信数=%d，期望 0", got)
	}
	m2 := b.Metrics()
	// 死信只计过 1 次；重投成功后 ack=1，requeue=1，dead 仍=1（历史终态不回滚）。
	if m2.Acked != 1 || m2.DeadLettered != 1 || m2.Requeued != 1 || m2.Inflight != 0 {
		t.Fatalf("重投后终态计数错乱: %s", m2)
	}
	t.Logf("用例2.4: 重投成功，死信清空；最终指标[%s]", m2)
	t.Log("【场景2】通过：重试退避可配置、死信带可区分原因、重投幂等、计数不错乱")

	_ = fmt.Sprintf
}
