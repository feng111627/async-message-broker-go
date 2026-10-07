package broker

import (
	"context"
	"testing"
	"time"
)

func TestSmokeDeliverAck(t *testing.T) {
	b := New(Config{
		QueueCapacity:     10,
		VisibilityTimeout: 2 * time.Second,
		MaxRetries:        2,
		Backoff:           ConstantBackoff{Delay: time.Millisecond},
		SweepInterval:     5 * time.Millisecond,
	})
	b.SetLogger(nil)
	defer b.Close(context.Background())

	c, err := b.Subscribe("g1", "orders", "c1")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ctx := context.Background()
	if _, err := b.Publish(ctx, "orders", &Message{Payload: []byte("hello")}, false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	d, err := c.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if string(d.Message.Payload) != "hello" {
		t.Fatalf("payload=%q", d.Message.Payload)
	}
	if err := d.Ack(); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// 重复 Ack 必须失败且不重复计数。
	if err := d.Ack(); err == nil {
		t.Fatal("duplicate ack should fail")
	}
	time.Sleep(20 * time.Millisecond)
	m := b.Metrics()
	if m.Acked != 1 {
		t.Fatalf("acked=%d want 1; %s", m.Acked, m)
	}
	if m.Inflight != 0 {
		t.Fatalf("inflight=%d want 0; %s", m.Inflight, m)
	}
	t.Logf("冒烟通过: %s", m)
}
