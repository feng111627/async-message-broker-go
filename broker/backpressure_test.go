package broker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// 场景4a：无消费者、队列容量为 N，Publish 超过容量必须明确拒绝（ErrQueueFull），
// 并且从 Stats().QueueDepth 看得出积压。
func TestBackpressureReject(t *testing.T) {
	cfg := fastConfig()
	cfg.QueueCapacity = 5
	cfg.BlockOnFull = false
	b := New(cfg)
	// 不订阅任何消费者，消息全部积压在组队列。
	createGroup(b, "g")

	accepted, rejected := 0, 0
	for i := 0; i < 10; i++ {
		err := b.Publish(context.Background(), &Message{ID: fmt.Sprintf("m%d", i), Topic: "t"})
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrQueueFull):
			rejected++
		default:
			t.Fatalf("非预期错误: %v", err)
		}
	}
	if accepted != 5 || rejected != 5 {
		t.Fatalf("accepted=%d rejected=%d，应各为 5", accepted, rejected)
	}
	depth := b.Stats().QueueDepth
	if depth != 5 {
		t.Fatalf("QueueDepth=%d want 5（积压必须可见）", depth)
	}
	t.Logf("[业务:背压-拒绝] 容量=5: 接受=%d 明确拒绝=%d QueueDepth=%d 一眼可见",
		accepted, rejected, depth)

	// 消费者上线并 Ack，积压清空后新投递应重新成功。
	ch, _, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "drain", Buffer: 1})
	drained := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			d := <-ch
			if err := d.Ack(); err != nil {
				t.Errorf("ack: %v", err)
			}
		}
		close(drained)
	}()
	if !waitFor(2*time.Second, func() bool { return b.Stats().QueueDepth == 0 }) {
		t.Fatal("消费后 QueueDepth 未降到 0")
	}
	if err := b.Publish(context.Background(), &Message{ID: "m-again", Topic: "t"}); err != nil {
		t.Fatalf("腾出空间后应能投递: %v", err)
	}
	t.Logf("[结果] 消费腾出空间后新投递成功")
	go func() {
		if d, ok := <-ch; ok {
			_ = d.Ack()
		}
	}()
	<-drained
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景4b：BlockOnFull=true 时 Publish 阻塞；ctx 取消时及时返回；队列腾出空间后恢复。
func TestBackpressureBlockAndCancel(t *testing.T) {
	cfg := fastConfig()
	cfg.QueueCapacity = 2
	cfg.BlockOnFull = true
	cfg.ReclaimInterval = 15 * time.Millisecond
	b := New(cfg)
	// 先建组但不订阅消费者，使消息稳定积压。
	createGroup(b, "g")
	for i := 0; i < 2; i++ {
		mustPublish(t, b, &Message{ID: fmt.Sprintf("m%d", i), Topic: "t"})
	}

	published := make(chan error, 1)
	go func() {
		published <- b.Publish(context.Background(), &Message{ID: "m-blocked", Topic: "t"})
	}()
	select {
	case err := <-published:
		t.Fatalf("队列满时应阻塞，但立即返回了: %v", err)
	case <-time.After(100 * time.Millisecond):
		t.Logf("[业务:背压-阻塞] 容量=2 已满，第三次投递阻塞中")
	}

	// 消费者上线并 Ack 一条 -> 腾出未确认名额 -> 阻塞的 Publish 应成功。
	ch, _, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "worker", Buffer: 1})
	select {
	case d := <-ch:
		if err := d.Ack(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker 未收到")
	}
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("腾出空间后 Publish 应成功: %v", err)
		}
		t.Logf("[结果] 消费腾出位置后阻塞的 Publish 成功")
	case <-time.After(2 * time.Second):
		t.Fatal("阻塞的 Publish 在腾出空间后仍未返回")
	}

	// 持续消费，避免后续关闭卡在在途。
	drainerDone := make(chan struct{})
	go func() {
		for d := range ch {
			_ = d.Ack()
		}
		close(drainerDone)
	}()

	// 再测一次：未确认名额再次满时用 ctx 取消阻塞中的 Publish。
	// 等 worker 把积压消费完，然后停掉它并重新制造满载状态。
	if !waitFor(2*time.Second, func() bool { return b.Stats().QueueDepth == 0 }) {
		t.Fatal("前置积压未清空")
	}
	if err := b.Unsubscribe("g", "worker"); err != nil {
		t.Fatal(err)
	}
	<-drainerDone
	// 此时容量已空，重新灌满 2 条。
	mustPublish(t, b, &Message{ID: "n1", Topic: "t"})
	mustPublish(t, b, &Message{ID: "n2", Topic: "t"})

	blockedCtx, cancel := context.WithCancel(context.Background())
	pubErr := make(chan error, 1)
	go func() {
		pubErr <- b.Publish(blockedCtx, &Message{ID: "m-cancel", Topic: "t"})
	}()
	select {
	case <-pubErr:
		t.Fatal("应当处于阻塞状态")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-pubErr:
		if err == nil {
			t.Fatal("ctx 取消后应返回错误")
		}
		t.Logf("[结果] ctx 取消后阻塞投递及时返回: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消后 Publish 仍未返回")
	}

	// 收尾：消费掉仍积压的 n1/n2，使 Close 能等在途清零。
	chFin, _, _ := b.Subscribe("g", SubscribeOptions{ConsumerID: "fin", Buffer: 2})
	finDone := make(chan struct{})
	go func() {
		for i := 0; i < 2; i++ {
			d := <-chFin
			_ = d.Ack()
		}
		close(finDone)
	}()
	<-finDone
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 场景4c：关闭中的 broker 拒绝新投递，语义明确。
func TestPublishAfterCloseRejected(t *testing.T) {
	b := New(fastConfig())
	if err := b.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(context.Background(), &Message{ID: "x", Topic: "t"}); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("关闭后投递 want ErrBrokerClosed, got %v", err)
	}
	if _, _, err := b.Subscribe("g", SubscribeOptions{}); !errors.Is(err, ErrBrokerClosed) {
		t.Fatalf("关闭后订阅 want ErrBrokerClosed, got %v", err)
	}
	t.Logf("[结果] Close 后 Publish/Subscribe 均返回 ErrBrokerClosed")
}

func TestGracefulShutdownWaitsForInflight(t *testing.T) {
	cfg := fastConfig()
	cfg.ShutdownTimeout = 3 * time.Second
	b := New(cfg)
	ch, _, _ := b.Subscribe("g", SubscribeOptions{Buffer: 8})

	const n = 20
	for i := 0; i < n; i++ {
		mustPublish(t, b, &Message{ID: fmt.Sprintf("m%d", i), Topic: "t"})
	}
	var processed int64
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for d := range ch {
			// 模拟每条消息有一定处理耗时，Close 必须等它们做完。
			time.Sleep(20 * time.Millisecond)
			if err := d.Ack(); err != nil {
				t.Errorf("处理中收到非预期结算错误（可能在关闭后）: %v", err)
				return
			}
			atomic.AddInt64(&processed, 1)
		}
	}()

	// 等消息都派发出去（进入在途或缓冲），再发起关闭。
	if !waitFor(2*time.Second, func() bool { return b.Stats().QueueDepth == 0 }) {
		t.Fatalf("未全部派发: depth=%d", b.Stats().QueueDepth)
	}
	start := time.Now()
	if err := b.Close(context.Background()); err != nil {
		t.Fatalf("优雅关闭不应超时: %v", err)
	}
	elapsed := time.Since(start)
	<-workerDone

	if processed != n {
		t.Fatalf("只处理了一半? processed=%d want %d", processed, n)
	}
	s := b.Stats()
	if s.Acked != n || s.Inflight != 0 {
		t.Fatalf("关闭后状态对不上: %+v", s)
	}
	t.Logf("[业务:退出收尾] Close 等待 %d 条在途全部 Ack，耗时=%v，acked=%d inflight=0",
		n, elapsed.Round(time.Millisecond), s.Acked)
}

// 场景5b：消费者卡死、在途无法收尾时，Close 到点必须返回明确的超时错误，
// 而不是无限等待或静默丢失。
func TestShutdownTimeoutIsExplicit(t *testing.T) {
	cfg := fastConfig()
	cfg.AckTimeout = 10 * time.Second
	cfg.ShutdownTimeout = 150 * time.Millisecond
	cfg.ReclaimInterval = 20 * time.Millisecond
	b := New(cfg)
	ch, _, _ := b.Subscribe("g", SubscribeOptions{Buffer: 1})
	mustPublish(t, b, &Message{ID: "stuck", Topic: "t"})
	select {
	case <-ch: // 取走但永不确认，也永不注销
	case <-time.After(2 * time.Second):
		t.Fatal("未收到")
	}
	t.Logf("[业务:退出收尾-超时] 1 条在途永不确认，ShutdownTimeout=150ms")
	err := b.Close(context.Background())
	if !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("want ErrShutdownTimeout, got %v", err)
	}
	t.Logf("[结果] Close 到点返回明确错误: %v", err)
}
