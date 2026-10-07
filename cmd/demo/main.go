// Command demo 在单进程内用本地内存 broker 演示：
//   - 多生产者并发投递
//   - 一个消费组内多消费者抢占、显式 Ack/Nack
//   - 失败按指数退避重试，超上限进死信，随后人工重投
//   - 队列背压（容量满拒绝）
//   - 收到 SIGINT/SIGTERM 后等在途消息收尾再退出
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/feng111627/async-message-broker-go/broker"
)

func main() {
	cfg := broker.DefaultConfig()
	cfg.QueueCapacity = 8
	cfg.MaxAttempts = 3
	cfg.BackoffInitial = 100 * time.Millisecond
	cfg.BackoffMax = 500 * time.Millisecond
	cfg.AckTimeout = 2 * time.Second
	cfg.ReclaimInterval = 50 * time.Millisecond
	cfg.ShutdownTimeout = 10 * time.Second
	cfg.BlockOnFull = false // 队列满时明确拒绝，便于观察背压

	b := broker.New(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	const group = "orders"
	var wg sync.WaitGroup

	// 3 个消费者，按 messageID 决定行为，使各分支可解释。
	startConsumer := func(id string, behavior func(*broker.Delivery) error) {
		ch, cid, err := b.Subscribe(group, broker.SubscribeOptions{ConsumerID: id, Buffer: 4})
		if err != nil {
			panic(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range ch {
				err := behavior(d)
				if err != nil {
					fmt.Printf("[%s] %s 处理失败(attempt=%d): %v\n", cid, d.MessageID, d.Attempt, err)
					_ = d.Nack(err)
					continue
				}
				fmt.Printf("[%s] %s 处理成功(attempt=%d)\n", cid, d.MessageID, d.Attempt)
				_ = d.Ack()
			}
		}()
	}

	// id 为 fail-* 的消息前两次失败、第三次成功；poison 永远失败（演示死信）。
	makeBehavior := func() func(*broker.Delivery) error {
		return func(d *broker.Delivery) error {
			time.Sleep(50 * time.Millisecond)
			switch {
			case d.MessageID == "poison":
				return errors.New("poison message: always fails")
			case len(d.MessageID) >= 5 && d.MessageID[:5] == "fail-" && d.Attempt < 3:
				return fmt.Errorf("transient error on attempt %d", d.Attempt)
			default:
				return nil
			}
		}
	}
	startConsumer("worker-1", makeBehavior())
	startConsumer("worker-2", makeBehavior())
	startConsumer("worker-3", makeBehavior())

	// 2 个生产者并发投递；队列满时收到 ErrQueueFull，稍后重试。
	var pwg sync.WaitGroup
	publish := func(prefix string, n int) {
		defer pwg.Done()
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("%s%d", prefix, i)
			err := b.Publish(ctx, &broker.Message{ID: id, Topic: "orders.created"})
			if errors.Is(err, broker.ErrQueueFull) {
				fmt.Printf("[producer %s] 队列满，%s 被背压拒绝，稍后重试\n", prefix, id)
				time.Sleep(200 * time.Millisecond)
				i--
				continue
			}
			if err != nil {
				return
			}
		}
	}
	pwg.Add(2)
	go publish("ok-", 10)
	go publish("fail-", 6)

	// 额外投一条必然进死信的消息。
	_ = b.Publish(ctx, &broker.Message{ID: "poison", Topic: "orders.created"})

	// 等 poison 进入死信，演示"查看死信 -> 重投"（这里重投后仍会失败，再次进死信）。
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			dlq := b.DeadLetters()
			if len(dlq) == 0 {
				continue
			}
			for _, dl := range dlq {
				fmt.Printf("[dlq] 死信: id=%s 原因=%q 尝试=%d 最后消费者=%s\n",
					dl.Message.ID, dl.Reason, dl.Attempts, dl.LastConsumer)
			}
			return
		}
	}()

	pwg.Wait()

	// 给消费与重试留一点观察时间，随后主动进入收尾（也可由信号触发）。
	go func() {
		time.Sleep(1500 * time.Millisecond)
		stop()
	}()
	<-ctx.Done()

	fmt.Println("\n[shutdown] 收到退出信号，等待在途消息收尾...")
	if err := b.Close(context.Background()); err != nil {
		fmt.Printf("[shutdown] 关闭返回: %v\n", err)
	}
	wg.Wait()

	s := b.Stats()
	fmt.Printf("[shutdown] 完成。published=%d delivered=%d acked=%d retried=%d "+
		"reclaimed=%d dead=%d requeued=%d inflight=%d 剩余死信=%d\n",
		s.Published, s.Delivered, s.Acked, s.Retried, s.Reclaimed,
		s.DeadLettered, s.RequeueDLQ, s.Inflight, len(b.DeadLetters()))
	os.Exit(0)
}
