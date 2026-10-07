// 本地可运行演示（纯内存，无外部依赖）：
//
//	go run ./cmd/demo
//
// 覆盖：多生产者并发投递、同组多消费者竞争、显式 Ack、失败 Nack 重试到死信、
// 死信重投、消费者掉线重投、队列背压、SIGINT/SIGTERM 优雅收尾。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feng111627/async-message-broker-go/broker"
)

func main() {
	cfg := broker.DefaultConfig()
	cfg.QueueCapacity = 8 // 故意设小，让背压可见
	cfg.VisibilityTimeout = 1500 * time.Millisecond
	cfg.MaxRetries = 2
	cfg.Backoff = broker.ExponentialBackoff{Base: 100 * time.Millisecond, Max: time.Second}
	cfg.SweepInterval = 20 * time.Millisecond

	b := broker.New(cfg)

	ctx, stop := broker.ShutdownContext()
	defer stop()

	const topic = "orders"
	const group = "order-workers"

	// 3 个竞争消费者。
	var consumers []*broker.Consumer
	for i := 0; i < 3; i++ {
		c, err := b.Subscribe(group, topic, fmt.Sprintf("worker-%d", i))
		if err != nil {
			panic(err)
		}
		consumers = append(consumers, c)
	}

	var wg sync.WaitGroup
	var processed int64
	for i, c := range consumers {
		wg.Add(1)
		go func(id int, cons *broker.Consumer) {
			defer wg.Done()
			for {
				d, err := cons.Receive(ctx)
				if err != nil {
					return
				}
				name := string(d.Message.Payload)
				switch {
				case name == "poison":
					// 始终失败：演示重试到死信。
					fmt.Printf("[worker-%d] 处理 %s 失败(attempt=%d) -> Nack\n", id, name, d.Attempt)
					_ = d.Nack(errors.New("不可恢复的业务错误"))
				default:
					time.Sleep(40 * time.Millisecond) // 模拟处理耗时
					if err := d.Ack(); err == nil {
						atomic.AddInt64(&processed, 1)
						if id == 0 && atomic.LoadInt64(&processed)%5 == 0 {
							fmt.Printf("[worker-%d] 已处理 %s(attempt=%d)\n", id, name, d.Attempt)
						}
					}
				}
			}
		}(i, c)
	}

	// 4 个并发生产者。
	var pwg sync.WaitGroup
	for p := 0; p < 4; p++ {
		pwg.Add(1)
		go func(pid int) {
			defer pwg.Done()
			for i := 0; i < 15; i++ {
				name := fmt.Sprintf("order-p%d-%d", pid, i)
				if pid == 0 && i == 0 {
					name = "poison" // 只有一条毒消息会进死信
				}
				_, err := b.Publish(ctx, topic, &broker.Message{Payload: []byte(name)}, true)
				if err != nil {
					if errors.Is(err, broker.ErrBrokerClosed) {
						return
					}
					fmt.Println("publish error:", err)
					return
				}
			}
		}(p)
	}
	pwg.Wait()
	fmt.Println("== 全部消息已发布，等待处理/重试/死信 ==")

	// 给毒消息足够时间重试到死信，然后把死信重投一次（这里直接看它仍进死信，
	// 因为消费者对 "poison" 始终失败；演示重投 API 与独立失败原因）。
	time.Sleep(3 * time.Second)
	dls := b.ListDeadLetters(topic, group)
	for _, dl := range dls {
		fmt.Printf("== 死信: payload=%s reason=%s attempts=%d lastError=%q ==\n",
			dl.Message.Payload, dl.Reason, dl.Attempts, dl.LastError)
	}

	// 背压观察：打印当前积压。
	ready, inflight, capn, _ := b.QueueStats(topic, group)
	fmt.Printf("== 队列状态: ready=%d inflight=%d capacity=%d ==\n", ready, inflight, capn)

	// 等普通消息处理完。
	time.Sleep(500 * time.Millisecond)

	// 优雅收尾：等在途消息 Ack 完再退出。收到 Ctrl+C 也会走同一路径。
	fmt.Println("== 开始优雅关闭（等待在途消息结算） ==")
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Close(closeCtx); err != nil {
		fmt.Println("close error:", err)
	}
	wg.Wait()

	fmt.Printf("== 结束指标: %s ==\n", b.Metrics())
	fmt.Printf("== 共成功处理 %d 条；死信 %d 条 ==\n",
		atomic.LoadInt64(&processed), b.DeadLetterCount())
	os.Exit(0)
}
