# async-message-broker-go

一个**纯本地内存**实现的异步消息投递 / 消费组件（无外部中间件依赖），用于演示并验证在
**并发与异常**情况下，投递语义如何做到可靠、可解释。

- 多生产者并发投递；多消费者按**消费组**订阅
- 组内一条消息同一时刻只投给一个消费者；**显式 Ack 才算成功**
- 未确认消息在**消费者掉线**或**确认超时**后，自动重投给组内其他存活消费者——不丢、不卡死
- 失败**指数退避重试**，超过可配置上限进入**死信队列（DLQ）**，带可区分的失败原因，可**重投回炉**
- 积压时**背压**：队列满可选择"明确拒绝"或"阻塞（可取消）"，积压量通过 Stats 可见
- 进程收到退出信号后**优雅收尾**：等在途消息处理完再退出；到点未完成则返回明确错误，不静默丢消息

> 数据保存在内存里，定位是"本地可跑、语义清晰的参考实现与组件"，不是持久化消息中间件。

## 快速开始

```bash
go run ./cmd/demo      # 单进程演示：并发投递/抢占/重试/死信/背压/信号收尾
go test ./... -v       # 运行全部场景测试，日志按 [业务:...] / [结果:...] 输出
go test ./... -race    # 带竞态检测
```

## 核心 API

```go
b := broker.New(broker.DefaultConfig()) // 或自定义 broker.Config

// 订阅消费组；组内负载均衡（轮询），组间广播
ch, consumerID, err := b.Subscribe("orders", broker.SubscribeOptions{
    ConsumerID: "worker-1", // 可选，默认自动生成
    Buffer:     4,          // 消费者本地通道缓冲 = 该消费者未取走的在途上限
})

// 生产者
err := b.Publish(ctx, &broker.Message{ID: "idempotency-key", Topic: "orders.created"})

// 消费者
for d := range ch {
    if handle(d) != nil {
        _ = d.Nack(err)    // 显式否认：触发退避重试 / 死信
    } else {
        _ = d.Ack()        // 显式确认：仅 Ack 后消息才算处理成功
    }
}

// 死信
for _, dl := range b.DeadLetters() { /* dl.Message / dl.Reason / dl.Attempts ... */ }
_ = b.RequeueDeadLetter(ctx, index) // 把死信重新投回原消费组（尝试计数清零）

_ = b.Close(ctx) // 优雅关闭
```

## 投递与确认语义

| 语义 | 行为 |
| --- | --- |
| 组内互斥 | 一条消息在某消费组内，同一时刻**只在一个消费者的在途记录里**，只投给一个消费者 |
| 组间广播 | 一条消息会派发给**所有已存在的消费组**，各组独立维护确认与计数 |
| 显式确认 | 消费者必须调用 `Delivery.Ack()` 才算成功；`Nack(reason)` 表示本次处理失败 |
| 结算幂等 | 每个 `Delivery` 只能结算一次。重复 `Ack/Nack`（含先 Ack 后 Nack）返回 `ErrAlreadySettled`，计数不重复累加 |
| 迟到结算 | 消息已因超时/掉线被回收后，旧 `Delivery` 再 `Ack/Nack` 返回 `ErrConsumerGone`，不会把消息重复确认 |
| 消息身份 | 重投会生成新的 `Delivery`（新投递序号），但 `Message.ID` 不变，业务可用它做处理幂等 |

> 说明：组件保证"至少一次（at-least-once）"投递——掉线/超时会重投。**业务处理的幂等性
> 应由消费方基于稳定的 `Message.ID` 保证**；组件侧保证计数与状态不因重试/重投而错乱。

### 消费者掉线 / 确认超时重投

- **显式掉线**：调用 `Unsubscribe`（或等价的消费者注销）后，该消费者名下所有在途消息
  **立即**回收重投给组内其他存活成员，其投递通道随后关闭。
- **假死（不确认也不注销）**：在途消息超过 `AckTimeout` 未结算，由后台扫描
  （频率 `ReclaimInterval`）回收并重新派发。
- 掉线/超时回收属于**投递层重投，不计为"消费失败"**，因此**不消耗重试次数**、
  不会因此进入死信（见 `Delivery.Attempt` 保持不变）。
- 回收消息回到组队列**队首**，避免被新消息持续插队而饥饿。

## 重试与死信规则

- `MaxAttempts` 为最大尝试次数（**含首次**）：一条消息被 `Nack` 到第 `MaxAttempts` 次即进死信。
  - `Message.MaxAttempts` 非零时按消息覆盖全局值；取值 `<0` 表示**无限重试**。
- 每次 `Nack` 后按指数退避重新排队：
  `delay = min(BackoffInitial * BackoffMultiplier^(attempt-1), BackoffMax)`。
- 进入死信时记录可区分的失败上下文：`Message / Group / Reason / Attempts /
  LastConsumer / FailedAt`。`Reason` 即最后一次 `Nack(reason)` 的错误文本，不同原因天然可区分。
- `RequeueDeadLetter` 把消息重新投回**原消费组**，移除该死信记录，并以 `attempt=1`
  **重新开始计数**；消息 `ID` 不变。历史死信计数不会因为重放而重复累加，重放只走正常的
  delivered / acked（或再次失败进死信）流程。
- 重投的消息在队列满时同样受背压约束（拒绝或阻塞），不会挤掉新消息。

### 计数口径（Stats）

| 字段 | 累加时机（每事件恰好一次） |
| --- | --- |
| `Published` | `Publish` 被系统接受一次 |
| `Delivered` | 消息一次实际派发（重试/重投各算一次；尝试发送但通道满会同步回滚，不计） |
| `Acked` | 一次在途被 `Ack` |
| `Retried` | 一次 `Nack` 后决定退避重试 |
| `DeadLettered` | 一条消息达到上限进入死信 |
| `Reclaimed` | 一次因消费者掉线 / 确认超时导致的回收重投 |
| `RequeueDLQ` | 一次从死信重投回队列 |
| `Inflight` | 当前在途数（已派发未结算），实时增减 |
| `QueueDepth` | 当前排队 + 退避中的积压数 |

## 背压

容量按**消费组**维度、按"组内尚未被最终确认的消息总量"计算（排队 + 退避 + 在途）。
进入消费者通道缓冲但还没 Ack 的消息仍占名额，避免空闲的大缓冲消费者无限吸收消息。

- `BlockOnFull=false`（默认）：满了 `Publish` **立即返回 `ErrQueueFull`**，调用方可稍后重试。
- `BlockOnFull=true`：满了 `Publish` **阻塞**，直到腾出名额；可用 `ctx` 取消
  （返回 `context.Canceled/DeadlineExceeded`），broker 关闭时返回 `ErrBrokerClosed`。
- `QueueCapacity < 0` 表示无界（请自行评估风险）。

## 退出收尾（优雅关闭）

`Close(ctx)` 分两阶段：

1. **停止接收**新投递 / 新订阅（返回 `ErrBrokerClosed`），但**派发循环继续工作**：
   把队列剩余消息派发给存活消费者，等待在途消息被 Ack/Nack 完成。
2. 收尾完成，或达到 `min(ctx, ShutdownTimeout)` 时限后，关闭所有消费者通道、退出后台循环。

- 正常情况下：等待所有在途结算完才返回 `nil`，不会出现"只处理一半"或状态对不上。
- 到点仍有未确认消息：返回 `ErrShutdownTimeout`（`errors.Is` 可判定），明确告知有多少
  工作没能在时限内完成，而不是无限等待或静默丢弃。
- 典型用法配合信号：`signal.NotifyContext(ctx, SIGINT, SIGTERM)` → `b.Close(context.Background())`。

## 本地如何验证

```bash
# 1) 看演示（背压拒绝、attempt 递增、死信原因、收尾 inflight=0 都会打印）
go run ./cmd/demo

# 2) 跑场景测试，日志中 [业务:X] 是覆盖的场景，[结果:...] 是断言要点
go test ./... -v

# 3) 并发正确性用 -race；稳定性可多跑几轮
go test ./... -race -count=5
```

### 自动化测试覆盖的场景

| 测试 | 覆盖业务 |
| --- | --- |
| `TestConcurrentProducersAndGroupPreemption` | 8 生产者 × 6 消费者并发抢占：不重不漏、轮询公平、计数自洽 |
| `TestMultipleGroupsEachGetCopy` | 组间广播：不同组各拿一份，独立确认 |
| `TestRetryToDeadLetter` | 一直失败 → 退避重试到 `MaxAttempts` → 进死信，attempt/计数精确 |
| `TestDeadLetterReasonsAreDistinguishable` | 多条死信携带彼此可区分的失败原因 |
| `TestRequeueDeadLetter` | 死信重投回炉、计数重置、不重复累加、越界报错 |
| `TestDuplicateSettlementCountsOnce` | 重复 Ack/Nack 只生效一次，不错乱计数 |
| `TestConsumerGoneRequeue` | 消费者掉线后在途消息立即被组内他人接管确认，不进死信 |
| `TestAckTimeoutReclaim` | 消费者假死，超过 AckTimeout 消息重投，不卡死 |
| `TestLateAckAfterReclaimRejected` | 重投后旧消费者迟到 Ack 被拒，不重复确认 |
| `TestBackpressureReject` | 队列满明确拒绝 `ErrQueueFull`，积压 QueueDepth 可见，腾出空间后恢复 |
| `TestBackpressureBlockAndCancel` | 满时阻塞、消费后解除阻塞、ctx 取消及时返回 |
| `TestPublishAfterCloseRejected` | 关闭后投递/订阅明确拒绝 |
| `TestGracefulShutdownWaitsForInflight` | Close 等所有在途 Ack 完再退，状态一致 |
| `TestShutdownDrainsRetryDuringDrain` | Close 期间消息在退避重试，收尾会等它重试到成功，不半途丢失 |
| `TestShutdownTimeoutIsExplicit` | 在途无法收尾时到点返回明确的 `ErrShutdownTimeout` |

## 主要配置（broker.Config）

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `QueueCapacity` | 1024 | 每组未确认消息容量；`<0` 无界 |
| `MaxAttempts` | 3 | 最大尝试次数（含首次）；`<0` 无限重试 |
| `BackoffInitial/Max/Multiplier` | 50ms / 2s / 2 | 指数退避 |
| `AckTimeout` | 5s | 在途多久未确认视为超时可重投 |
| `ReclaimInterval` | 100ms | 超时回收扫描频率 |
| `BlockOnFull` | false | 满时阻塞还是立即拒绝 |
| `ShutdownTimeout` | 10s | Close 等待在途收尾的时限 |
