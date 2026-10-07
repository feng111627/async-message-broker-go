# async-message-broker-go

一个**纯本地、零外部依赖**的异步消息投递与消费组件（Go）。broker 和消费者都用
进程内资源模拟，重点不在性能，而在于：**并发与异常场景下投递语义可靠、行为可解释**。

- 多生产者并发投递，多消费者按**消费组**订阅
- 同组内一条消息同一时间只被一个消费者拿到，必须**显式 Ack** 才算成功
- 未确认消息在消费者掉线 / 处理超时后**重投**给组内其他存活消费者，不丢不卡
- 可配置**失败重试 + 退避**，超限进入**死信队列（DLQ）**并保留可区分原因，支持**死信重投**
- 重试 / 重投下计数与终态**幂等**，不重复累加、不错乱
- 队列积压时的**背压**（明确拒绝或可观察地阻塞）
- 进程退出时**优雅收尾**：等在途消息处理完再退，不出现半完成状态

---

## 一、快速开始

```bash
go run ./cmd/demo            # 本地演示：并发投递/竞争/重试死信/重投/背压/优雅退出
go test ./... -v             # 运行全部场景测试（带中文业务日志）
go test ./broker/ -race -v   # 加竞态检测
```

最小用法：

```go
b := broker.New(broker.DefaultConfig())

c, _ := b.Subscribe("order-group", "orders", "worker-1")
go func() {
    for d := range c.Deliveries() {
        if err := handle(d); err != nil {
            _ = d.Nack(err)   // 失败：按退避重试，超限进死信
            continue
        }
        _ = d.Ack()           // 成功：必须显式确认
    }
}()

_, _ = b.Publish(ctx, "orders", &broker.Message{Payload: []byte(`{"id":1}`})}, true)
// 进程退出：
_ = b.Close(ctx)              // 等待在途消息全部结算后再返回
```

---

## 二、投递与确认语义

### 1. 组间广播、组内竞争

- `Publish(topic)` 的消息会为**当前订阅该 topic 的每个消费组**各存一份独立副本。
  不同组互不影响（类似发布/订阅）。
- **同一消费组内**，一条消息同一时间只会投递给组内的**一个**消费者（竞争消费）。
- 消息必须由消费者调用 `Delivery.Ack()` 才算处理成功；`Nack(err)` 表示本次失败。
  不确认就不算成功 —— 这是“至少一次（at-least-once）”语义。

> 因为是至少一次投递，消费者逻辑应尽量**幂等**（用消息 ID / 业务键去重）。
> broker 侧已保证：即使重复投递，同一条消息在 broker 的终态计数也只结算一次。

### 2. 租约（lease）与显式确认的幂等

每次投递都带一个单调递增的“租约代号”（内部 `leaseGen`，对用户暴露为投递序号）。

- 消息被领取后进入 **inflight（在途）**，`Ack` / `Nack` 只对**当前这次投递**有效。
- 一旦消息因超时 / 掉线被回收并重新投递，**旧 Delivery 上迟到的 `Ack` / `Nack` 一律失效**
  （返回 `ErrUnknownDelivery`），因此**不会**出现“消费者 A、B 各确认一次”的双重结算。
- 对同一次投递重复 `Ack` / `Nack` 也只有第一次生效，之后返回错误，**计数不重复累加**。

### 3. 消费者掉线 / 超时重投

未确认的在途消息会在两类情况下被回收，优先重投给组内**其他存活消费者**
（只有该消费者一人时才会回到它自己）：

- **优雅掉线**：消费者调用 `Close()`，其持有的未确认消息**立即**被回收重投，无需等超时。
- **崩溃 / 处理超时**：消费者不退出但长时间不 Ack：
  - 超过 `VisibilityTimeout`（可见性超时）→ 回收重投；
  - 若配置了 `HeartbeatTimeout`，且消费者持有消息期间长时间不调用 `Heartbeat()` → 回收重投。

回收后旧消费者即使“复活”并补 Ack，也会因租约失效而被拒绝，消息不丢、不会一直卡着。

### 4. 生命周期状态

```
            Publish
               │
               ▼
          pending(ready)
               │  被某消费者领取（attempt=1）
               ▼
          inflight ──Ack()──────────────► acked        （终态，成功 +1）
               │
        Nack/超时/掉线（未超上限）
               ▼
          retry（退避 notBefore）
               │ 退避到期
               ▼
          pending → inflight（attempt=2,3,...）
               │
        仍失败且 attempts 超过 MaxRetries
               ▼
            dead（进入 DLQ）             （终态，死信 +1）
```

---

## 三、重试与死信规则

### 1. 重试次数

`MaxRetries` 表示**额外重投次数**：

- 首次投递 + `MaxRetries` 次重投后仍未成功确认 → 进入死信。
- 例：`MaxRetries = 2` ⇒ 共尝试 **3** 次（attempt 1/2/3），第 3 次仍失败即进死信。
- `Nack`（显式失败）与“超时 / 心跳丢失 / 掉线回收”**共用同一套次数预算**，
  不会因为混用两种失败方式而绕过上限。

### 2. 退避

- `Backoff` 决定每次重投前的等待：内置
  - `ConstantBackoff{Delay}`：固定退避；
  - `ExponentialBackoff{Base, Max}`：`Base * 2^(attempt-1)`，以 `Max` 封顶。
- 退避到期后消息自动回到待投递队列；掉线消息因为有退避，能避免立刻又被同一个抖动消费者抢到。

### 3. 死信原因（可区分）

每条死信 `DeadLetter` 都带有可机读、可区分的 `Reason` 与最后一次错误 `LastError`：

| Reason | 含义 |
|---|---|
| `max_retries_exceeded` | 消费失败（Nack / 超时 / 掉线）累计尝试次数超过上限 |
| `visibility_timeout` | 多次投递始终未在可见性 / 心跳期限内确认而被终止（关闭兜底同理） |

并记录 `Attempts`、最初 `EnqueuedAt`、进入死信时间 `DeadAt`。

查询与重投：

```go
dls := b.ListDeadLetters("orders", "order-group") // 可按 topic/group 过滤
for _, dl := range dls {
    fmt.Println(dl.Reason, dl.Attempts, dl.LastError)
}
_ = b.RequeueDeadLetter(ctx, dls[0].MessageID(), true) // 重新投回原 topic/group
```

> `DeadLetter` 的消息 ID 通过 `ListDeadLetters` 返回，配合 `RequeueDeadLetter` 使用。

### 4. 死信重投的计数规则

- 重投会把死信从 DLQ 移除，作为**全新一次生命周期**重新入队，`attempts` 归零，
  因此它会重新获得完整的重试预算。
- 重新消费成功后：`Acked +1`、`Requeued +1`；历史上的 `DeadLettered` **不回滚也不再加**。
  即“失败→死信→重投→成功”全过程里，成功计数与死信计数各只反映一次真实终态，**不错乱**。
- 若原消费组已无人订阅，`RequeueDeadLetter` 返回 `ErrNoSubscription` 且死信**保留**不丢。

---

## 四、背压（积压处理）

每个 `(topic, group)` 队列长度受 `QueueCapacity` 约束（计数含 `ready + 退避中 + inflight + 已预留`）。

`Publish(ctx, topic, msg, block)` 的 `block` 参数（或默认的 `Config.BlockWhenFull`）：

- **`block=false`**：队列满立即返回明确错误 `broker.ErrQueueFull`，调用方必须处理
  （可落本地缓冲 / 降级 / 稍后重发），不会被静默吞掉。
- **`block=true`**：队列满时**阻塞**等待空位，直到有空位、`ctx` 取消或 broker 关闭。
  等待期间指标 `BlockedWaits` 增加，背压可观察。

多消费组场景下，一次发布是**原子**的：要么所有订阅组都接受，要么整体失败
（两阶段“预留空位 → 提交”协议），绝不会出现只写进了部分组的半成品状态。

实时观察：

```go
ready, inflight, capacity, _ := b.QueueStats("orders", "order-group")
m := b.Metrics() // Published / Acked / Retried / Redelivered / DeadLettered /
                 // Requeued / PublishFull(被拒绝) / BlockedWaits(阻塞等待) / Inflight
```

---

## 五、退出收尾（优雅停机）

```go
ctx, stop := broker.ShutdownContext() // 捕获 SIGINT / SIGTERM
defer stop()
// ...收到信号后：
_ = b.Close(ctx) // 或带超时 context.WithTimeout
```

`Close` 的行为：

1. 标记关闭中，**拒绝一切新的 `Publish` / `Subscribe`**（返回 `ErrBrokerClosed`）；
2. **等待所有在途消息到达终态**（被 Ack 成功，或按规则进入重试 / 死信），再停止调度；
   因此不会出现“`Close` 已返回但消息只处理了一半、状态对不上”；
3. 若传入的 `ctx` 在收尾完成前到期，则对仍在途的消息做**强制重排**：未超上限的
   进入退避队列、超上限的进入死信 —— 消息仍有明确归属，不滞留在“投出去但无主”的状态；
4. 关闭所有消费者投递通道（`Receive` 返回 `ErrConsumerClosed`）。

> 说明：纯内存组件在进程真正退出后，**尚未投递 / 退避中 / 死信中的消息不会持久化到磁盘**。
> 优雅收尾保证的是“**在途消息不半处理**”；若需要跨进程的持久化，应在 `Publish`
> 被拒绝 / 返回错误或关闭前由调用方写入自己的持久层。

---

## 六、指标（可解释性）

`broker.Metrics()` 返回快照，终态计数以“单条消息的单次终态”为准，重试不重复累加：

| 指标 | 含义 |
|---|---|
| `Published` | 成功发布数（多组各一份时按一次发布计 1） |
| `Acked` | 显式确认成功的消息数（每条消息终态只 +1） |
| `Retried` | 进入退避等待的次数（过程计数） |
| `Redelivered` | 发生重新投递的次数（过程计数） |
| `DeadLettered` | 进入死信的消息数（每条只 +1，重投不回滚） |
| `Requeued` | 从死信重投的次数 |
| `PublishFull` | 非阻塞模式被**明确拒绝**的次数 |
| `BlockedWaits` | 阻塞模式因队列满进入**等待**的次数 |
| `Inflight` | 当前在途（已领取未结算）消息数，收尾时应归 0 |

---

## 七、主要配置（`broker.Config`）

| 字段 | 默认 | 说明 |
|---|---|---|
| `QueueCapacity` | 1000 | 每组队列容量；`<=0` 无界 |
| `BlockWhenFull` | false | Publish 默认满队列策略（可用单次 block 参数覆盖） |
| `VisibilityTimeout` | 10s | 领取后多久未确认视为超时 |
| `HeartbeatTimeout` | 0（关） | 持有消息期间多久无心跳视为掉线 |
| `MaxRetries` | 3 | 额外重投上限（首次 + N 次后仍失败进死信） |
| `Backoff` | 50ms 固定 | 重投退避策略 |
| `SweepInterval` | 20ms | 后台超时/退避扫描间隔（越小回收越及时） |
| `DeliveryBuffer` | 1 | 每消费者投递通道缓冲（>1 表示允许少量预取） |

---

## 八、自动化测试覆盖

`broker/` 目录下按场景组织，运行 `go test ./... -v` 可看到每类场景的用例与结果：

| 测试 | 覆盖场景 |
|---|---|
| `TestConcurrentPublishAndCompetingConsumers` | 8 生产者并发投递、4 消费者竞争，断言**不丢不重**、每条只 Ack 一次、负载均衡分布 |
| `TestRetryToDLQAndRequeue` | 失败重试到上限进死信、死信原因区分、重复 Nack 幂等、死信重投后计数不错乱 |
| `TestConsumerCrashRedelivery_OnClose` | 消费者优雅 Close，未确认消息立即重投给存活者，旧 Ack 失效 |
| `TestConsumerCrashRedelivery_OnVisibilityTimeout` | 消费者崩溃不退出，超时回收重投，迟到 Ack 被拒 |
| `TestBackpressure_RejectAndBlock` | 队列满明确返回 `ErrQueueFull`、阻塞等待、空位后恢复 |
| `TestGracefulShutdown_WaitsForInflight` | 关闭时等待在途全部 Ack、无半完成状态、关闭后拒绝新负载 |

---

## 九、代码结构

```
broker/
  types.go        公共类型、状态/死信原因常量、错误、Config
  backoff.go      退避策略（固定 / 指数）
  broker.go       Broker、内部 entry/groupState 与两阶段预留协议
  publish.go      多组原子扇出发布与背压
  dispatcher.go   每个 (topic,group) 一个串行调度循环：分发/结算/退避/超时回收/死信
  consumer.go     消费者：Receive/Ack/Nack/Heartbeat/Close
  dlq.go          内存死信队列（按消息 ID 去重）
  dlq_api.go      死信查询与重投
  manage.go       Subscribe / Metrics / QueueStats / Close 优雅收尾
  signal.go       SIGINT/SIGTERM 捕获
  *_test.go       四类场景的并发与异常测试
cmd/demo/main.go  本地可运行演示
```

## 十、设计要点：为什么并发下仍然可靠

每个 `(topic, group)` 由**唯一的串行调度 goroutine** 驱动，ready / 退避 / inflight /
消费者集合这些共享状态只在该循环内修改。生产者发布、消费者 Ack/Nack、心跳、关闭
都通过通道以事件形式提交给该循环。因此：

- “同组同一条消息同一时间只投给一个消费者”由串行模型天然保证，无需在多处加锁竞争；
- 结算只认当前租约代号，过期投递的确认一律丢弃，杜绝双重结算；
- 终态（acked/dead）一旦写入即不可逆，保证计数幂等。
