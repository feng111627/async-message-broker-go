// Package broker 是一个纯本地内存的异步消息投递 / 消费组件。
//
// 语义速览：
//   - 多生产者并发 Publish；多消费者按消费组 Subscribe，组内一条消息同一时刻只投给一个
//     消费者，组间广播。
//   - 消费者必须对 Delivery 显式 Ack；Nack(reason) 触发指数退避重试，达到 MaxAttempts
//     进入死信队列（带失败原因），可通过 RequeueDeadLetter 重投。
//   - 消费者 Unsubscribe 或在途消息超过 AckTimeout 未确认时，消息回收重投给组内他人，
//     不丢、不卡死，且不消耗重试次数。
//   - 队列容量按组内未确认总量计算；满时按 BlockOnFull 选择立即返回 ErrQueueFull 或
//     阻塞（可被 ctx 取消）。
//   - Close 先停收新消息并继续派发、等在途收尾，到点未完返回 ErrShutdownTimeout。
//
// 投递为至少一次（at-least-once）；消费方应基于稳定的 Message.ID 保证处理幂等。
package broker
