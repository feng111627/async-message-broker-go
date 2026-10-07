package broker

// 场景化测试入口：
//   scenario_competing_test.go  —— 场景1：并发抢占（多生产者 + 同组竞争）
//   scenario_retry_dlq_test.go  —— 场景2：失败重试到死信 + 死信重投 + 计数不重复
//   scenario_crash_test.go      —— 场景3：消费者掉线/超时，未确认消息重投
//   scenario_shutdown_test.go   —— 场景4：背压拒绝/阻塞 + 退出信号优雅收尾
//
// 运行：go test ./... -v
