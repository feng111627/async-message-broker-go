package broker

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// ShutdownContext 返回一个在收到 SIGINT/SIGTERM 时取消的 context，
// 便于主程序在退出时执行 broker.Close 完成在途消息收尾。
func ShutdownContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
