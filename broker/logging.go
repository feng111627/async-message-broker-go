package broker

import (
	"fmt"
	"log"
	"os"
)

// 默认日志：输出到 stderr，可通过 Broker.SetLogger 替换。
func defaultLogger() func(format string, args ...any) {
	l := log.New(os.Stderr, "[broker] ", log.LstdFlags|log.Lmicroseconds)
	return func(format string, args ...any) {
		l.Output(2, fmt.Sprintf(format, args...))
	}
}

func nopLogger() func(format string, args ...any) {
	return func(format string, args ...any) {}
}
