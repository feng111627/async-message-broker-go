package broker

import "time"

// BackoffStrategy 根据当前尝试次数（从 1 开始）返回下一次重投前的等待时间。
// 参数 attempt 是刚刚失败的那次尝试序号（1 = 第一次消费就失败）。
type BackoffStrategy interface {
	Next(attempt int) time.Duration
}

// ConstantBackoff 固定退避。
type ConstantBackoff struct {
	Delay time.Duration
}

func (b ConstantBackoff) Next(attempt int) time.Duration {
	if b.Delay <= 0 {
		return 0
	}
	return b.Delay
}

// ExponentialBackoff 指数退避：Base * 2^(attempt-1)，并以 Max 封顶。
type ExponentialBackoff struct {
	Base time.Duration
	Max  time.Duration
}

func (b ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt; i++ {
		d *= 2
		if b.Max > 0 && d >= b.Max {
			return b.Max
		}
	}
	if b.Max > 0 && d > b.Max {
		return b.Max
	}
	if d < 0 {
		return b.Max
	}
	return d
}
