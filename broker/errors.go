package broker

import "errors"

// 投递与消费过程中可能出现的、需要调用方显式处理的错误。
var (
	ErrQueueFull       = errors.New("broker: queue is full (backpressure rejected)")
	ErrBrokerClosed    = errors.New("broker: broker is closed")
	ErrConsumerClosed  = errors.New("broker: consumer is closed")
	ErrAlreadySettled  = errors.New("broker: delivery already settled")
	ErrConsumerGone    = errors.New("broker: consumer no longer exists")
	ErrDLQMessageGone  = errors.New("broker: dead letter message not found")
	ErrInvalidArgument = errors.New("broker: invalid argument")
	ErrShutdownTimeout = errors.New("broker: shutdown timed out with in-flight messages")
)
