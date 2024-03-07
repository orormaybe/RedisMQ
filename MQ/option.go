package MQ

import "time"

type ProducerOptions struct {
	msgQueueLen int
}

type ProducerOption func(opts *ProducerOptions)

func repairProducer(opts *ProducerOptions) {
	if opts.msgQueueLen <= 0 {
		opts.msgQueueLen = 500
	}
}

func WithMsgQueueLen(len int) ProducerOption {
	return func(opts *ProducerOptions) {
		opts.msgQueueLen = len

	}
}

type ConsumerOptions struct {
	// 每轮接收消息的超时时长
	receiveTimeout time.Duration
	// 处理消息的最大重试次数，超过此次数时，消息会被投递到死信队列
	maxRetryLimit int
	// 死信队列，可以由使用方自定义实现
	deadLetterMailbox DeadLetterMailbox
	// 投递死信流程超时阈值
	deadLetterDeliverTimeout time.Duration
	// 处理消息流程超时阈值
	handleMsgsTimeout time.Duration
}

type ConsumerOption func(opts *ConsumerOptions)

func WithReceiveTimeout(dur time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.receiveTimeout = dur

	}
}

func WithMaxRetryLimit(n int) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.maxRetryLimit = n
	}

}

func WithDeadLetterDeliverTimeout(dur time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadLetterDeliverTimeout = dur
	}
}

func WithHandleMsgsTimeout(dur time.Duration) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.handleMsgsTimeout = dur
	}
}

func WithDeadLetterMailbox(mailbox DeadLetterMailbox) ConsumerOption {
	return func(opts *ConsumerOptions) {
		opts.deadLetterMailbox = mailbox
	}

}

func repairConsumer(opts *ConsumerOptions) {
	if opts.receiveTimeout < 0 {
		opts.receiveTimeout = 2 * time.Second
	}

	if opts.maxRetryLimit < 0 {
		opts.maxRetryLimit = 3
	}

	if opts.deadLetterMailbox == nil {
		opts.deadLetterMailbox = NewDeadLetterLogger()
	}

	if opts.deadLetterDeliverTimeout <= 0 {
		opts.deadLetterDeliverTimeout = time.Second
	}

	if opts.handleMsgsTimeout <= 0 {
		opts.handleMsgsTimeout = time.Second
	}
}
