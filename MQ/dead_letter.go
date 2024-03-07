package MQ

import (
	"context"
	"github.com/orormaybe/RedisMQ/client"
	"github.com/orormaybe/RedisMQ/log"
)

// 死信队列，当消息处理失败达到指定次数时，会被投递到此处
type DeadLetterMailbox interface {
	Deliver(ctx context.Context, msg *client.MsgEntity) error
}

// 默认使用的死信队列，仅仅对消息失败的信息进行日志打印
type DeadLetterLogger struct{}

func NewDeadLetterLogger() *DeadLetterLogger {
	return &DeadLetterLogger{}
}

func (d *DeadLetterLogger) Deliver(ctx context.Context, msg *client.MsgEntity) error {
	log.GetDefaultLogger().Errorf("msg fail execeed retry limit, msg id: %s", msg.MsgID)
	return nil
}
