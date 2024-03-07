package MQ

import (
	"context"
	"github.com/orormaybe/RedisMQ/client"
	"testing"
	"time"
)

type DemoDeadLetterMailbox struct {
	do func(msg *client.MsgEntity)
}

func NewDemoDeadLetterMailbox(do func(msg *client.MsgEntity)) *DemoDeadLetterMailbox {
	return &DemoDeadLetterMailbox{
		do: do,
	}
}

func (d *DemoDeadLetterMailbox) Deliver(ctx context.Context, msg *client.MsgEntity) error {
	d.do(msg)
	return nil
}

func TestConsumer(t *testing.T) {
	c := client.NewClient(network, address, password)
	callbackFunc := func(ctx context.Context, msg *client.MsgEntity) error {
		t.Logf("receive msg, msg id: %s, msg key: %s, msg val: %s", msg.MsgID, msg.Key, msg.Val)
		return nil
	}
	demoDeadLetterMailbox := NewDemoDeadLetterMailbox(func(msg *client.MsgEntity) {
		t.Logf("receive dead letter, msg id: %s, msg key: %s, msg val: %s", msg.MsgID, msg.Key, msg.Val)
	})
	consumer, err := NewConsumer(c, topic, consumerGroup, consumerID, callbackFunc, WithMaxRetryLimit(2), WithReceiveTimeout(2*time.Second), WithDeadLetterMailbox(demoDeadLetterMailbox))
	if err != nil {
		t.Error(err)
		return
	}
	defer consumer.Stop()
	<-time.After(20 * time.Minute)

}
