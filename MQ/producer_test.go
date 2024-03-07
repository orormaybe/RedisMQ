package MQ

import (
	"context"
	"github.com/orormaybe/RedisMQ/client"
	"testing"
)

const (
	network       = "tcp"
	address       = "47.96.167.87:6379"
	password      = "123456"
	topic         = "test20"
	consumerGroup = "gr30"
	consumerID    = "cu1"
)

func TestProducer_SendMsg(t *testing.T) {
	client := client.NewClient(network, address, password)
	p := NewProducer(client)
	reply, err := p.SendMsg(context.Background(), topic, "test30", "val21")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(reply)
}
