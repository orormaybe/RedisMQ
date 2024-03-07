package MQ

import (
	"context"
	"github.com/orormaybe/RedisMQ/client"
)

type Producer struct {
	client *client.Client
	opts   *ProducerOptions
}

func NewProducer(client *client.Client, opts ...ProducerOption) *Producer {
	p := &Producer{
		client: client,
		opts:   &ProducerOptions{},
	}
	for _, opt := range opts {
		opt(p.opts)
	}
	repairProducer(p.opts)
	return p

}

func (p *Producer) SendMsg(ctx context.Context, topic, key, val string) (string, error) {
	return p.client.XADD(ctx, topic, p.opts.msgQueueLen, key, val)
}
