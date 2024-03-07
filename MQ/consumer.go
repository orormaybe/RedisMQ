package MQ

import (
	"context"
	"errors"
	"github.com/orormaybe/RedisMQ/client"
	"github.com/orormaybe/RedisMQ/log"
)

type MsgCallback func(ctx context.Context, msg *client.MsgEntity) error

type Consumer struct {
	// redis 客户端，基于 redis 实现 message queue
	client *client.Client
	// consumer 生命周期管理
	ctx  context.Context
	stop context.CancelFunc
	// 接收到 msg 时执行的回调函数，由使用方定义
	callbackFunc MsgCallback
	// 消费的 topic
	topic string
	// 所属的消费者组
	groupID string
	// 当前节点的消费者 id
	consumerID string
	// 各消息累计失败次数
	failureCnts map[client.MsgEntity]int
	// 一些用户自定义的配置
	opts *ConsumerOptions
}

func NewConsumer(rc *client.Client, topic, groupID, consumerID string, callbackFunc MsgCallback, opts ...ConsumerOption) (*Consumer, error) {

	ctx, stop := context.WithCancel(context.Background())
	c := &Consumer{
		ctx:          ctx,
		stop:         stop,
		client:       rc,
		topic:        topic,
		groupID:      groupID,
		consumerID:   consumerID,
		callbackFunc: callbackFunc,
		opts:         &ConsumerOptions{},
		failureCnts:  make(map[client.MsgEntity]int),
	}
	if err := c.checkParam(); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(c.opts)
	}
	repairConsumer(c.opts)
	go c.run()
	return c, nil
}

func (c *Consumer) checkParam() error {
	if c.callbackFunc == nil {
		return errors.New("callback function can't be empty")
	}

	if c.client == nil {
		return errors.New("redis client can't be empty")
	}

	if c.topic == "" || c.consumerID == "" || c.groupID == "" {
		return errors.New("topic | group_id | consumer_id can't be empty")
	}

	return nil
}

func (c *Consumer) Stop() {
	c.stop()
}

func (c *Consumer) run() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		msgs, err := c.receive()
		if err != nil {
			log.GetDefaultLogger().Errorf("receive msg failed, err: %v", err)
			continue
		}
		tctx, _ := context.WithTimeout(c.ctx, c.opts.handleMsgsTimeout)
		c.handlerMsgs(tctx, msgs)

		tctx, _ = context.WithTimeout(c.ctx, c.opts.deadLetterDeliverTimeout)
		c.deliverDeadLetter(tctx)

		pendingMsgs, err := c.receivePending()
		if err != nil {
			log.GetDefaultLogger().Errorf("pending msg received failed, err: %v", err)
			continue
		}

		tctx, _ = context.WithTimeout(c.ctx, c.opts.handleMsgsTimeout)
		c.handlerMsgs(tctx, pendingMsgs)
	}

}

func (c *Consumer) receive() ([]*client.MsgEntity, error) {
	msgs, err := c.client.XReadGroup(c.ctx, c.groupID, c.consumerID, c.topic, int(c.opts.receiveTimeout.Milliseconds()))
	if err != nil && !errors.Is(err, client.ErrNoMsg) {
		return nil, err
	}
	return msgs, nil
}

func (c *Consumer) receivePending() ([]*client.MsgEntity, error) {
	msgs, err := c.client.XReadGroupPending(c.ctx, c.groupID, c.consumerID, c.topic)
	if err != nil && !errors.Is(err, client.ErrNoMsg) {
		return nil, err
	}
	return msgs, nil
}

func (c *Consumer) handlerMsgs(ctx context.Context, msgs []*client.MsgEntity) {
	for _, msg := range msgs {
		if err := c.callbackFunc(ctx, msg); err != nil {
			c.failureCnts[*msg]++
			continue
		}
		if err := c.client.XACK(ctx, c.topic, c.groupID, msg.MsgID); err != nil {
			log.GetDefaultLogger().Errorf("msg ack failed, msg id: %s, err: %v", msg.MsgID, err)
			continue
		}
		delete(c.failureCnts, *msg)

	}
}

func (c *Consumer) deliverDeadLetter(ctx context.Context) {
	for msg, failureCnt := range c.failureCnts {
		if failureCnt < c.opts.maxRetryLimit {
			continue
		}
		if err := c.opts.deadLetterMailbox.Deliver(ctx, &msg); err != nil {
			log.GetDefaultLogger().Errorf("dead letter deliver failed, msg id: %s, err: %v", msg.MsgID, err)
		}
		if err := c.client.XACK(ctx, c.topic, c.groupID, msg.MsgID); err != nil {
			log.GetDefaultLogger().Errorf("msg ack failed, msg id: %s, err: %v", msg.MsgID, err)
			continue
		}
		delete(c.failureCnts, msg)
	}
}
