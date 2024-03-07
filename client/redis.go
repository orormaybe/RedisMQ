package client

import (
	"context"
	"errors"
	"fmt"
	"github.com/demdxx/gocast"
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

var ErrNoMsg = errors.New("no msg received")
var ErrInvlidMsg = errors.New("invalid msg format")

type MsgEntity struct {
	MsgID string
	Key   string
	Val   string
}

type Client struct {
	opts *ClientOptions
	pool *redis.Pool
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := &Client{
		opts: &ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}
	for _, opt := range opts {
		opt(c.opts)
	}
	c.pool = c.getRedisPool()
	repairClient(c.opts)
	return c

}

func NewClientWithPool(pool *redis.Pool, opts ...ClientOption) *Client {
	c := &Client{
		pool: pool,
		opts: &ClientOptions{},
	}
	for _, opt := range opts {
		opt(c.opts)
	}
	repairClient(c.opts)
	return c

}

func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     c.opts.maxIdle,
		IdleTimeout: time.Duration(c.opts.idleTimeoutSeconds) * time.Second,
		MaxActive:   c.opts.maxActive,
		Wait:        c.opts.wait,
		Dial: func() (redis.Conn, error) {
			conn, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return conn, nil

		},
		TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (c *Client) getRedisConn() (redis.Conn, error) {
	if c.opts.address == "" {
		panic("Cannot get redis address from config")
	}
	var dialOpts []redis.DialOption
	if len(c.opts.password) > 0 {
		dialOpts = append(dialOpts, redis.DialPassword(c.opts.password))
	}
	conn, err := redis.DialContext(context.Background(), c.opts.network, c.opts.address, dialOpts...)
	if err != nil {
		return nil, err
	}
	return conn, nil

}

func (c *Client) XADD(ctx context.Context, topic string, maxLen int, key, val string) (string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return redis.String(conn.Do("XADD", topic, "MAXLEN", maxLen, "*", key, val))
}

func (c *Client) XACK(ctx context.Context, topic, groupID, msgID string) error {
	if topic == "" || groupID == "" || msgID == "" {
		return errors.New("redis XACK topic | group_id | msg_ id can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	reply, err := redis.Int64(conn.Do("XACK", topic, groupID, msgID))
	if err != nil {
		return err
	}
	if reply != 1 {
		return fmt.Errorf("invalid reply: %d", reply)
	}
	return nil

}

func (c *Client) XGroupCreate(ctx context.Context, topic, group string) (string, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	return redis.String(conn.Do("XGROUP", "CREATE", topic, group, "0-0"))
}

func (c *Client) xReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMiliSeconds int, pending bool) ([]*MsgEntity, error) {
	if groupID == "" || consumerID == "" || topic == "" {
		return nil, errors.New("redis XREADGROUP groupID/consumerID/topic can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var rawReply any
	if pending {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "STREAMS", topic, "0-0")
	} else {
		rawReply, err = conn.Do("XREADGROUP", "GROUP", groupID, consumerID, "BLOCK", timeoutMiliSeconds, "STREAMS", topic, ">")
	}
	if err != nil {
		return nil, err
	}
	reply, _ := rawReply.([]any)
	if len(reply) == 0 {
		return nil, ErrNoMsg
	}
	replyElement, _ := reply[0].([]interface{})
	if len(replyElement) != 2 {
		return nil, ErrInvlidMsg
	}
	var msgs []*MsgEntity
	rawMsgs, _ := replyElement[1].([]interface{})
	for _, rawMsg := range rawMsgs {
		_msg, _ := rawMsg.([]interface{})
		if len(_msg) != 2 {
			return nil, ErrInvlidMsg
		}
		msgID := gocast.ToString(_msg[0])
		msgBody, _ := _msg[1].([]interface{})
		if len(msgBody) != 2 {
			return nil, ErrInvlidMsg
		}
		msgKey := gocast.ToString(msgBody[0])
		msgVal := gocast.ToString(msgBody[1])
		msgs = append(msgs, &MsgEntity{
			MsgID: msgID,
			Key:   msgKey,
			Val:   msgVal,
		})

	}
	return msgs, nil

}
func (c *Client) XReadGroupPending(ctx context.Context, groupID, consumerID, topic string) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, 0, true)
}

func (c *Client) XReadGroup(ctx context.Context, groupID, consumerID, topic string, timeoutMiliSeconds int) ([]*MsgEntity, error) {
	return c.xReadGroup(ctx, groupID, consumerID, topic, timeoutMiliSeconds, false)
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", errors.New("redis GET key can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return redis.String(conn.Do("GET", key))
}

func (c *Client) Set(ctx context.Context, key, value string) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET key or value can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	reply, err := conn.Do("SET", key, value)
	if err != nil {
		return -1, err
	}
	if replyStr, ok := reply.(string); ok && strings.ToLower(replyStr) == "ok" {
		return 1, nil
	}
	return redis.Int64(reply, err)

}

func (c *Client) SetEX(ctx context.Context, key, value string, expireSeconds int64) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET keyNX or value can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	reply, err := conn.Do("SET", key, value, "EX", expireSeconds)
	if err != nil {
		return -1, err
	}
	if replyStr, ok := reply.(string); ok && strings.ToLower(replyStr) == "ok" {
		return 1, nil
	}
	return redis.Int64(reply, err)
}

func (c *Client) SetNX(ctx context.Context, key, value string) (int64, error) {
	if key == "" || value == "" {
		return -1, errors.New("redis SET keyNX or value can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	reply, err := conn.Do("SET", key, value, "NX")
	if err != nil {
		return -1, err
	}
	if replyStr, ok := reply.(string); ok && strings.ToLower(replyStr) == "ok" {
		return 1, nil
	}
	return redis.Int64(reply, err)

}

func (c *Client) Del(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("redis DEL key can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("DEL", key)
	return err

}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return -1, errors.New("redis INCR key can't be empty")
	}
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	return redis.Int64(conn.Do("INCR", key))
}
