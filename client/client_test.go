package client

import (
	"context"
	"fmt"
	"testing"
)

const (
	network  = "tcp"
	address  = "47.96.167.87:6379"
	password = "123456"
)

func TestClient(t *testing.T) {
	client := NewClient(network, address, password)
	res, err := client.XADD(context.Background(), "test7", 30, "key2", "val3")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(res)
}

func TestSet(t *testing.T) {
	client := NewClient(network, address, password)
	reply, err := client.Set(context.Background(), "hellowc", "wc")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(reply)
}

func TestSetEX(t *testing.T) {
	client := NewClient(network, address, password)
	reply, err := client.SetEX(context.Background(), "hellowcex", "wc", 20)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(reply)
}

func TestXadd(t *testing.T) {
	client := NewClient(network, address, password)
	reply, err := client.XADD(context.Background(), "test8", 30, "key3", "val4")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(reply)
}

func TestClient_XGroupCreate(t *testing.T) {
	client := NewClient(network, address, password)
	reply, err := client.XGroupCreate(context.Background(), "test8", "gr20")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(reply)
}

func TestXReadGroup(t *testing.T) {
	client := NewClient(network, address, password)
	reply, err := client.XReadGroup(context.Background(), "gr20", "cu1", "test8", 20000)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(len(reply))
	for _, a_reply := range reply {
		t.Log(*a_reply)
	}
	t.Log(reply)

}
