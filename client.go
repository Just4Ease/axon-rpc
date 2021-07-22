package axonrpc

import (
	"context"
	"errors"
	"github.com/Just4Ease/axon/v2"
	"github.com/Just4Ease/axon/v2/codec/msgpack"
	"github.com/Just4Ease/axon/v2/messages"
	"log"
)

type ClientConnInterface interface {
	// Invoke performs a unary RPC and returns after the response is received
	// into reply.
	//Invoke(ctx context.Context, method string, args interface{}, reply interface{}) error
	Invoke(ctx context.Context, method string, args interface{}, reply interface{}) error
	// NewStream begins a streaming RPC.
	//NewStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (ClientStream, error)
}

type tClient struct {
	axon.EventStore
}

func (t tClient) Invoke(ctx context.Context, method string, args interface{}, reply interface{}) error {
	m := msgpack.Marshaler{}
	data, err := m.Marshal(args)
	if err != nil {
		log.Printf("failed to marshal input in %s with the following erros: %s", method, err)
		return err
	}

	msg := messages.NewMessage()
	msg.WithSubject(method)
	msg.WithType(messages.RequestMessage)
	msg.WithBody(data)
	result, err := t.EventStore.Request(msg)
	if err != nil {
		return err
	}

	if result.Type == messages.ErrorMessage {
		return errors.New(result.Error)
	}

	return m.Unmarshal(result.Body, reply)
}

func NewClient(store axon.EventStore) ClientConnInterface {
	return &tClient{store}
}
