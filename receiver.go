package skynetclusterd

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/cloudwego/netpoll"
)

const (
	headerSize = 2
)

var _ netpoll.OnPrepare = prepare
var _ netpoll.OnConnect = connect
var _ netpoll.OnRequest = handle

type connkey struct{}

var ctxkey connkey

func prepare(conn netpoll.Connection) context.Context {
	agent := NewRecvAgent(conn)
	ctx := context.WithValue(context.Background(), ctxkey, agent)
	return ctx
}

func connect(ctx context.Context, conn netpoll.Connection) context.Context {
	agent := ctx.Value(ctxkey).(*RecvAgent)
	conn.AddCloseCallback(func(conn netpoll.Connection) error {
		agent.CloseCh <- struct{}{}
		return nil
	})
	return ctx
}

func handle(ctx context.Context, conn netpoll.Connection) error {
	reader := conn.Reader()
	bLen, err := reader.ReadBinary(headerSize)
	if err != nil {
		return err
	}
	pkgsize := int(binary.BigEndian.Uint16(bLen))
	pkg, err := reader.Slice(pkgsize)
	if err != nil {
		return err
	}

	agent := ctx.Value(ctxkey).(*RecvAgent)
	if agent == nil {
		addr := conn.RemoteAddr().String()
		return errors.New("invalid conn node agent" + addr)
	}

	//msg, err = codec.DecodeReq(pkg, agent.LargeRequest)
	agent.Recv <- pkg
	return nil
}

func Open(address string) (netpoll.EventLoop, error) {
	listener, err := netpoll.CreateListener("tcp", address)
	if err != nil {
		return nil, err
	}

	eventLoop, err := netpoll.NewEventLoop(
		handle,
		netpoll.WithOnPrepare(prepare),
		netpoll.WithOnConnect(connect),
		netpoll.WithReadTimeout(time.Second),
	)
	if err != nil {
		return nil, err
	}

	err = eventLoop.Serve(listener)
	return eventLoop, err
}

func Shutdown(eventLoop netpoll.EventLoop, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	eventLoop.Shutdown(ctx)
}
