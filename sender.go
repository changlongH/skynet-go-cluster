package skynetclusterd

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/changlongH/skynet_cluster/codec"
	"github.com/cloudwego/netpoll"
	"github.com/cloudwego/netpoll/mux"
)

type (
	Request struct {
		RespCh chan *codec.RespPack
	}

	SenderAgent struct {
		Name    string
		conn    netpoll.Connection
		wqueue  *mux.ShardQueue // use for write
		CloseCh chan struct{}

		Recv          chan netpoll.Reader
		LargeResponse map[uint32]*codec.RespPack

		IncSession  uint32
		ReqSessions map[uint32]*Request
	}

	SenderMgr struct {
		Lock      sync.RWMutex
		NodeAgent map[string]*SenderAgent
	}
)

var (
	senderMgr *SenderMgr
	_once     sync.Once
)

func getSenderMgr() *SenderMgr {
	_once.Do(func() {
		senderMgr = &SenderMgr{
			NodeAgent: map[string]*SenderAgent{},
		}
	})
	return senderMgr
}

func NewSenderAgent(nodeName string, conn netpoll.Connection) *SenderAgent {
	agent := &SenderAgent{
		Name:          nodeName,
		conn:          conn,
		wqueue:        mux.NewShardQueue(mux.ShardSize, conn),
		Recv:          make(chan netpoll.Reader, 1000),
		CloseCh:       make(chan struct{}),
		LargeResponse: make(map[uint32]*codec.RespPack),
		IncSession:    1,
		ReqSessions:   make(map[uint32]*Request),
	}

	go agent.WaitResponse()

	//remoteAddr := conn.RemoteAddr().String()
	return agent
}

func (agent *SenderAgent) PostRequest(msg *codec.ReqPack) error {
	writer := netpoll.NewLinkBuffer()
	err := codec.EncodeReq(writer, msg)
	if err != nil {
		return err
	}

	// Put puts the buffer getter back to the queue.
	agent.wqueue.Add(func() (buf netpoll.Writer, isNil bool) {
		return writer, false
	})
	return nil
}

func (agent *SenderAgent) GenSession() uint32 {
	session := agent.IncSession
	agent.IncSession = session + 1
	return session
}

func (agent *SenderAgent) WaitResponse() {
	defer func() {
		if err := recover(); err != nil {
			if agent.conn.IsActive() {
				agent.conn.Close()
			}
		}
		mgr := getSenderMgr()
		mgr.Lock.Lock()
		delete(mgr.NodeAgent, agent.Name)
		mgr.Lock.Unlock()
		for session, req := range agent.ReqSessions {
			resp := &codec.RespPack{
				Ok:      false,
				Session: session,
				Message: []byte("socket close"),
			}
			req.RespCh <- resp
		}
	}()

	go func() error {
		for {
			reader := agent.conn.Reader()
			bLen, err := reader.ReadBinary(headerSize)
			if err != nil {
				return err
			}
			pkgsize := int(binary.BigEndian.Uint16(bLen))
			pkg, err := reader.Slice(pkgsize)
			if err != nil {
				return err
			}
			agent.Recv <- pkg
		}
	}()

	var err error
	var msg *codec.RespPack
	for {
		select {
		case pkg := <-agent.Recv:
			msg, err = codec.DecodeResp(pkg, agent.LargeResponse)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			session := msg.Session
			if req, ok := agent.ReqSessions[session]; ok {
				req.RespCh <- msg
			}
		case <-agent.CloseCh:
			return
		}
	}
}

func GetNodeSenderAgent(node string) (*SenderAgent, error) {
	mgr := getSenderMgr()
	if agent, ok := mgr.NodeAgent[node]; ok {
		return agent, nil
	}

	mgr.Lock.Lock()
	addr, ok := GetRegisterNodeAddr(node)
	mgr.Lock.Unlock()
	if !ok {
		return nil, fmt.Errorf("not found tcp addr node:%s", node)
	}
	conn, err := netpoll.DialConnection("tcp", addr, time.Second*5)
	if err != nil {
		return nil, err
	}

	agent := NewSenderAgent(node, conn)
	conn.AddCloseCallback(func(connection netpoll.Connection) error {
		agent.CloseCh <- struct{}{}
		return nil
	})

	mgr.Lock.Lock()
	if _, ok := mgr.NodeAgent[node]; !ok {
		mgr.NodeAgent[node] = agent
	} else {
		agent.CloseCh <- struct{}{}
	}
	mgr.Lock.Unlock()
	return agent, nil
}

func Call(ctx context.Context, node, service, cmd string, args string) (bool, string) {
	agent, err := GetNodeSenderAgent(node)
	if err != nil {
		return false, err.Error()
	}
	pack := &codec.ReqPack{
		Addr: codec.Addr{
			Id:   0,
			Name: service,
		},
		Session: agent.GenSession(),
		Cmd:     cmd,
		Message: []byte(args),
	}

	resp := &Request{
		RespCh: make(chan *codec.RespPack),
	}
	agent.ReqSessions[pack.Session] = resp

	err = agent.PostRequest(pack)
	if err != nil {
		return false, err.Error()
	}

	select {
	case <-ctx.Done():
		return false, "timeout"
	case msg := <-resp.RespCh:
		return msg.Ok, string(msg.Message)
	}
}

func Send(ctx context.Context, node, service, cmd string, args string) error {
	agent, err := GetNodeSenderAgent(node)
	if err != nil {
		return err
	}

	pack := &codec.ReqPack{
		Addr: codec.Addr{
			Id:   0,
			Name: service,
		},
		Session: 0,
		Cmd:     cmd,
		Message: []byte(args),
	}

	err = agent.PostRequest(pack)
	if err != nil {
		return err
	}
	return nil
}
