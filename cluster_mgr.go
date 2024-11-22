package skynetclusterd

import (
	"fmt"
	"sync"

	"github.com/changlongH/skynet_cluster/codec"
	"github.com/cloudwego/netpoll"
	"github.com/cloudwego/netpoll/mux"
)

type (
	RecvAgent struct {
		conn    netpoll.Connection
		wqueue  *mux.ShardQueue // use for write
		CloseCh chan struct{}

		Recv         chan netpoll.Reader // 接收网络包
		LargeRequest map[uint32]*codec.ReqPack
	}

	nodeRegister struct {
		sync.RWMutex
		register map[string]string // name -> addr and addr -> name
	}
)

var (
	clusterReg = nodeRegister{
		register: make(map[string]string),
	}
)

// "test": "192.168.1.195:6001"
func RegisterNode(name string, addr string) {
	clusterReg.Lock()
	defer clusterReg.Unlock()
	clusterReg.register[name] = addr
	clusterReg.register[addr] = name
}

func UnRegisterNode(name string) {
	clusterReg.Lock()
	defer clusterReg.Unlock()

	if addr, ok := clusterReg.register[name]; ok {
		delete(clusterReg.register, addr)
	}
	delete(clusterReg.register, name)
}

func GetRegisterNodeAddr(name string) (string, bool) {
	clusterReg.RLock()
	defer clusterReg.RUnlock()
	addr, ok := clusterReg.register[name]
	return addr, ok
}

// TODO: reload addr 需要changenode 对于已建立的链接处理
func ReloadConfig(conf map[string]string) {
	for name, addr := range conf {
		RegisterNode(name, addr)
	}
}

func NewRecvAgent(conn netpoll.Connection) *RecvAgent {
	agent := &RecvAgent{
		conn:         conn,
		wqueue:       mux.NewShardQueue(mux.ShardSize, conn),
		Recv:         make(chan netpoll.Reader, 1000),
		CloseCh:      make(chan struct{}),
		LargeRequest: make(map[uint32]*codec.ReqPack),
	}
	go agent.Start()

	//remoteAddr := conn.RemoteAddr().String()
	return agent
}

func (agent *RecvAgent) Response(msg *codec.RespPack) {
	writer := netpoll.NewLinkBuffer()
	err := codec.EncodeResp(writer, msg)
	if err != nil {
		return
	}

	// Put puts the buffer getter back to the queue.
	agent.wqueue.Add(func() (buf netpoll.Writer, isNil bool) {
		return writer, false
	})
}

func (agent *RecvAgent) Start() {
	defer func() {
		if err := recover(); err != nil {
			if agent.conn.IsActive() {
				agent.conn.Close()
			}
		}
	}()

	var err error
	var msg *codec.ReqPack
	for {
		select {
		case pkg := <-agent.Recv:
			msg, err = codec.DecodeReq(pkg, agent.LargeRequest)
			if err != nil {
				if msg != nil && msg.Session > 0 {
					agent.Response(&codec.RespPack{
						Session: msg.Session,
						Ok:      false,
						Message: []byte(err.Error()),
					})
				}
				continue
			}

			if msg != nil {
				// TODO: 消息分发
				//args := msg.Message
				msg.Message = []byte{}
				if msg.Session > 0 {
					text := fmt.Sprintf("recv msg addr:{%d:%s} session:%d", msg.Addr.Id, msg.Addr.Name, msg.Session)
					resp := &codec.RespPack{
						Session: msg.Session,
						Ok:      true,
						Message: []byte(text),
					}
					agent.Response(resp)
				}
			}
		case <-agent.CloseCh:
			return
		}
	}
}
