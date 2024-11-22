package codec

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cloudwego/netpoll"
)

type (
	Addr struct {
		Id   uint32
		Name string
	}
	ReqPack struct {
		Addr    Addr   // 4-7
		Session uint32 // 8-11
		Cmd     string
		Message []byte
	}
)

// NOTE: kitex 不支持整数ID服务地址调用
func unpackReqNumber(pkg netpoll.Reader) (*ReqPack, error) {
	len := pkg.Len()
	if len < 8 {
		errmsg := fmt.Sprintf("Invalid cluster message (size=%d)", len)
		return nil, errors.New(errmsg)
	}

	// address(4)
	bLen, err := pkg.ReadBinary(4)
	if err != nil {
		return nil, err
	}
	sid := binary.LittleEndian.Uint32(bLen)

	// session(4)
	bLen, err = pkg.ReadBinary(4)
	if err != nil {
		return nil, err
	}
	session := binary.LittleEndian.Uint32(bLen)

	req := &ReqPack{
		Addr:    Addr{Id: sid},
		Session: session,
	}
	// cmd
	bcmd, err := unpackString(pkg)
	if err != nil {
		return req, err
	}
	req.Cmd = string(bcmd)

	// args
	data, err := unpackString(pkg)
	if err != nil {
		return req, err
	}
	req.Message = data
	//msg := string(data)
	return req, nil
}

// 解析一个字符串地址类型的完整包
func unpackReqStr(pkg netpoll.Reader) (*ReqPack, error) {
	len := pkg.Len()
	if len < 2 {
		errmsg := fmt.Sprintf("Invalid cluster message (size=%d)", len)
		return nil, errors.New(errmsg)
	}
	bLen, err := pkg.ReadByte()
	if err != nil {
		return nil, err
	}
	namesize := int(bLen)
	if len < namesize+5 {
		errmsg := fmt.Sprintf("Invalid cluster message (size=%d)", len)
		return nil, errors.New(errmsg)
	}

	// 1 + namesize
	sname, err := pkg.ReadString(namesize)
	if err != nil {
		return nil, err
	}

	// session(4)
	bSession, err := pkg.ReadBinary(4)
	if err != nil {
		return nil, err
	}
	session := binary.BigEndian.Uint32(bSession)
	req := &ReqPack{
		Addr:    Addr{Name: sname},
		Session: session,
	}

	// cmd
	bcmd, err := unpackString(pkg)
	if err != nil {
		return req, err
	}
	req.Cmd = string(bcmd)

	// args
	data, err := unpackString(pkg)
	if err != nil {
		return req, err
	}
	req.Message = data

	return req, nil
}

// 解析整数地址的一个大包 头部
func unpackLargeReqNumber(pkg netpoll.Reader, largeReq map[uint32]*ReqPack, push bool) (*ReqPack, error) {
	len := pkg.Len()
	if len != 12 {
		errmsg := fmt.Sprintf("invalid cluster message size %d (multi req must be 13)", len)
		return nil, errors.New(errmsg)
	}
	// address(4)
	bLen, _ := pkg.ReadBinary(4)
	sid := binary.LittleEndian.Uint32(bLen)
	// session(4)
	bLen, _ = pkg.ReadBinary(4)
	session := binary.LittleEndian.Uint32(bLen)
	req := &ReqPack{
		Addr:    Addr{Id: sid},
		Session: session,
	}
	// msgsize(4)
	bSize, _ := pkg.ReadBinary(4)
	msgsize := binary.LittleEndian.Uint32(bSize)

	req.Message = make([]byte, 0, msgsize)
	//req.Bytes = []byte{}
	largeReq[session] = req
	return nil, nil
}

// 解析一个字符串地址的大包头部
func unpackLargeReqStr(pkg netpoll.Reader, largeReq map[uint32]*ReqPack, push bool) (*ReqPack, error) {
	len := pkg.Len()
	if len < 2 {
		errmsg := fmt.Sprintf("Invalid cluster message (size=%d)", len)
		return nil, errors.New(errmsg)
	}
	bSize, err := pkg.ReadByte()
	if err != nil {
		return nil, err
	}
	namesize := int(bSize)
	if len < namesize+8 {
		errmsg := fmt.Sprintf("Invalid cluster message (size=%d)", len)
		return nil, errors.New(errmsg)
	}

	// 1 + namesize
	sname, _ := pkg.ReadString(namesize)
	// session(4)
	bLen, _ := pkg.ReadBinary(4)
	session := binary.LittleEndian.Uint32(bLen)
	req := &ReqPack{
		Addr:    Addr{Name: sname},
		Session: session,
	}
	// msgsize(4)
	bLen, _ = pkg.ReadBinary(4)
	msgsize := binary.LittleEndian.Uint32(bLen)
	req.Message = make([]byte, 0, msgsize)
	largeReq[session] = req
	return nil, nil
}

// 解析大包的一部分
func unpackLargeReqPart(pkg netpoll.Reader, largeReq map[uint32]*ReqPack, lastPart bool) (*ReqPack, error) {
	sz := pkg.Len()
	if sz < 4 {
		errmsg := fmt.Sprintf("Invalid cluster multi part message (headersz=%d)", sz)
		return nil, errors.New(errmsg)
	}
	bLen, _ := pkg.ReadBinary(4)
	session := binary.LittleEndian.Uint32(bLen)
	//data, _ := pkg.ReadBinary(sz - 4)

	req, ok := largeReq[session]
	if !ok {
		errmsg := fmt.Sprintf("invalid large req part session=%d", session)
		return nil, errors.New(errmsg)
	}
	var p, _ = pkg.Next(sz - 4)
	req.Message = append(req.Message, p...)
	if lastPart {
		delete(largeReq, session)
		args, err := unpackStringsFromBytes(req.Message)
		if err != nil {
			return req, err
		}
		req.Cmd = args[0]
		req.Message = []byte(args[1])
	}
	return req, nil
}

func DecodeReq(pkg netpoll.Reader, largeReq map[uint32]*ReqPack) (*ReqPack, error) {
	defer pkg.Release()

	len := pkg.Len()
	if len == 0 {
		return nil, errors.New("invalid request package size=0")
	}

	msgType, err := pkg.ReadByte()
	if err != nil {
		return nil, err
	}

	switch msgType {
	case 0:
		return unpackReqNumber(pkg)
	case 1:
		// request
		return unpackLargeReqNumber(pkg, largeReq, false)
	case '\x41':
		// push
		return unpackLargeReqNumber(pkg, largeReq, true)
	case 2:
		return unpackLargeReqPart(pkg, largeReq, false)
	case 3:
		return unpackLargeReqPart(pkg, largeReq, true)
	case 4:
		return nil, errors.New("nonsupport trace msg")
	case '\x80':
		return unpackReqStr(pkg)
	case '\x81':
		// request
		return unpackLargeReqStr(pkg, largeReq, false)
	case '\xc1':
		// push
		return unpackLargeReqStr(pkg, largeReq, true)
	default:
		errmsg := fmt.Sprintf("invalid req package (type=%d)", msgType)
		return nil, errors.New(errmsg)
	}
}

func EncodeReq(writer netpoll.Writer, msg *ReqPack) error {
	bytes := packString(msg.Cmd, string(msg.Message))
	sz := uint32(len(bytes))

	var isPush = false
	var session = msg.Session
	if session <= 0 {
		isPush = true
	}

	if msg.Addr.Id == 0 && msg.Addr.Name == "" {
		return errors.New("invalid request addr")
	}

	// first WORD is size of the package with big-endian
	if sz < PartSize {
		if msg.Addr.Id > 0 {
			header, _ := writer.Malloc(2)
			// header byte(1)+addr(4)+session(4)=9
			binary.BigEndian.PutUint16(header, uint16(sz+9))
			writer.WriteByte(0) // type 0
			addr, _ := writer.Malloc(4)
			binary.LittleEndian.PutUint32(addr, msg.Addr.Id)
		} else {
			header, _ := writer.Malloc(2)
			namelen := uint32(len(msg.Addr.Name))
			binary.BigEndian.PutUint16(header, uint16(sz+6+namelen))
			writer.WriteByte(0x80) // type 0x80
			writer.WriteByte(byte(namelen))
			writer.WriteString(msg.Addr.Name)
		}
		wsession, _ := writer.Malloc(4)
		binary.LittleEndian.PutUint32(wsession, session)
		writer.WriteBinary(bytes)
	} else {
		if msg.Addr.Id > 0 {
			header, _ := writer.Malloc(2)
			// multi part header byte(1)+addr(4)+session(4)+msgsize(4)=13
			binary.BigEndian.PutUint16(header, uint16(sz+13))
			if isPush {
				writer.WriteByte(0x41)
			} else {
				writer.WriteByte(1)
			}
			addr, _ := writer.Malloc(4)
			binary.LittleEndian.PutUint32(addr, msg.Addr.Id)
		} else {
			header, _ := writer.Malloc(2)
			namelen := uint32(len(msg.Addr.Name))
			// multi part header byte(1)+addr(1)+session(4)+msgsize(4)=10
			binary.BigEndian.PutUint16(header, uint16(namelen+10))
			if isPush {
				writer.WriteByte(0xc1)
			} else {
				writer.WriteByte(0x81)
			}
			writer.WriteByte(byte(namelen))
			writer.WriteString(msg.Addr.Name)
		}
		wsession, _ := writer.Malloc(4)
		binary.LittleEndian.PutUint32(wsession, session)
		wsize, _ := writer.Malloc(4)
		binary.LittleEndian.PutUint32(wsize, sz)

		part := int((sz-1)/PartSize + 1)
		index := uint32(0)
		for i := 0; i < part; i++ {
			var s uint32
			var bType byte
			if sz > PartSize {
				s = PartSize
				bType = 2 // multi part
			} else {
				s = sz
				bType = 3 // multi end
			}
			// type(1)+session(4)=5
			header, _ := writer.Malloc(2)
			binary.BigEndian.PutUint16(header, uint16(s+5))
			writer.WriteByte(bType)
			session, _ := writer.Malloc(4)
			binary.LittleEndian.PutUint32(session, msg.Session)
			writer.WriteBinary(bytes[index : index+s])
			index = s
			sz = sz - s
		}
	}
	return nil
}
