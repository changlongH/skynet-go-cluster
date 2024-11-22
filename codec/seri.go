package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cloudwego/netpoll"
)

func unpackString(pkg netpoll.Reader) ([]byte, error) {
	header, err := pkg.ReadByte()
	if err != nil {
		return nil, err
	}

	vType := header & 0x7
	vLen := int(header >> 3)

	switch vType {
	/*
		case 0:
			// lua nil
		case 1:
			// boolean
		case 2:
			// lua number
		case 3:
			// lua userdate
		case 6:
			// lua table
	*/
	case 4:
		// 短字符串
		return pkg.ReadBinary(vLen)
	case 5:
		// 长字符串
		if vLen == 2 || vLen == 4 {
			bsize, err := pkg.ReadBinary(vLen)
			if err != nil {
				return nil, err
			}
			var size int
			if vLen == 2 {
				plen := binary.LittleEndian.Uint16(bsize)
				size = int(plen)
			} else {
				plen := binary.LittleEndian.Uint32(bsize)
				size = int(plen)
			}
			return pkg.ReadBinary(size)
		} else {
			errmsg := fmt.Sprintf("nonsupport data invalid stream (type=%d,cookie=%d)", vType, vLen)
			return nil, errors.New(errmsg)
		}
	default:
		errmsg := fmt.Sprintf("nonsupport data unpack (type=%d)", vType)
		return nil, errors.New(errmsg)
	}
}

func combineType(t, v uint8) uint8 {
	return t | v<<3
}

func packString(strs ...string) []byte {
	buffer := bytes.Buffer{}

	for _, s := range strs {
		len := len(s)
		if len < 32 {
			n := combineType(4, uint8(len))
			buffer.WriteByte(n)
			if len > 0 {
				buffer.WriteString(s)
			}
		} else {
			if len < 0x10000 {
				n := combineType(5, 2)
				buffer.WriteByte(n)
				buffer.Grow(2)
				binary.LittleEndian.AppendUint16(buffer.Bytes(), uint16(len))
			} else {
				n := combineType(5, 4)
				buffer.WriteByte(n)
				buffer.Grow(4)
				binary.LittleEndian.AppendUint32(buffer.Bytes(), uint32(len))
			}
			buffer.WriteString(s)
		}
	}
	return buffer.Bytes()
}

func unpackStringsFromBytes(data []byte) ([]string, error) {
	strs := []string{}
	for {
		datasz := len(data)
		if datasz <= 0 {
			return strs, nil
		}
		header := data[0]
		vType := header & 0x7
		vLen := byte(header >> 3)
		if datasz < int(vLen+1) {
			return nil, fmt.Errorf("unpackString datasz=%d,vLen=%d", datasz, vLen)
		}

		switch vType {
		case 4:
			// 短字符串
			strLen := int(vLen)
			start := 1
			end := start + strLen + 1
			s := data[start:end]
			strs = append(strs, string(s))
			data = data[end:]
			continue
		case 5:
			// 长字符串
			if vLen == 2 || vLen == 4 {
				headerLen := data[1 : vLen+1]
				var strLen int
				if vLen == 2 {
					strLen = int(binary.LittleEndian.Uint16(headerLen))
				} else {
					strLen = int(binary.LittleEndian.Uint32(headerLen))
				}
				start := int(1 + vLen)
				end := start + strLen + 1
				s := data[start:end]
				strs = append(strs, string(s))
				data = data[end:]
				continue
			} else {
				errmsg := fmt.Sprintf("nonsupport data invalid stream (type=%d,cookie=%d)", vType, vLen)
				return nil, errors.New(errmsg)
			}
		default:
			errmsg := fmt.Sprintf("nonsupport data unpack (type=%d)", vType)
			return nil, errors.New(errmsg)
		}
	}
}
