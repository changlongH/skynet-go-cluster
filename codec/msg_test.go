package codec

import (
	"fmt"
	"testing"
)

func TestPackString(t *testing.T) {
	msg := packString("cmd", "abcdefghijklmnopqrstuvwxyz1234567890")
	fmt.Println(len(msg), string(msg))
}
