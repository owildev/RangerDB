package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

func TestHeaderBinaryWrite(t *testing.T) {
	var b bytes.Buffer
	hdr := header{magic: magicBytes, version: version, flags: 0}
	err := binary.Write(&b, binary.LittleEndian, hdr)
	fmt.Println("err:", err)
	fmt.Println("bytes written:", b.Len())
}
