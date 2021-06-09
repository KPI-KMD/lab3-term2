package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key, valueType, value string
}

func (e *entry) Encode() []byte {
	kl := len(e.key)
	tl := len(e.valueType)
	vl := len(e.value)
	size := kl + tl + vl + 16
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(tl))
	copy(res[kl+12:], e.valueType)
	binary.LittleEndian.PutUint32(res[kl+tl+12:], uint32(vl))
	copy(res[kl+tl+16:], e.value)
	return res
}

func (e *entry) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	tl := binary.LittleEndian.Uint32(input[kl+8:])
	typeBuf := make([]byte, tl)
	copy(typeBuf, input[kl+12:kl+12+tl])
	e.valueType = string(typeBuf)

	vl := binary.LittleEndian.Uint32(input[kl+tl+12:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+tl+16:kl+tl+16+vl])
	e.value = string(valBuf)
}

func readValue(in *bufio.Reader) (string, string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", "", err
	}

	dataValue := make([]byte, valSize)
	n, err := in.Read(dataValue)
	if err != nil {
		return "", "", err
	}
	if n != valSize {
		return "", "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", "", err
	}
	typeSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", "", err
	}

	dataType := make([]byte, typeSize)
	n, err = in.Read(dataType)
	if err != nil {
		return "", "", err
	}
	if n != typeSize {
		return "", "", fmt.Errorf("can't read type bytes (read %d, expected %d)", n, valSize)
	}

	return string(dataType), string(dataValue), nil
}
