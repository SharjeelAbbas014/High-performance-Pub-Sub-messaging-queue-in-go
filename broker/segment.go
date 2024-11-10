package broker

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultMaxSegmentSize = 1024 * 1024 * 1024
)

type MessageEntry struct {
	Offset    uint64
	Timestamp int64
	Size      uint32
	Payload   []byte
}

type Segment struct {
	offset  uint64
	file    *os.File
	size    uint64
	maxSize uint64
}

func newSegment(dir string, baseOffset uint64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &Segment{
		offset:  baseOffset,
		file:    file,
		maxSize: defaultMaxSegmentSize,
	}, nil
}

func (s *Segment) append(msg []byte) (uint64, error) {
	entry := MessageEntry{
		Offset:    s.offset,
		Timestamp: time.Now().UnixNano(),
		Size:      uint32(len(msg)),
		Payload:   msg,
	}

	header := make([]byte, 20)
	binary.BigEndian.PutUint64(header[0:8], entry.Offset)
	binary.BigEndian.PutUint64(header[8:16], uint64(entry.Timestamp))
	binary.BigEndian.PutUint32(header[16:20], entry.Size)

	if _, err := s.file.Write(header); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(msg); err != nil {
		return 0, err
	}

	offset := s.offset
	s.offset++
	s.size += uint64(20 + len(msg))

	return offset, nil
}
