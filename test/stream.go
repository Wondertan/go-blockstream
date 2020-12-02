package test

import (
	"io"
)

type Stream struct {
	read  *io.PipeReader
	write *io.PipeWriter
}

func StreamPair() (*Stream, *Stream) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return &Stream{r1, w2}, &Stream{r2, w1}
}

func (s *Stream) Read(b []byte) (n int, err error) {
	return s.read.Read(b)
}

func (s *Stream) Write(b []byte) (n int, err error) {
	return s.write.Write(b)
}

func (s *Stream) Close() error {
	return s.write.Close()
}
