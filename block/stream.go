package block

import (
	"context"
	"errors"
	"io"

	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("blockstream")

const outputSize = 32

type stream struct {
	ctx    context.Context
	queue  *RequestQueue
	outB chan blocks.Block
	outErr chan error
}

func NewStream(ctx context.Context) *stream {
	s := &stream{
		ctx: ctx,
		queue: NewRequestQueue(ctx.Done()),
		outB: make(chan blocks.Block, outputSize),
		outErr: make(chan error, 1),
	}
	go s.stream()
	return s
}

func (s *stream) Enqueue(reqs ...*Request) {
	s.queue.Enqueue(reqs...)
}

func (s *stream) Blocks() <-chan blocks.Block {
	return s.outB
}

func (s *stream) Errors() <-chan error {
	return s.outErr
}

func (s *stream) Error(err error) {
	select {
	case s.outErr <- err:
	case <-s.ctx.Done():
		return
	}
}

func (s *stream) stream() {
	defer close(s.outB)
	defer close(s.outErr)

	for {
		req := s.queue.Back()
		if req == nil {
			return
		}

		for {
			bs, err := req.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				for _, id := range req.Remains() {
					select {
					case s.outErr <- &Error{Cid: id, Err: err}:
					case <-s.ctx.Done():
						return
					}
				}
			}

			for _, b := range bs {
				select {
				case s.outB <- b:
				case <-s.ctx.Done():
					return
				}
			}
		}

		s.queue.PopBack()
	}
}
