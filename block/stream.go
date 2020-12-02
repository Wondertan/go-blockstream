package block

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("blockstream")

const outputSize = 32

type stream struct {
	ctx    context.Context
	queue  *RequestQueue
	output chan blocks.Block
}

func NewStream(ctx context.Context) *stream {
	s := &stream{ctx: ctx, queue: NewRequestQueue(ctx.Done()), output: make(chan blocks.Block, outputSize)}
	go s.stream()
	return s
}

func (s *stream) Enqueue(reqs ...Request) {
	s.queue.Enqueue(reqs...)
}

func (s *stream) Output() <-chan blocks.Block {
	return s.output
}

func (s *stream) stream() {
	defer close(s.output)
	for {
		req := s.queue.Back()
		if req == nil {
			return
		}

		for {
			o, ok := req.Next()
			if !ok {
				break
			}

			b, err := o.Get()
			if err != nil {
				log.Errorf("Aborting stream, request error: %s", err)
				return
			}

			select {
			case s.output <- b:
			case <-s.ctx.Done():
				return
			}
		}

		s.queue.PopBack()
	}
}
