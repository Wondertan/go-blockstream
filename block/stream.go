package block

import (
	"context"
	"errors"
	"io"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("blockstream")

const outputSize = 32

type Stream struct {
	ctx         context.Context
	queue       *RequestQueue
	out         chan Result
	done, close chan struct{}
}

func NewStream(ctx context.Context) *Stream {
	done, cls := make(chan struct{}), make(chan struct{})
	s := &Stream{
		ctx:   ctx,
		queue: NewRequestQueue(cls),
		out:   make(chan Result, outputSize),
		done:  done,
		close: cls,
	}
	go s.stream()
	return s
}

func (s *Stream) Done() <-chan struct{} {
	return s.done
}

func (s *Stream) Close() {
	close(s.close)
}

func (s *Stream) Enqueue(reqs ...*Request) {
	s.queue.Enqueue(reqs...)
}

func (s *Stream) Output() <-chan Result {
	return s.out
}

func (s *Stream) stream() {
	defer close(s.out)
	defer close(s.done)

	for {
		req := s.queue.Back()
		if req == nil {
			return
		}

		for {
			bs, err := req.Next()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					for _, id := range req.Remains() {
						select {
						case s.out <- Result{Cid: id, Error: err}:
						case <-s.ctx.Done():
							return
						}
					}
				}

				break
			}

			for _, b := range bs {
				select {
				case s.out <- Result{Cid: b.Cid(), Block: b}:
				case <-s.ctx.Done():
					return
				}
			}
		}

		s.queue.PopBack()
	}
}
