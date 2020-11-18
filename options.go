package blockstream

import (
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func Store(bs blockstore.Blockstore) SessionOption {
	return func(opts *sessionOpts) {
		opts.bs = bs
	}
}

func Save(s bool) SessionOption {
	return func(opts *sessionOpts) {
		opts.save = s
	}
}

type SessionOption func(*sessionOpts)

type sessionOpts struct {
	save bool
	bs blockstore.Blockstore
}

func (so *sessionOpts) parse(opts ...SessionOption) {
	for _, opt := range opts {
		opt(so)
	}
}
