package blockstream

import (
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func Offline(off bool) SessionOption {
	return func(opts *sessionOpts) {
		opts.offline = off
	}
}

func Blockstore(bs blockstore.Blockstore) SessionOption {
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
	save, offline bool
	bs            blockstore.Blockstore
}

func (so *sessionOpts) parse(opts ...SessionOption) {
	for _, opt := range opts {
		opt(so)
	}

	if so.offline && so.bs == nil {
		panic("must have Blockstore for offline mode")
	}
}
