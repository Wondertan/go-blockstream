package blocknet

import (
	"errors"

	blockstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/Wondertan/go-blockstream/blocknet/pb"
)

const (
	NULL      = pb.NULL
	UNKNOWN   = pb.UNKNOWN
	NOT_FOUND = pb.NOT_FOUND
)

var unknownError = errors.New("blockstream: unknown error from remote peer")

var errorMap = map[MessageErrorCode]error{
	NULL:      nil,
	UNKNOWN:   unknownError,
	NOT_FOUND: blockstore.ErrNotFound,
}

func codeFor(given error) MessageErrorCode {
	for code, err := range errorMap {
		if errors.Is(given, err) {
			return code
		}
	}

	return UNKNOWN
}

func errorFor(code MessageErrorCode) error {
	return errorMap[code]
}
