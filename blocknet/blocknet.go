package blocknet

import (
	"context"

	"github.com/Wondertan/go-serde"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream/blocknet/pb"
)

var log = logging.Logger("blockstream/net")

const (
	REQUEST  = pb.REQUEST
	RESPONSE = pb.RESPONSE
	ERROR    = pb.ERROR
	HAVE     = pb.HAVE
	CANCEL   = pb.CANCEL
)

var maxMessageSize = network.MessageSizeMax

type (
	MessageType      = pb.BlockStream_Type
	MessageErrorCode = pb.BlockStream_ErrorCode
)

type Message interface {
	serde.Message

	GetType() MessageType
}

type (
	ResponderHandler func(context.Context, *Responder)
)

type Network interface {
	ID() peer.ID
	Requester(context.Context, peer.ID) (*Requester, error)
}
