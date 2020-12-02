package blocknet

import (
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/Wondertan/go-blockstream/blocknet/pb"
)

func BlocksRequestMsg(id cid.Cid, ids []cid.Cid) Message {
	msg := &pb.BlockStream{Type: REQUEST, Id: id.Bytes(), Cids: make([][]byte, len(ids))}
	for i, id := range ids {
		msg.Cids[i] = id.Bytes()
	}
	return msg
}

func BlocksRequest(in Message) (id cid.Cid, ids []cid.Cid, err error) {
	msg := in.(*pb.BlockStream)

	id, err = cid.Cast(msg.Id)
	if err != nil {
		return cid.Cid{}, nil, fmt.Errorf("can't cast ID of the request: %w", err)
	}

	ids = make([]cid.Cid, len(msg.GetCids()))
	for i, b := range msg.GetCids() {
		ids[i], err = cid.Cast(b)
		if err != nil {
			return id, ids, fmt.Errorf("can't cast CID of a requested block: %w", err)
		}
	}

	return
}

func BlocksResponseMsg(id cid.Cid, bs []blocks.Block) Message {
	msg := &pb.BlockStream{Type: RESPONSE, Id: id.Bytes(), Blocks: make([][]byte, len(bs))}
	for i, b := range bs {
		msg.Blocks[i] = b.RawData()
	}
	return msg
}

func BlocksResponse(in Message) (cid.Cid, [][]byte, error) {
	msg := in.(*pb.BlockStream)

	id, err := cid.Cast(msg.GetId())
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("can't cast cid of the request: %w", err)
	}

	return id, msg.Blocks, nil
}

func ErrorResponseMsg(id cid.Cid, err error) Message {
	return &pb.BlockStream{Type: ERROR, Id: id.Bytes(), Error: codeFor(err)}
}

func ErrorResponse(in Message) (cid.Cid, error, error) {
	msg := in.(*pb.BlockStream)

	id, err := cid.Cast(msg.GetId())
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("can't cast cid of the request: %w", err)
	}

	return id, errorFor(msg.GetError()), nil
}

func HaveRequestMsg(id cid.Cid) Message {
	return &pb.BlockStream{Type: RESPONSE, Id: id.Bytes()}
}

func HaveRequest(in Message) (cid.Cid, error) {
	msg := in.(*pb.BlockStream)

	id, err := cid.Cast(msg.GetId())
	if err != nil {
		return cid.Undef, fmt.Errorf("can't cast cid of the request: %w", err)
	}

	return id, nil
}

func CancelRequestMsg(id cid.Cid) Message {
	return &pb.BlockStream{Type: CANCEL, Id: id.Bytes()}
}

func CancelRequest(in Message) (cid.Cid, error) {
	msg := in.(*pb.BlockStream)

	id, err := cid.Cast(msg.GetId())
	if err != nil {
		return cid.Undef, fmt.Errorf("can't cast cid of the request: %w", err)
	}

	return id, nil
}
