package blockstream

import (
	"fmt"
	"io"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/Wondertan/go-serde"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/network"

	"github.com/Wondertan/go-blockstream/pb"
)

var maxMsgSize = network.MessageSizeMax

func giveHand(rw io.ReadWriter, out access.Token) error {
	err := writeToken(rw, out)
	if err != nil {
		return err
	}

	in, err := readToken(rw)
	if err != nil {
		return err
	}

	if in != out {
		return fmt.Errorf("streaming: handshake failed: tokens are not equal(exp: %s, recv: %s)", out, in)
	}

	return nil
}

func takeHand(rw io.ReadWriter, check onToken) (access.Token, error) {
	token, err := readToken(rw)
	if err != nil {
		return "", err
	}

	err = check(token)
	if err != nil {
		return "", err
	}

	err = writeToken(rw, token)
	if err != nil {
		return "", err
	}

	return token, nil
}

func writeToken(w io.Writer, token access.Token) error {
	_, err := serde.Write(w, &pb.BlockStream{Type: pb.HANDSHAKE, Token: string(token)})
	if err != nil {
		return fmt.Errorf("can't writeLoop token: %w", err)
	}

	return nil
}

func readToken(r io.Reader) (access.Token, error) {
	msg := new(pb.BlockStream)
	_, err := serde.Read(r, msg)
	if err != nil {
		return "", fmt.Errorf("can't read token: %w", err)
	}

	if msg.Type != pb.HANDSHAKE {
		return "nil", fmt.Errorf("unexpected message type - %s", msg.Type)
	}

	return access.Token(msg.Token), nil
}

func writeBlocksReq(w io.Writer, id uint32, ids []cid.Cid) error {
	req := &pb.BlockStream{Type: pb.REQUEST, Id: id, Cids: make([][]byte, len(ids))}
	for i, id := range ids {
		req.Cids[i] = id.Bytes()
	}

	_, err := serde.Write(w, req)
	if err != nil {
		return fmt.Errorf("can't writeLoop blocks request: %w", err)
	}

	return nil
}

func readBlocksReq(r io.Reader) (uint32, []cid.Cid, error) {
	msg := new(pb.BlockStream)
	_, err := serde.Read(r, msg)
	if err != nil {
		return 0, nil, fmt.Errorf("can't read blocks request: %w", err)
	}

	if msg.Type != pb.REQUEST {
		return 0, nil, fmt.Errorf("unexpected message type - %s", msg.Type)
	}

	ids := make([]cid.Cid, len(msg.Cids))
	for i, b := range msg.Cids {
		ids[i], err = cid.Cast(b)
		if err != nil {
			return 0, ids, fmt.Errorf("can't cast cid of requested block: %w", err)
		}
	}

	return msg.Id, ids, nil
}

func writeBlocksResp(rw io.Writer, id uint32, bs []blocks.Block) error {
	msg := &pb.BlockStream{Type: pb.RESPONSE, Id: id, Blocks: make([][]byte, len(bs))}
	for i, b := range bs {
		msg.Blocks[i] = b.RawData()
	}

	_, err := serde.Write(rw, msg)
	if err != nil {
		return fmt.Errorf("can't writeLoop blocks response: %w", err)
	}

	return nil
}

func readBlocksResp(rw io.Reader) (uint32, [][]byte, error) {
	msg := new(pb.BlockStream)
	_, err := serde.Read(rw, msg)
	if err != nil {
		return 0, nil, fmt.Errorf("can't read blocks response: %w", err)
	}

	if msg.Type != pb.RESPONSE {
		return 0, nil, fmt.Errorf("unexpected message type - %s", msg.Type)
	}

	return msg.Id, msg.Blocks, nil
}

func newBlockCheckCid(data []byte, expected cid.Cid) (blocks.Block, error) {
	actual, err := expected.Prefix().Sum(data)
	if err != nil {
		return nil, err
	}

	b, _ := blocks.NewBlockWithCid(data, actual)
	if !expected.Equals(actual) {
		return b, blocks.ErrWrongHash
	}

	return b, nil
}
