package blockstream

import (
	"errors"
	"fmt"
	"io"

	"github.com/Wondertan/go-serde"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/network"

	"github.com/Wondertan/go-blockstream/pb"
)

var maxMsgSize = network.MessageSizeMax

func giveHand(rw io.ReadWriter, out Token) error {
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

func checkHand(rw io.ReadWriter, check onToken) (Token, error) {
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

func writeToken(w io.Writer, token Token) error {
	_, err := serde.Write(w, &pb.Message{Type: pb.Message_REQ, Token: string(token)})
	if err != nil {
		return fmt.Errorf("can't write token: %w", err)
	}

	return nil
}

func readToken(r io.Reader) (Token, error) {
	msg := new(pb.Message)
	_, err := serde.Read(r, msg)
	if err != nil {
		return "", fmt.Errorf("can't read token: %w", err)
	}

	return Token(msg.Token), nil
}

func writeBlocksReq(w io.Writer, ids []cid.Cid) error {
	req := &pb.Message{Type: pb.Message_REQ, Cids: make([][]byte, len(ids))}
	for i, id := range ids {
		req.Cids[i] = id.Bytes()
	}

	_, err := serde.Write(w, req)
	if err != nil {
		return fmt.Errorf("can't write blocks request: %w", err)
	}

	return nil
}

func readBlocksReq(r io.Reader) ([]cid.Cid, error) {
	msg := new(pb.Message)
	_, err := serde.Read(r, msg)
	if err != nil {
		return nil, fmt.Errorf("can't read blocks request: %w", err)
	}

	if msg.Type != pb.Message_REQ {
		return nil, fmt.Errorf("unexpected message type - %s", msg.Type)
	}

	ids := make([]cid.Cid, len(msg.Cids))
	for i, b := range msg.Cids {
		ids[i], err = cid.Cast(b)
		if err != nil {
			return ids, fmt.Errorf("can't cast cid of requested block: %w", err)
		}
	}

	return ids, nil
}

func writeBlocksResp(rw io.ReadWriter, bs []blocks.Block) error {
	msg := &pb.Message{Type: pb.Message_RESP, Blocks: make([][]byte, len(bs))}
	for i, b := range bs {
		msg.Blocks[i] = b.RawData()
	}

	_, err := serde.Write(rw, msg)
	if err != nil {
		return fmt.Errorf("can't write blocks response: %w", err)
	}

	return nil
}

func readBlocksResp(rw io.ReadWriter, ids []cid.Cid) ([]blocks.Block, error) {
	msg := new(pb.Message)
	_, err := serde.Read(rw, msg)
	if err != nil {
		return nil, fmt.Errorf("can't read blocks response: %w", err)
	}

	if msg.Type != pb.Message_RESP {
		return nil, fmt.Errorf("unexpected message type - %s", msg.Type)
	}

	bs := make([]blocks.Block, len(msg.Blocks))
	for i, b := range msg.Blocks {
		bs[i], err = newBlockCheckCid(b, ids[i])
		if err != nil {
			if errors.Is(err, blocks.ErrWrongHash) {
				log.Error(err)
				log.Errorf("Expected: %s, Actual: %s", ids[i], bs[i])
			}
			return bs, err
		}
	}

	return bs, nil
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
