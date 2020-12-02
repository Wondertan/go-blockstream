package blocknet

//
// func TestRequesterResponder(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
//
// 	bs, ids := test.RandBlocks(t, rand.Reader, 8, 256)
// }
//
// func pair(ctx context.Context) (*Requester, *Responder) {
// 	m := &fakeManager{}
// 	m1, m2 := messengerPair()
// 	return NewRequester(ctx, m1), NewResponder(ctx, m2, m)
// }
//
// func messengerPair() (Messenger, Messenger) {
// 	s1, s2 := test.StreamPair()
// 	return NewMessenger(s1, "peer1", CloseLogger), NewMessenger(s2, "peer2", CloseLogger)
// }
//
// type fakeManager struct {
//
// }
//
// func (f *fakeManager) NewRequest(id block.RequestID, id2 peer.ID, cids []cid.Cid) block.PeerRequest {
//
// }
//
// func (f *fakeManager) Request(id block.RequestID, id2 peer.ID) block.PeerRequest {
//
// }
