package block

//
// type requestManager struct {
// 	reqs sync.Map
// 	bs blockstore.Blockstore
// }
//
// func (rm *requestManager) NewRequest(ctx context.Context, ids []cid.Cid) (*Request, error) {
// 	id, err := requestID(ids)
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	val, ok := rm.reqs.Load(id)
// 	if ok {
// 		return val.(*Request), nil
// 	}
//
// 	req := NewRequest(ctx, id, ids)
// 	rm.reqs.Store(id, req)
// 	return req, nil
// }
//
// func (rm *requestManager) Request(id RequestID) (*Request, error) {
// 	panic("implement me")
// }
//



