package types

import "github.com/ipfs/go-cid"

// 完整区块定义
type FullBlock struct {
	// 区块头
	Header        *BlockHeader
	BlsMessages   []*Message
	SecpkMessages []*SignedMessage
}

func (fb *FullBlock) Cid() cid.Cid {
	return fb.Header.Cid()
}
