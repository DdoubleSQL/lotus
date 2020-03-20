package types

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-amt-ipld"
	"github.com/filecoin-project/lotus/chain/actors/aerrors"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// 合约状态存储
type Storage interface {
	Put(cbg.CBORMarshaler) (cid.Cid, aerrors.ActorError)
	Get(cid.Cid, cbg.CBORUnmarshaler) aerrors.ActorError

	GetHead() cid.Cid

	// Commit sets the new head of the actors state as long as the current
	// state matches 'oldh'
	Commit(oldh cid.Cid, newh cid.Cid) aerrors.ActorError
}

type StateTree interface {
	SetActor(addr address.Address, act *Actor) error
	GetActor(addr address.Address) (*Actor, error)
}

type VMContext interface {
	Message() *Message
	Origin() address.Address
	Ipld() *hamt.CborIpldStore // ipfs,存储层接口
	Send(to address.Address, method uint64, value BigInt, params []byte) ([]byte, aerrors.ActorError)
	BlockHeight() uint64
	GasUsed() BigInt
	Storage() Storage
	StateTree() (StateTree, aerrors.ActorError)
	VerifySignature(sig *Signature, from address.Address, data []byte) aerrors.ActorError
	ChargeGas(uint64) aerrors.ActorError
	GetRandomness(height uint64) ([]byte, aerrors.ActorError)
	GetBalance(address.Address) (BigInt, aerrors.ActorError)
	Sys() *VMSyscalls

	Context() context.Context
}

const CommitmentBytesLen = 32

type PublicSectorInfo struct {
	SectorID uint64
	CommR    [CommitmentBytesLen]byte
}

/**
 Ticket 用于领导选举？
 Expected Consensus每一轮会生成一个Ticket，
 每个节点通过一定的计算，确定是否是该轮的Leader。
 如果选为Leader，节点可以打包区块。

 [Filecoin规范②--时空证明](https://juejin.im/post/5e12edf4f265da5d363e308d)

矿工正在存储的无报错扇区的一个子集允许他们使用一个PartialTicket去尝试一次“领导选举”，
且使用任一个PartialTicket都可导出一个有效的用于“领导选举”的ChallengeTicket。抓取一
个获胜ChallengeTicket的可能性依赖于扇区大小和总存储量。矿工在每一个指定epoch内获得的
奖励，与他们生成的获胜ChallengeTickets数量成正比。因此这也激励着矿工们在一个“领导选举”
内，为了展现他们的完整算力而尽可能多地检查被允许的存储。矿工在一个给定epoch内能产生的“选
举证明”数量将决定其赚取多少区块奖励。

 */
type Candidate struct {
	SectorID             uint64
	PartialTicket        [32]byte
	Ticket               [32]byte
	SectorChallengeIndex uint64
}

type VMSyscalls struct {
	ValidatePoRep      func(context.Context, address.Address, uint64, []byte, []byte, []byte, []byte, []byte, uint64) (bool, aerrors.ActorError)
	VerifyFallbackPost func(ctx context.Context,
		sectorSize uint64,
		sectorInfo []PublicSectorInfo,
		challengeSeed []byte,
		proof []byte,
		candidates []Candidate,
		proverID address.Address,
		faults uint64) (bool, error)
}

type storageWrapper struct {
	s Storage
}

func (sw *storageWrapper) Put(i cbg.CBORMarshaler) (cid.Cid, error) {
	c, err := sw.s.Put(i)
	if err != nil {
		return cid.Undef, err
	}

	return c, nil
}

func (sw *storageWrapper) Get(c cid.Cid, out cbg.CBORUnmarshaler) error {
	if err := sw.s.Get(c, out); err != nil {
		return err
	}

	return nil
}

func WrapStorage(s Storage) amt.Blocks {
	return &storageWrapper{s}
}
