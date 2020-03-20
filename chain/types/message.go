package types

import (
	"bytes"
	"fmt"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/go-address"
)


/**

一定不要传输，也不能在消息池中维护，或者包含进区块内“语义不合法”的消息。 一条语义合法的签名消息需要：

- 有一个格式良好的，非空的From地址
- 有一个非负的CallSeqNum
- 其Value不比0小也不能比代币总供应量2e9 * 1e18大
- 其MethodNum非负
- 除非MethodNum为0，否则其 Params 不为空
- 其GasPrice非负
- 其GasLimit至少要等于消耗的gas量，消耗量与消息序列化后的字节大小有关
- 其GasLimit不能大于网络中的gas limit参数
 */
type Message struct {
	To   address.Address
	From address.Address

	Nonce uint64

	Value BigInt

	GasPrice BigInt
	GasLimit BigInt

	Method uint64
	Params []byte
}

func DecodeMessage(b []byte) (*Message, error) {
	var msg Message
	if err := msg.UnmarshalCBOR(bytes.NewReader(b)); err != nil {
		return nil, err
	}

	return &msg, nil
}

func (m *Message) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := m.MarshalCBOR(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *Message) ToStorageBlock() (block.Block, error) {
	data, err := m.Serialize()
	if err != nil {
		return nil, err
	}

	pref := cid.NewPrefixV1(cid.DagCBOR, multihash.BLAKE2B_MIN+31)
	c, err := pref.Sum(data)
	if err != nil {
		return nil, err
	}

	return block.NewBlockWithCid(data, c)
}

func (m *Message) Cid() cid.Cid {
	b, err := m.ToStorageBlock()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal message: %s", err)) // I think this is maybe sketchy, what happens if we try to serialize a message with an undefined address in it?
	}

	return b.Cid()
}

func (m *Message) RequiredFunds() BigInt {
	return BigAdd(
		m.Value,
		BigMul(m.GasPrice, m.GasLimit),
	)
}

func (m *Message) VMMessage() *Message {
	return m
}

func (m *Message) Equals(o *Message) bool {
	return m.Cid() == o.Cid()
}
