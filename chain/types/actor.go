package types

import (
	"fmt"

	"github.com/ipfs/go-cid"
)

var ErrActorNotFound = fmt.Errorf("actor not found")

type Actor struct {
	/**
	 * 对以太坊的Account数据很像
	 */
	Code    cid.Cid
	/**
	 * Head是该账号状态数据树root
	 */
	Head    cid.Cid
	Nonce   uint64
	/**
	 * balance是该actor 代币FIL余额
	 */
	Balance BigInt
}
