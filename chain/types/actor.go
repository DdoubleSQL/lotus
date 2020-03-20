package types

import (
	"fmt"

	"github.com/ipfs/go-cid"
)

var ErrActorNotFound = fmt.Errorf("actor not found")

type Actor struct {
	/**
	 * 对以太坊的Account数据很像
	 * 类似角色类型枚举
	 */
	Code    cid.Cid
	/**
	 * Head是该账号状态数据树root
	 */
	Head    cid.Cid
	/**
	 * 重放保护、下一条从本角色发出的消息的期望序列号
	 */
	Nonce   uint64
	/**
	 * balance是该actor 代币FIL余额
	 */
	Balance BigInt
}
