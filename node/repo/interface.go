package repo

import (
	"errors"

	"github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multiaddr"

	"github.com/filecoin-project/lotus/chain/types"
)

var (
	ErrNoAPIEndpoint     = errors.New("API not running (no endpoint)")
	ErrNoAPIToken        = errors.New("API token not set")
	ErrRepoAlreadyLocked = errors.New("repo is already locked")
	ErrClosedRepo        = errors.New("repo is no longer open")
)

/*
       封装节点持久化数据的操作接口

Repo repo.Repo
Repo类型是接口,封装了节点各种持久化数据的操作接口.

*/
type Repo interface {
	// APIEndpoint returns multiaddress for communication with Lotus API
	APIEndpoint() (multiaddr.Multiaddr, error)

	// APIToken returns JWT API Token for use in operations that require auth
	APIToken() ([]byte, error)

	// Lock locks the repo for exclusive use.
	Lock(RepoType) (LockedRepo, error)
}

type LockedRepo interface {
	// Close closes repo and removes lock.
	Close() error

	// Returns datastore defined in this repo.
	// [【Filecoin相关】Filecoin源码解析–Repo相关数据结构](https://ipfser.org/2019/03/21/filecoinrepo/)
	// 区块数据、钱包、链数据、交易数据怎么存？分别是哪些代码？
	// 数据操作：当前当方法-> datastore.Batching.Datastore的实现
	// go-ds-badger2/datastore.go创建函数
	// go-ipfs-blockstore@v0.1.1/blockstore.go创建函数、实现了block的存取
	Datastore(namespace string) (datastore.Batching, error)

	// Returns config in this repo
	Config() (interface{}, error)

	// SetAPIEndpoint sets the endpoint of the current API
	// so it can be read by API clients
	SetAPIEndpoint(multiaddr.Multiaddr) error

	// SetAPIToken sets JWT API Token for CLI
	SetAPIToken([]byte) error

	// KeyStore returns store of private keys for Filecoin transactions
	KeyStore() (types.KeyStore, error)

	// Path returns absolute path of the repo
	Path() string
}
