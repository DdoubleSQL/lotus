package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/lotus/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"

	amt "github.com/filecoin-project/go-amt-ipld"
	"github.com/filecoin-project/lotus/chain/types"

	lru "github.com/hashicorp/golang-lru"
	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-blockservice"
	car "github.com/ipfs/go-car"
	"github.com/ipfs/go-cid"
	dstore "github.com/ipfs/go-datastore"
	hamt "github.com/ipfs/go-hamt-ipld"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	dag "github.com/ipfs/go-merkledag"
	cbg "github.com/whyrusleeping/cbor-gen"
	pubsub "github.com/whyrusleeping/pubsub"
	"golang.org/x/xerrors"
)

var log = logging.Logger("chainstore")

var chainHeadKey = dstore.NewKey("head")

// 封装了对链的访问接口
// 如何构建一个协议里面的链？
// 构建block、tipsets、chain
type ChainStore struct {
	bs bstore.Blockstore
	ds dstore.Datastore

	heaviestLk sync.Mutex
	heaviest   *types.TipSet

	bestTips *pubsub.PubSub
	pubLk    sync.Mutex

	tstLk   sync.Mutex
	tipsets map[uint64][]cid.Cid

	reorgCh          chan<- reorg
	headChangeNotifs []func(rev, app []*types.TipSet) error

	mmCache *lru.ARCCache
	tsCache *lru.ARCCache

	vmcalls *types.VMSyscalls
}

func NewChainStore(bs bstore.Blockstore, ds dstore.Batching, vmcalls *types.VMSyscalls) *ChainStore {
	c, _ := lru.NewARC(2048)
	tsc, _ := lru.NewARC(4096)
	cs := &ChainStore{
		bs:       bs,
		ds:       ds,
		bestTips: pubsub.New(64),
		tipsets:  make(map[uint64][]cid.Cid),
		mmCache:  c,
		tsCache:  tsc,
		vmcalls:  vmcalls,
	}

	cs.reorgCh = cs.reorgWorker(context.TODO())

	hcnf := func(rev, app []*types.TipSet) error {
		cs.pubLk.Lock()
		defer cs.pubLk.Unlock()

		notif := make([]*HeadChange, len(rev)+len(app))

		for i, r := range rev {
			notif[i] = &HeadChange{
				Type: HCRevert,
				Val:  r,
			}
		}
		for i, r := range app {
			notif[i+len(rev)] = &HeadChange{
				Type: HCApply,
				Val:  r,
			}
		}

		cs.bestTips.Pub(notif, "headchange")
		return nil
	}

	hcmetric := func(rev, app []*types.TipSet) error {
		ctx := context.Background()
		for _, r := range app {
			stats.Record(ctx, metrics.ChainNodeHeight.M(int64(r.Height())))
		}
		return nil
	}

	cs.headChangeNotifs = append(cs.headChangeNotifs, hcnf, hcmetric)

	return cs
}

func (cs *ChainStore) Load() error {
	head, err := cs.ds.Get(chainHeadKey)
	if err == dstore.ErrNotFound {
		log.Warn("no previous chain state found")
		return nil
	}
	if err != nil {
		return xerrors.Errorf("failed to load chain state from datastore: %w", err)
	}

	var tscids []cid.Cid
	if err := json.Unmarshal(head, &tscids); err != nil {
		return xerrors.Errorf("failed to unmarshal stored chain head: %w", err)
	}

	ts, err := cs.LoadTipSet(types.NewTipSetKey(tscids...))
	if err != nil {
		return xerrors.Errorf("loading tipset: %w", err)
	}

	cs.heaviest = ts

	return nil
}

func (cs *ChainStore) writeHead(ts *types.TipSet) error {
	data, err := json.Marshal(ts.Cids())
	if err != nil {
		return xerrors.Errorf("failed to marshal tipset: %w", err)
	}

	if err := cs.ds.Put(chainHeadKey, data); err != nil {
		return xerrors.Errorf("failed to write chain head to datastore: %w", err)
	}

	return nil
}

const (
	HCRevert  = "revert"
	HCApply   = "apply"
	HCCurrent = "current"
)

type HeadChange struct {
	Type string
	Val  *types.TipSet
}

func (cs *ChainStore) SubHeadChanges(ctx context.Context) chan []*HeadChange {
	cs.pubLk.Lock()
	subch := cs.bestTips.Sub("headchange")
	head := cs.GetHeaviestTipSet()
	cs.pubLk.Unlock()

	out := make(chan []*HeadChange, 16)
	out <- []*HeadChange{{
		Type: HCCurrent,
		Val:  head,
	}}

	go func() {
		defer close(out)
		var unsubOnce sync.Once

		for {
			select {
			case val, ok := <-subch:
				if !ok {
					log.Warn("chain head sub exit loop")
					return
				}
				if len(out) > 0 {
					log.Warnf("head change sub is slow, has %d buffered entries", len(out))
				}
				select {
				case out <- val.([]*HeadChange):
				case <-ctx.Done():
				}
			case <-ctx.Done():
				unsubOnce.Do(func() {
					go cs.bestTips.Unsub(subch)
				})
			}
		}
	}()
	return out
}

func (cs *ChainStore) SubscribeHeadChanges(f func(rev, app []*types.TipSet) error) {
	cs.headChangeNotifs = append(cs.headChangeNotifs, f)
}

func (cs *ChainStore) SetGenesis(b *types.BlockHeader) error {
	ts, err := types.NewTipSet([]*types.BlockHeader{b})
	if err != nil {
		return err
	}

	if err := cs.PutTipSet(context.TODO(), ts); err != nil {
		return err
	}

	return cs.ds.Put(dstore.NewKey("0"), b.Cid().Bytes())
}

func (cs *ChainStore) PutTipSet(ctx context.Context, ts *types.TipSet) error {
	for _, b := range ts.Blocks() {
		if err := cs.PersistBlockHeaders(b); err != nil {
			return err
		}
	}

	expanded, err := cs.expandTipset(ts.Blocks()[0])
	if err != nil {
		return xerrors.Errorf("errored while expanding tipset: %w", err)
	}
	log.Debugf("expanded %s into %s\n", ts.Cids(), expanded.Cids())

	// 是不是新块？？不对！是不是当前产生的(ing)tipsets中最大权重块要更新
	if err := cs.MaybeTakeHeavierTipSet(ctx, expanded); err != nil {
		return xerrors.Errorf("MaybeTakeHeavierTipSet failed in PutTipSet: %w", err)
	}
	return nil
}

// 或许产生了新的权重块，检查下
func (cs *ChainStore) MaybeTakeHeavierTipSet(ctx context.Context, ts *types.TipSet) error {
	cs.heaviestLk.Lock()
	defer cs.heaviestLk.Unlock()
	w, err := cs.Weight(ctx, ts)
	if err != nil {
		return err
	}
	heaviestW, err := cs.Weight(ctx, cs.heaviest)
	if err != nil {
		return err
	}

	if w.GreaterThan(heaviestW) {
		// TODO: don't do this for initial sync. Now that we don't have a
		// difference between 'bootstrap sync' and 'caught up' sync, we need
		// some other heuristic（启动法）.
		// 是新块
		return cs.takeHeaviestTipSet(ctx, ts)
	}
	return nil
}

// 这个结构体的存在意义是什么
// chain/store/store.go:309
// takeHeaviestTipSet
type reorg struct {
	old *types.TipSet
	new *types.TipSet
}

// reorgCh的初始化
func (cs *ChainStore) reorgWorker(ctx context.Context) chan<- reorg {
	out := make(chan reorg, 32)
	go func() {
		defer log.Warn("reorgWorker quit")

		for {
			select {
			case r := <-out:
				revert, apply, err := cs.ReorgOps(r.old, r.new)
				if err != nil {
					log.Error("computing reorg ops failed: ", err)
					continue
				}

				// reverse the apply array
				for i := len(apply)/2 - 1; i >= 0; i-- {
					opp := len(apply) - 1 - i
					apply[i], apply[opp] = apply[opp], apply[i]
				}

				for _, hcf := range cs.headChangeNotifs {
					if err := hcf(revert, apply); err != nil {
						log.Error("head change func errored (BAD): ", err)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (cs *ChainStore) takeHeaviestTipSet(ctx context.Context, ts *types.TipSet) error {
	_, span := trace.StartSpan(ctx, "takeHeaviestTipSet")
	defer span.End()

	if cs.heaviest != nil { // buf
		if len(cs.reorgCh) > 0 {
			log.Warnf("Reorg channel running behind, %d reorgs buffered", len(cs.reorgCh))
		}
		// 当前生产新tipset需要调整，发消息给reorg
		cs.reorgCh <- reorg{
			old: cs.heaviest,
			new: ts,
		}
	} else {
		log.Warnf("no heaviest tipset found, using %s", ts.Cids())
	}

	span.AddAttributes(trace.BoolAttribute("newHead", true))

	log.Infof("New heaviest tipset! %s (height=%d)", ts.Cids(), ts.Height())
	cs.heaviest = ts

	if err := cs.writeHead(ts); err != nil {
		log.Errorf("failed to write chain head: %s", err)
		return nil
	}

	return nil
}

// SetHead sets the chainstores current 'best' head node.
// This should only be called if something is broken and needs fixing
// 这个有意思。。启动设置链的最新状态（全局全网）
func (cs *ChainStore) SetHead(ts *types.TipSet) error {
	cs.heaviestLk.Lock()
	defer cs.heaviestLk.Unlock()
	return cs.takeHeaviestTipSet(context.TODO(), ts)
}

// 是不是DHT的一部分啊？？
func (cs *ChainStore) Contains(ts *types.TipSet) (bool, error) {
	for _, c := range ts.Cids() {
		has, err := cs.bs.Has(c)
		if err != nil {
			return false, err
		}

		if !has {
			return false, nil
		}
	}
	return true, nil
}

func (cs *ChainStore) GetBlock(c cid.Cid) (*types.BlockHeader, error) {
	sb, err := cs.bs.Get(c)
	if err != nil {
		return nil, err
	}

	return types.DecodeBlock(sb.RawData())
}

func (cs *ChainStore) LoadTipSet(tsk types.TipSetKey) (*types.TipSet, error) {
	v, ok := cs.tsCache.Get(tsk)
	if ok {
		return v.(*types.TipSet), nil
	}

	var blks []*types.BlockHeader
	for _, c := range tsk.Cids() {
		b, err := cs.GetBlock(c)
		if err != nil {
			return nil, xerrors.Errorf("get block %s: %w", c, err)
		}

		blks = append(blks, b)
	}

	ts, err := types.NewTipSet(blks)
	if err != nil {
		return nil, err
	}

	cs.tsCache.Add(tsk, ts)

	return ts, nil
}

// returns true if 'a' is an ancestor of 'b'
func (cs *ChainStore) IsAncestorOf(a, b *types.TipSet) (bool, error) {
	if b.Height() <= a.Height() {
		return false, nil
	}

	cur := b
	for !a.Equals(cur) && cur.Height() > a.Height() {
		next, err := cs.LoadTipSet(b.Parents())
		if err != nil {
			return false, err
		}

		cur = next
	}

	return cur.Equals(a), nil
}

func (cs *ChainStore) NearestCommonAncestor(a, b *types.TipSet) (*types.TipSet, error) {
	l, _, err := cs.ReorgOps(a, b)
	if err != nil {
		return nil, err
	}

	return cs.LoadTipSet(l[len(l)-1].Parents())
}

// 将入参的两个链ts调整至同高度，返回中间的调整过程
// 左ts比较新，迭代取左ts的父亲们，并这些爸爸放到同一个列表
// 同理右ts
func (cs *ChainStore) ReorgOps(a, b *types.TipSet) ([]*types.TipSet, []*types.TipSet, error) {
	left := a
	right := b

	var leftChain, rightChain []*types.TipSet
	for !left.Equals(right) {
		if left.Height() > right.Height() {
			leftChain = append(leftChain, left)
			par, err := cs.LoadTipSet(left.Parents())
			if err != nil {
				return nil, nil, err
			}

			left = par
		} else {
			rightChain = append(rightChain, right)
			par, err := cs.LoadTipSet(right.Parents())
			if err != nil {
				log.Infof("failed to fetch right.Parents: %s", err)
				return nil, nil, err
			}

			right = par
		}
	}

	return leftChain, rightChain, nil
}

func (cs *ChainStore) GetHeaviestTipSet() *types.TipSet {
	cs.heaviestLk.Lock()
	defer cs.heaviestLk.Unlock()
	return cs.heaviest
}

// 新的区块加入当前tipsets,5
// 单节点记录，在内存中记录，相当于内存的缓存
// 转看本文件的AddBlock方法，类似的功能，只是目的不同
func (cs *ChainStore) AddToTipSetTracker(b *types.BlockHeader) error {
	cs.tstLk.Lock()
	defer cs.tstLk.Unlock()

	tss := cs.tipsets[b.Height]
	for _, oc := range tss {
		if oc == b.Cid() {
			log.Debug("tried to add block to tipset tracker that was already there")
			return nil
		}
	}

	cs.tipsets[b.Height] = append(tss, b.Cid())

	// TODO: do we want to look for slashable submissions here? might as well...

	return nil
}

// 批量存储区块
func (cs *ChainStore) PersistBlockHeaders(b ...*types.BlockHeader) error {
	sbs := make([]block.Block, len(b))

	for i, header := range b {
		var err error
		sbs[i], err = header.ToStorageBlock()
		if err != nil {
			return err
		}
	}

	batchSize := 256
	calls := len(b) / batchSize

	var err error
	for i := 0; i <= calls; i++ {
		start := batchSize * i
		end := start + batchSize
		if end > len(b) {
			end = len(b)
		}

		err = multierr.Append(err, cs.bs.PutMany(sbs[start:end]))
	}

	return err
}

type storable interface {
	ToStorageBlock() (block.Block, error)
}

// 参与全链的记账
// 添加消息/交易。也就是打包前，生产新的区块，
// （出块）区块头、链的交易和签名交易等等消息
// 用于链的存储和同步
func PutMessage(bs bstore.Blockstore, m storable) (cid.Cid, error) {
	b, err := m.ToStorageBlock()
	if err != nil {
		return cid.Undef, err
	}

	if err := bs.Put(b); err != nil {
		return cid.Undef, err
	}

	return b.Cid(), nil
}

func (cs *ChainStore) PutMessage(m storable) (cid.Cid, error) {
	return PutMessage(cs.bs, m)
}

func (cs *ChainStore) expandTipset(b *types.BlockHeader) (*types.TipSet, error) {
	// Hold lock for the whole function for now, if it becomes a problem we can
	// fix pretty easily
	cs.tstLk.Lock()
	defer cs.tstLk.Unlock()

	all := []*types.BlockHeader{b}

	tsets, ok := cs.tipsets[b.Height]
	if !ok {
		return types.NewTipSet(all)
	}

	inclMiners := map[address.Address]bool{b.Miner: true}
	for _, bhc := range tsets {
		if bhc == b.Cid() {
			continue
		}

		h, err := cs.GetBlock(bhc)
		if err != nil {
			return nil, xerrors.Errorf("failed to load block (%s) for tipset expansion: %w", bhc, err)
		}

		if inclMiners[h.Miner] {
			log.Warnf("Have multiple blocks from miner %s at height %d in our tipset cache", h.Miner, h.Height)
			continue
		}

		if types.CidArrsEqual(h.Parents, b.Parents) {
			all = append(all, h)
			inclMiners[h.Miner] = true
		}
	}

	// TODO: other validation...?

	return types.NewTipSet(all)
}

// 新的区块加入
// 1. 修改当前的tipset内容规模
// 2. 修改权重，主要是reorg
func (cs *ChainStore) AddBlock(ctx context.Context, b *types.BlockHeader) error {
	if err := cs.PersistBlockHeaders(b); err != nil {
		return err
	}

	ts, err := cs.expandTipset(b)
	if err != nil {
		return err
	}

	if err := cs.MaybeTakeHeavierTipSet(ctx, ts); err != nil {
		return xerrors.Errorf("MaybeTakeHeavierTipSet failed: %w", err)
	}

	return nil
}

func (cs *ChainStore) GetGenesis() (*types.BlockHeader, error) {
	data, err := cs.ds.Get(dstore.NewKey("0"))
	if err != nil {
		return nil, err
	}

	c, err := cid.Cast(data)
	if err != nil {
		return nil, err
	}

	genb, err := cs.bs.Get(c)
	if err != nil {
		return nil, err
	}

	return types.DecodeBlock(genb.RawData())
}

// 查签了名的消息交易等
func (cs *ChainStore) GetCMessage(c cid.Cid) (ChainMsg, error) {
	// 查消息
	m, err := cs.GetMessage(c)
	// 查到了
	if err == nil {
		return m, nil
	}
	// 未知异常？？？？
	if err != bstore.ErrNotFound {
		log.Warn("GetCMessage: unexpected error getting unsigned message: %s", err)
	}

	// 消息签名
	return cs.GetSignedMessage(c)
}

func (cs *ChainStore) GetMessage(c cid.Cid) (*types.Message, error) {
	sb, err := cs.bs.Get(c)
	if err != nil {
		log.Errorf("get message get failed: %s: %s", c, err)
		return nil, err
	}

	return types.DecodeMessage(sb.RawData())
}

func (cs *ChainStore) GetSignedMessage(c cid.Cid) (*types.SignedMessage, error) {
	sb, err := cs.bs.Get(c)
	if err != nil {
		log.Errorf("get message get failed: %s: %s", c, err)
		return nil, err
	}

	return types.DecodeSignedMessage(sb.RawData())
}

// Array Mapped Trie
//
// 牛逼了？？发现了DTH？？？
//
// [3 Trie树](https://blog.csdn.net/wydyd110/article/details/82225499#3%20Trie树)
// [Hash array mapped trie（HAMT） HASH算法](https://blog.csdn.net/HiZhanYue/article/details/86293703)
//  
// Trie树，即字典树，又称单词查找树或键树，是一种树形结构，是一种哈希树的变种。典型应用是用于统计和排序大量的字符串
// （但不仅限于字符串），所以经常被搜索引擎系统用于文本词频统计。它的优点是：最大限度地减少无谓的字符串比较，查询效率
//  比哈希表高。
//
//	Trie的核心思想是空间换时间。利用字符串的公共前缀来降低查询时间的开销以达到提高效率的目的。 
//
//	HAMT实现了几乎类似哈希表的速度，同时更经济地使用内存。此外，哈希表可能必须定期调整大小，这是一项昂贵的操作，而
//	HAMT则会动态增长。通常，HAMT性能通过具有N个时隙的多个的较大根表来改善; 一些HAMT变体允许根部懒惰地生长，对性能
//	的影响可以忽略不计。
//
//  @param root cid.Cid
//  @return []cid.Cid
func (cs *ChainStore) readAMTCids(root cid.Cid) ([]cid.Cid, error) {
	bs := amt.WrapBlockstore(cs.bs)
	a, err := amt.LoadAMT(bs, root)
	if err != nil {
		return nil, xerrors.Errorf("amt load: %w", err)
	}

	var cids []cid.Cid
	for i := uint64(0); i < a.Count; i++ {
		var c cbg.CborCid
		if err := a.Get(i, &c); err != nil {
			return nil, xerrors.Errorf("failed to load cid from amt: %w", err)
		}

		cids = append(cids, cid.Cid(c))
	}

	return cids, nil
}

type ChainMsg interface {
	Cid() cid.Cid
	VMMessage() *types.Message
	ToStorageBlock() (block.Block, error)
}

// xxxx的交易 in the given ts
// 消息的费用结算
func (cs *ChainStore) MessagesForTipset(ts *types.TipSet) ([]ChainMsg, error) {
	applied := make(map[address.Address]uint64)
	balances := make(map[address.Address]types.BigInt)

	cst := hamt.CSTFromBstore(cs.bs)
	st, err := state.LoadStateTree(cst, ts.Blocks()[0].ParentStateRoot)
	if err != nil {
		return nil, xerrors.Errorf("failed to load state tree")
	}

	// a是消息的发送发地址
	// 记录下该地址的余额 map[Nonce]Balance
	preloadAddr := func(a address.Address) error {
		if _, ok := applied[a]; !ok {
			act, err := st.GetActor(a)
			if err != nil {
				return err
			}

			applied[a] = act.Nonce
			balances[a] = act.Balance
		}
		return nil
	}

	var out []ChainMsg
	for _, b := range ts.Blocks() {
		bms, sms, err := cs.MessagesForBlock(b)
		if err != nil {
			return nil, xerrors.Errorf("failed to get messages for block: %w", err)
		}

		cmsgs := make([]ChainMsg, 0, len(bms)+len(sms))
		for _, m := range bms {
			cmsgs = append(cmsgs, m)
		}
		for _, sm := range sms {
			cmsgs = append(cmsgs, sm)
		}

		for _, cm := range cmsgs {
			m := cm.VMMessage()
			// 加载出消息发送方的账户余额信息
			if err := preloadAddr(m.From); err != nil {
				return nil, err
			}

			// 发送序列的检查是否一致
			if applied[m.From] != m.Nonce {
				continue
			}
			applied[m.From]++

			// 检查消息发送者的钱够不够
			if balances[m.From].LessThan(m.RequiredFunds()) {
				continue
			}
			// 大数减法，资金扣除
			balances[m.From] = types.BigSub(balances[m.From], m.RequiredFunds())

			// 输出
			out = append(out, cm)
		}
	}

	return out, nil
}

type mmCids struct {
	bls   []cid.Cid
	secpk []cid.Cid
}

func (cs *ChainStore) readMsgMetaCids(mmc cid.Cid) ([]cid.Cid, []cid.Cid, error) {
	o, ok := cs.mmCache.Get(mmc)
	if ok {
		mmcids := o.(*mmCids)
		return mmcids.bls, mmcids.secpk, nil
	}

	// 从blockstore中取出chain statetree
	cst := hamt.CSTFromBstore(cs.bs)
	var msgmeta types.MsgMeta
	if err := cst.Get(context.TODO(), mmc, &msgmeta); err != nil {
		return nil, nil, xerrors.Errorf("failed to load msgmeta: %w", err)
	}

	blscids, err := cs.readAMTCids(msgmeta.BlsMessages)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading bls message cids for block: %w", err)
	}

	secpkcids, err := cs.readAMTCids(msgmeta.SecpkMessages)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading secpk message cids for block: %w", err)
	}

	cs.mmCache.Add(mmc, &mmCids{
		bls:   blscids,
		secpk: secpkcids,
	})

	return blscids, secpkcids, nil
}

// TODO 暂时还没有看明白这个函数的使用目的
func (cs *ChainStore) GetPath(ctx context.Context, from types.TipSetKey, to types.TipSetKey) ([]*HeadChange, error) {
	fts, err := cs.LoadTipSet(from)
	if err != nil {
		return nil, xerrors.Errorf("loading from tipset %s: %w", from, err)
	}
	tts, err := cs.LoadTipSet(to)
	if err != nil {
		return nil, xerrors.Errorf("loading to tipset %s: %w", to, err)
	}
	// 如果fts区块高度大与rts，说明做的结算回滚！！应该是这样的
	// 如果是fts比rts低，说明做的结算时apply！！

	revert, apply, err := cs.ReorgOps(fts, tts)
	if err != nil {
		return nil, xerrors.Errorf("error getting tipset branches: %w", err)
	}

	path := make([]*HeadChange, len(revert)+len(apply))
	for i, r := range revert {
		path[i] = &HeadChange{Type: HCRevert, Val: r}
	}
	for j, i := 0, len(apply)-1; i >= 0; j, i = j+1, i-1 {
		path[j+len(revert)] = &HeadChange{Type: HCApply, Val: apply[i]}
	}
	return path, nil
}

// 从一个区块中取出里面的交易消息
func (cs *ChainStore) MessagesForBlock(b *types.BlockHeader) ([]*types.Message, []*types.SignedMessage, error) {
	blscids, secpkcids, err := cs.readMsgMetaCids(b.Messages)
	if err != nil {
		return nil, nil, err
	}

	blsmsgs, err := cs.LoadMessagesFromCids(blscids)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading bls messages for block: %w", err)
	}

	secpkmsgs, err := cs.LoadSignedMessagesFromCids(secpkcids)
	if err != nil {
		return nil, nil, xerrors.Errorf("loading secpk messages for block: %w", err)
	}

	return blsmsgs, secpkmsgs, nil
}
// 一个区块的父亲交易收据获取
func (cs *ChainStore) GetParentReceipt(b *types.BlockHeader, i int) (*types.MessageReceipt, error) {
	bs := amt.WrapBlockstore(cs.bs)
	a, err := amt.LoadAMT(bs, b.ParentMessageReceipts)
	if err != nil {
		return nil, xerrors.Errorf("amt load: %w", err)
	}

	var r types.MessageReceipt
	if err := a.Get(uint64(i), &r); err != nil {
		return nil, err
	}

	return &r, nil
}

// 抽出cids对应的交易消息
func (cs *ChainStore) LoadMessagesFromCids(cids []cid.Cid) ([]*types.Message, error) {
	msgs := make([]*types.Message, 0, len(cids))
	for i, c := range cids {
		m, err := cs.GetMessage(c)
		if err != nil {
			return nil, xerrors.Errorf("failed to get message: (%s):%d: %w", err, c, i)
		}

		msgs = append(msgs, m)
	}

	return msgs, nil
}

// 抽出cids对应的交易消息
func (cs *ChainStore) LoadSignedMessagesFromCids(cids []cid.Cid) ([]*types.SignedMessage, error) {
	msgs := make([]*types.SignedMessage, 0, len(cids))
	for i, c := range cids {
		m, err := cs.GetSignedMessage(c)
		if err != nil {
			return nil, xerrors.Errorf("failed to get message: (%s):%d: %w", err, c, i)
		}

		msgs = append(msgs, m)
	}

	return msgs, nil
}

func (cs *ChainStore) Blockstore() bstore.Blockstore {
	return cs.bs
}

func (cs *ChainStore) VMSys() *types.VMSyscalls {
	return cs.vmcalls
}

// FullTipSet
func (cs *ChainStore) TryFillTipSet(ts *types.TipSet) (*FullTipSet, error) {
	var out []*types.FullBlock

	for _, b := range ts.Blocks() {
		bmsgs, smsgs, err := cs.MessagesForBlock(b)
		if err != nil {
			// TODO: check for 'not found' errors, and only return nil if this
			// is actually a 'not found' error
			return nil, nil
		}

		fb := &types.FullBlock{
			Header:        b,
			BlsMessages:   bmsgs,
			SecpkMessages: smsgs,
		}

		out = append(out, fb)
	}
	return NewFullTipSet(out), nil
}

func drawRandomness(t *types.Ticket, round int64) []byte {
	h := sha256.New()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(round))

	h.Write(t.VRFProof)
	h.Write(buf[:])

	return h.Sum(nil)
}

func (cs *ChainStore) GetRandomness(ctx context.Context, blks []cid.Cid, round int64) ([]byte, error) {
	_, span := trace.StartSpan(ctx, "store.GetRandomness")
	defer span.End()
	span.AddAttributes(trace.Int64Attribute("round", round))

	for {
		nts, err := cs.LoadTipSet(types.NewTipSetKey(blks...))
		if err != nil {
			return nil, err
		}

		mtb := nts.MinTicketBlock()

		if int64(nts.Height()) <= round {
			return drawRandomness(nts.MinTicketBlock().Ticket, round), nil
		}

		// special case for lookback behind genesis block
		// TODO(spec): this is not in the spec, need to sync that
		if mtb.Height == 0 {

			// round is negative
			thash := drawRandomness(mtb.Ticket, round*-1)

			// for negative lookbacks, just use the hash of the positive tickethash value
			h := sha256.Sum256(thash)
			return h[:], nil
		}

		blks = mtb.Parents
	}
}

// 查找高度h的ts，且满足高度h对应的ts在给定的ts之前
func (cs *ChainStore) GetTipsetByHeight(ctx context.Context, h uint64, ts *types.TipSet) (*types.TipSet, error) {
	if ts == nil {
		ts = cs.GetHeaviestTipSet()
	}

	if h > ts.Height() {
		return nil, xerrors.Errorf("looking for tipset with height less than start point")
	}

	if ts.Height()-h > build.ForkLengthThreshold {
		log.Warnf("expensive call to GetTipsetByHeight, seeking %d levels", ts.Height()-h)
	}

	for {
		pts, err := cs.LoadTipSet(ts.Parents())
		if err != nil {
			return nil, err
		}

		if h > pts.Height() {
			return ts, nil
		}

		ts = pts
	}
}

func recurseLinks(bs blockstore.Blockstore, root cid.Cid, in []cid.Cid) ([]cid.Cid, error) {
	data, err := bs.Get(root)
	if err != nil {
		return nil, err
	}

	top, err := cbg.ScanForLinks(bytes.NewReader(data.RawData()))
	if err != nil {
		return nil, err
	}

	in = append(in, top...)
	for _, c := range top {
		var err error
		in, err = recurseLinks(bs, c, in)
		if err != nil {
			return nil, err
		}
	}

	return in, nil
}

// 写入文件
func (cs *ChainStore) Export(ctx context.Context, ts *types.TipSet, w io.Writer) error {
	if ts == nil {
		ts = cs.GetHeaviestTipSet()
	}
	bsrv := blockservice.New(cs.bs, nil)
	dserv := dag.NewDAGService(bsrv)
	return car.WriteCarWithWalker(ctx, dserv, ts.Cids(), w, func(nd format.Node) ([]*format.Link, error) {
		var b types.BlockHeader
		if err := b.UnmarshalCBOR(bytes.NewBuffer(nd.RawData())); err != nil {
			return nil, err
		}

		var out []*format.Link
		for _, p := range b.Parents {
			out = append(out, &format.Link{Cid: p})
		}

		cids, err := recurseLinks(cs.bs, b.Messages, nil)
		if err != nil {
			return nil, err
		}

		for _, c := range cids {
			out = append(out, &format.Link{Cid: c})
		}

		if b.Height == 0 {
			cids, err := recurseLinks(cs.bs, b.ParentStateRoot, nil)
			if err != nil {
				return nil, err
			}

			for _, c := range cids {
				out = append(out, &format.Link{Cid: c})
			}
		}

		return out, nil
	})
}

// 从文件导入
func (cs *ChainStore) Import(r io.Reader) (*types.TipSet, error) {
	header, err := car.LoadCar(cs.Blockstore(), r)
	if err != nil {
		return nil, xerrors.Errorf("loadcar failed: %w", err)
	}

	root, err := cs.LoadTipSet(types.NewTipSetKey(header.Roots...))
	if err != nil {
		return nil, xerrors.Errorf("failed to load root tipset from chainfile: %w", err)
	}

	return root, nil
}

type chainRand struct {
	cs   *ChainStore
	blks []cid.Cid
	bh   uint64
}

func NewChainRand(cs *ChainStore, blks []cid.Cid, bheight uint64) vm.Rand {
	return &chainRand{
		cs:   cs,
		blks: blks,
		bh:   bheight,
	}
}

func (cr *chainRand) GetRandomness(ctx context.Context, round int64) ([]byte, error) {
	return cr.cs.GetRandomness(ctx, cr.blks, round)
}

func (cs *ChainStore) GetTipSetFromKey(tsk types.TipSetKey) (*types.TipSet, error) {
	if tsk.IsEmpty() {
		return cs.GetHeaviestTipSet(), nil
	} else {
		return cs.LoadTipSet(tsk)
	}
}
