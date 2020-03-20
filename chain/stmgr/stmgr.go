package stmgr

import (
	"context"
	"fmt"
	"sync"

	"github.com/filecoin-project/go-address"
	amt "github.com/filecoin-project/go-amt-ipld"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	bls "github.com/filecoin-project/filecoin-ffi"
	"github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/trace"
)

var log = logging.Logger("statemgr")

type StateManager struct {
	cs *store.ChainStore

	stCache  map[string][]cid.Cid
	// computeWait 看样子是个耗时的计算
	compWait map[string]chan struct{}
	stlk     sync.Mutex
	newVM    func(cid.Cid, uint64, vm.Rand, address.Address, blockstore.Blockstore, *types.VMSyscalls) (*vm.VM, error)
}

func NewStateManager(cs *store.ChainStore) *StateManager {
	return &StateManager{
		newVM:    vm.NewVM,
		cs:       cs,
		stCache:  make(map[string][]cid.Cid),
		compWait: make(map[string]chan struct{}),
	}
}

func cidsToKey(cids []cid.Cid) string {
	var out string
	for _, c := range cids {
		out += c.KeyString()
	}
	return out
}

// 结算、存储、开启下一轮的区块开采
// @param ts 给定一个ts
// @return st stateroot
// @return rec receipt
// @return err
func (sm *StateManager) TipSetState(ctx context.Context, ts *types.TipSet) (st cid.Cid, rec cid.Cid, err error) {
	ctx, span := trace.StartSpan(ctx, "tipSetState")
	defer span.End()
	if span.IsRecordingEvents() {
		span.AddAttributes(trace.StringAttribute("tipset", fmt.Sprint(ts.Cids())))
	}

	ck := cidsToKey(ts.Cids())
	sm.stlk.Lock()
	// 该ts是否在计算中
	cw, cwok := sm.compWait[ck]
	// 在计算
	if cwok {
		// 开放compWait的读权限
		sm.stlk.Unlock()
		span.AddAttributes(trace.BoolAttribute("waited", true))
		select {
		case <-cw: // 当close(ch)会触发这个分支
			// 禁止读
			sm.stlk.Lock()
		case <-ctx.Done(): // 已经closed，nil
			return cid.Undef, cid.Undef, ctx.Err()
		}
	}
	cached, ok := sm.stCache[ck]
	if ok {
		sm.stlk.Unlock()
		span.AddAttributes(trace.BoolAttribute("cache", true))
		return cached[0], cached[1], nil
	}
	ch := make(chan struct{})
	sm.compWait[ck] = ch

	// tsState的异步缓存更新
	defer func() {
		sm.stlk.Lock()
		delete(sm.compWait, ck)
		if st != cid.Undef {
			sm.stCache[ck] = []cid.Cid{st, rec}
		}
		sm.stlk.Unlock()
		close(ch)
	}()

	sm.stlk.Unlock()

	if ts.Height() == 0 {
		// NB: This is here because the process that executes blocks requires that the
		// block miner reference a valid miner in the state tree. Unless we create some
		// magical genesis miner, this won't work properly, so we short circuit here
		// This avoids the question of 'who gets paid the genesis block reward'
		return ts.Blocks()[0].ParentStateRoot, ts.Blocks()[0].ParentMessageReceipts, nil
	}

	// tsState的同步获取
	st, rec, err = sm.computeTipSetState(ctx, ts.Blocks(), nil)
	if err != nil {
		return cid.Undef, cid.Undef, err
	}

	return st, rec, nil
}

// 网络出块，做结算，存储，以及开启下一轮的区块
// @param blks 同一个ts中的blks
// @param cb 回调函数
// @return stateroot
// @return receiptroot
func (sm *StateManager) computeTipSetState(ctx context.Context, blks []*types.BlockHeader, cb func(cid.Cid, *types.Message, *vm.ApplyRet) error) (cid.Cid, cid.Cid, error) {
	ctx, span := trace.StartSpan(ctx, "computeTipSetState")
	defer span.End()

	for i := 0; i < len(blks); i++ {
		for j := i + 1; j < len(blks); j++ {
			if blks[i].Miner == blks[j].Miner {
				return cid.Undef, cid.Undef,
					xerrors.Errorf("duplicate miner in a tipset (%s %s)",
						blks[i].Miner, blks[j].Miner)
			}
		}
	}

	pstate := blks[0].ParentStateRoot
	if len(blks[0].Parents) > 0 { // don't support forks on genesis
		parent, err := sm.cs.GetBlock(blks[0].Parents[0])
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("getting parent block: %w", err)
		}

		// 处理分叉 bug fix
		pstate, err = sm.handleStateForks(ctx, blks[0].ParentStateRoot, blks[0].Height, parent.Height)
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("error handling state forks: %w", err)
		}
	}

	cids := make([]cid.Cid, len(blks))
	for i, v := range blks {
		cids[i] = v.Cid()
	}

	r := store.NewChainRand(sm.cs, cids, blks[0].Height)

	vmi, err := sm.newVM(pstate, blks[0].Height, r, address.Undef, sm.cs.Blockstore(), sm.cs.VMSys())
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("instantiating VM failed: %w", err)
	}

	netact, err := vmi.StateTree().GetActor(actors.NetworkAddress)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("failed to get network actor: %w", err)
	}
	// 应该给与的出块奖励数额
	reward := vm.MiningReward(netact.Balance)
	for tsi, b := range blks {
		// 取出网络
		netact, err = vmi.StateTree().GetActor(actors.NetworkAddress)
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("failed to get network actor: %w", err)
		}
		vmi.SetBlockMiner(b.Miner)

		// 取出矿工的拥有者
		owner, err := GetMinerOwner(ctx, sm, pstate, b.Miner)
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("failed to get owner for miner %s: %w", b.Miner, err)
		}

		act, err := vmi.StateTree().GetActor(owner)
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("failed to get miner owner actor")
		}

		// 区块奖励发放，网络给矿工发奖励
		if err := vm.Transfer(netact, act, reward); err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("failed to deduct funds from network actor: %w", err)
		}

		// all block miners created a valid post, go update the actor state
		postSubmitMsg := &types.Message{
			From:     actors.NetworkAddress,
			Nonce:    netact.Nonce,
			To:       b.Miner,
			Method:   actors.MAMethods.SubmitElectionPoSt,
			GasPrice: types.NewInt(0),
			GasLimit: types.NewInt(10000000000),
			Value:    types.NewInt(0),
		}
		ret, err := vmi.ApplyMessage(ctx, postSubmitMsg)
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("submit election post message for block %s (miner %s) invocation failed: %w", b.Cid(), b.Miner, err)
		}
		if ret.ExitCode != 0 {
			return cid.Undef, cid.Undef, xerrors.Errorf("submit election post invocation returned nonzero exit code: %d (err = %s, block = %s, miner = %s, tsi = %d)", ret.ExitCode, ret.ActorErr, b.Cid(), b.Miner, tsi)
		}
	}

	// TODO: can't use method from chainstore because it doesnt let us know who the block miners were
	applied := make(map[address.Address]uint64)
	balances := make(map[address.Address]types.BigInt)

	preloadAddr := func(a address.Address) error {
		if _, ok := applied[a]; !ok {
			act, err := vmi.StateTree().GetActor(a)
			if err != nil {
				return err
			}

			applied[a] = act.Nonce
			balances[a] = act.Balance
		}
		return nil
	}

	var receipts []cbg.CBORMarshaler
	for _, b := range blks {
		vmi.SetBlockMiner(b.Miner)

		bms, sms, err := sm.cs.MessagesForBlock(b)
		if err != nil {
			return cid.Undef, cid.Undef, xerrors.Errorf("failed to get messages for block: %w", err)
		}

		cmsgs := make([]store.ChainMsg, 0, len(bms)+len(sms))
		for _, m := range bms {
			cmsgs = append(cmsgs, m)
		}
		for _, sm := range sms {
			cmsgs = append(cmsgs, sm)
		}

		for _, cm := range cmsgs {
			m := cm.VMMessage()
			if err := preloadAddr(m.From); err != nil {
				return cid.Undef, cid.Undef, err
			}

			if applied[m.From] != m.Nonce {
				continue
			}
			applied[m.From]++

			if balances[m.From].LessThan(m.RequiredFunds()) {
				continue
			}
			balances[m.From] = types.BigSub(balances[m.From], m.RequiredFunds())

			r, err := vmi.ApplyMessage(ctx, m)
			if err != nil {
				return cid.Undef, cid.Undef, err
			}

			receipts = append(receipts, &r.MessageReceipt)

			if cb != nil {
				if err := cb(cm.Cid(), m, r); err != nil {
					return cid.Undef, cid.Undef, err
				}
			}
		}
	}

	// TODO: this nonce-getting is a tiny bit ugly
	// 下一轮Epoch开始
	ca, err := vmi.StateTree().GetActor(actors.CronAddress)
	if err != nil {
		return cid.Undef, cid.Undef, err
	}

	ret, err := vmi.ApplyMessage(ctx, &types.Message{
		To:       actors.CronAddress,
		From:     actors.CronAddress,
		Nonce:    ca.Nonce,
		Value:    types.NewInt(0),
		GasPrice: types.NewInt(0),
		GasLimit: types.NewInt(1 << 30), // Make super sure this is never too little
		Method:   actors.CAMethods.EpochTick,
		Params:   nil,
	})
	if err != nil {
		return cid.Undef, cid.Undef, err
	}
	if ret.ExitCode != 0 {
		return cid.Undef, cid.Undef, xerrors.Errorf("CheckProofSubmissions exit was non-zero: %d", ret.ExitCode)
	}

	// 区块打包
	bs := amt.WrapBlockstore(sm.cs.Blockstore())
	rectroot, err := amt.FromArray(bs, receipts)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("failed to build receipts amt: %w", err)
	}

	st, err := vmi.Flush(ctx)
	if err != nil {
		return cid.Undef, cid.Undef, xerrors.Errorf("vm flush failed: %w", err)
	}

	return st, rectroot, nil
}

// 从指定ts的statetree中取出指定地址的actor
func (sm *StateManager) GetActor(addr address.Address, ts *types.TipSet) (*types.Actor, error) {
	if ts == nil {
		ts = sm.cs.GetHeaviestTipSet()
	}

	stcid := ts.ParentState()

	cst := hamt.CSTFromBstore(sm.cs.Blockstore())
	state, err := state.LoadStateTree(cst, stcid)
	if err != nil {
		return nil, xerrors.Errorf("load state tree: %w", err)
	}

	return state.GetActor(addr)
}

// 查看指定ts中指定地址的账户余额
func (sm *StateManager) GetBalance(addr address.Address, ts *types.TipSet) (types.BigInt, error) {
	act, err := sm.GetActor(addr, ts)
	if err != nil {
		if xerrors.Is(err, types.ErrActorNotFound) {
			return types.NewInt(0), nil
		}
		return types.EmptyInt, xerrors.Errorf("get actor: %w", err)
	}

	return act.Balance, nil
}

func (sm *StateManager) ChainStore() *store.ChainStore {
	return sm.cs
}

// 从存储中取出指定地址和ts的st
func (sm *StateManager) LoadActorState(ctx context.Context, a address.Address, out interface{}, ts *types.TipSet) (*types.Actor, error) {
	act, err := sm.GetActor(a, ts)
	if err != nil {
		return nil, err
	}

	cst := hamt.CSTFromBstore(sm.cs.Blockstore())
	if err := cst.Get(ctx, act.Head, out); err != nil {
		return nil, err
	}

	return act, nil
}

// 取指定ts和地址的公钥地址
func (sm *StateManager) ResolveToKeyAddress(ctx context.Context, addr address.Address, ts *types.TipSet) (address.Address, error) {
	switch addr.Protocol() {
	case address.BLS, address.SECP256K1:
		return addr, nil
	case address.Actor:
		return address.Undef, xerrors.New("cannot resolve actor address to key address")
	default:
	}

	if ts == nil {
		ts = sm.cs.GetHeaviestTipSet()
	}

	st, _, err := sm.TipSetState(ctx, ts)
	if err != nil {
		return address.Undef, xerrors.Errorf("resolve address failed to get tipset state: %w", err)
	}

	cst := hamt.CSTFromBstore(sm.cs.Blockstore())
	tree, err := state.LoadStateTree(cst, st)
	if err != nil {
		return address.Undef, xerrors.Errorf("failed to load state tree")
	}

	return vm.ResolveToKeyAddr(tree, cst, addr)
}

// 取指定ts和地址的公钥内容
func (sm *StateManager) GetBlsPublicKey(ctx context.Context, addr address.Address, ts *types.TipSet) (pubk bls.PublicKey, err error) {
	kaddr, err := sm.ResolveToKeyAddress(ctx, addr, ts)
	if err != nil {
		return pubk, xerrors.Errorf("failed to resolve address to key address: %w", err)
	}

	if kaddr.Protocol() != address.BLS {
		return pubk, xerrors.Errorf("address must be BLS address to load bls public key")
	}

	copy(pubk[:], kaddr.Payload())
	return pubk, nil
}

func (sm *StateManager) GetReceipt(ctx context.Context, msg cid.Cid, ts *types.TipSet) (*types.MessageReceipt, error) {
	m, err := sm.cs.GetCMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to load message: %w", err)
	}

	r, err := sm.tipsetExecutedMessage(ts, msg, m.VMMessage())
	if err != nil {
		return nil, err
	}

	if r != nil {
		return r, nil
	}

	_, r, err = sm.searchBackForMsg(ctx, ts, m)
	if err != nil {
		return nil, fmt.Errorf("failed to look back through chain for message: %w", err)
	}

	return r, nil
}

func (sm *StateManager) WaitForMessage(ctx context.Context, mcid cid.Cid) (*types.TipSet, *types.MessageReceipt, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	msg, err := sm.cs.GetCMessage(mcid)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load message: %w", err)
	}

	// 订阅链的最新状态变更消息
	tsub := sm.cs.SubHeadChanges(ctx)

	head, ok := <-tsub
	if !ok {
		return nil, nil, fmt.Errorf("SubHeadChanges stream was invalid")
	}

	// 订阅成功，会持有一个当前仅含链的最新ts
	if len(head) != 1 {
		return nil, nil, fmt.Errorf("SubHeadChanges first entry should have been one item")
	}

	if head[0].Type != store.HCCurrent {
		return nil, nil, fmt.Errorf("expected current head on SHC stream (got %s)", head[0].Type)
	}

	r, err := sm.tipsetExecutedMessage(head[0].Val, mcid, msg.VMMessage())
	if err != nil {
		return nil, nil, err
	}

	if r != nil {
		return head[0].Val, r, nil
	}

	// 回溯链，找到msg所在的ts, 收据
	// 异步查找
	var backTs *types.TipSet
	var backRcp *types.MessageReceipt
	backSearchWait := make(chan struct{})
	go func() {
		fts, r, err := sm.searchBackForMsg(ctx, head[0].Val, msg)
		if err != nil {
			log.Warnf("failed to look back through chain for message: %w", err)
			return
		}

		backTs = fts
		backRcp = r
		close(backSearchWait)
	}()

	for {
		select {
		case notif, ok := <-tsub:
			if !ok {
				return nil, nil, ctx.Err()
			}
			for _, val := range notif {
				switch val.Type {
				case store.HCRevert:
					continue
				case store.HCApply:
					r, err := sm.tipsetExecutedMessage(val.Val, mcid, msg.VMMessage())
					if err != nil {
						return nil, nil, err
					}
					if r != nil {
						return val.Val, r, nil
					}
				}
			}
		case <-backSearchWait:
			if backTs != nil {
				return backTs, backRcp, nil
			}
			backSearchWait = nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
}

// 从入参ts往前查找m的收据
func (sm *StateManager) searchBackForMsg(ctx context.Context, from *types.TipSet, m store.ChainMsg) (*types.TipSet, *types.MessageReceipt, error) {

	cur := from
	for {
		if cur.Height() == 0 {
			// it ain't here!
			return nil, nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, nil, nil
		default:
		}

		act, err := sm.GetActor(m.VMMessage().From, cur)
		if err != nil {
			return nil, nil, err
		}

		if act.Nonce < m.VMMessage().Nonce {
			// nonce on chain is before message nonce we're looking for, its
			// not going to be here
			return nil, nil, nil
		}

		ts, err := sm.cs.LoadTipSet(cur.Parents())
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load tipset during msg wait searchback: %w", err)
		}

		// 回放检查消息的执行
		r, err := sm.tipsetExecutedMessage(ts, m.Cid(), m.VMMessage())
		if err != nil {
			return nil, nil, fmt.Errorf("checking for message execution during lookback: %w", err)
		}

		if r != nil {
			return ts, r, nil
		}

		cur = ts
	}
}

func (sm *StateManager) tipsetExecutedMessage(ts *types.TipSet, msg cid.Cid, vmm *types.Message) (*types.MessageReceipt, error) {
	// The genesis block did not execute any messages
	if ts.Height() == 0 {
		return nil, nil
	}

	pts, err := sm.cs.LoadTipSet(ts.Parents())
	if err != nil {
		return nil, err
	}

	cm, err := sm.cs.MessagesForTipset(pts)
	if err != nil {
		return nil, err
	}

	for ii := range cm {
		// iterate in reverse because we going backwards through the chain
		i := len(cm) - ii - 1
		m := cm[i]

		if m.VMMessage().From == vmm.From { // cheaper to just check origin first
			if m.VMMessage().Nonce == vmm.Nonce {
				if m.Cid() == msg {
					return sm.cs.GetParentReceipt(ts.Blocks()[0], i)
				}

				// this should be that message
				return nil, xerrors.Errorf("found message with equal nonce as the one we are looking for (F:%s n %d, TS: %s n%d)",
					msg, vmm.Nonce, m.Cid(), m.VMMessage().Nonce)
			}
			if m.VMMessage().Nonce < vmm.Nonce {
				return nil, nil // don't bother looking further
			}
		}
	}

	return nil, nil
}

// ts -> state tree -> addresses
func (sm *StateManager) ListAllActors(ctx context.Context, ts *types.TipSet) ([]address.Address, error) {
	if ts == nil {
		ts = sm.cs.GetHeaviestTipSet()
	}
	st, _, err := sm.TipSetState(ctx, ts)
	if err != nil {
		return nil, err
	}

	// chain state tree
	cst := hamt.CSTFromBstore(sm.cs.Blockstore())
	r, err := hamt.LoadNode(ctx, cst, st)
	if err != nil {
		return nil, err
	}

	var out []address.Address
	err = r.ForEach(ctx, func(k string, val interface{}) error {
		addr, err := address.NewFromBytes([]byte(k))
		if err != nil {
			return xerrors.Errorf("address in state tree was not valid: %w", err)
		}
		out = append(out, addr)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

// 存储参与者余额
func (sm *StateManager) MarketBalance(ctx context.Context, addr address.Address, ts *types.TipSet) (actors.StorageParticipantBalance, error) {
	var state actors.StorageMarketState
	if _, err := sm.LoadActorState(ctx, actors.StorageMarketAddress, &state, ts); err != nil {
		return actors.StorageParticipantBalance{}, err
	}
	cst := hamt.CSTFromBstore(sm.cs.Blockstore())
	b, _, err := actors.GetMarketBalances(ctx, cst, state.Balances, addr)
	if err != nil {
		return actors.StorageParticipantBalance{}, err
	}

	return b[0], nil
}

func (sm *StateManager) ValidateChain(ctx context.Context, ts *types.TipSet) error {
	tschain := []*types.TipSet{ts}
	for ts.Height() != 0 {
		next, err := sm.cs.LoadTipSet(ts.Parents())
		if err != nil {
			return err
		}

		tschain = append(tschain, next)
		ts = next
	}

	lastState := tschain[len(tschain)-1].ParentState()
	// 从后向前更新？？？
	for i := len(tschain) - 1; i >= 0; i-- {
		cur := tschain[i]
		log.Infof("computing state (height: %d, ts=%s)", cur.Height(), cur.Cids())
		if cur.ParentState() != lastState {
			return xerrors.Errorf("tipset chain had state mismatch at height %d", cur.Height())
		}
		// 链状态的更新
		st, _, err := sm.TipSetState(ctx, cur)
		if err != nil {
			return err
		}
		lastState = st
	}

	return nil
}

func (sm *StateManager) SetVMConstructor(nvm func(cid.Cid, uint64, vm.Rand, address.Address, blockstore.Blockstore, *types.VMSyscalls) (*vm.VM, error)) {
	sm.newVM = nvm
}
