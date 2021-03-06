package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"

	"golang.org/x/xerrors"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-filestore"
	chunker "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/libp2p/go-libp2p-core/peer"
	"go.uber.org/fx"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/shared/tokenamount"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/markets/utils"
	"github.com/filecoin-project/lotus/node/impl/full"
	"github.com/filecoin-project/lotus/node/impl/paych"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
)

type API struct {
	fx.In

	full.ChainAPI
	full.StateAPI
	full.WalletAPI
	paych.PaychAPI

	SMDealClient storagemarket.StorageClient
	RetDiscovery retrievalmarket.PeerResolver
	Retrieval    retrievalmarket.RetrievalClient
	Chain        *store.ChainStore

	LocalDAG   dtypes.ClientDAG
	Blockstore dtypes.ClientBlockstore
	Filestore  dtypes.ClientFilestore `optional:"true"`
}
/**
client发起一笔交易
params { fileCid, walletAddr, storageMinerAddr, epochPrice }
return { dealCid }
交易的市场端处理在node/modules/storageminer.go:HandleDeals
存储市场的数据传送给对端代码实现入口
  - go-fil-markets/storagemarket/impl/provider.go:75
  - go-fil-markets/storagemarket/impl/provider.go:207
*/
func (a *API) ClientStartDeal(ctx context.Context, data cid.Cid, addr address.Address, miner address.Address, epochPrice types.BigInt, blocksDuration uint64) (*cid.Cid, error) {
	exist, err := a.WalletHas(ctx, addr)
	if err != nil {
		return nil, xerrors.Errorf("failed getting addr from wallet: %w", addr)
	}
	if !exist {
		return nil, xerrors.Errorf("provided address doesn't exist in wallet")
	}

	pid, err := a.StateMinerPeerID(ctx, miner, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("failed getting peer ID: %w", err)
	}

	mw, err := a.StateMinerWorker(ctx, miner, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("failed getting miner worker: %w", err)
	}
	/**
	StorageProviderInfo {
		Address    address.Address // actor address
		Owner      address.Address // Account that owns this miner
		Worker     address.Address // Worker account for this miner. signs messages
		SectorSize uint64         // Amount of space in each sector committed to the network by this miner.
		PeerID     peer.ID         // Libp2p identity that should be used when connecting to this miner.
	}

	Worker     address.Address
	// This will be the key that is used to sign blocks created by this miner, and
	// sign messages sent on behalf of this miner to commit sectors, submit PoSts, and
	// other day to day miner activities.
	 */
	// 选一个存储矿工
	providerInfo := utils.NewStorageProviderInfo(miner, mw, 0, pid)
	// DealFlow Negotiation-init
	result, err := a.SMDealClient.ProposeStorageDeal(
		ctx,
		addr,
		&providerInfo,
		data,
		storagemarket.Epoch(math.MaxUint64),
		storagemarket.Epoch(blocksDuration),
		utils.ToSharedTokenAmount(epochPrice),
		tokenamount.Empty)

	if err != nil {
		return nil, xerrors.Errorf("failed to start deal: %w", err)
	}

	return &result.ProposalCid, nil
}

func (a *API) ClientListDeals(ctx context.Context) ([]api.DealInfo, error) {
	deals, err := a.SMDealClient.ListInProgressDeals(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]api.DealInfo, len(deals))
	for k, v := range deals {
		out[k] = api.DealInfo{
			ProposalCid: v.ProposalCid,
			State:       v.State,
			Provider:    v.Proposal.Provider,

			PieceRef: v.Proposal.PieceRef,
			Size:     v.Proposal.PieceSize,

			PricePerEpoch: utils.FromSharedTokenAmount(v.Proposal.StoragePricePerEpoch),
			Duration:      v.Proposal.Duration,

			DealID: v.DealID,
		}
	}

	return out, nil
}

/**
@param cid.Cid d 交易id
@return DealInfo 交易信息
 */
func (a *API) ClientGetDealInfo(ctx context.Context, d cid.Cid) (*api.DealInfo, error) {
	v, err := a.SMDealClient.GetInProgressDeal(ctx, d)
	if err != nil {
		return nil, err
	}
	return &api.DealInfo{
		ProposalCid:   v.ProposalCid,
		State:         v.State,
		Provider:      v.Proposal.Provider,
		PieceRef:      v.Proposal.PieceRef,
		Size:          v.Proposal.PieceSize,
		PricePerEpoch: utils.FromSharedTokenAmount(v.Proposal.StoragePricePerEpoch),
		Duration:      v.Proposal.Duration,
		DealID:        v.DealID,
	}, nil
}

func (a *API) ClientHasLocal(ctx context.Context, root cid.Cid) (bool, error) {
	// TODO: check if we have the ENTIRE dag

	offExch := merkledag.NewDAGService(blockservice.New(a.Blockstore, offline.Exchange(a.Blockstore)))
	_, err := offExch.Get(ctx, root)
	if err == ipld.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (a *API) ClientFindData(ctx context.Context, root cid.Cid) ([]api.QueryOffer, error) {
	peers, err := a.RetDiscovery.GetPeers(root)
	if err != nil {
		return nil, err
	}

	out := make([]api.QueryOffer, len(peers))
	for k, p := range peers {
		queryResponse, err := a.Retrieval.Query(ctx, p, root.Bytes(), retrievalmarket.QueryParams{})
		if err != nil {
			out[k] = api.QueryOffer{Err: err.Error(), Miner: p.Address, MinerPeerID: p.ID}
		} else {
			out[k] = api.QueryOffer{
				Root:        root,
				Size:        queryResponse.Size,
				MinPrice:    utils.FromSharedTokenAmount(queryResponse.PieceRetrievalPrice()),
				Miner:       p.Address, // TODO: check
				MinerPeerID: p.ID,
			}
		}
	}

	return out, nil
}

func (a *API) ClientImport(ctx context.Context, path string) (cid.Cid, error) {
	f, err := os.Open(path)
	if err != nil {
		return cid.Undef, err
	}
	stat, err := f.Stat()
	if err != nil {
		return cid.Undef, err
	}

	file, err := files.NewReaderPathFile(path, f, stat)
	if err != nil {
		return cid.Undef, err
	}

	bufferedDS := ipld.NewBufferedDAG(ctx, a.LocalDAG)

	params := ihelper.DagBuilderParams{
		Maxlinks:   build.UnixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
		NoCopy:     true,
	}

	db, err := params.New(chunker.NewSizeSplitter(file, int64(build.UnixfsChunkSize)))
	if err != nil {
		return cid.Undef, err
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	if err := bufferedDS.Commit(); err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}

/**
 * @param ctx
 * @param io.Reader f client 要付费存储的文件流
 * @return cid cid.Cid 文件存好后的DAG的cid
 */
func (a *API) ClientImportLocal(ctx context.Context, f io.Reader) (cid.Cid, error) {
	file := files.NewReaderFile(f)

	bufferedDS := ipld.NewBufferedDAG(ctx, a.LocalDAG)

	// 构建file的存储入参
	params := ihelper.DagBuilderParams{
		Maxlinks:   build.UnixfsLinksPerLevel,
		RawLeaves:  true,
		CidBuilder: nil,
		Dagserv:    bufferedDS,
	}

	// file分块存储
	db, err := params.New(chunker.NewSizeSplitter(file, int64(build.UnixfsChunkSize)))
	if err != nil {
		return cid.Undef, err
	}
	// DAG 构建
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}
	// 存入本地，并返回cid和状态
	return nd.Cid(), bufferedDS.Commit()
}

func (a *API) ClientListImports(ctx context.Context) ([]api.Import, error) {
	if a.Filestore == nil {
		return nil, errors.New("listing imports is not supported with in-memory dag yet")
	}
	next, err := filestore.ListAll(a.Filestore, false)
	if err != nil {
		return nil, err
	}

	// TODO: make this less very bad by tracking root cids instead of using ListAll

	out := make([]api.Import, 0)
	for {
		r := next()
		if r == nil {
			return out, nil
		}
		if r.Offset != 0 {
			continue
		}
		out = append(out, api.Import{
			Status:   r.Status,
			Key:      r.Key,
			FilePath: r.FilePath,
			Size:     r.Size,
		})
	}
}

func (a *API) ClientRetrieve(ctx context.Context, order api.RetrievalOrder, path string) error {
	if order.MinerPeerID == "" {
		pid, err := a.StateMinerPeerID(ctx, order.Miner, types.EmptyTSK)
		if err != nil {
			return err
		}

		order.MinerPeerID = pid
	}

	retrievalResult := make(chan error, 1)

	unsubscribe := a.Retrieval.SubscribeToEvents(func(event retrievalmarket.ClientEvent, state retrievalmarket.ClientDealState) {
		if bytes.Equal(state.PieceCID, order.Root.Bytes()) {
			switch event {
			case retrievalmarket.ClientEventError:
				retrievalResult <- xerrors.New("Retrieval Error")
			case retrievalmarket.ClientEventComplete:
				retrievalResult <- nil
			}
		}
	})

	a.Retrieval.Retrieve(
		ctx,
		order.Root.Bytes(),
		retrievalmarket.NewParamsV0(types.BigDiv(order.Total, types.NewInt(order.Size)).Int, 0, 0),
		utils.ToSharedTokenAmount(order.Total),
		order.MinerPeerID,
		order.Client,
		order.Miner)
	select {
	case <-ctx.Done():
		return xerrors.New("Retrieval Timed Out")
	case err := <-retrievalResult:
		if err != nil {
			return xerrors.Errorf("RetrieveUnixfs: %w", err)
		}
	}

	unsubscribe()

	nd, err := a.LocalDAG.Get(ctx, order.Root)
	if err != nil {
		return xerrors.Errorf("ClientRetrieve: %w", err)
	}
	file, err := unixfile.NewUnixfsFile(ctx, a.LocalDAG, nd)
	if err != nil {
		return xerrors.Errorf("ClientRetrieve: %w", err)
	}
	return files.WriteTo(file, path)
}

/**
客户端查询要价

目前还没有代码调用这个函数

可以试着加上交易所的代码实现交易匹配功能
 */
func (a *API) ClientQueryAsk(ctx context.Context, p peer.ID, miner address.Address) (*types.SignedStorageAsk, error) {
	info := utils.NewStorageProviderInfo(miner, address.Undef, 0, p)
	signedAsk, err := a.SMDealClient.GetAsk(ctx, info)
	if err != nil {
		return nil, err
	}
	return utils.FromSignedStorageAsk(signedAsk)
}
