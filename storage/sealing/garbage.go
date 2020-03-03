package sealing

import (
	"bytes"
	"context"
	"io"
	"math"
	"math/bits"
	"math/rand"

	sectorbuilder "github.com/filecoin-project/go-sectorbuilder"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
)

func (m *Sealing) pledgeReader(size uint64, parts uint64) io.Reader {
	parts = 1 << bits.Len64(parts) // round down to nearest power of 2
	if size/parts < 127 {
		parts = size / 127
	}

	piece := sectorbuilder.UserBytesForSectorSize((size/127 + size) / parts)

	readers := make([]io.Reader, parts)
	for i := range readers {
		readers[i] = io.LimitReader(rand.New(rand.NewSource(42+int64(i))), int64(piece))
	}

	return io.MultiReader(readers...)
}

// 该方法主要作用是为每隔扇区生成一个凭据，
// 并把每隔凭据封装成一个交易信息，提交到链上，
// 并解析出链上的提交信息进行判断交易id是否一致,存储数据；
// 返回信息为分片信息数组
func (m *Sealing) pledgeSector(ctx context.Context, sectorID uint64, existingPieceSizes []uint64, sizes ...uint64) ([]Piece, error) {
	if len(sizes) == 0 {
		return nil, nil
	}

	log.Infof("Pledge %d, contains %+v", sectorID, existingPieceSizes)

	deals := make([]actors.StorageDealProposal, len(sizes))
	for i, size := range sizes {
		// 通过32GB扇区大小的随机数据生成commP结果。
		commP, err := m.fastPledgeCommitment(size, uint64(1))
		if err != nil {
			return nil, err
		}

		// 由commP生成StorageDealProposal结构。
		sdp := actors.StorageDealProposal{
			PieceRef:             commP[:],
			PieceSize:            size,
			Client:               m.worker,
			Provider:             m.maddr,
			ProposalExpiration:   math.MaxUint64,
			Duration:             math.MaxUint64 / 2, // /2 because overflows
			StoragePricePerEpoch: types.NewInt(0),
			StorageCollateral:    types.NewInt(0),
			ProposerSignature:    nil, // nil because self dealing
		}

		deals[i] = sdp
	}

	log.Infof("Publishing deals for %d", sectorID)

	// 将StorageDealProposal结构通过CBOR序列化生成参数结果。
	params, aerr := actors.SerializeParams(&actors.PublishStorageDealsParams{
		Deals: deals,
	})
	if aerr != nil {
		return nil, xerrors.Errorf("serializing PublishStorageDeals params failed: ", aerr)
	}

	smsg, err := m.api.MpoolPushMessage(ctx, &types.Message{
		To:       actors.StorageMarketAddress,
		From:     m.worker,
		Value:    types.NewInt(0),
		GasPrice: types.NewInt(0),
		GasLimit: types.NewInt(1000000),
		Method:   actors.SMAMethods.PublishStorageDeals,
		Params:   params,
	})
	if err != nil {
		return nil, err
	}
	// 将结果作为Message的参数，推送到链上进行校验。等待链上反馈
	r, err := m.api.StateWaitMsg(ctx, smsg.Cid()) // TODO: more finality
	if err != nil {
		return nil, err
	}
	if r.Receipt.ExitCode != 0 {
		log.Error(xerrors.Errorf("publishing deal failed: exit %d", r.Receipt.ExitCode))
	}
	//当等到链上校验通过时，将返回结果CBOR序列化生成resp结构。
	var resp actors.PublishStorageDealResponse
	if err := resp.UnmarshalCBOR(bytes.NewReader(r.Receipt.Return)); err != nil {
		return nil, err
	}
	// 从链上消息中解析出DealID，看是否一致
	if len(resp.DealIDs) != len(sizes) {
		return nil, xerrors.New("got unexpected number of DealIDs from PublishStorageDeals")
	}

	log.Infof("Deals for sector %d: %+v", sectorID, resp.DealIDs)

	// 根据链上确认的结果，首先将piece的信息存到sector里面
	// 写入32G文件至~/.lotusstorage/staging/目录，并生成commP，由此commP以及DealID生成扇区pieces信息。
	out := make([]Piece, len(sizes))
	for i, size := range sizes {
		// 数据填充
		ppi, err := m.sb.AddPiece(ctx, size, sectorID, m.pledgeReader(size, uint64(1)), existingPieceSizes)
		if err != nil {
			return nil, xerrors.Errorf("add piece: %w", err)
		}

		existingPieceSizes = append(existingPieceSizes, size)

		out[i] = Piece{
			DealID: resp.DealIDs[i],
			Size:   ppi.Size,
			CommP:  ppi.CommP[:],
		}
	}

	return out, nil
}

func (m *Sealing) PledgeSector() error {
	go func() {
		ctx := context.TODO() // we can't use the context from command which invokes
		// this, as we run everything here async, and it's cancelled when the
		// command exits

		// 获取扇区大小
		//  一共多少个分片,是否跟生成默克尔树的分块对应?
		size := sectorbuilder.UserBytesForSectorSize(m.sb.SectorSize())

		// 获取扇区id
		sid, err := m.sb.AcquireSectorId()
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		// 生成扇区pieces
		// 产生分片数组，该方法中会将生成的签名信息提交到链上，重点方法
		pieces, err := m.pledgeSector(ctx, sid, []uint64{}, size)
		if err != nil {
			log.Errorf("%+v", err)
			return
		}

		// 产生新扇区生成任务
		if err := m.newSector(context.TODO(), sid, pieces[0].DealID, pieces[0].ppi()); err != nil {
			log.Errorf("%+v", err)
			return
		}
	}()
	return nil
}
