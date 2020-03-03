package sealing

import (
	"context"

	sectorbuilder "github.com/filecoin-project/go-sectorbuilder"
	"github.com/filecoin-project/go-sectorbuilder/fs"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/statemachine"
)

/**
主要是判断扇区数据是否完整，将没填满的扇区填充完整，之后将状态更改为 Unsealed状态
打包的状态，将没有填满数据的扇区填满
 */
func (m *Sealing) handlePacking(ctx statemachine.Context, sector SectorInfo) error {
	log.Infow("performing filling up rest of the sector...", "sector", sector.SectorID)

	var allocated uint64
	for _, piece := range sector.Pieces {
		allocated += piece.Size
	}

	ubytes := sectorbuilder.UserBytesForSectorSize(m.sb.SectorSize())

	if allocated > ubytes {
		return xerrors.Errorf("too much data in sector: %d > %d", allocated, ubytes)
	}

	fillerSizes, err := fillersFromRem(ubytes - allocated)
	if err != nil {
		return err
	}

	if len(fillerSizes) > 0 {
		log.Warnf("Creating %d filler pieces for sector %d", len(fillerSizes), sector.SectorID)
	}

	// 此处调用  pledgeSector将扇区填满
	pieces, err := m.pledgeSector(ctx.Context(), sector.SectorID, sector.existingPieces(), fillerSizes...)
	if err != nil {
		return xerrors.Errorf("filling up the sector (%v): %w", fillerSizes, err)
	}

	// 数据填充完毕后，扇区的状态转换到了Unsealed状态
	// SectorPacked -> ?
	return ctx.Send(SectorPacked{pieces: pieces})
}

func (m *Sealing) handleUnsealed(ctx statemachine.Context, sector SectorInfo) error {
	// Sanity 通情达理、常理、精神正常
	if err := checkPieces(ctx.Context(), sector, m.api); err != nil { // Sanity check state
		switch err.(type) {
		case *ErrApi:
			log.Errorf("handleUnsealed: api error, not proceeding: %+v", err)
			return nil
		case *ErrInvalidDeals:
			return ctx.Send(SectorPackingFailed{xerrors.Errorf("invalid deals in sector: %w", err)})
		case *ErrExpiredDeals: // Probably not much we can do here, maybe re-pack the sector?
			return ctx.Send(SectorPackingFailed{xerrors.Errorf("expired deals in sector: %w", err)})
		default:
			return xerrors.Errorf("checkPieces sanity check error: %w", err)
		}
	}

	log.Infow("performing sector replication...", "sector", sector.SectorID)
	// 调用随机函数返回一个随机选票(包含区块高度，和票据)
	// 随机函数在初始化矿工生成的，运用的反射，具体需要详细查看 ?
	ticket, err := m.tktFn(ctx.Context())
	if err != nil {
		return ctx.Send(SectorSealFailed{xerrors.Errorf("getting ticket failed: %w", err)})
	}
	// 开始进行密封的操作,主要根据源数据产生加密数据,产生一份副本
	/**
	   判断在 .lotusstorage 文件下几个目录是存在 cache,staged,sealed；
	   调用rust库的代码生成相关的凭据
	 */
	rspco, err := m.sb.SealPreCommit(ctx.Context(), sector.SectorID, *ticket, sector.pieceInfos())
	if err != nil {
		return ctx.Send(SectorSealFailed{xerrors.Errorf("seal pre commit failed: %w", err)})
	}

	// 更改状态，把数据的唯一复制凭据信息，和随机数相关更新
	return ctx.Send(SectorSealed{
		commD: rspco.CommD[:],
		commR: rspco.CommR[:],
		ticket: SealTicket{
			BlockHeight: ticket.BlockHeight,
			TicketBytes: ticket.TicketBytes[:],
		},
	})
}

// 主要是讲消息广播到链上去，并把该消息cid存起来；主要是让区块到了指定的高度验证数据的有效性
func (m *Sealing) handlePreCommitting(ctx statemachine.Context, sector SectorInfo) error {
	if err := checkSeal(ctx.Context(), m.maddr, sector, m.api); err != nil {
		switch err.(type) {
		case *ErrApi:
			log.Errorf("handlePreCommitting: api error, not proceeding: %+v", err)
			return nil
		case *ErrBadCommD: // TODO: Should this just back to packing? (not really needed since handleUnsealed will do that too)
			return ctx.Send(SectorSealFailed{xerrors.Errorf("bad CommD error: %w", err)})
		case *ErrExpiredTicket:
			return ctx.Send(SectorSealFailed{xerrors.Errorf("bad CommD error: %w", err)})
		default:
			return xerrors.Errorf("checkSeal sanity check error: %w", err)
		}
	}

	// 要发到链上的消息
	params := &actors.SectorPreCommitInfo{
		SectorNumber: sector.SectorID,

		CommR:     sector.CommR,
		SealEpoch: sector.Ticket.BlockHeight,
		DealIDs:   sector.deals(),
	}
	enc, aerr := actors.SerializeParams(params)
	if aerr != nil {
		return ctx.Send(SectorPreCommitFailed{xerrors.Errorf("could not serialize commit sector parameters: %w", aerr)})
	}
	// 封装消息体
	msg := &types.Message{
		To:       m.maddr,
		From:     m.worker,
		Method:   actors.MAMethods.PreCommitSector,
		Params:   enc,
		Value:    types.NewInt(0), // TODO: need to ensure sufficient collateral
		GasLimit: types.NewInt(1000000 /* i dont know help */),
		GasPrice: types.NewInt(1),
	}

	log.Info("submitting precommit for sector: ", sector.SectorID)
	// 广播
	smsg, err := m.api.MpoolPushMessage(ctx.Context(), msg)
	if err != nil {
		return ctx.Send(SectorPreCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}
	// 将收到的消息cid 保存
	return ctx.Send(SectorPreCommitted{message: smsg.Cid()})
}

/*
等待链上消息，获取链上区块高度和一个延时区块量，
	              定义两个在指定区块高度执行（密封seed更新）和回滚方法

该过程主要是等待之前
	生成扇区唯一副本和凭据广播到链上的消息，
等待之后，
	根据当前的区块的高度加上一个延时变量(预估5分钟左右)，
	生成在该区块时执行的方法，和回滚的方法。
	状态更改 Committing
*/
func (m *Sealing) handleWaitSeed(ctx statemachine.Context, sector SectorInfo) error {
	// would be ideal to just use the events.Called handler, but it wouldnt be able to handle individual message timeouts
	log.Info("Sector precommitted: ", sector.SectorID)
	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.PreCommitMessage)
	if err != nil {
		return ctx.Send(SectorPreCommitFailed{err})
	}

	if mw.Receipt.ExitCode != 0 {
		log.Error("sector precommit failed: ", mw.Receipt.ExitCode)
		err := xerrors.Errorf("sector precommit failed: %d", mw.Receipt.ExitCode)
		return ctx.Send(SectorPreCommitFailed{err})
	}
	log.Info("precommit message landed on chain: ", sector.SectorID)
	// 区块的高度+定义的延时量（8-1）
	randHeight := mw.TipSet.Height() + build.InteractivePoRepDelay - 1 // -1 because of how the messages are applied
	log.Infof("precommit for sector %d made it on chain, will start proof computation at height %d", sector.SectorID, randHeight)
	// 一个是在区块到达一定的高度执行的方法和回滚的方法
	err = m.events.ChainAt(func(ectx context.Context, ts *types.TipSet, curH uint64) error {
		// 根据区块高度和ts key生成随机数
		rand, err := m.api.ChainGetRandomness(ectx, ts.Key(), int64(randHeight))
		if err != nil {
			err = xerrors.Errorf("failed to get randomness for computing seal proof: %w", err)

			ctx.Send(SectorFatalError{error: err})
			return err
		}
		// 更改状态 -> Committing
		ctx.Send(SectorSeedReady{seed: SealSeed{
			BlockHeight: randHeight,
			TicketBytes: rand,
		}})

		return nil
	}, func(ctx context.Context, ts *types.TipSet) error {
		log.Warn("revert in interactive commit sector step")
		// TODO: need to cancel running process and restart...
		return nil
	}, build.InteractivePoRepConfidence, mw.TipSet.Height()+build.InteractivePoRepDelay)
	if err != nil {
		log.Warn("waitForPreCommitMessage ChainAt errored: ", err)
	}

	return nil
}

/*
    Committing：链上随机挑战（指定区块高度执行的方法）产生PoRep，将产生PORep的证据提交到链，链上验证
 */
func (m *Sealing) handleCommitting(ctx statemachine.Context, sector SectorInfo) error {
	log.Info("scheduling seal proof computation...")

	// 产生复制证明凭据
	// 这个是重点关注的方法，产生复制证明的证明凭据
	proof, err := m.sb.SealCommit(ctx.Context(), sector.SectorID, sector.Ticket.SB(), sector.Seed.SB(), sector.pieceInfos(), sector.rspco())
	if err != nil {
		return ctx.Send(SectorComputeProofFailed{xerrors.Errorf("computing seal proof failed: %w", err)})
	}

	// TODO: Consider splitting states and persist proof for faster recovery

	params := &actors.SectorProveCommitInfo{
		Proof:    proof,
		SectorID: sector.SectorID,
		DealIDs:  sector.deals(),
	}

	enc, aerr := actors.SerializeParams(params)
	if aerr != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("could not serialize commit sector parameters: %w", aerr)})
	}

	msg := &types.Message{
		To:       m.maddr,
		From:     m.worker,
		Method:   actors.MAMethods.ProveCommitSector,
		Params:   enc,
		Value:    types.NewInt(0), // TODO: need to ensure sufficient collateral
		GasLimit: types.NewInt(1000000 /* i dont know help */),
		GasPrice: types.NewInt(1),
	}

	// TODO: check seed / ticket are up to date
	// 把包含证明文件的消息广播
	smsg, err := m.api.MpoolPushMessage(ctx.Context(), msg)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("pushing message to mpool: %w", err)})
	}
	// 更改状态 -> storage/sealing/fsm.go:205
	return ctx.Send(SectorCommitted{
		proof:   proof,
		message: smsg.Cid(),
	})
}

// 主要是接受链上的消息，判断状态，将扇区状态更改为 proving，存储成功
func (m *Sealing) handleCommitWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.CommitMessage == nil {
		log.Errorf("sector %d entered commit wait state without a message cid", sector.SectorID)
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("entered commit wait with no commit cid")})
	}
	// 等待链上广播来的消息
	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.CommitMessage)
	if err != nil {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("failed to wait for porep inclusion: %w", err)})
	}

	if mw.Receipt.ExitCode != 0 {
		return ctx.Send(SectorCommitFailed{xerrors.Errorf("submitting sector proof failed (exit=%d, msg=%s) (t:%x; s:%x(%d); p:%x)", mw.Receipt.ExitCode, sector.CommitMessage, sector.Ticket.TicketBytes, sector.Seed.TicketBytes, sector.Seed.BlockHeight, sector.Proof)})
	}
	// 最终产生算力，更改扇区状态 -> handleFinalizeSector
	return ctx.Send(SectorProving{})
}

func (m *Sealing) handleFinalizeSector(ctx statemachine.Context, sector SectorInfo) error {
	// TODO: Maybe wait for some finality

	if err := m.sb.FinalizeSector(ctx.Context(), sector.SectorID); err != nil {
		if !xerrors.Is(err, fs.ErrNoSuitablePath) {
			return ctx.Send(SectorFinalizeFailed{xerrors.Errorf("finalize sector: %w", err)})
		}
		log.Warnf("finalize sector: %v", err)
	}

	if err := m.sb.DropStaged(ctx.Context(), sector.SectorID); err != nil {
		return ctx.Send(SectorFinalizeFailed{xerrors.Errorf("drop staged: %w", err)})
	}

	return ctx.Send(SectorFinalized{})
}

func (m *Sealing) handleFaulty(ctx statemachine.Context, sector SectorInfo) error {
	// TODO: check if the fault has already been reported, and that this sector is even valid

	// TODO: coalesce faulty sector reporting
	bf := types.NewBitField()
	bf.Set(sector.SectorID)

	enc, aerr := actors.SerializeParams(&actors.DeclareFaultsParams{bf})
	if aerr != nil {
		return xerrors.Errorf("failed to serialize declare fault params: %w", aerr)
	}

	msg := &types.Message{
		To:       m.maddr,
		From:     m.worker,
		Method:   actors.MAMethods.DeclareFaults,
		Params:   enc,
		Value:    types.NewInt(0), // TODO: need to ensure sufficient collateral
		GasLimit: types.NewInt(1000000 /* i dont know help */),
		GasPrice: types.NewInt(1),
	}

	smsg, err := m.api.MpoolPushMessage(ctx.Context(), msg)
	if err != nil {
		return xerrors.Errorf("failed to push declare faults message to network: %w", err)
	}

	return ctx.Send(SectorFaultReported{reportMsg: smsg.Cid()})
}

func (m *Sealing) handleFaultReported(ctx statemachine.Context, sector SectorInfo) error {
	if sector.FaultReportMsg == nil {
		return xerrors.Errorf("entered fault reported state without a FaultReportMsg cid")
	}

	mw, err := m.api.StateWaitMsg(ctx.Context(), *sector.FaultReportMsg)
	if err != nil {
		return xerrors.Errorf("failed to wait for fault declaration: %w", err)
	}

	if mw.Receipt.ExitCode != 0 {
		log.Errorf("UNHANDLED: declaring sector fault failed (exit=%d, msg=%s) (id: %d)", mw.Receipt.ExitCode, *sector.FaultReportMsg, sector.SectorID)
		return xerrors.Errorf("UNHANDLED: submitting fault declaration failed (exit %d)", mw.Receipt.ExitCode)
	}

	return ctx.Send(SectorFaultedFinal{})
}
