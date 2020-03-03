package sealing

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/lib/statemachine"
)

/*
Finite state machine 有限状态机
 */
func (m *Sealing) Plan(events []statemachine.Event, user interface{}) (interface{}, error) {
	next, err := m.plan(events, user.(*SectorInfo))
	if err != nil || next == nil {
		return nil, err
	}

	return func(ctx statemachine.Context, si SectorInfo) error {
		err := next(ctx, si)
		if err != nil {
			log.Errorf("unhandled sector error (%d): %+v", si.SectorID, err)
			return nil
		}

		return nil
	}, nil
}

var fsmPlanners = []func(events []statemachine.Event, state *SectorInfo) error{
	api.UndefinedSectorState: planOne(on(SectorStart{}, api.Packing)),
	api.Packing:              planOne(on(SectorPacked{}, api.Unsealed)),
	api.Unsealed: planOne(
		on(SectorSealed{}, api.PreCommitting),
		on(SectorSealFailed{}, api.SealFailed),
		on(SectorPackingFailed{}, api.PackingFailed),
	),
	api.PreCommitting: planOne(
		on(SectorSealFailed{}, api.SealFailed),
		on(SectorPreCommitted{}, api.WaitSeed),
		on(SectorPreCommitFailed{}, api.PreCommitFailed),
	),
	api.WaitSeed: planOne(
		on(SectorSeedReady{}, api.Committing),
		on(SectorPreCommitFailed{}, api.PreCommitFailed),
	),
	api.Committing: planCommitting,
	api.CommitWait: planOne(
		on(SectorProving{}, api.FinalizeSector),
		on(SectorCommitFailed{}, api.CommitFailed),
	),

	api.FinalizeSector: planOne(
		on(SectorFinalized{}, api.Proving),
	),

	api.Proving: planOne(
		on(SectorFaultReported{}, api.FaultReported),
		on(SectorFaulty{}, api.Faulty),
	),

	api.SealFailed: planOne(
		on(SectorRetrySeal{}, api.Unsealed),
	),
	api.PreCommitFailed: planOne(
		on(SectorRetryPreCommit{}, api.PreCommitting),
		on(SectorRetryWaitSeed{}, api.WaitSeed),
		on(SectorSealFailed{}, api.SealFailed),
	),

	api.Faulty: planOne(
		on(SectorFaultReported{}, api.FaultReported),
	),
	api.FaultedFinal: final,
}

func (m *Sealing) plan(events []statemachine.Event, state *SectorInfo) (func(statemachine.Context, SectorInfo) error, error) {
	/////
	// First process all events

	for _, event := range events {
		l := Log{
			Timestamp: uint64(time.Now().Unix()),
			Message:   fmt.Sprintf("%+v", event),
			Kind:      fmt.Sprintf("event;%T", event.User),
		}

		if err, iserr := event.User.(xerrors.Formatter); iserr {
			l.Trace = fmt.Sprintf("%+v", err)
		}

		state.Log = append(state.Log, l)
	}

	p := fsmPlanners[state.State]
	if p == nil {
		return nil, xerrors.Errorf("planner for state %s not found", api.SectorStates[state.State])
	}

	if err := p(events, state); err != nil {
		return nil, xerrors.Errorf("running planner for state %s failed: %w", api.SectorStates[state.State], err)
	}

	/////
	// Now decide what to do next

	/*

	Incoming：获取pieces数组大小，扇区id，每个pieces填充随机数，每个pieces生成凭据
	          封装到交易信息中，广播到链，等待链上反馈，验证添加pieces到扇区，耗费cpu内存
	Packing：检查扇区的完整性，不完整扇区，填充完整，更新扇区信息
	Unsealed：密封数据，给每个数据片产生产生加密副本，产生大量的缓存数据，耗费内存
	PreCommitting：将包含扇区存储凭据的相关信息广播到链
	WaitSeed：等待链上消息，获取链上区块高度和一个延时区块量，
	              定义两个在指定区块高度执行（密封seed更新）和回滚方法
	Committing：链上随机挑战（指定区块高度执行的方法）产生PoRep，将产生PORep的证据提交到链，链上验证
	CommitWait：等待链上的消息，接受链上的消息，验证状态，如果成功将状态转成Proving

	整个完整的存储过程结束
		*   Empty
		|   |
		|   v
		*<- Packing <- incoming
		|   |
		|   v
		*<- Unsealed <--> SealFailed
		|   |
		|   v
		*   PreCommitting <--> PreCommitFailed
		|   |                  ^
		|   v                  |
		*<- WaitSeed ----------/
		|   |||
		|   vvv      v--> SealCommitFailed
		*<- Committing
		|   |        ^--> CommitFailed
		|   v             ^
		*<- CommitWait ---/
		|   |
		|   v
		*<- Proving
		|
		v
		FailedUnrecoverable

		UndefinedSectorState <- ¯\_(ツ)_/¯
		    |                     ^
		    *---------------------/

	*/

	switch state.State {
	// Happy path
	case api.Packing:
		return m.handlePacking, nil
	case api.Unsealed:
		return m.handleUnsealed, nil
	case api.PreCommitting:
		return m.handlePreCommitting, nil
	case api.WaitSeed:
		return m.handleWaitSeed, nil
	case api.Committing:
		return m.handleCommitting, nil
	case api.CommitWait:
		return m.handleCommitWait, nil
	case api.FinalizeSector:
		return m.handleFinalizeSector, nil
	case api.Proving:
		// TODO: track sector health / expiration
		log.Infof("Proving sector %d", state.SectorID)

	// Handled failure modes
	case api.SealFailed:
		return m.handleSealFailed, nil
	case api.PreCommitFailed:
		return m.handlePreCommitFailed, nil
	case api.SealCommitFailed:
		log.Warnf("sector %d entered unimplemented state 'SealCommitFailed'", state.SectorID)
	case api.CommitFailed:
		log.Warnf("sector %d entered unimplemented state 'CommitFailed'", state.SectorID)

		// Faults
	case api.Faulty:
		return m.handleFaulty, nil
	case api.FaultReported:
		return m.handleFaultReported, nil

	// Fatal errors
	case api.UndefinedSectorState:
		log.Error("sector update with undefined state!")
	case api.FailedUnrecoverable:
		log.Errorf("sector %d failed unrecoverably", state.SectorID)
	default:
		log.Errorf("unexpected sector update state: %d", state.State)
	}

	return nil, nil
}

func planCommitting(events []statemachine.Event, state *SectorInfo) error {
	for _, event := range events {
		switch e := event.User.(type) {
		case globalMutator:
			if e.applyGlobal(state) {
				return nil
			}
		case SectorCommitted: // the normal case
			e.apply(state)
			state.State = api.CommitWait
		case SectorSeedReady: // seed changed :/
			if e.seed.Equals(&state.Seed) {
				log.Warnf("planCommitting: got SectorSeedReady, but the seed didn't change")
				continue // or it didn't!
			}
			log.Warnf("planCommitting: commit Seed changed")
			e.apply(state)
			state.State = api.Committing
			return nil
		case SectorComputeProofFailed:
			state.State = api.SealCommitFailed
		case SectorSealFailed:
			state.State = api.CommitFailed
		case SectorCommitFailed:
			state.State = api.CommitFailed
		default:
			return xerrors.Errorf("planCommitting got event of unknown type %T, events: %+v", event.User, events)
		}
	}
	return nil
}

func (m *Sealing) restartSectors(ctx context.Context) error {
	trackedSectors, err := m.ListSectors()
	if err != nil {
		log.Errorf("loading sector list: %+v", err)
	}

	for _, sector := range trackedSectors {
		if err := m.sectors.Send(sector.SectorID, SectorRestart{}); err != nil {
			log.Errorf("restarting sector %d: %+v", sector.SectorID, err)
		}
	}

	// TODO: Grab on-chain sector set and diff with trackedSectors

	return nil
}

func (m *Sealing) ForceSectorState(ctx context.Context, id uint64, state api.SectorState) error {
	return m.sectors.Send(id, SectorForceState{state})
}

func final(events []statemachine.Event, state *SectorInfo) error {
	return xerrors.Errorf("didn't expect any events in state %s, got %+v", api.SectorStates[state.State], events)
}

func on(mut mutator, next api.SectorState) func() (mutator, api.SectorState) {
	return func() (mutator, api.SectorState) {
		return mut, next
	}
}

func planOne(ts ...func() (mut mutator, next api.SectorState)) func(events []statemachine.Event, state *SectorInfo) error {
	return func(events []statemachine.Event, state *SectorInfo) error {
		if len(events) != 1 {
			for _, event := range events {
				if gm, ok := event.User.(globalMutator); ok {
					gm.applyGlobal(state)
					return nil
				}
			}
			return xerrors.Errorf("planner for state %s only has a plan for a single event only, got %+v", api.SectorStates[state.State], events)
		}

		if gm, ok := events[0].User.(globalMutator); ok {
			gm.applyGlobal(state)
			return nil
		}

		for _, t := range ts {
			mut, next := t()

			if reflect.TypeOf(events[0].User) != reflect.TypeOf(mut) {
				continue
			}

			if err, iserr := events[0].User.(error); iserr {
				log.Warnf("sector %d got error event %T: %+v", state.SectorID, events[0].User, err)
			}

			events[0].User.(mutator).apply(state)
			state.State = next
			return nil
		}

		return xerrors.Errorf("planner for state %s received unexpected event %T (%+v)", api.SectorStates[state.State], events[0].User, events[0])
	}
}
