package paych

import (
	"fmt"
	"io"

	"github.com/filecoin-project/lotus/chain/types"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

var _ = xerrors.Errorf

func (t *VoucherInfo) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.Voucher (types.SignedVoucher) (struct)
	if err := t.Voucher.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Proof ([]uint8) (slice)
	if len(t.Proof) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.Proof was too long")
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajByteString, uint64(len(t.Proof)))); err != nil {
		return err
	}
	if _, err := w.Write(t.Proof); err != nil {
		return err
	}
	return nil
}

func (t *VoucherInfo) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Voucher (types.SignedVoucher) (struct)

	{

		pb, err := br.PeekByte()
		if err != nil {
			return err
		}
		if pb == cbg.CborNull[0] {
			var nbuf [1]byte
			if _, err := br.Read(nbuf[:]); err != nil {
				return err
			}
		} else {
			t.Voucher = new(types.SignedVoucher)
			if err := t.Voucher.UnmarshalCBOR(br); err != nil {
				return err
			}
		}

	}
	// t.Proof ([]uint8) (slice)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if extra > cbg.ByteArrayMaxLen {
		return fmt.Errorf("t.Proof: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	t.Proof = make([]byte, extra)
	if _, err := io.ReadFull(br, t.Proof); err != nil {
		return err
	}
	return nil
}

func (t *ChannelInfo) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{134}); err != nil {
		return err
	}

	// t.Channel (address.Address) (struct)
	if err := t.Channel.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Control (address.Address) (struct)
	if err := t.Control.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Target (address.Address) (struct)
	if err := t.Target.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Direction (uint64) (uint64)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.Direction))); err != nil {
		return err
	}

	// t.Vouchers ([]*paych.VoucherInfo) (slice)
	if len(t.Vouchers) > cbg.MaxLength {
		return xerrors.Errorf("Slice value in field t.Vouchers was too long")
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajArray, uint64(len(t.Vouchers)))); err != nil {
		return err
	}
	for _, v := range t.Vouchers {
		if err := v.MarshalCBOR(w); err != nil {
			return err
		}
	}

	// t.NextLane (uint64) (uint64)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.NextLane))); err != nil {
		return err
	}
	return nil
}

func (t *ChannelInfo) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 6 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Channel (address.Address) (struct)

	{

		if err := t.Channel.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.Control (address.Address) (struct)

	{

		if err := t.Control.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.Target (address.Address) (struct)

	{

		if err := t.Target.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.Direction (uint64) (uint64)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajUnsignedInt {
		return fmt.Errorf("wrong type for uint64 field")
	}
	t.Direction = uint64(extra)
	// t.Vouchers ([]*paych.VoucherInfo) (slice)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("t.Vouchers: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}
	if extra > 0 {
		t.Vouchers = make([]*VoucherInfo, extra)
	}
	for i := 0; i < int(extra); i++ {

		var v VoucherInfo
		if err := v.UnmarshalCBOR(br); err != nil {
			return err
		}

		t.Vouchers[i] = &v
	}

	// t.NextLane (uint64) (uint64)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajUnsignedInt {
		return fmt.Errorf("wrong type for uint64 field")
	}
	t.NextLane = uint64(extra)
	return nil
}