package util

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/mit-dci/utreexo/accumulator"
)

type Hash [32]byte

//simutil.Hash is just [32]byte
var MainnetGenHash = Hash{
	0x6f, 0xe2, 0x8c, 0x0a, 0xb6, 0xf1, 0xb3, 0x72,
	0xc1, 0xa6, 0xa2, 0x46, 0xae, 0x63, 0xf7, 0x4f,
	0x93, 0x1e, 0x83, 0x65, 0xe1, 0x5a, 0x08, 0x9c,
	0x68, 0xd6, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00,
}

var TestNet3GenHash = Hash{
	0x43, 0x49, 0x7f, 0xd7, 0xf8, 0x26, 0x95, 0x71,
	0x08, 0xf4, 0xa3, 0x0f, 0xd9, 0xce, 0xc3, 0xae,
	0xba, 0x79, 0x97, 0x20, 0x84, 0xe9, 0x0e, 0xad,
	0x01, 0xea, 0x33, 0x09, 0x00, 0x00, 0x00, 0x00,
}

// HashFromString hahes the given string with sha256
func HashFromString(s string) Hash {
	return sha256.Sum256([]byte(s))
}

type ProofAndHeight struct {
	Proof  []byte
	Height int32
}

// Tx defines a bitcoin transaction that provides easier and more efficient
// manipulation of raw transactions.  It also memoizes the hash for the
// transaction on its first access so subsequent accesses don't have to repeat
// the relatively expensive hashing operations.
type zProofTx struct {
	msgTx         *wire.MsgTx // Underlying MsgTx
	txHash        *Hash       // Cached transaction hash
	txHashWitness *Hash       // Cached transaction witness hash
	txHasWitness  *bool       // If the transaction has witness data
	txIndex       int         // Position within a block or TxIndexUnknown
}

// RawHeaderData is used for blk*.dat offsetfile building
// Used for ordering blocks as they aren't stored in order in the blk files.
// Includes 32 bytes of sha256 hash along with other variables
// needed for offsetfile building.
type RawHeaderData struct {
	// CurrentHeaderHash is the double hashed 32 byte header
	CurrentHeaderHash [32]byte
	// Prevhash is the 32 byte previous header included in the 80byte header.
	// Needed for ordering
	Prevhash [32]byte
	// FileNum is the blk*.dat file number
	FileNum [4]byte
	// Offset is where it is in the .dat file.
	Offset [4]byte
}

// UBlock is a regular block, with Udata stuck on
type UBlock struct {
	Block     wire.MsgBlock
	ExtraData UData
	Height    int32
}

type UData struct {
	AccProof       accumulator.BatchProof
	UtxoData       []LeafData
	RememberLeaves []bool
}

// LeafData is all the data that goes into a leaf in the utreexo accumulator
type LeafData struct {
	BlockHash [32]byte
	Outpoint  wire.OutPoint
	Height    int32
	Coinbase  bool
	Amt       int64
	PkScript  []byte
}

// turn a LeafData into bytes
func (l *LeafData) ToString() (s string) {
	s = l.Outpoint.String()
	// s += fmt.Sprintf(" bh %x ", l.BlockHash)
	s += fmt.Sprintf("h %d ", l.Height)
	s += fmt.Sprintf("cb %v ", l.Coinbase)
	s += fmt.Sprintf("amt %d ", l.Amt)
	s += fmt.Sprintf("pks %x ", l.PkScript)
	s += fmt.Sprintf("%x", l.LeafHash())
	return
}

func LeafDataFromBytes(b []byte) (LeafData, error) {
	var l LeafData
	if len(b) < 80 {
		return l, fmt.Errorf("%x for leafdata, need 80 bytes", b)
	}
	copy(l.BlockHash[:], b[0:32])
	copy(l.Outpoint.Hash[:], b[32:64])
	l.Outpoint.Index = binary.BigEndian.Uint32(b[64:68])
	l.Height = int32(binary.BigEndian.Uint32(b[68:72]))
	if l.Height&1 == 1 {
		l.Coinbase = true
	}
	l.Height >>= 1
	l.Amt = int64(binary.BigEndian.Uint64(b[72:80]))
	l.PkScript = b[80:]

	return l, nil
}

// ToBytes turns LeafData into bytes
func (l *LeafData) ToBytes() []byte {
	hcb := l.Height << 1
	if l.Coinbase {
		hcb |= 1
	}

	var buf bytes.Buffer

	buf.Write(l.BlockHash[:])
	buf.Write(l.Outpoint.Hash[:])
	binary.Write(&buf, binary.BigEndian, l.Outpoint.Index)
	binary.Write(&buf, binary.BigEndian, hcb)
	binary.Write(&buf, binary.BigEndian, l.Amt)
	buf.Write(l.PkScript)

	return buf.Bytes()
}

// compact serialization for LeafData:
// don't need to send BlockHash; figure it out from height
// don't need to send outpoint, it's already in the msgBlock
// can use tags for PkScript
// so it's just height, coinbaseness, amt, pkscript tag

// turn a LeafData into bytes (compact, for sending in blockProof) -
// don't hash this, it doesn't commit to everything
func (l *LeafData) ToCompactBytes() []byte {
	l.Height <<= 1
	if l.Coinbase {
		l.Height |= 1
	}

	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, l.Height)
	binary.Write(&buf, binary.BigEndian, l.Amt)
	buf.Write(l.PkScript)

	return buf.Bytes()
}

// LeafDataFromCompactBytes doesn't fill in blockhash, outpoint, and in
// most cases PkScript, so something else has to fill those in later.
func LeafDataFromCompactBytes(b []byte) (LeafData, error) {
	var l LeafData
	if len(b) < 13 {
		return l, fmt.Errorf("Not long enough for leafdata, need 80 bytes")
	}
	l.Height = int32(binary.BigEndian.Uint32(b[0:4]))
	if l.Height&1 == 1 {
		l.Coinbase = true
	}
	l.Height >>= 1
	l.Amt = int64(binary.BigEndian.Uint64(b[4:12]))
	l.PkScript = b[12:]

	return l, nil
}

// turn a LeafData into a LeafHash
func (l *LeafData) LeafHash() [32]byte {
	return sha256.Sum256(l.ToBytes())
}

func LeafDataFromTxo(txo wire.TxOut) (LeafData, error) {
	var l LeafData

	return l, nil
}

// BlockProof serialization:
// There's a bunch of variable length things (the batchProof.hashes, and the
// LeafDatas) so we prefix lengths for those.  Ordering is:
// batch proof length (4 bytes)
// batch proof
// Bunch of LeafDatas, prefixed with 2-byte lengths
func (ud *UData) ToBytes() (b []byte) {
	batchBytes := ud.AccProof.ToBytes()
	b = make([]byte, 4 /*len(batchBytes)*/ +len(batchBytes))
	// first stick the batch proof on the beginning
	binary.BigEndian.PutUint32(b, uint32(len(batchBytes)))
	copy(b[4:], batchBytes)

	// next, all the leafDatas
	for _, ld := range ud.UtxoData {
		ldb := ld.ToBytes()
		b = append(b, PrefixLen16(ldb)...)
	}

	return
}

// network serialization for UBlocks (regular block with udata)
// First 4 bytes is (big endian) lenght of the udata.
// Then there's just a wire.MsgBlock with the regular serialization.
// So basically udata, then a block, that's it.

// Looks like "height" doesn't get sent over this way, but maybe that's OK.

func (ub *UBlock) Deserialize(r io.Reader) (err error) {
	err = ub.Block.Deserialize(r)
	if err != nil {
		return
	}

	var uDataLen, bytesRead uint32
	var n int

	err = binary.Read(r, binary.BigEndian, &uDataLen)
	if err != nil {
		return
	}

	udataBytes := make([]byte, uDataLen)

	for bytesRead < uDataLen {
		n, err = r.Read(udataBytes[bytesRead:])
		if err != nil {
			return
		}
		bytesRead += uint32(n)
	}

	ub.ExtraData, err = UDataFromBytes(udataBytes)
	return
}

func (ub *UBlock) Serialize(w io.Writer) (err error) {
	err = ub.Block.Serialize(w)
	if err != nil {
		return
	}

	udataBytes := ub.ExtraData.ToBytes()
	err = binary.Write(w, binary.BigEndian, uint32(len(udataBytes)))
	if err != nil {
		return
	}

	_, err = w.Write(udataBytes)

	return
}

func UDataFromBytes(b []byte) (ud UData, err error) {

	if len(b) < 4 {
		err = fmt.Errorf("block proof too short %d bytes", len(b))
		return
	}
	batchLen := binary.BigEndian.Uint32(b[:4])
	if batchLen > uint32(len(b)-4) {
		err = fmt.Errorf("block proof says %d bytes but %d remain",
			batchLen, len(b)-4)
		return
	}
	b = b[4:]
	batchProofBytes := b[:batchLen]
	leafDataBytes := b[batchLen:]
	ud.AccProof, err = accumulator.FromBytesBatchProof(batchProofBytes)
	if err != nil {
		return
	}
	// got the batch proof part; now populate the leaf data part
	// first there are as many leafDatas as there are proof targets
	ud.UtxoData = make([]LeafData, len(ud.AccProof.Targets))
	var ldb []byte
	// loop until we've filled in every leafData (or something breaks first)
	for i, _ := range ud.UtxoData {
		// fmt.Printf("leaf %d dataBytes %x ", i, leafDataBytes)
		ldb, leafDataBytes, err = PopPrefixLen16(leafDataBytes)
		if err != nil {
			return
		}
		ud.UtxoData[i], err = LeafDataFromBytes(ldb)
		if err != nil {
			return
		}
	}

	return ud, nil
}

// TODO use compact leafDatas in the block proofs -- probably 50%+ space savings
// Also should be default / the only serialization.  Whenever you've got the
// block proof, you've also got the block, so should always be OK to omit the
// data that's already in the block.

func UDataFromCompactBytes(b []byte) (UData, error) {
	var ud UData

	return ud, nil
}

func (ud *UData) ToCompactBytes() (b []byte) {
	return
}
