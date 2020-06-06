package bridgenode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/btcsuite/btcd/wire"
	"github.com/mit-dci/utreexo/util"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// Wire Protocol version
// Some btcd lib requires this as an argument
// Technically the version is 70013 but many btcd
// code is passing 0 on some Deserialization methods
const pver uint32 = 0

// MaxMessagePayload is the maximum bytes a message can be regardless of other
// individual limits imposed by messages themselves.
const MaxMessagePayload = (1024 * 1024 * 32) // 32MB

// BlockReader is a wrapper around GetRawBlockFromFile so that the process
// can be made into a goroutine. As long as it's running, it keeps sending
// the entire blocktxs and height to bchan with TxToWrite type.
// It also puts in the proofs.  This will run on the archive server, and the
// data will be sent over the network to the CSN.
func BlockAndRevReader(
	blockChan chan BlockAndRev,
	maxHeight, curHeight int32) {
	for curHeight != maxHeight {
		blk, rb, err := GetRawBlockFromFile(curHeight, util.OffsetFilePath)
		if err != nil {
			panic(err)
		}

		bnr := BlockAndRev{Height: curHeight, Blk: blk, Rev: rb}

		blockChan <- bnr
		curHeight++
	}
}

// GetRawBlocksFromFile reads the blocks from the given .dat file and
// returns those blocks.
// Skips the genesis block. If you search for block 0, it will give you
// block 1.
func GetRawBlockFromFile(tipnum int32, offsetFileName string) (
	block wire.MsgBlock, rBlock RevBlock, err error) {
	if tipnum == 0 {
		err = fmt.Errorf("Block 0 is not in blk files or utxo set")
		return
	}
	tipnum--

	var datFile, offset, revOffset uint32

	offsetFile, err := os.Open(offsetFileName)
	if err != nil {
		return
	}
	defer offsetFile.Close() // file always closes

	// offset file consists of 12 bytes per block
	// tipnum * 12 gives us the correct position for that block
	_, err = offsetFile.Seek(int64(12*tipnum), 0)
	if err != nil {
		return
	}

	// Read file and offset for the block
	err = binary.Read(offsetFile, binary.BigEndian, &datFile)
	if err != nil {
		return
	}
	err = binary.Read(offsetFile, binary.BigEndian, &offset)
	if err != nil {
		return
	}
	err = binary.Read(offsetFile, binary.BigEndian, &revOffset)
	if err != nil {
		return
	}

	blockFileName := fmt.Sprintf("blk%05d.dat", datFile)
	revFileName := fmt.Sprintf("rev%05d.dat", datFile)

	blockFile, err := os.Open(blockFileName)
	if err != nil {
		return
	}
	defer blockFile.Close() // file always closes

	// +8 skips the 8 bytes of magicbytes and load size
	_, err = blockFile.Seek(int64(offset)+8, 0)
	if err != nil {
		return
	}

	// TODO this is probably expensive. fix
	err = block.Deserialize(blockFile)
	if err != nil {
		return
	}

	revFile, err := os.Open(revFileName)
	if err != nil {
		return
	}
	defer revFile.Close() // file always closes

	revFile.Seek(int64(revOffset), 0)
	err = rBlock.Deserialize(revFile)
	if err != nil {
		return
	}

	return
}

// BlockAndRev is a regular block and a rev block stuck together
type BlockAndRev struct {
	Height int32
	Rev    RevBlock
	Blk    wire.MsgBlock
}

/*
 * All types here follow the Bitcoin Core implementation of the
 * Undo blocks. One difference is that all the vectors are replaced
 * with slices. This is just a language difference.
 *
 * Compression/Decompression and VarInt functions are all taken/using
 * btcsuite packages.
 */

// RevBlock is the structure of how a block is stored in the
// rev*.dat file the Bitcoin Core generates
type RevBlock struct {
	Magic [4]byte   // Network magic bytes
	Size  [4]byte   // size of the BlockUndo record
	Txs   []*TxUndo // acutal undo record
	Hash  [32]byte  // 32 byte double sha256 hash of the block
}

// TxUndo contains the TxInUndo records.
// see github.com/bitcoin/bitcoin/src/undo.h
type TxUndo struct {
	TxIn []*TxInUndo
}

// TxInUndo is the stucture of the undo transaction
// Eveything is uncompressed here
// see github.com/bitcoin/bitcoin/src/undo.h
type TxInUndo struct {
	Height int32

	// Version of the original tx that created this tx
	Varint uint64

	// scriptPubKey of the spent UTXO
	PKScript []byte

	// Value of the spent UTXO
	Amount int64

	// Whether if the TxInUndo is a coinbase or not
	// Not actually included in the rev*.dat files
	Coinbase bool
}

// Deserialize takes a reader and reads a single block
// Only initializes the Block var in RevBlock
func (rb *RevBlock) Deserialize(r io.Reader) error {
	txCount, err := wire.ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	for i := uint64(0); i < txCount; i++ {
		var tx TxUndo
		err := tx.Deserialize(r)
		if err != nil {
			return err
		}
		rb.Txs = append(rb.Txs, &tx)
	}
	return nil
}

// Deserialize takes a reader and reads all the TxUndo data
func (tx *TxUndo) Deserialize(r io.Reader) error {

	// Read the Variable Integer
	count, err := wire.ReadVarInt(r, pver)
	if err != nil {
		return err
	}

	for i := uint64(0); i < count; i++ {
		var in TxInUndo
		err := readTxInUndo(r, &in)
		if err != nil {
			return err
		}
		tx.TxIn = append(tx.TxIn, &in)
	}
	return nil
}

// readTxInUndo reads all the TxInUndo from the reader to the passed in txInUndo
// variable
func readTxInUndo(r io.Reader, ti *TxInUndo) error {
	// nCode is how height is saved to the rev files
	nCode, _ := deserializeVLQ(r)
	ti.Height = int32(nCode / 2) // Height is saved as actual height * 2
	ti.Coinbase = nCode&1 == 1   // Coinbase is odd. Saved as height * 2 + 1

	// Only TxInUndos that have the height greater than 0
	// Has varint that isn't 0. see
	// github.com/bitcoin/bitcoin/blob/9cc7eba1b5651195c05473004c00021fe3856f30/src/undo.h#L42
	if ti.Height > 0 {
		varint, err := wire.ReadVarInt(r, pver)
		if err != nil {
			return err
		}
		if varint != 0 {
			return fmt.Errorf("varint is %d", varint)
		}
		ti.Varint = varint
	}

	amount, _ := deserializeVLQ(r)
	ti.Amount = decompressTxOutAmount(amount)

	ti.PKScript = decompressScript(r)
	if ti.PKScript == nil {
		panic("nil pkscript")
	}

	return nil
}

// OpenIndexFile returns the db with only read only option enabled
func OpenIndexFile() *leveldb.DB {
	s := "/home/calvin/.bitcoin/testnet3/blocks/index"
	lvdb, err := leveldb.OpenFile(s, nil)
	if err != nil {
		panic(err)
	}

	ro := new(opt.ReadOptions)
	ro.DontFillCache = true
	return lvdb
}

// CBlockFileIndex is a reimplementation of the Bitcoin Core
// class CBlockFileIndex
type CBlockFileIndex struct {
	Version int32  // nVersion info of the block
	Height  int32  // Height of the block
	Status  int32  // validation status of the block in Bitcoin Core
	TxCount int32  // tx count in the block
	File    int32  // file num
	DataPos uint32 // blk*.dat file offset
	UndoPos uint32 // rev*.dat file offset
}

// GetBlockIndexInfo returns a
func GetBlockIndexInfo(h [32]byte, lvdb *leveldb.DB) CBlockFileIndex {
	// 0x62 is hex representation of ascii 'b' (98), which is used
	// appended to block keys in leveldb
	lookup := append([]byte{0x62}, h[:]...)

	fvalue, err := lvdb.Get(lookup, nil)
	if err != nil {
		panic(err)
	}
	first := bytes.NewReader(fvalue)

	cbIdx := ReadCBlockFileIndex(first)
	return cbIdx
}

func ReadCBlockFileIndex(r io.ReadSeeker) (cbIdx CBlockFileIndex) {
	// not sure if nVersion is correct...?
	nVersion, _ := deserializeVLQ(r)
	cbIdx.Version = int32(nVersion)

	nHeight, _ := deserializeVLQ(r)
	cbIdx.Height = int32(nHeight)

	// nStatus is incorrect but everything else correct. Probably reading this wrong
	nStatus, _ := deserializeVLQ(r)
	cbIdx.Status = int32(nStatus)

	nTx, _ := deserializeVLQ(r)
	cbIdx.TxCount = int32(nTx)

	nFile, _ := deserializeVLQ(r)
	cbIdx.File = int32(nFile)

	nDataPos, _ := deserializeVLQ(r)
	cbIdx.DataPos = uint32(nDataPos)

	nUndoPos, _ := deserializeVLQ(r)
	cbIdx.UndoPos = uint32(nUndoPos)

	/*
		// I don't know why this is needed but it's needed
		// No documentation to be found on the Bitcoin Core side
		r.Seek(3, 1)

		GetBlockHeader(r)
	*/

	return cbIdx
}

/*
func GetBlockHeader(r io.Reader) {
	nVersion, _ := deserializeVLQ(r)
	fmt.Println("nVersion", nVersion)

	var hashPrev, hashMerkleRoot Hash
	r.Read(hashPrev[:])
	fmt.Printf("hashPrev: %x\n", reverse(hashPrev[:]))

	r.Read(hashMerkleRoot[:])
	fmt.Printf("hashMerkleRoot: %x\n", reverse(hashMerkleRoot[:]))

	var buf [4]byte
	r.Read(buf[:])

	t := binary.LittleEndian.Uint32(buf[:])
	fmt.Println("time: ", t)
	fmt.Println("time: ", time.Unix(int64(t), 0))

	r.Read(buf[:])
	fmt.Printf("nBits in bytes: 0x%x\n", buf)

	r.Read(buf[:])
	fmt.Printf("nonce in bytes: 0x%x\n", buf)
}
*/
