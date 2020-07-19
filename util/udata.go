package util

import (
	"fmt"
	"sync"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

// ProofsProveBlock checks the consistency of a UBlock.  Does the proof prove
// all the inputs in the block?
func (ub *UBlock) ProofsProveBlock(inputSkipList []uint32) bool {
	// get the outpoints that need proof
	proveOPs := blockToDelOPs(&ub.Block, inputSkipList)

	// ensure that all outpoints are provided in the extradata
	if len(proveOPs) != len(ub.ExtraData.UtxoData) {
		fmt.Printf("%d outpoints need proofs but only %d proven\n",
			len(proveOPs), len(ub.ExtraData.UtxoData))
		return false
	}
	for i, _ := range ub.ExtraData.UtxoData {
		if proveOPs[i] != ub.ExtraData.UtxoData[i].Outpoint {
			fmt.Printf("block/utxoData mismatch %s v %s\n",
				proveOPs[i].String(), ub.ExtraData.UtxoData[i].Outpoint.String())
			return false
		}
	}
	return true
}

// Verify checks the consistency of uData: that the utxos are proven in the
// batchproof
func (ud *UData) Verify(nl uint64, h uint8) bool {

	// this is really ugly and basically copies the whole thing to avoid
	// destroying it while verifying...

	presort := make([]uint64, len(ud.AccProof.Targets))
	copy(presort, ud.AccProof.Targets)

	ud.AccProof.SortTargets()
	mp, err := ud.AccProof.Reconstruct(nl, h)
	if err != nil {
		fmt.Printf(" Reconstruct failed %s\n", err.Error())
		return false
	}

	// make sure the udata is consistent, with the same number of leafDatas
	// as targets in the accumulator batch proof
	if len(ud.AccProof.Targets) != len(ud.UtxoData) {
		fmt.Printf("Verify failed: %d targets but %d leafdatas\n",
			len(ud.AccProof.Targets), len(ud.UtxoData))
	}

	for i, pos := range presort {
		hashInProof, exists := mp[pos]
		if !exists {
			fmt.Printf("Verify failed: Target %d not in map\n", pos)
			return false
		}
		// check if leafdata hashes to the hash in the proof at the target
		if ud.UtxoData[i].LeafHash() != hashInProof {
			fmt.Printf("Verify failed: txo %s position %d leafdata %x proof %x\n",
				ud.UtxoData[i].Outpoint.String(), pos,
				ud.UtxoData[i].LeafHash(), hashInProof)
			sib, exists := mp[pos^1]
			if exists {
				fmt.Printf("sib exists, %x\n", sib)
			}
			return false
		}
	}
	// return to presorted target list
	ud.AccProof.Targets = presort
	return true
}

// ToUtxoView converts a UData into a btcd blockchain.UtxoViewpoint
// all the data is there, just a bit different format.
// Note that this needs blockchain.NewUtxoEntry() in btcd
func (ud *UData) ToUtxoView() *blockchain.UtxoViewpoint {
	v := blockchain.NewUtxoViewpoint()
	m := v.Entries()
	// loop through leafDatas and convert them into UtxoEntries (pretty much the
	// same thing
	for _, ld := range ud.UtxoData {
		txo := wire.NewTxOut(ld.Amt, ld.PkScript)
		utxo := blockchain.NewUtxoEntry(txo, ld.Height, ld.Coinbase)
		m[ld.Outpoint] = utxo
	}

	return v
}

/*
blockchain.NewUtxoEntry() looks like this:
>>>>>>> master
// NewUtxoEntry returns a new UtxoEntry built from the arguments.
func NewUtxoEntry(
	txOut *wire.TxOut, blockHeight int32, isCoinbase bool) *UtxoEntry {
	var cbFlag txoFlags
	if isCoinbase {
		cbFlag |= tfCoinBase
	}

	return &UtxoEntry{
		amount:      txOut.Value,
		pkScript:    txOut.PkScript,
		blockHeight: blockHeight,
		packedFlags: cbFlag,
	}
}
*/

// CheckBlock does all internal block checks for a UBlock
// right now checks the signatures
func (ub *UBlock) CheckBlock(outskip []uint32) bool {
	//valChan chan *TxValidateItems, errChan chan error) bool {

	// NOTE Whatever happens here is done a million times
	// be efficient here
	view := ub.ExtraData.ToUtxoView()
	viewMap := view.Entries()
	var txonum uint32

	sigCache := txscript.NewSigCache(0)
	hashCache := txscript.NewHashCache(0)

	//txCount := int32(len(ub.Block.Transactions))

	for txnum, tx := range ub.Block.Transactions {
		outputsInTx := uint32(len(tx.TxOut))
		if txnum == 0 {
			txonum += outputsInTx
			continue // skip checks for coinbase TX for now.  Or maybe it'll work?
		}
		/* add txos to the UtxoView if they're also consumed in this block
		(will be on the output skiplist from DedupeBlock)
		The order we do this in should ensure that a incorrectly ordered
		sequence (tx 5 spending tx 8) will fail here.
		*/
		// Two conditions checked:
		// 1. outskip exists
		// 2. first outskip position is less than the current
		for len(outskip) > 0 && outskip[0] < txonum+outputsInTx {
			//
			idx := outskip[0] - txonum

			skipTxo := wire.NewTxOut(tx.TxOut[idx].Value, tx.TxOut[idx].PkScript)
			skippedEntry := blockchain.NewUtxoEntry(skipTxo, ub.Height, false)
			skippedOutpoint := wire.OutPoint{Hash: tx.TxHash(), Index: idx}

			viewMap[skippedOutpoint] = skippedEntry
			outskip = outskip[1:] // pop off from output skiplist
		}
		txonum += outputsInTx
	}

	var wg sync.WaitGroup
	for txnum, tx := range ub.Block.Transactions {
		if txnum == 0 {
			//txonum += outputsInTx
			continue // skip checks for coinbase TX for now.  Or maybe it'll work?
		}
		utilTx := btcutil.NewTx(tx)
		wg.Add(1)
		go func(w *sync.WaitGroup, tx *btcutil.Tx) {
			//outputsInTx := uint32(len(tx.TxOut))

			///*
			//txChan <- *utilTx
			// hardcoded testnet3 for now
			_, err := blockchain.CheckTransactionInputs(
				utilTx, ub.Height, view, &chaincfg.TestNet3Params)
			if err != nil {
				fmt.Printf("Tx %s fails CheckTransactionInputs: %s\n",
					utilTx.Hash().String(), err.Error())
				panic(err)
				//return false
			}

			//fmt.Println("ValidateTransactionScripts start")
			// no scriptflags for now
			err = blockchain.ValidateTransactionScripts(
				utilTx, view, 0, sigCache, hashCache) //, valChan, errChan)
			if err != nil {
				fmt.Printf("Tx %s fails ValidateTransactionScripts: %s\n",
					utilTx.Hash().String(), err.Error())
				panic(err)
				//return false
			}
			//fmt.Println("ValidateTransactionScripts end")
			//*/

			/* add txos to the UtxoView if they're also consumed in this block
			(will be on the output skiplist from DedupeBlock)
			The order we do this in should ensure that a incorrectly ordered
			sequence (tx 5 spending tx 8) will fail here.
			*/
			// Two conditions checked:
			// 1. outskip exists
			// 2. first outskip position is less than the current
			/*
				for len(outskip) > 0 && outskip[0] < txonum+outputsInTx {
					//
					idx := outskip[0] - txonum

					skipTxo := wire.NewTxOut(tx.TxOut[idx].Value, tx.TxOut[idx].PkScript)
					skippedEntry := blockchain.NewUtxoEntry(skipTxo, ub.Height, false)
					skippedOutpoint := wire.OutPoint{Hash: tx.TxHash(), Index: idx}

					viewMap[skippedOutpoint] = skippedEntry
					outskip = outskip[1:] // pop off from output skiplist
				}
				txonum += outputsInTx
			*/
			w.Done()
		}(&wg, utilTx)
	}
	wg.Wait()
	/*
		for _, tx := range ub.Block.Transactions {
			btctx := btcutil.NewTx(tx)

			// hardcoded testnet3 for now
			_, err := blockchain.CheckTransactionInputs(
				btctx, ub.Height, view, &chaincfg.TestNet3Params)
			if err != nil {
				e := fmt.Errorf("Tx %s fails CheckTransactionInputs: %s\n",
					btctx.Hash().String(), err.Error())
				fmt.Println(e)
				return false
			}
			///*
				if txnum == 0 {
					txonum += outputsInTx
					continue // skip checks for coinbase TX for now.  Or maybe it'll work?
				}

			txChan <- *btctx
		}
	*/

	return true
}

// txValidate validates the txs that are sent through the channel
func txValidate(txCount, height int32, txChan chan btcutil.Tx,
	sigCache *txscript.SigCache, hashCache *txscript.HashCache,
	badChan chan error, goodChan chan bool, view *blockchain.UtxoViewpoint) {

	// spawn goroutines for all the txs in this block
	for i := int32(0); i < txCount; i++ {
		tx := <-txChan

		// hardcoded testnet3 for now
		_, err := blockchain.CheckTransactionInputs(
			&tx, height, view, &chaincfg.TestNet3Params)
		if err != nil {
			e := fmt.Errorf("Tx %s fails CheckTransactionInputs: %s\n",
				tx.Hash().String(), err.Error())
			badChan <- e
		}

		// no scriptflags for now
		err = blockchain.ValidateTransactionScripts(
			&tx, view, 0, sigCache, hashCache)
		if err != nil {
			e := fmt.Errorf("Tx %s fails ValidateTransactionScripts: %s\n",
				tx.Hash().String(), err.Error())
			badChan <- e
		}
	}
	//fmt.Println("END")
	goodChan <- true
}

/*
// txValidate validates the txs that are sent through the channel
func txValidate(txCount, height int32, txChan chan btcutil.Tx,
	sigCache *txscript.SigCache, hashCache *txscript.HashCache,
	badChan chan error, goodChan chan bool, view *blockchain.UtxoViewpoint) {

	var wg sync.WaitGroup
	// spawn goroutines for all the txs in this block
	for i := int32(0); i < txCount; i++ {
		wg.Add(1)
		go func(g *sync.WaitGroup) {
			tx := <-txChan

			// hardcoded testnet3 for now
			_, err := blockchain.CheckTransactionInputs(
				&tx, height, view, &chaincfg.TestNet3Params)
			if err != nil {
				e := fmt.Errorf("Tx %s fails CheckTransactionInputs: %s\n",
					tx.Hash().String(), err.Error())
				badChan <- e
			}

			// no scriptflags for now
			err = blockchain.ValidateTransactionScripts(
				&tx, view, 0, sigCache, hashCache)
			if err != nil {
				e := fmt.Errorf("Tx %s fails ValidateTransactionScripts: %s\n",
					tx.Hash().String(), err.Error())
				badChan <- e
			}
			wg.Done()
		}(&wg)
	}
	wg.Wait()
	//fmt.Println("END")
	goodChan <- true
}
*/
