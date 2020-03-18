// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.
package consensus

import (
	"fmt"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcutil"
	"github.com/mit-dci/utreexo/utreexo"
)

// ValidateForstTx validates a tx per consensus rules for the Utreexo Forest tree
// Does not validate if a tx is a standard tx
func ValidateForestTx(tx *btcutil.Tx, txHeight int32, f *utreexo.Forest) error {

	// Perform preliminary sanity checks on the transaction.  This makes
	// use of blockchain which contains the invariant rules for what
	// transactions are allowed into blocks.
	err := blockchain.CheckTransactionSanity(tx)
	if err != nil {
		return fmt.Errorf("Transaction invalid")
	}

	// A standalone transaction must not be a coinbase transaction.
	if blockchain.IsCoinBase(tx) {
		return fmt.Errorf("Transaction invalid")
	}

	_, err = CheckTransactionInputs(tx, txHeight, f)
	if err != nil {
		return fmt.Errorf("Transaction invalid")
	}

	// NOTE: if you modify this code to accept non-standard transactions,
	// you should add code here to check that the transaction does a
	// reasonable number of ECDSA signature verifications.
	// TODO(kcalvinalvin): idk I guess we don't care about this for now
	// No one is gonna attack a Utreexo node but I guess this is something
	// to keep in mind for the future.

	// Don't allow transactions with an excessive number of signature
	// operations which would result in making it impossible to mine.  Since
	// the coinbase address itself can contain signature operations, the
	// maximum allowed signature operations per transaction is less than
	// the maximum allowed signature operations per block.
	// TODO(roasbeef): last bool should be conditional on segwit activation
	_, err = blockchain.GetSigOpCost(tx, false, utxoView, true, true)
	if err != nil {
		return fmt.Errorf("Transaction invalid")
	}
	// Verify crypto signatures for each input and reject the transaction if
	// any don't verify.
	err = blockchain.ValidateTransactionScripts(tx, utxoView,
		txscript.StandardVerifyFlags, mp.cfg.SigCache,
		mp.cfg.HashCache)
	if err != nil {
		return fmt.Errorf("Transaction invalid")
	}

	return nil
}

// CheckTransactionInputs performs a series of checks on the inputs to a
// transaction to ensure they are valid.  An example of some of the checks
// include ensuring the coinbase seasoning requirements are met,
// detecting double spends, validating all values and fees
// are in the legal range and the total output amount doesn't exceed the input
// amount, and verifying the signatures to prove the spender was the owner of
// the bitcoins and therefore allowed to spend them.  As it checks the inputs,
// it also calculates the total fees for the transaction and returns that value.
//
// NOTE: The transaction MUST have already been sanity checked with the
// CheckTransactionSanity function prior to calling this function.
//
// NOTE: In Utreexo version of this, we *don't* check to see if the tx
// is a UTXO. This MUST be done by VerifyProof() in the Utreexo lib
func CheckTransactionInputs(tx *btcutil.Tx, txHeight int32, f *utreexo.Forest) (int64, error) {
	// Coinbase transactions have no inputs.
	if blockchain.IsCoinBase(tx) {
		return 0, nil
	}

	txHash := tx.Hash()
	var totalSatoshiIn int64
	for txInIndex, txIn := range tx.MsgTx().TxIn {

		// Ensure the referenced input transaction is available.
		utxo := txIn.PreviousOutPoint

		// utxoHash is currently the way that leaves are stored in Utreexo
		utxoHash := utreexo.HashFromString(utxo.String())

		// Look for the leaf in the tree
		found := f.FindLeaf(utxoHash)
		if !found {
			return 0, fmt.Errorf("output %v referenced from"+
				"transaction %s:%d not found in the forest tree",
				utxo.String(), tx.Hash(), txInIndex)
		}

		// Ensure the transaction is not spending coins which have not
		// yet reached the required coinbase maturity.
		if utxo.IsCoinBase() {
			originHeight := utxo.BlockHeight()
			blocksSincePrev := txHeight - originHeight
			coinbaseMaturity := int32(100)
			if blocksSincePrev < coinbaseMaturity {
				str := fmt.Sprintf("tried to spend coinbase "+
					"transaction output %v from height %v "+
					"at height %v before required maturity "+
					"of %v blocks", txIn.PreviousOutPoint,
					originHeight, txHeight,
					coinbaseMaturity)
				return 0, ruleError(ErrImmatureSpend, str)
			}
		}

		// Ensure the transaction amounts are in range.  Each of the
		// output values of the input transactions must not be negative
		// or more than the max allowed per transaction.  All amounts in
		// a transaction are in a unit value known as a satoshi.  One
		// bitcoin is a quantity of satoshi as defined by the
		// SatoshiPerBitcoin constant.
		originTxSatoshi := utxo.Amount()
		if originTxSatoshi < 0 {
			str := fmt.Sprintf("transaction output has negative "+
				"value of %v", btcutil.Amount(originTxSatoshi))
			return 0, ruleError(ErrBadTxOutValue, str)
		}
		if originTxSatoshi > btcutil.MaxSatoshi {
			str := fmt.Sprintf("transaction output value of %v is "+
				"higher than max allowed value of %v",
				btcutil.Amount(originTxSatoshi),
				btcutil.MaxSatoshi)
			return 0, ruleError(ErrBadTxOutValue, str)
		}

		// The total of all outputs must not be more than the max
		// allowed per transaction.  Also, we could potentially overflow
		// the accumulator so check for overflow.
		lastSatoshiIn := totalSatoshiIn
		totalSatoshiIn += originTxSatoshi
		if totalSatoshiIn < lastSatoshiIn ||
			totalSatoshiIn > btcutil.MaxSatoshi {
			str := fmt.Sprintf("total value of all transaction "+
				"inputs is %v which is higher than max "+
				"allowed value of %v", totalSatoshiIn,
				btcutil.MaxSatoshi)
			return 0, ruleError(ErrBadTxOutValue, str)
		}
	}

	// Calculate the total output amount for this transaction.  It is safe
	// to ignore overflow and out of range errors here because those error
	// conditions would have already been caught by checkTransactionSanity.
	var totalSatoshiOut int64
	for _, txOut := range tx.MsgTx().TxOut {
		totalSatoshiOut += txOut.Value
	}

	// Ensure the transaction does not spend more than its inputs.
	if totalSatoshiIn < totalSatoshiOut {
		str := fmt.Sprintf("total value of all transaction inputs for "+
			"transaction %v is %v which is less than the amount "+
			"spent of %v", txHash, totalSatoshiIn, totalSatoshiOut)
		return 0, ruleError(ErrSpendTooHigh, str)
	}

	// NOTE: bitcoind checks if the transaction fees are < 0 here, but that
	// is an impossible condition because of the check above that ensures
	// the inputs are >= the outputs.
	txFeeInSatoshi := totalSatoshiIn - totalSatoshiOut
	return txFeeInSatoshi, nil
}
