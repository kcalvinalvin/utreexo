package util

import (
	"fmt"
	"math"
	"runtime"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
)

type TxValidateItems struct {
	txInIndex int
	txIn      *wire.TxIn
	tx        *btcutil.Tx
	sigHashes *txscript.TxSigHashes
	Utxo      *blockchain.UtxoEntry
	flags     txscript.ScriptFlags
	sigCache  *txscript.SigCache
}

// TxValidator provides a type which asynchronously validates transaction
// inputs.  It provides several channels for communication and a processing
// function that is intended to be in run multiple goroutines.
type TxValidator struct {
	items      *TxValidateItems
	quitChan   chan struct{}
	resultChan chan error
	utxoView   *blockchain.UtxoViewpoint
	flags      txscript.ScriptFlags
	sigCache   *txscript.SigCache
	hashCache  *txscript.HashCache
}

// StartValidators starts goroutines that calculate the elliptical cure
// cryptography. Goroutines started are 2 times that of runtime.NumCPU()
func StartValidators(validateChan chan *TxValidateItems,
	errChan chan error, quitChan chan struct{}) {

	// Channels started should be 3 times that of how many cpu
	// threads there are
	maxGoRoutines := runtime.NumCPU() * 3
	if maxGoRoutines <= 0 {
		maxGoRoutines = 1
	}

	for i := 0; i < maxGoRoutines; i++ {
		go NewTxValidator(validateChan, errChan, quitChan)
	}
}

// ruleError creates an RuleError given a set of arguments.
func ruleError(c blockchain.ErrorCode, desc string) blockchain.RuleError {
	return blockchain.RuleError{ErrorCode: c, Description: desc}
}

func NewTxValidator(validateChan chan *TxValidateItems,
	errChan chan error, quitChan chan struct{}) {
out:
	for {
		select {
		case txVI := <-validateChan:
			// Create a new script engine for the script pair.
			sigScript := txVI.txIn.SignatureScript
			witness := txVI.txIn.Witness
			pkScript := txVI.Utxo.PkScript()
			inputAmount := txVI.Utxo.Amount()
			vm, err := txscript.NewEngine(txVI.Utxo.PkScript(), txVI.tx.MsgTx(),
				txVI.txInIndex, txVI.flags, txVI.sigCache, txVI.sigHashes,
				inputAmount)
			if err != nil {
				str := fmt.Sprintf("failed to parse input "+
					"%s:%d which references output %v - "+
					"%v (input witness %x, input script "+
					"bytes %x, prev output script bytes %x)",
					txVI.tx.Hash(), txVI.txInIndex,
					txVI.txIn.PreviousOutPoint, err, witness,
					sigScript, pkScript)
				err := ruleError(blockchain.ErrScriptMalformed, str)
				panic(err)
				//errChan <- err
				//break out
			}

			// Execute the script pair.
			if err := vm.Execute(); err != nil {
				str := fmt.Sprintf("failed to validate input "+
					"%s:%d which references output %v - "+
					"%v (input witness %x, input script "+
					"bytes %x, prev output script bytes %x)",
					txVI.tx.Hash(), txVI.txInIndex,
					txVI.txIn.PreviousOutPoint, err, witness,
					sigScript, pkScript)
				err := ruleError(blockchain.ErrScriptValidation, str)
				//errChan <- err
				panic(err)
				//break out
			}
			errChan <- nil
		case <-quitChan:
			break out
		}
	}
}

// ValidateTransactionScripts validates the scripts for the passed transaction
// using multiple goroutines.
func ValidateTransactionScripts(tx *btcutil.Tx, utxoView *blockchain.UtxoViewpoint,
	flags txscript.ScriptFlags, sigCache *txscript.SigCache,
	hashCache *txscript.HashCache, validateChan chan *TxValidateItems, errChan chan error) error {

	// First determine if segwit is active according to the scriptFlags. If
	// it isn't then we don't need to interact with the HashCache.
	segwitActive := flags&txscript.ScriptVerifyWitness == txscript.ScriptVerifyWitness

	// If the hashcache doesn't yet has the sighash midstate for this
	// transaction, then we'll compute them now so we can re-use them
	// amongst all worker validation goroutines.
	if segwitActive && tx.MsgTx().HasWitness() &&
		!hashCache.ContainsHashes(tx.Hash()) {
		hashCache.AddSigHashes(tx.MsgTx())
	}

	var cachedHashes *txscript.TxSigHashes
	if segwitActive && tx.MsgTx().HasWitness() {
		// The same pointer to the transaction's sighash midstate will
		// be re-used amongst all validation goroutines. By
		// pre-computing the sighash here instead of during validation,
		// we ensure the sighashes
		// are only computed once.
		cachedHashes, _ = hashCache.GetSigHashes(tx.Hash())
	}

	// Collect all of the transaction inputs and required information for
	// validation.
	txIns := tx.MsgTx().TxIn
	//txValItems := make([]*txValidateItems, 0, len(txIns))
	//count := 0
	go func() {
		for txInIdx, txIn := range txIns {
			// Skip coinbases.
			if txIn.PreviousOutPoint.Index == math.MaxUint32 {
				continue
			}

			utxo := utxoView.LookupEntry(txIn.PreviousOutPoint)
			if utxo == nil {
				str := fmt.Sprintf("unable to find unspent "+
					"output %v referenced from "+
					"transaction %s:%d",
					txIn.PreviousOutPoint, tx.Hash(),
					txInIdx)
				err := ruleError(blockchain.ErrMissingTxOut, str)
				panic(err)
			}
			txVI := &TxValidateItems{
				txInIndex: txInIdx,
				txIn:      txIn,
				tx:        tx,
				sigHashes: cachedHashes,
				flags:     flags,
				sigCache:  nil,
				Utxo:      utxo,
			}
			validateChan <- txVI
			//count++
			//txValItems = append(txValItems, txVI)
		}
	}()
	//fmt.Println("sent txs to validtor")
	for i := 0; i < len(txIns); i++ {
		err := <-errChan
		//fmt.Println("errchan received")
		if err != nil {
			return err
		}
		//fmt.Println("i: ", i)
	}
	//fmt.Println("all verification finished")

	// Validate all of the inputs.
	//validator := newTxValidator(*txValItems, utxoView, flags, sigCache, hashCache)
	//return validator.Validate(txValItems)
	return nil
}
