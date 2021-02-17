package bridgenode

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// DbWorker writes & reads/deletes everything to the db.
// It also generates TTLResultBlocks to send to the flat file worker
func DbWorker(
	dbWorkChan chan ttlRawBlock, ttlResultChan chan ttlResultBlock,
	lvdb *leveldb.DB, wg *sync.WaitGroup) {

	val := make([]byte, 4)

	for {
		dbBlock := <-dbWorkChan
		var batch leveldb.Batch
		// build the batch for writing to levelDB.
		// Just outpoints to index within block
		for i, op := range dbBlock.newTxos {
			binary.BigEndian.PutUint32(val, uint32(i))
			batch.Put(op[:], val)
		}
		// write all the new utxos in the batch to the DB
		err := lvdb.Write(&batch, nil)
		if err != nil {
			fmt.Println(err.Error())
		}
		batch.Reset()

		var trb ttlResultBlock

		trb.Height = dbBlock.blockHeight
		trb.Created = make([]txoStart, len(dbBlock.spentTxos))

		// now read from the DB all the spent txos and find their
		// position within their creation block
		for i, op := range dbBlock.spentTxos {
			batch.Delete(op[:]) // add this outpoint for deletion
			idxBytes, err := lvdb.Get(op[:], nil)
			if err != nil {
				fmt.Printf("can't find %x in db\n", op)
				panic(err)
			}

			// skip txos that live 0 blocks as they'll be deduped out of the
			// proofs anyway
			if dbBlock.spentStartHeights[i] != dbBlock.blockHeight {
				trb.Created[i].indexWithinBlock = binary.BigEndian.Uint32(idxBytes)
				trb.Created[i].createHeight = dbBlock.spentStartHeights[i]
			}
		}
		// send to flat ttl writer
		ttlResultChan <- trb
		err = lvdb.Write(&batch, nil) // actually delete everything
		if err != nil {
			fmt.Println(err.Error())
		}

		wg.Done()
	}
}

// MemTTLdb is the in-memory cached ttldb
type MemTTLdb struct {
	// in-memory cache of the ttls
	cache map[[36]byte]uint32

	// cache that's not yet flushed to disk yet
	// these k-v only on memory until flushed
	//unflushedCache map[[36]byte]uint32

	// dirty keys that are to be deleted
	dirty [][36]byte

	// the database itself on disk
	ttlDB *leveldb.DB

	// makes the Flush() wait for the in-process writes
	flushWait *sync.WaitGroup
}

// NewMemTTLdb returns an empty MemTTLdb
func NewMemTTLdb() *MemTTLdb {
	return &MemTTLdb{
		cache: make(map[[36]byte]uint32),
		//unflushedCache: make(map[[36]byte]uint32),
	}
}

// initMemDB initiatizes the membdb by buffering the ttdlb into
// a map in memory
func (mdb *MemTTLdb) InitMemDB(ttldbPath string, lvdbOpt *opt.Options) error {
	fmt.Println("Loading ttldb from disk...")
	// Open ttldb
	var err error
	mdb.ttlDB, err = leveldb.OpenFile(ttldbPath, lvdbOpt)
	if err != nil {
		return err
	}

	mdb.flushWait = &sync.WaitGroup{}

	var outpoint [36]byte
	iter := mdb.ttlDB.NewIterator(nil, nil)
	for iter.Next() {
		// key should be a serialized outpoint. Outpoints are 36 byte
		// 32 byte hash + 4 byte index within block
		if len(iter.Key()) != 36 {
			return fmt.Errorf("TTLDB corrupted."+
				"Outpoint should be 36 bytes but read %v\n",
				len(iter.Key()))
		}

		copy(outpoint[:], iter.Key()[:])
		mdb.cache[outpoint] = binary.BigEndian.Uint32(iter.Value())

	}

	iter.Release()
	err = iter.Error()
	if err != nil {
		return err
	}

	fmt.Println("Finished loading ttldb from disk")
	return nil
}

// Get grabs a key-value from the map. Returns a bool to indicate whether the
// key was present. It also sets that key as dirty to be removed from the
// ttldb. This is because a single utxo is only going to be accessed once.
// This function is not safe for concurrent access.
func (mdb *MemTTLdb) Get(key [36]byte) (uint32, error) {
	mdb.flushWait.Add(1)
	defer mdb.flushWait.Done()
	value, found := mdb.cache[key]

	// if found, delete from in-memory cache and set dirty so it'll
	// be removed from the disk db
	if found {
		delete(mdb.cache, key)
		mdb.dirty = append(mdb.dirty, key)
	} else {
		// If it's not found, the key doesn't exist
		return value, ErrKeyNotFound
	}

	return value, nil
}

// Put puts a key-value in the cache. Does not alter the on disk ttldb.
// This function is not safe for concurrent access.
func (mdb *MemTTLdb) Put(key [36]byte, value uint32) {
	mdb.flushWait.Add(1)
	defer mdb.flushWait.Done()
	mdb.cache[key] = value
}

// Flush flushes the memory database to disk. This function is not safe for
// concurrent access
func (mdb *MemTTLdb) Flush() error {
	fmt.Println("Flushing ttldb cache")
	mdb.flushWait.Wait()

	// Add since sigint will call the flush again. Make that flush wait
	// That one would save/delete whatever happend between this flush and
	// whenever the signal was given.
	mdb.flushWait.Add(1)
	defer mdb.flushWait.Done()

	val := make([]byte, 4)

	// Save the key-value pairs to the on-disk database
	for cacheKey, cacheValue := range mdb.cache {
		binary.BigEndian.PutUint32(val, uint32(cacheValue))
		err := mdb.ttlDB.Put(cacheKey[:], val, nil)
		if err != nil {
			return err
		}
	}
	mdb.cache = make(map[[36]byte]uint32) // empty map

	// reset dirty
	mdb.dirty = mdb.dirty[:0]

	return nil
}

// Close closes the memory database
func (mdb *MemTTLdb) Close() error {
	// Flush whatever we have left in the memory
	err := mdb.Flush()
	if err != nil {
		return err
	}

	// Add the dirty keys to be deleted
	for _, dirtyKey := range mdb.dirty {
		err = mdb.ttlDB.Delete(dirtyKey[:], nil) // actually delete everything
		if err != nil {
			return err
		}
	}

	return mdb.ttlDB.Close()
}

// MemDbWorker writes & reads/deletes everything to the memory cache and
// flushes the in-memory cache during shutdown.
// It also generates TTLResultBlocks to send to the flat file worker
func MemDbWorker(
	dbWorkChan chan ttlRawBlock, ttlResultChan chan ttlResultBlock,
	memTTLdb *MemTTLdb, wg *sync.WaitGroup) {

	for {
		dbBlock := <-dbWorkChan

		// build the batch for writing to levelDB.
		// Just outpoints to index within block
		for i, op := range dbBlock.newTxos {
			memTTLdb.Put(op, uint32(i))
		}

		var trb ttlResultBlock

		trb.Height = dbBlock.blockHeight
		trb.Created = make([]txoStart, len(dbBlock.spentTxos))

		// now read from the DB all the spent txos and find their
		// position within their creation block
		for i, op := range dbBlock.spentTxos {
			idx, err := memTTLdb.Get(op)
			if err != nil {
				if err == ErrKeyNotFound {
					err := fmt.Errorf("can't find %x in db\n", op)
					panic(err)
				}

				panic(err)
			}

			// skip txos that live 0 blocks as they'll be deduped out of the
			// proofs anyway
			if dbBlock.spentStartHeights[i] != dbBlock.blockHeight {
				trb.Created[i].indexWithinBlock = idx
				trb.Created[i].createHeight = dbBlock.spentStartHeights[i]
			}
		}
		// send to flat ttl writer
		ttlResultChan <- trb

		wg.Done()
	}
}
