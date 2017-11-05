// Copyright 2017 Tendermint. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// BadgerDB enables us the use of BadgerDB a fast key value store in pure Go.
// Typical usage follows:
//  db, err := db.NewBadgerDB("badger", targetDir)
//  if err != nil {
//	log.Fatalf("badgerDB initialization: %v", err)
//  }
//  defer db.Close()
//
//  For the asynchronous set
//  db.Set([]byte("foo"), []byte("bar"))
//  db.SetSync([]byte("true"), []byte("tendermint"))
//  db.Delete([]byte("true"))
//  db.Deletesync([]byte("true"))
//  iter := db.Iterator()
//  for {
//     key, value := iter.Key(), iter.Value()
//     fmt.Printf("key: %v value: %v\n", key, value)
//  }
//
// For better performance, perform Sets/Writes in batches
//  batch := db.NewBatch()
//  for i := 0; i < 100; i++ {
//	batch.Set(cmn.RandBytes(100), []byte("true!!!"))
//  }
//  batch.Write()

package db

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dgraph-io/badger"

	"github.com/tendermint/tmlibs/common"
)

func init() {
	registerDBCreator(BadgerDBBackendStr, badgerDBCreator, true)
}

type Options badger.Options

func badgerDBCreator(dbName, dir string) (DB, error) {
	return NewBadgerDB(dbName, dir)
}

var (
	_KB = int64(1024)
	_MB = 1024 * _KB
	_GB = 1024 * _MB
)

// NewBadgerDB creates a Badger key-value store backed to the
// directory dir supplied. If dir does not exist, we create it.
func NewBadgerDB(dbName, dir string) (*BadgerDB, error) {
	// BadgerDB doesn't expose a way for us to
	// create  a DB with the user's supplied name.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	opts := new(badger.Options)
	*opts = badger.DefaultOptions
	// Arbitrary size given that at Tendermint
	// we'll need huge KeyValue stores.
	opts.ValueLogFileSize = 1 * _GB
	opts.Dir = dir
	opts.ValueDir = dir

	return NewBadgerDBWithOptions((*Options)(opts))
}

// NewBadgerDBWithOptions creates a BadgerDB key value store
// gives the flexibility of initializing a database with the
// respective options.
func NewBadgerDBWithOptions(opts *Options) (*BadgerDB, error) {
	kv, err := badger.NewKV((*badger.Options)(opts))
	if err != nil {
		return nil, err
	}
	return &BadgerDB{kv: kv}, nil
}

type BadgerDB struct {
	kv *badger.KV
}

var _ DB = (*BadgerDB)(nil)

func (b *BadgerDB) Get(key []byte) []byte {
	valueItem := new(badger.KVItem)
	if err := b.kv.Get(key, valueItem); err != nil {
		// Unfortunate that Get can't return errors
		// TODO: Propose allowing DB's Get to return errors too.
		common.PanicCrisis(err)
	}
	var valueSave []byte
	err := valueItem.Value(func(origValue []byte) error {
		// TODO: Decide if we should just assign valueSave to origValue
		// since here we aren't dealing with iterators directly.
		valueSave = make([]byte, len(origValue))
		copy(valueSave, origValue)
		return nil
	})
	if err != nil {
		// TODO: ditto:: Propose allowing DB's Get to return errors too.
		common.PanicCrisis(err)
	}
	return valueSave
}

// noUserMeta signifies blank metadata for a kv entry
// since badger.Set accepts an optional userMeta field
var noUserMeta byte = 0x00

func panicOnErr(err error) {
	if err != nil {
		common.PanicCrisis(err)
	}
}

func (b *BadgerDB) Set(key, value []byte) {
	b.kv.SetAsync(key, value, noUserMeta, panicOnErr)
}

func (b *BadgerDB) SetSync(key, value []byte) {
	if err := b.kv.Set(key, value, noUserMeta); err != nil {
		common.PanicCrisis(err)
	}
}

func (b *BadgerDB) Delete(key []byte) {
	b.kv.DeleteAsync(key, panicOnErr)
}

func (b *BadgerDB) DeleteSync(key []byte) {
	if err := b.kv.Delete(key); err != nil {
		common.PanicCrisis(err)
	}
}

func (b *BadgerDB) Close() {
	if err := b.kv.Close(); err != nil {
		common.PanicCrisis(err)
	}
}

func (b *BadgerDB) Fprint(w io.Writer) {
	bIter := b.Iterator().(*badgerDBIterator)

	defer bIter.Release()

	var bw *bufio.Writer
	if bbw, ok := w.(*bufio.Writer); ok {
		bw = bbw
	} else {
		bw = bufio.NewWriter(w)
	}
	defer bw.Flush()

	i := uint64(0)
	for bIter.rewind(); bIter.valid(); bIter.Next() {
		k, v := bIter.kv()
		fmt.Fprintf(bw, "[%X]:\t[%X]\n", k, v)
		i += 1
		if i%1024 == 0 {
			bw.Flush()
			i = 0
		}
	}
}

func (b *BadgerDB) Print() {
	bw := bufio.NewWriter(os.Stdout)
	b.Fprint(bw)
}

func (b *BadgerDB) Iterator() Iterator {
	dbIter := b.kv.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,

		// Arbitrary PrefetchSize
		PrefetchSize: 10,
	})
	// Ensure that we are always at the zeroth item
	dbIter.Rewind()
	return &badgerDBIterator{iter: dbIter}
}

func (b *BadgerDB) IteratorPrefix(prefix []byte) Iterator {
	dbIter := b.kv.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,

		// Arbitrary PrefetchSize
		PrefetchSize: 10,
	})
	// Ensure that we are always at the zeroth item
	dbIter.Rewind()
	return &badgerDBIterator{iter: dbIter}
}

func (b *BadgerDB) Stats() map[string]string {
	return nil
}

func (b *BadgerDB) NewBatch() Batch {
	return &badgerDBBatch{db: b}
}

var _ Batch = (*badgerDBBatch)(nil)

type badgerDBBatch struct {
	entriesMu sync.Mutex
	entries   []*badger.Entry

	db *BadgerDB
}

func (bb *badgerDBBatch) Set(key, value []byte) {
	bb.entriesMu.Lock()
	bb.entries = append(bb.entries, &badger.Entry{
		Key:   key,
		Value: value,
	})
	bb.entriesMu.Unlock()
}

// Unfortunately Badger doesn't have a batch delete
// The closest that we can do is do a delete from the DB.
// Hesitant to do DeleteAsync because that changes the
// expected ordering
func (bb *badgerDBBatch) Delete(key []byte) {
	bb.db.Delete(key)
}

// Write commits all batch sets to the DB
func (bb *badgerDBBatch) Write() {
	bb.entriesMu.Lock()
	entries := bb.entries
	bb.entries = nil
	bb.entriesMu.Unlock()

	if len(entries) == 0 {
		return
	}
	if err := bb.db.kv.BatchSet(entries); err != nil {
		common.PanicCrisis(err)
	}

	var buf *bytes.Buffer // It'll be lazily allocated when needed
	for i, entry := range entries {
		if err := entry.Error; err != nil {
			if buf == nil {
				buf = new(bytes.Buffer)
			}
			fmt.Fprintf(buf, "#%d: entry err: %v\n", i, err)
		}
	}
	if buf != nil {
		common.PanicCrisis(buf.String())
	}
}

type badgerDBIterator struct {
	mu sync.RWMutex

	lastErr error

	iter *badger.Iterator
}

var _ Iterator = (*badgerDBIterator)(nil)

func (bi *badgerDBIterator) Next() bool {
	bi.mu.Lock()
	defer bi.mu.Unlock()

	if !bi.iter.Valid() {
		return false
	}
	bi.iter.Next()
	return bi.iter.Valid()
}

func (bi *badgerDBIterator) Key() []byte {
	bi.mu.Lock()
	defer bi.mu.Unlock()

	return bi.iter.Item().Key()
}

func (bi *badgerDBIterator) Value() []byte {
	bi.mu.Lock()
	defer bi.mu.Unlock()

	var valueSave []byte
	err := bi.iter.Item().Value(func(origValue []byte) error {
		valueSave = make([]byte, len(origValue))
		copy(valueSave, origValue)
		return nil
	})
	if err != nil {
		bi.setLastError(err)
	}
	return valueSave
}

func (bi *badgerDBIterator) kv() (key, value []byte) {
	var valueSave []byte
	bItem := bi.iter.Item()
	if bItem == nil {
		return nil, nil
	}
	err := bItem.Value(func(origValue []byte) error {
		valueSave = make([]byte, len(origValue))
		copy(valueSave, origValue)
		return nil
	})
	if err != nil {
		bi.setLastError(err)
	}
	return bItem.Key(), valueSave
}

func (bi *badgerDBIterator) Error() error {
	bi.mu.RLock()
	err := bi.lastErr
	bi.mu.RUnlock()
	return err
}

func (bi *badgerDBIterator) setLastError(err error) {
	bi.mu.Lock()
	bi.lastErr = err
	bi.mu.Unlock()
}

func (bi *badgerDBIterator) Release() {
	bi.iter.Close()
}

func (bi *badgerDBIterator) rewind()     { bi.iter.Rewind() }
func (bi *badgerDBIterator) valid() bool { return bi.iter.Valid() }
