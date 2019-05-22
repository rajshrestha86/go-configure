package db

import (
	"github.com/dgraph-io/badger"
)

type DB struct {
	badger *badger.DB
}

func NewBadgerDB(path string) DB {
	if path == "" {
		path = "/tmp/badger"
	}
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return DB{db}
}

func (db *DB) Close() error {
	return db.badger.Close()
}

func (db *DB) Read(key string) ([]byte, error) {
	var value []byte
	err := db.badger.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			txn.Discard()
			return err
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			txn.Discard()
			return err
		}
		return err
	})
	return value, err
}

func (db *DB) Write(key string, value []byte) error {
	err := db.badger.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		return err
	})
	return err
}

func (db *DB) ReadLock() {
	db.badger.RLock()
}

func (db *DB) WriteLock() {
	db.badger.Lock()
}

func (db *DB) WriteUnlock() {
	db.badger.Unlock()
}

func (db *DB) ReadUnlock() {
	db.badger.RUnlock()
}

func (db *DB) StartTransaction(transaction func(txn *badger.Txn) error) error {
	err := db.badger.Update(transaction)
	return err
}

func (db *DB) Delete(key string) error {
	err := db.badger.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		return err
	})
	return err
}
