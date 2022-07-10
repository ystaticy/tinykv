package mvcc

import (
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
	"math"
	"reflect"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	data, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	return ParseLock(data)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	delete := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: delete})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	//return nil, nil
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(key, txn.StartTS))
	var err error
	var value []byte
	var write *Write
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if !reflect.DeepEqual(userKey, key) {
			return nil, nil
		}
		value, err = item.Value()
		if err != nil {
			return nil, err
		}
		write, err = ParseWrite(value)
		if err != nil {
			return nil, err
		}
		if write.Kind == WriteKindDelete {
			return nil, nil
		}
		if write.Kind == WriteKindPut {
			break
		}
		if write.Kind == WriteKindRollback {
			// rollback, continue find older write value
			continue
		}
	}
	if !iter.Valid() {
		return nil, nil
	}

	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: put})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	delete := storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}
	txn.writes = append(txn.writes, storage.Modify{Data: delete})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	//return nil, 0, nil
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	for iter.Seek(EncodeKey(key, math.MaxUint64)); iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if !reflect.DeepEqual(key, userKey) {
			return nil, 0, nil
		}
		value, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(value)
		if err != nil {
			return nil, 0, err
		}
		if write.StartTS == txn.StartTS {
			return write, decodeTimestamp(item.Key()), nil
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	//return nil, 0, nil
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()

	iter.Seek(EncodeKey(key, math.MaxUint64))
	if !iter.Valid() {
		return nil, 0, nil
	}
	item := iter.Item()
	userKey := DecodeUserKey(item.Key())
	if !reflect.DeepEqual(key, userKey) {
		return nil, 0, nil
	}
	value, err := item.Value()
	if err != nil {
		return nil, 0, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return nil, 0, err
	}
	return write, decodeTimestamp(item.Key()), nil
}

func (txn *MvccTxn) Rollback(key []byte, locked bool) {
	txn.PutWrite(key, txn.StartTS, &Write{
		StartTS: txn.StartTS,
		Kind:    WriteKindRollback,
	})
	txn.DeleteValue(key)
	if locked {
		txn.DeleteLock(key)
	}
}

func (txn *MvccTxn) CurrValue(key []byte) ([]byte, error) {
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, txn.StartTS))
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
