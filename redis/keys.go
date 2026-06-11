package redis

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/redcon"
)

func existsKeys(conn redcon.Conn, db *badger.DB, keys ...[]byte) {
	var count = 0
	err := db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			_, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
			if err == nil {
				count++
			} else if err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteInt(count)
}

func expireKey(conn redcon.Conn, db *badger.DB, key []byte, seconds int) {
	var updated = 0
	err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			updated = 0
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			return err
		}

		e := badger.NewEntry(rawKeyPrefix(key, currentDb(conn)), valCopy).WithTTL(time.Duration(seconds) * time.Second).WithMeta(item.UserMeta())
		err = txn.SetEntry(e)
		if err != nil {
			return err
		}
		updated = 1
		return nil
	})

	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}
	conn.WriteInt(updated)
}

func ttlKey(conn redcon.Conn, db *badger.DB, key []byte) {
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				conn.WriteInt(-2)
				return nil
			}
			conn.WriteError("ERR " + err.Error())
			return nil
		}
		expiresAt := item.ExpiresAt()
		now := uint64(time.Now().Unix())
		if expiresAt == 0 || expiresAt <= now {
			conn.WriteInt(-1)
			return nil
		}
		conn.WriteInt(int(expiresAt - now))
		return nil
	})
}

func pttlKey(conn redcon.Conn, db *badger.DB, key []byte) {
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				conn.WriteInt(-2)
				return nil
			}
			conn.WriteError("ERR " + err.Error())
			return nil
		}
		expiresAt := item.ExpiresAt()
		now := uint64(time.Now().Unix())
		if expiresAt == 0 || expiresAt <= now {
			conn.WriteInt(-1)
			return nil
		}
		conn.WriteInt64(int64(expiresAt-now) * 1000)
		return nil
	})
}

func delKeys(conn redcon.Conn, db *badger.DB, keys ...[]byte) {
	var numDeleted = 0
	err := db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			_, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
			if err == badger.ErrKeyNotFound {
				continue
			}
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			err = txn.Delete(rawKeyPrefix(key, currentDb(conn)))
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			numDeleted++
		}
		return nil
	})
	if err != nil {
		return
	}
	conn.WriteInt(numDeleted)
}

func typeOfKey(conn redcon.Conn, db *badger.DB, key []byte) {
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				conn.WriteString("none")
				return nil
			}
			conn.WriteError("ERR " + err.Error())
			return nil
		}
		meta := item.UserMeta()
		var typeStr string
		switch redisValueType(meta) {
		case RedisString:
			typeStr = "string"
		case RedisList:
			typeStr = "list"
		case RedisSet:
			typeStr = "set"
		case RedisSortedSet:
			typeStr = "zset"
		case RedisHash:
			typeStr = "hash"
		case RedisStream:
			typeStr = "stream"
		case RedisVectorSet:
			typeStr = "vectorset"
		case RedisBloom:
			typeStr = "bloom"
		case RedisJSON:
			typeStr = "json"
		default:
			typeStr = "unknown"
		}
		conn.WriteString(typeStr)
		return nil
	})
}

func renameKey(conn redcon.Conn, db *badger.DB, oldKey, newKey []byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(oldKey, currentDb(conn)))
		if err != nil {
			conn.WriteError("ERR no such key")
			return nil
		}
		var valCopy []byte
		valCopy, err = copyItemValue(item)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		e := badger.NewEntry(rawKeyPrefix(newKey, currentDb(conn)), valCopy).WithMeta(item.UserMeta())
		err = txn.SetEntry(e)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		err = txn.Delete(rawKeyPrefix(oldKey, currentDb(conn)))
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return err
		}

		conn.WriteString("OK")
		return nil
	})
}

func renameNXKey(conn redcon.Conn, db *badger.DB, oldKey, newKey []byte) {
	_ = db.Update(func(txn *badger.Txn) error {
		var renamed = false
		_, err := txn.Get(rawKeyPrefix(newKey, currentDb(conn)))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				item, err := txn.Get(rawKeyPrefix(oldKey, currentDb(conn)))
				if err != nil {
					conn.WriteError("no such key")
					return nil
				}
				var valCopy []byte
				valCopy, err = copyItemValue(item)
				if err != nil {
					log.Debug().Msg("rename get err")
					conn.WriteError("ERR " + err.Error())
					return err
				}
				e := badger.NewEntry(rawKeyPrefix(newKey, currentDb(conn)), valCopy).WithMeta(item.UserMeta())
				err = txn.SetEntry(e)
				if err != nil {
					log.Debug().Msg("rename set err")
					conn.WriteError("ERR " + err.Error())
					return err
				}
				err = txn.Delete(rawKeyPrefix(oldKey, currentDb(conn)))
				if err != nil {
					log.Debug().Msg("rename delete err")
					conn.WriteError("ERR " + err.Error())
					return err
				}
				renamed = true
			} else {
				return err
			}
		}

		if renamed {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return nil
	})
}

