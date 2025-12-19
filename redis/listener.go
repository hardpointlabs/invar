package redis

import (
	"log"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

var addr = ":6379"

const prefixSeparator = ":"

func currentDbPrefix(conn redcon.Conn) []byte {
	return []byte(strconv.Itoa(currentDb(conn)) + prefixSeparator)
}

func rawKeyPrefix(keyName []byte, dbSlot int) []byte {
	return append([]byte(strconv.Itoa(dbSlot)+prefixSeparator), keyName...)
}

type ClientInfo struct {
	Id uint64
}

func setContext(conn redcon.Conn) {
	var ctx = conn.Context()
	if ctx == nil {
		clientInfo := &ClientInfo{Id: rand.Uint64N(1 << 63)}
		conn.SetContext(clientInfo)
	}
}

var syncMap sync.Map

func connectionId(conn redcon.Conn) uint64 {
	return (conn.Context()).(*ClientInfo).Id
}

func currentDb(conn redcon.Conn) int {
	value, _ := syncMap.LoadOrStore(connectionId(conn), 0)
	return value.(int)
}

func setCurrentDb(conn redcon.Conn, dbIndex int) {
	syncMap.Store(connectionId(conn), dbIndex)
}

func getKeys(conn redcon.Conn, db *badger.DB, keys ...[]byte) {
	conn.WriteArray(len(keys))
	_ = db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(rawKeyPrefix(key, currentDb(conn)))
			if err != nil {
				conn.WriteNull()
				continue
			}
			var valCopy []byte
			err = item.Value(func(val []byte) error {
				valCopy = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				conn.WriteError("ERR " + err.Error())
				return err
			}
			conn.WriteBulk(valCopy)
		}
		return nil
	})
}

func Serve(db *badger.DB) {
	var ps redcon.PubSub
	go log.Printf("started redis listener at %s", addr)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			setContext(conn)

			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "select":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				dbIndex, err := strconv.Atoi(string(cmd.Args[1]))
				if err != nil || dbIndex < 0 {
					conn.WriteError("ERR invalid DB index")
					return
				}
				setCurrentDb(conn, dbIndex)
				conn.WriteString("OK")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "client":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				subCmd := strings.ToLower(string(cmd.Args[1]))
				switch subCmd {
				default:
					conn.WriteError("subcommand not supported")
				case "id":
					conn.WriteUint64((conn.Context()).(*ClientInfo).Id)
				case "info":
					infoString := "id=" + strconv.FormatUint(connectionId(conn), 10) + " db=" + strconv.Itoa(currentDb(conn)) + "\r\n"
					conn.WriteBulkString(infoString)
					return
				}
			case "flushall":
				err := db.DropAll()
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteString("OK")
			case "flushdb":
				err := db.DropPrefix(currentDbPrefix(conn))
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteString("OK")
			case "dbsize":
				// NOTE: this is O(n) as opposed to O(1) in redis!
				// Do not use this routinely in production!
				db.View(func(txn *badger.Txn) error {
					opts := badger.DefaultIteratorOptions
					opts.PrefetchSize = 100
					opts.Prefix = currentDbPrefix(conn)
					it := txn.NewIterator(opts)
					defer it.Close()
					var count int64 = 0
					for it.Rewind(); it.Valid(); it.Next() {
						count++
					}
					conn.WriteInt64(count)
					return nil
				})
			case "exists":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var count = 0
				err := db.View(func(txn *badger.Txn) error {
					for _, key := range cmd.Args[1:] {
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
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				err := db.Update(func(txn *badger.Txn) error {
					e := badger.NewEntry(rawKeyPrefix(cmd.Args[1], currentDb(conn)), cmd.Args[2])
					err := txn.SetEntry(e)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}

				conn.WriteString("OK")
			case "setex":
				if len(cmd.Args) != 4 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				i, err := strconv.Atoi(string(cmd.Args[3]))
				if err != nil {
					conn.WriteError("Error")
					return
				}

				expTime := time.Duration(i) * time.Second
				err = db.Update(func(txn *badger.Txn) error {
					e := badger.NewEntry(rawKeyPrefix(cmd.Args[1], currentDb(conn)), cmd.Args[2]).WithTTL(expTime)
					err := txn.SetEntry(e)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}

				conn.WriteString("OK")
			case "strlen":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						conn.WriteInt(0)
						return nil
					}
					var valCopy []byte
					err = item.Value(func(val []byte) error {
						valCopy = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteInt(len(valCopy))
					return nil
				})
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						conn.WriteNull()
						return nil
					}
					var valCopy []byte
					err = item.Value(func(val []byte) error {
						valCopy = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteBulk(valCopy)
					return nil
				})
			case "mget":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				getKeys(conn, db, cmd.Args[1:]...)
			case "getset":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.Update(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						// Key does not exist, just set the new value
						e := badger.NewEntry(rawKeyPrefix(cmd.Args[1], currentDb(conn)), cmd.Args[2])
						err = txn.SetEntry(e)
						if err != nil {
							conn.WriteError("ERR " + err.Error())
							return err
						}
						conn.WriteNull()
						return nil
					}
					var valCopy []byte
					err = item.Value(func(val []byte) error {
						valCopy = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}

					// Set the new value
					e := badger.NewEntry(rawKeyPrefix(cmd.Args[1], currentDb(conn)), cmd.Args[2])
					err = txn.SetEntry(e)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}
					conn.WriteBulk(valCopy)
					return nil
				})
			case "getdel":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.Update(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						conn.WriteNull()
						return nil
					}
					var valCopy []byte
					err = item.Value(func(val []byte) error {
						valCopy = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}

					err = txn.Delete(cmd.Args[1])
					if err != nil {
						log.Println("getdel error:", err)
						conn.WriteNull()
						return nil
					}
					conn.WriteBulk(valCopy)
					return nil
				})
			case "move":
				conn.WriteError("ERR MOVE not implemented")
			case "rename":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				_ = db.Update(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						conn.WriteError("ERR no such key")
						return nil
					}
					var valCopy []byte
					err = item.Value(func(val []byte) error {
						valCopy = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}

					// Set the new key
					e := badger.NewEntry(rawKeyPrefix(cmd.Args[2], currentDb(conn)), valCopy)
					err = txn.SetEntry(e)
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}

					// Delete the old key
					err = txn.Delete(cmd.Args[1])
					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return err
					}

					conn.WriteString("OK")
					return nil
				})
			case "renamenx":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.Update(func(txn *badger.Txn) error {
					var renamed = false
					_, err := txn.Get(rawKeyPrefix(cmd.Args[2], currentDb(conn)))
					if err != nil {
						if err == badger.ErrKeyNotFound {
							item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
							if err != nil {
								conn.WriteError("no such key")
								return nil
							}
							var valCopy []byte
							err = item.Value(func(val []byte) error {
								valCopy = append([]byte{}, val...)
								return nil
							})
							if err != nil {
								log.Println("fdsdfdsfsd")
								conn.WriteError("ERR " + err.Error())
								return err
							}
							// Set the new key
							e := badger.NewEntry(rawKeyPrefix(cmd.Args[2], currentDb(conn)), valCopy)
							err = txn.SetEntry(e)
							if err != nil {
								log.Println("something hello")
								conn.WriteError("ERR " + err.Error())
								return err
							}
							// Delete the old key
							err = txn.Delete(cmd.Args[1])
							if err != nil {
								log.Println("I dunnot, tried to delete")
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
			case "setnx":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				_ = db.Update(func(txn *badger.Txn) error {
					var set = false
					_, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					log.Println(string(cmd.Args[1]))
					if err != nil {
						// key does not exist, set it
						if err == badger.ErrKeyNotFound {
							e := badger.NewEntry(rawKeyPrefix(cmd.Args[1], currentDb(conn)), cmd.Args[2])
							err = txn.SetEntry(e)
							if err != nil {
								return err
							}
							set = true
						} else {
							return err
						}
					}

					if err != nil {
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					if set {
						conn.WriteInt(1)
					} else {
						conn.WriteInt(0)
					}
					return nil
				})
			case "pttl":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						if err == badger.ErrKeyNotFound {
							conn.WriteInt(-2)
							return nil
						}
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					ttl := item.ExpiresAt()
					// redis expects the TTL in seconds
					now := uint64(time.Now().UnixNano() / 1e6)
					remaining := int64(ttl) - int64(now)
					if remaining <= 0 {
						conn.WriteInt(-1)
						return nil
					}
					conn.WriteInt64(remaining)
					return nil
				})
			case "ttl":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						if err == badger.ErrKeyNotFound {
							conn.WriteInt(-2)
							return nil
						}
						conn.WriteError("ERR " + err.Error())
						return nil
					}
					ttl := item.ExpiresAt()
					// redis expects the TTL in seconds
					now := uint64(time.Now().UnixNano() / 1e6)
					remaining := int64(ttl) - int64(now)
					if remaining <= 0 {
						conn.WriteInt(-1)
						return nil
					}
					conn.WriteInt(int(remaining / 1000))
					return nil
				})
			case "expire":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				seconds, err := strconv.Atoi(string(cmd.Args[2]))
				if err != nil {
					conn.WriteError("ERR value is not an integer or out of range")
					return
				}

				var updated = 0
				err = db.Update(func(txn *badger.Txn) error {
					item, err := txn.Get(rawKeyPrefix(cmd.Args[1], currentDb(conn)))
					if err != nil {
						updated = 0
						return nil
					}
					var valCopy []byte
					err = item.Value(func(val []byte) error {
						valCopy = append([]byte{}, val...)
						return nil
					})
					if err != nil {
						return err
					}

					// Set the new key
					e := badger.NewEntry(rawKeyPrefix(cmd.Args[2], currentDb(conn)), valCopy).WithTTL(time.Duration(seconds) * time.Second)
					err = txn.SetEntry(e)
					return err
				})

				if err != nil {
					conn.WriteError("ERR " + err.Error())
				}
				conn.WriteInt(updated)
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var numUpdated = 0
				err := db.Update(func(txn *badger.Txn) error {
					for _, key := range cmd.Args[1:] {
						err := txn.Delete(key)
						if err != nil && err != badger.ErrKeyNotFound {
							conn.WriteError("ERR " + err.Error())
							return err
						}
						numUpdated++
					}
					return nil
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				conn.WriteInt(numUpdated)
			case "publish":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
			case "subscribe", "psubscribe":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				command := strings.ToLower(string(cmd.Args[0]))
				for i := 1; i < len(cmd.Args); i++ {
					if command == "psubscribe" {
						ps.Psubscribe(conn, string(cmd.Args[i]))
					} else {
						ps.Subscribe(conn, string(cmd.Args[i]))
					}
				}
			}
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
