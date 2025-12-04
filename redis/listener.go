package redis

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/tidwall/redcon"
)

var addr = ":6379"

func Serve(db *badger.DB) {
	var ps redcon.PubSub
	go log.Printf("started redis listener at %s", addr)
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "exists":
				if len(cmd.Args) < 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var count = 0
				err := db.View(func(txn *badger.Txn) error {
					for _, key := range cmd.Args[1:] {
						_, err := txn.Get(key)
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
					e := badger.NewEntry(cmd.Args[1], cmd.Args[2])
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
					e := badger.NewEntry(cmd.Args[1], cmd.Args[2]).WithTTL(expTime)
					err := txn.SetEntry(e)
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}

				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				_ = db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(cmd.Args[1])
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
			case "setnx":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				var existed = true
				err := db.Update(func(txn *badger.Txn) error {
					_, err := txn.Get(cmd.Args[1])
					if err != nil {
						if err == badger.ErrKeyNotFound {
							e := badger.NewEntry(cmd.Args[1], cmd.Args[2])
							err = txn.SetEntry(e)
							if err != nil {
								return err
							}
							existed = false
							return nil
						}
						return err
					}
					return err
				})
				if err != nil {
					conn.WriteError("ERR " + err.Error())
					return
				}
				if existed {
					conn.WriteInt(0)
				} else {
					conn.WriteInt(1)
				}
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
