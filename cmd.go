package main

import "time"

// durationArg is a time.Duration that can be parsed from flag text via
// encoding.TextUnmarshaler, which go-arg uses for custom flag types.
type durationArg time.Duration

func (d *durationArg) UnmarshalText(b []byte) error {
	parsed, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = durationArg(parsed)
	return nil
}

// ---- subcommand argument structs ----

type versionCmd struct{}

type redisCmd struct {
	ListenAddr listenAddr `arg:"--listen-addr" default:"tcp::6379" help:"listen address (tcp:<addr> or unix:<path>)"`
	backendCmd
}

type mongoCmd struct {
	ListenAddr listenAddr `arg:"--listen-addr" default:"tcp::27017" help:"listen address (tcp:<addr> or unix:<path>)"`
	backendCmd
}

// backendCmd selects the KeyValueStore implementation. It is embedded into
// each protocol subcommand so that badger/slatedb appear directly beneath
// them, e.g. `./invar redis badger --data-dir /tmp/db`.
type backendCmd struct {
	Badger  *badgerCmd  `arg:"subcommand:badger"  help:"use BadgerDB as the storage backend"`
	SlateDB *slateDBCmd `arg:"subcommand:slatedb" help:"use SlateDB as the storage backend"`
}

type badgerCmd struct {
	DataDir string `arg:"required,--data-dir,help:path to the BadgerDB data directory"`

	ValueLogFileSize int64  `arg:"--value-log-file-size,help:size of each value log file in bytes"`
	MemTableSize     int64  `arg:"--memtable-size,help:maximum size of the LSM memtable in bytes"`
	BlockSize        int64  `arg:"--block-size,help:size of a data block in bytes"`
	Compression      string `arg:"--compression,help:SSTable compression codec: none snappy or zstd"`
	SyncWrites       bool   `arg:"--sync-writes,help:fsync every write to the value log"`
}

type slateDBCmd struct {
	ObjectStoreURL string `arg:"required,--object-store-url,help:e.g. s3://mybucket/"`
	Path           string `arg:"--path,help:local path for SlateDB state" default:"/tmp/invar-slatedb"`
	EnvFile        string `arg:"--env-file,help:optional .env file with AWS_* vars"`

	FlushInterval         durationArg `arg:"--flush-interval,help:memtable flush interval (e.g. 250ms)"`
	ManifestPollInterval  durationArg `arg:"--manifest-poll-interval,help:how often to poll the manifest (e.g. 1s)" default:"10s"`
	Compression           string      `arg:"--compression,help:SSTable compression codec: none snappy zlib lz4 or zstd" default:"zstd"`
	ObjectStoreMaxRetries int         `arg:"--object-store-max-retries,help:max retries for object store operations (0 disables retries; prevents infinite retry loops on unrecoverable errors)" default:"3"`
}
