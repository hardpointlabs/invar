//go:build slatedb

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hardpointlabs/invar/kv"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

// openSlateDB opens a SlateDB-backed KeyValueStore from the CLI subcommand.
// It is compiled only when the slatedb build tag is set.
func openSlateDB(c *slateDBCmd) (kv.KeyValueStore, error) {
	if c.EnvFile != "" {
		if err := loadDotEnv(c.EnvFile); err != nil {
			return nil, err
		}
	}

	settings := slatedb.SettingsDefault()
	if c.FlushInterval != 0 {
		if err := settings.Set("flush_interval", strconv.Quote(slateDuration(time.Duration(c.FlushInterval)))); err != nil {
			return nil, err
		}
	}
	if c.ManifestPollInterval != 0 {
		if err := settings.Set("manifest_poll_interval", strconv.Quote(slateDuration(time.Duration(c.ManifestPollInterval)))); err != nil {
			return nil, err
		}
	}
	if c.Compression != "" {
		valueJSON, err := slateCompressionCodec(c.Compression)
		if err != nil {
			return nil, err
		}
		if err := settings.Set("compression_codec", valueJSON); err != nil {
			return nil, err
		}
	}
	if c.ObjectStoreMaxRetries >= 0 {
		// Bounded object store retries: without this, SlateDB retries
		// unrecoverable errors (e.g. a mock returning 500 for ListObjectsV2)
		// forever, so open() hangs and no listener ever binds.
		if err := settings.Set("object_store_max_retries", strconv.Itoa(c.ObjectStoreMaxRetries)); err != nil {
			return nil, err
		}
	}

	return kv.NewSlateDB(kv.SlateDBOpts{
		Path:           c.Path,
		ObjectStoreURL: c.ObjectStoreURL,
		Settings:       settings,
	})
}

// slateCompressionCodec maps a CLI compression codec name to the JSON literal
// that SlateDB's settings deserialize from (a quoted variant name, or the bare
// null for "no compression").
func slateCompressionCodec(name string) (string, error) {
	switch strings.ToLower(name) {
	case "none", "":
		return "null", nil
	case "snappy":
		return `"Snappy"`, nil
	case "zlib":
		return `"Zlib"`, nil
	case "lz4":
		return `"Lz4"`, nil
	case "zstd":
		return `"Zstd"`, nil
	default:
		return "", fmt.Errorf("invalid compression codec %q (must be none, snappy, zlib, lz4, or zstd)", name)
	}
}

// slateDuration formats d the same way SlateDB serializes Duration values so
// the settings round-trip cleanly through its JSON config.
func slateDuration(d time.Duration) string {
	secs := d / time.Second
	millis := (d % time.Second) / time.Millisecond
	switch {
	case secs > 0 && millis > 0:
		return fmt.Sprintf("%ds+%03dms", secs, millis)
	case millis > 0:
		return fmt.Sprintf("%03dms", millis)
	default:
		return fmt.Sprintf("%ds", secs)
	}
}

// loadDotEnv reads a KEY=VALUE .env file and applies its variables to the
// process environment, without overriding variables that are already set.
// This is used to supply AWS_* credentials to the object store resolver.
func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	return scanner.Err()
}
