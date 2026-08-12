//go:build !slatedb

package main

import (
	"errors"

	"github.com/hardpointlabs/invar/kv"
)

// openSlateDB is replaced by the slatedb build tag's implementation. In a
// plain build the subcommand is still accepted for CLI compatibility but
// cannot open a store.
func openSlateDB(_ *slateDBCmd) (kv.KeyValueStore, error) {
	return nil, errors.New("this build of invar does not include the slatedb backend (rebuild with the slatedb build tag)")
}
