package testutil

import (
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
)

func NewTestSession(t *testing.T) (*common.Session, kv.KeyValueStore) {
	t.Helper()
	kvs := kv.InMemoryBadger(t)
	t.Cleanup(func() { kvs.Close() })
	session := common.NewSession(kvs)
	return session, kvs
}
