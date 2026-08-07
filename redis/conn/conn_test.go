package conn

import (
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/common"
)

func TestDbSizeDbOp(t *testing.T) {
	seed := func(t *testing.T, session *common.Session, keys ...string) {
		t.Helper()
		if err := session.KVS().Update(func(tx kv.Tx) error {
			for _, k := range keys {
				if err := tx.Set(session.NewPublicEntry([]byte(k), []byte("v"))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	runDbSize := func(t *testing.T, session *common.Session) int64 {
		t.Helper()
		op := DbSize(session)
		tx := session.KVS().Begin(false)
		defer tx.Discard()
		result, err := op.DbOp(tx)
		if err != nil {
			t.Fatalf("DbOp returned error: %v", err)
		}
		count, ok := result.(int64)
		if !ok {
			t.Fatalf("DbOp returned %T, want int64", result)
		}
		return count
	}

	t.Run("counts keys in the current DB", func(t *testing.T) {
		kvs := kv.InMemoryBadger(t)
		defer kvs.Close()
		session := common.NewSession(kvs)

		seed(t, session, "a", "b", "c")
		if got := runDbSize(t, session); got != 3 {
			t.Fatalf("DbSize = %d, want 3", got)
		}
	})

	t.Run("empty DB returns zero", func(t *testing.T) {
		kvs := kv.InMemoryBadger(t)
		defer kvs.Close()
		session := common.NewSession(kvs)

		if got := runDbSize(t, session); got != 0 {
			t.Fatalf("DbSize = %d, want 0", got)
		}
	})

	t.Run("scoped to the current DB only", func(t *testing.T) {
		kvs := kv.InMemoryBadger(t)
		defer kvs.Close()
		session := common.NewSession(kvs)

		seed(t, session, "only-in-db0")
		session.SwitchDB(1)
		seed(t, session, "only-in-db1")

		session.SwitchDB(0)
		if got := runDbSize(t, session); got != 1 {
			t.Fatalf("DbSize in DB 0 = %d, want 1", got)
		}
		session.SwitchDB(1)
		if got := runDbSize(t, session); got != 1 {
			t.Fatalf("DbSize in DB 1 = %d, want 1", got)
		}
	})
}
