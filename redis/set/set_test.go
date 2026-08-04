package set

import (
	"testing"

	"github.com/hardpointlabs/invar/kv"
	"github.com/hardpointlabs/invar/redis/testutil"
)

func TestSAdd(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 3 {
		t.Errorf("expected 3 added, got %d", added)
	}

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 3 {
		t.Errorf("expected card 3, got %d", count.(int))
	}
}

func TestSAddDuplicates(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 2 {
		t.Fatalf("expected 2 added, got %d", added)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Errorf("expected 1 new member, got %d", added)
	}

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 3 {
		t.Errorf("expected card 3, got %d", count.(int))
	}
}

func TestSAddEmptySet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	var added any
	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key)
		var err error
		added, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if added != 0 {
		t.Errorf("expected 0 added, got %d", added)
	}
}

func TestSRem(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var removed any
	err = db.Update(func(tx kv.Tx) error {
		op := SRem(session, key, []byte("a"), []byte("x"))
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Errorf("expected 1 removed, got %d", removed)
	}

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 2 {
		t.Errorf("expected card 2, got %d", count.(int))
	}
}

func TestSRemMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	var removed any
	err := db.Update(func(tx kv.Tx) error {
		op := SRem(session, []byte("nonexistent"), []byte("a"))
		var err error
		removed, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Errorf("expected 0 removed, got %d", removed)
	}
}

func TestSRemAllMembers(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(tx kv.Tx) error {
		op := SRem(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 0 {
		t.Errorf("expected card 0 after removing all, got %d", count.(int))
	}

	members, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(members.([][]byte)) != 0 {
		t.Errorf("expected 0 members, got %d", len(members.([][]byte)))
	}
}

func TestSCardMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, []byte("nonexistent"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 0 {
		t.Errorf("expected card 0, got %d", count.(int))
	}
}

func TestSMembers(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	seen := make(map[string]bool)
	for _, m := range members {
		seen[string(m)] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Errorf("expected member %q", want)
		}
	}
}

func TestSMembersMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, []byte("nonexistent"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([][]byte)) != 0 {
		t.Errorf("expected empty members, got %d", len(result.([][]byte)))
	}
}

func TestSIsMember(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	ok, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SIsMember(session, key, []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok.(bool) {
		t.Error("expected member 'a' to be present")
	}

	ok, err = db.Read(func(tx kv.Tx) (any, error) {
		op := SIsMember(session, key, []byte("x"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok.(bool) {
		t.Error("expected member 'x' to be absent")
	}
}

func TestSIsMemberMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	ok, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SIsMember(session, []byte("nonexistent"), []byte("a"))
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok.(bool) {
		t.Error("expected member to be absent from missing key")
	}
}

func TestSPop(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var popped any
	err = db.Update(func(tx kv.Tx) error {
		op := SPop(session, key)
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(popped.([]byte)) != "a" && string(popped.([]byte)) != "b" && string(popped.([]byte)) != "c" {
		t.Errorf("expected a known member, got %q", popped)
	}

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 2 {
		t.Errorf("expected card 2 after pop, got %d", count.(int))
	}
}

func TestSPopEmptySet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	var popped any
	err := db.Update(func(tx kv.Tx) error {
		op := SPop(session, []byte("nonexistent"))
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if popped != nil {
		t.Errorf("expected nil pop, got %q", popped)
	}
}

func TestSPopLastMember(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var popped any
	err = db.Update(func(tx kv.Tx) error {
		op := SPop(session, key)
		var err error
		popped, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if popped == nil {
		t.Fatal("expected a popped member")
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([][]byte)) != 0 {
		t.Errorf("expected empty set after popping last member, got %d", len(result.([][]byte)))
	}
}

func TestSRandMember(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SRandMember(session, key, 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if string(members[0]) != "a" && string(members[0]) != "b" && string(members[0]) != "c" {
		t.Errorf("expected a known member, got %q", members[0])
	}

	count, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, key)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.(int) != 3 {
		t.Errorf("expected card unchanged at 3, got %d", count.(int))
	}
}

func TestSRandMemberCount(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("a"), []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SRandMember(session, key, 2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	if members[0][0] == members[1][0] {
		t.Errorf("expected distinct members without replacement, got %q and %q", members[0], members[1])
	}
}

func TestSRandMemberMissingKey(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SRandMember(session, []byte("nonexistent"), 1)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.([][]byte)) != 0 {
		t.Errorf("expected empty result, got %d", len(result.([][]byte)))
	}
}

func TestSMove(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	src := []byte("src")
	dst := []byte("dst")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, src, []byte("m"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var moved any
	err = db.Update(func(tx kv.Tx) error {
		op := SMove(session, src, dst, []byte("m"))
		var err error
		moved, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !moved.(bool) {
		t.Error("expected move to succeed")
	}

	srcCount, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, src)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if srcCount.(int) != 0 {
		t.Errorf("expected src card 0, got %d", srcCount.(int))
	}

	dstCount, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SCard(session, dst)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	if dstCount.(int) != 1 {
		t.Errorf("expected dst card 1, got %d", dstCount.(int))
	}
}

func TestSMoveMissingMember(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	var moved any
	err := db.Update(func(tx kv.Tx) error {
		op := SMove(session, []byte("src"), []byte("dst"), []byte("m"))
		var err error
		moved, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if moved.(bool) {
		t.Error("expected move to fail for missing member")
	}
}

func TestSMoveSameSet(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	key := []byte("myset")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, key, []byte("m"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var moved any
	err = db.Update(func(tx kv.Tx) error {
		op := SMove(session, key, key, []byte("m"))
		var err error
		moved, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !moved.(bool) {
		t.Error("expected move within same set to succeed for present member")
	}

	err = db.Update(func(tx kv.Tx) error {
		op := SMove(session, key, key, []byte("x"))
		var err error
		moved, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if moved.(bool) {
		t.Error("expected move within same set to fail for absent member")
	}
}

func TestSDiff(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	s1 := []byte("s1")
	s2 := []byte("s2")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, s1, []byte("a"), []byte("b"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, s2, []byte("b"), []byte("d"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SDiff(session, s1, s2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	seen := make(map[string]bool)
	for _, m := range members {
		seen[string(m)] = true
	}
	if !seen["a"] || !seen["c"] {
		t.Errorf("expected members a and c, got %v", members)
	}
}

func TestSInter(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	s1 := []byte("s1")
	s2 := []byte("s2")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, s1, []byte("a"), []byte("b"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, s2, []byte("b"), []byte("c"), []byte("d"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SInter(session, s1, s2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}
	seen := make(map[string]bool)
	for _, m := range members {
		seen[string(m)] = true
	}
	if !seen["b"] || !seen["c"] {
		t.Errorf("expected members b and c, got %v", members)
	}
}

func TestSUnion(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	s1 := []byte("s1")
	s2 := []byte("s2")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, s1, []byte("a"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, s2, []byte("b"), []byte("c"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SUnion(session, s1, s2)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	seen := make(map[string]bool)
	for _, m := range members {
		seen[string(m)] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Errorf("expected member %q", want)
		}
	}
}

func TestSDiffStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	s1 := []byte("s1")
	s2 := []byte("s2")
	dest := []byte("dest")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, s1, []byte("a"), []byte("b"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, s2, []byte("b"), []byte("d"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, dest, []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count any
	err = db.Update(func(tx kv.Tx) error {
		op := SDiffStore(session, dest, s1, s2)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected stored count 2, got %d", count)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, dest)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 2 {
		t.Fatalf("expected 2 members in dest, got %d", len(members))
	}
	seen := make(map[string]bool)
	for _, m := range members {
		seen[string(m)] = true
	}
	if !seen["a"] || !seen["c"] {
		t.Errorf("expected members a and c, got %v", members)
	}
}

func TestSInterStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	s1 := []byte("s1")
	s2 := []byte("s2")
	dest := []byte("dest")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, s1, []byte("a"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, s2, []byte("b"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, dest, []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count any
	err = db.Update(func(tx kv.Tx) error {
		op := SInterStore(session, dest, s1, s2)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected stored count 1, got %d", count)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, dest)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 1 || string(members[0]) != "b" {
		t.Errorf("expected member b, got %v", members)
	}
}

func TestSUnionStore(t *testing.T) {
	session, db := testutil.NewTestSession(t)

	s1 := []byte("s1")
	s2 := []byte("s2")
	dest := []byte("dest")

	err := db.Update(func(tx kv.Tx) error {
		op := SAdd(session, s1, []byte("a"), []byte("b"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, s2, []byte("b"), []byte("c"))
		if _, err := op.DbOp(tx); err != nil {
			return err
		}
		op = SAdd(session, dest, []byte("x"))
		_, err := op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	var count any
	err = db.Update(func(tx kv.Tx) error {
		op := SUnionStore(session, dest, s1, s2)
		var err error
		count, err = op.DbOp(tx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("expected stored count 3, got %d", count)
	}

	result, err := db.Read(func(tx kv.Tx) (any, error) {
		op := SMembers(session, dest)
		return op.DbOp(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	members := result.([][]byte)
	if len(members) != 3 {
		t.Fatalf("expected 3 members in dest, got %d", len(members))
	}
	seen := make(map[string]bool)
	for _, m := range members {
		seen[string(m)] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seen[want] {
			t.Errorf("expected member %q", want)
		}
	}
}
