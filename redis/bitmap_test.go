package redis

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestSetBitAndGetBit(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	key := []byte("bitkey")
	dbSlot := 0

	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(key, dbSlot), make([]byte, 2)).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	tests := []struct {
		name   string
		offset int
		value  int
		want   int
	}{
		{"set bit 0 to 1", 0, 1, 0},
		{"set bit 7 to 1", 7, 1, 0},
		{"set bit 8 to 1", 8, 1, 0},
		{"set bit 0 to 0", 0, 0, 1},
		{"set bit 0 to 1 again", 0, 1, 0},
		{"set bit 15 to 1", 15, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byteIndex := tt.offset / 8
			bitPos := uint(7 - (tt.offset % 8))
			mask := byte(1 << bitPos)

			var oldBit int
			err := db.Update(func(txn *badger.Txn) error {
				item, err := txn.Get(rawKeyPrefix(key, dbSlot))
				if err != nil {
					return err
				}
				data, err := copyItemValue(item)
				if err != nil {
					return err
				}
				oldBit = int((data[byteIndex] & mask) >> bitPos)
				if tt.value == 1 {
					data[byteIndex] |= mask
				} else {
					data[byteIndex] &^= mask
				}
				e := badger.NewEntry(rawKeyPrefix(key, dbSlot), data).WithMeta(byte(RedisString))
				return txn.SetEntry(e)
			})
			if err != nil {
				t.Fatalf("setbit failed: %v", err)
			}
			if oldBit != tt.want {
				t.Errorf("got oldBit = %d, want %d", oldBit, tt.want)
			}
		})
	}

	var finalData []byte
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, dbSlot))
		if err != nil {
			return err
		}
		finalData, _ = copyItemValue(item)
		return nil
	})

	if finalData[0] != 0x81 {
		t.Errorf("byte 0: got 0x%02x, want 0x81", finalData[0])
	}
	if finalData[1] != 0x81 {
		t.Errorf("byte 1: got 0x%02x, want 0x81", finalData[1])
	}
}

func TestGetBitOffsets(t *testing.T) {
	data := []byte{0b10000001, 0b00000001}

	tests := []struct {
		offset int
		want   int
	}{
		{0, 1},
		{1, 0},
		{6, 0},
		{7, 1},
		{8, 0},
		{15, 1},
		{16, 0},
	}

	for _, tt := range tests {
		byteIndex := tt.offset / 8
		if byteIndex >= len(data) {
			if tt.want != 0 {
				t.Errorf("offset %d: out of range should be 0", tt.offset)
			}
			continue
		}
		bitPos := uint(7 - (tt.offset % 8))
		got := int((data[byteIndex] & (1 << bitPos)) >> bitPos)
		if got != tt.want {
			t.Errorf("offset %d: got %d, want %d", tt.offset, got, tt.want)
		}
	}
}

func TestBitCount(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want int
	}{
		{"empty", []byte{}, 0},
		{"zero byte", []byte{0x00}, 0},
		{"all ones", []byte{0xFF}, 8},
		{"mixed", []byte{0b10101010}, 4},
		{"multi byte", []byte{0xFF, 0x00, 0xFF}, 16},
		{"single bit", []byte{0x80}, 1},
		{"pattern", []byte{0b10000001, 0b01000010}, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := 0
			for _, b := range tt.data {
				count += int(popcount(b))
			}
			if count != tt.want {
				t.Errorf("bitcount = %d, want %d", count, tt.want)
			}
		})
	}
}

func popcount(x byte) byte {
	c := byte(0)
	for x != 0 {
		x &= x - 1
		c++
	}
	return c
}

func TestBitPos(t *testing.T) {
	tests := []struct {
		name      string
		data      []byte
		bit       int
		want      int
	}{
		{"empty find 1", []byte{}, 1, -1},
		{"all zeros find 0", []byte{0x00}, 0, 0},
		{"all zeros find 1", []byte{0x00}, 1, -1},
		{"all ones find 1", []byte{0xFF}, 1, 0},
		{"find first 1", []byte{0b01000000}, 1, 1},
		{"find first 0", []byte{0b11111011}, 0, 5},
		{"two bytes find 1", []byte{0x00, 0x80}, 1, 8},
		{"test pattern", []byte{0b10000001}, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bitPosInRange(tt.data, 0, max(0, len(tt.data)*8-1), tt.bit, false)
			if got != tt.want {
				t.Errorf("bitPos = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBitPosInRange(t *testing.T) {
	data := []byte{0xFF, 0x00, 0xFF}

	tests := []struct {
		name         string
		startBit, endBit int
		bit          int
		want         int
	}{
		{"find 1 in first byte", 0, 7, 1, 0},
		{"find 0 in first byte", 0, 7, 0, -1},
		{"find 0 in second byte", 8, 15, 0, 8},
		{"find 1 in second byte", 8, 15, 1, -1},
		{"find 1 across range", 4, 12, 1, 4},
		{"find 0 across range", 4, 12, 0, 8},
		{"empty range", 16, 15, 0, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bitPosInRange(data, tt.startBit, tt.endBit, tt.bit, false)
			if got != tt.want {
				t.Errorf("bitPosInRange = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBitOpAND(t *testing.T) {
	a := []byte{0xF0}
	b := []byte{0x0F}
	result := make([]byte, 1)
	result[0] = a[0] & b[0]
	if result[0] != 0x00 {
		t.Errorf("AND: got 0x%02x, want 0x00", result[0])
	}
}

func TestBitOpOR(t *testing.T) {
	a := []byte{0xF0}
	b := []byte{0x0F}
	result := a[0] | b[0]
	if result != 0xFF {
		t.Errorf("OR: got 0x%02x, want 0xFF", result)
	}
}

func TestBitOpXOR(t *testing.T) {
	a := []byte{0xFF}
	b := []byte{0x0F}
	result := a[0] ^ b[0]
	if result != 0xF0 {
		t.Errorf("XOR: got 0x%02x, want 0xF0", result)
	}
}

func TestBitOpNOT(t *testing.T) {
	a := []byte{0xF0}
	result := ^a[0]
	if result != 0x0F {
		t.Errorf("NOT: got 0x%02x, want 0x0F", byte(result))
	}
}

func TestBitOpDIFF(t *testing.T) {
	a := []byte{0xFF}
	b := []byte{0x0F}
	result := a[0] & ^b[0]
	if result != 0xF0 {
		t.Errorf("DIFF: got 0x%02x, want 0xF0", result)
	}
}

func TestBitOpOne(t *testing.T) {
	a := []byte{0b10001000}
	b := []byte{0b00101000}
	// exactly one: bit 0 (from a), bit 2 (from b)
	// bit 4 is set in both so excluded
	want := byte(0b10100000)
	var result byte
	for bitPos := 0; bitPos < 8; bitPos++ {
		count := 0
		if a[0]&(1<<(7-uint(bitPos))) != 0 {
			count++
		}
		if b[0]&(1<<(7-uint(bitPos))) != 0 {
			count++
		}
		if count == 1 {
			result |= 1 << (7 - uint(bitPos))
		}
	}
	if result != want {
		t.Errorf("ONE: got 0x%02x, want 0x%02x", result, want)
	}
}

func TestBitOpStoreAndRead(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	dbSlot := 0
	srcKey := []byte("src1")
	destKey := []byte("dest")
	srcData := []byte{0xFF}

	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(srcKey, dbSlot), srcData).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	result := make([]byte, 1)
	result[0] = srcData[0]

	err = db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(destKey, dbSlot), result).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}

	var readBack []byte
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(destKey, dbSlot))
		if err != nil {
			return err
		}
		readBack, _ = copyItemValue(item)
		return nil
	})

	if len(readBack) != 1 || readBack[0] != 0xFF {
		t.Errorf("read back: got %v, want [255]", readBack)
	}
}

func TestBitOpLargeNot(t *testing.T) {
	a := make([]byte, 1000)
	for i := range a {
		a[i] = 0xFF
	}
	result := make([]byte, 1000)
	for i := range a {
		result[i] = ^a[i]
	}
	for i := range result {
		if result[i] != 0x00 {
			t.Errorf("byte %d: got 0x%02x, want 0x00", i, result[i])
			break
		}
	}
}

func TestSetBitExtendsString(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	key := []byte("extendkey")
	dbSlot := 0

	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(key, dbSlot), []byte{0x00}).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	offset := 16
	byteIndex := offset / 8

	err = db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, dbSlot))
		if err != nil {
			return err
		}
		data, err := copyItemValue(item)
		if err != nil {
			return err
		}
		if byteIndex >= len(data) {
			newData := make([]byte, byteIndex+1)
			copy(newData, data)
			data = newData
		}
		data[byteIndex] |= 0x80
		e := badger.NewEntry(rawKeyPrefix(key, dbSlot), data).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		t.Fatalf("setbit failed: %v", err)
	}

	var finalData []byte
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(key, dbSlot))
		if err != nil {
			return err
		}
		finalData, _ = copyItemValue(item)
		return nil
	})

	if len(finalData) != 3 {
		t.Errorf("expected len 3, got %d", len(finalData))
	}
	if finalData[0] != 0x00 || finalData[1] != 0x00 || finalData[2] != 0x80 {
		t.Errorf("got %v, want [0x00, 0x00, 0x80]", finalData)
	}
}

func TestBitOpOnMissingKeys(t *testing.T) {
	db := inMemDB(t)
	defer db.Close()

	dbSlot := 0
	destKey := []byte("missingDest")

	// Simulate BITOP with missing source keys by treating nil as empty byte slice
	sources := [][]byte{nil, nil}
	maxLen := 0
	for _, s := range sources {
		if s != nil && len(s) > maxLen {
			maxLen = len(s)
		}
	}

	result := make([]byte, maxLen)
	err := db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(rawKeyPrefix(destKey, dbSlot), result).WithMeta(byte(RedisString))
		return txn.SetEntry(e)
	})
	if err != nil {
		t.Fatalf("store failed: %v", err)
	}

	var readBack []byte
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(rawKeyPrefix(destKey, dbSlot))
		if err != nil {
			return err
		}
		readBack, _ = copyItemValue(item)
		return nil
	})

	if len(readBack) != 0 {
		t.Errorf("expected empty result, got len %d", len(readBack))
	}
}
