package redis

import (
	"testing"
)

func TestMakeNewList(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"), []byte("value3"))
	if list.size != 3 {
		t.Error("Expected list size 3, got", list.size)
	}
	if string(list.name) != "mylist" {
		t.Error("Expected list name 'mylist', got", string(list.name))
	}
	if string(list.head.value) != "value1" {
		t.Error("Expected head value 'value1', got", string(list.head.value))
	}
	if string(list.tail.value) != "value3" {
		t.Error("Expected tail value 'value3', got", string(list.tail.value))
	}
	if list.head.prev != nil {
		t.Error("Expected head.prev to be nil, got ", string(list.head.prev.value))
	}
	if list.tail.next != nil {
		t.Error("Expected tail.next to be nil, got ", string(list.tail.next.value))
	}
}

func TestListIteration(t *testing.T) {
	list := makeNewList([]byte("mylist"), []byte("value1"), []byte("value2"), []byte("value3"))
	for item := range list.all() {
		if item.value == nil {
			t.Error("Expected non-nil value in list node")
		}
		if item.key == nil {
			t.Error("Expected non-nil key in list node")
		}
	}
}
