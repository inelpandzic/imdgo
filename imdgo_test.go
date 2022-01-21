package imdgo

import (
	"testing"
	"time"
)

func TestCacheNew(t *testing.T) {
	c := &Config{Members: []string{"127.0.0.1:0"}}
	cache, err := New(c)

	if err != nil {
		t.Fatalf("failed creating cache: %s", err.Error())
	}

	t.Cleanup(func() {
		cache.Close()
	})
}

func Test_getCurrentNodeAddress(t *testing.T) {
	want := "127.0.0.1:0"
	members := []string{want, "123.123.123.123:9"}
	got := getHostAddr(members)

	if got != want {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestCacheOperations(t *testing.T) {
	c := &Config{Members: []string{"127.0.0.1:0"}}
	cache, err := New(c)

	if err != nil {
		t.Fatalf("failed creating cache: %s", err.Error())
	}

	// Wait for the leader to be elected
	time.Sleep(3 * time.Second)

	value := "value"
	key := "key"

	t.Run("set operations", func(t *testing.T) {
		err := cache.Set(key, value)
		if err != nil {
			t.Fatalf("failed to put item: %s", err.Error())
		}
	})

	t.Run("get operations", func(t *testing.T) {
		got, err := cache.Get(key)
		if err != nil {
			t.Errorf("failed to get item: %s", err.Error())
		}

		want := value

		if got != want {
			t.Errorf("want %s but got %s", want, got)
		}
	})

	t.Run("delete operations", func(t *testing.T) {
		err := cache.Delete(key)
		if err != nil {
			t.Errorf("failed to delete item: %s", err.Error())
		}

		item, _ := cache.Get(key)
		if item != "" {
			t.Errorf("failed to delete item, it is still in the cache: %s", item)
		}
	})

	t.Cleanup(func() {
		cache.Close()
	})
}
