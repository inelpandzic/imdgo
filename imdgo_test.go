package imdgo

import (
	"log"
	"net"
	"os"
	"testing"
	"time"
)


func getLocalAddr() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	log.Printf("IMDGO: hostname: %s", hostname)

	addresses, err := net.LookupIP(hostname)
	if err != nil {
		panic(err)
	}

	return addresses[0].String()
}

func TestCacheNew(t *testing.T) {
	c := &Config{Members: []string{getLocalAddr()+":0"}}
	s, err := New(c)

	if err != nil {
		t.Fatalf("failed creating store: %s", err.Error())
	}

	t.Cleanup(func() {
		s.Close()
	})
}

func TestCacheOperations(t *testing.T) {
	c := &Config{Members: []string{getLocalAddr()+":0"}}
	s, err := New(c)

	if err != nil {
		t.Fatalf("failed creating store: %s", err.Error())
	}

	// Wait for the leader to be elected
	time.Sleep(3 * time.Second)

	value := "value"
	key := "key"

	t.Run("set operations", func(t *testing.T) {
		err := s.Set(key, value)
		if err != nil {
			t.Fatalf("failed to put item: %s", err.Error())
		}
	})

	t.Run("get operations", func(t *testing.T) {
		got, err := s.Get(key)
		if err != nil {
			t.Errorf("failed to get item: %s", err.Error())
		}

		want := value

		if got != want {
			t.Errorf("want %s but got %s", want, got)
		}
	})

	t.Run("delete operations", func(t *testing.T) {
		err := s.Delete(key)
		if err != nil {
			t.Errorf("failed to delete item: %s", err.Error())
		}

		item, _ := s.Get(key)
		if item != nil {
			t.Errorf("failed to delete item, it is still in the cache: %s", item)
		}
	})

	t.Cleanup(func() {
		s.Close()
	})
}

func Test_getCurrentNodeAddress(t *testing.T) {
	want := getLocalAddr()
	members := []string{want, "123.123.123.123:9"}
	got := getHostAddr(members)

	if got != want {
		t.Errorf("want %s, got %s", want, got)
	}
}
