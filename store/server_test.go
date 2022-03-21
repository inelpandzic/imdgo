package store

import (
	"io/ioutil"
	"testing"
	"time"
)

func Test_StoreServer(t *testing.T) {
	const leaderAddr = "localhost:0"
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := New(tmpDir, leaderAddr, []string{leaderAddr})

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Ensure raft and store server are up and running
	time.Sleep(3 * time.Second)

	const key = "key"
	const value = "value"

	t.Run("set a key handler", func(t *testing.T) {
		err := writeOnLeader(leaderAddr, key, value)
		if err != nil {
			t.Fatalf("failed writing on leader, err: %s", err.Error())
		}

		got, ok := s.Get(key)
		if !ok {
			t.Fatalf("failed getting the key, err: %s", err.Error())
		}

		if got != value {
			t.Fatalf("wanted %s but got %s", value, got)
		}
	})

	t.Run("delete a key handler", func(t *testing.T) {
		err := deleteOnLeader(leaderAddr, key)
		if err != nil {
			t.Fatalf("failed deleting on leader, err: %s", err.Error())
		}

		l := s.m.Count()
		if l != 0 {
			t.Errorf("map length should be 0, but got %d", l)
		}
	})

	t.Cleanup(func() {
		s.Close()
	})
}
