package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

var logger hclog.Logger

func init() {
    o := hclog.DefaultOptions
    o.Level = hclog.Debug
    logger = hclog.New(o)
}

// Test_StoreOpen tests that the store can be opened.
func Test_StoreOpen(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := New(tmpDir, "127.0.0.1", []string{"127.0.0.1"}, logger)

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	t.Cleanup(func() {
		s.Close()
		os.RemoveAll(tmpDir)
	})
}

// Test_StoreOpenSingleNode tests that a command can be applied to the log
func Test_StoreOpenSingleNode(t *testing.T) {
	tmpDir, _ := ioutil.TempDir("", "store_test")
	s := New(tmpDir, "127.0.0.1", []string{"127.0.0.1"}, logger)

	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	const key = "key"

	if err := s.Set(key, "bar"); err != nil {
		t.Fatalf("failed to set key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, ok := s.Get(key)
	if !ok {
		t.Fatalf("failed to get key: %s", key)
	}
	if value != "bar" {
		t.Fatalf("key has wrong value: %s", value)
	}

	if err := s.Delete(key); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	value, ok = s.Get(key)
	if ok {
		t.Fatalf("key %s should be empty, got value %s", key, value)
	}

	t.Cleanup(func() {
		s.Close()
		os.RemoveAll(tmpDir)
	})
}
