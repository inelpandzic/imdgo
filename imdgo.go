package imdgo

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

    "github.com/hashicorp/go-hclog"

	"github.com/inelpandzic/imdgo/store"
)

// Config is imdgo configuration
type Config struct {
	Members []string

	// LogLevel represents a log level. If the value does not match a known
	// logging level or is not set, INFO level is used.
	LogLevel string

	// Logger is a user-provided logger. If nil, a logger writing to
	// os.Stderr with LogLevel INFO is used.
	Logger hclog.Logger
}

// Store is a imdgo backing storage
type Store struct {
	s *store.S
}

// New creates new imdgo store. Store.Close() must be called in order
// to release all the resources and shut everything down.
func New(conf *Config) (*Store, error) {
	err := validateMembers(conf.Members)
	if err != nil {
		return nil, err
	}

	ex, err := os.Executable()
	if err != nil {
		return nil, err
	}

	addr := getHostAddr(conf.Members)
	s := store.New(filepath.Dir(ex), addr, conf.Members, conf.Logger)

	err = s.Open()
	if err != nil {
		return nil, err
	}

	return &Store{s: s}, nil
}

// Get gets a value for a given key. It will return nil and false
// if the value is not present.
func (s *Store) Get(key string) (interface{}, bool) {
	return s.s.Get(key)
}

// Set puts key with associated value into store
func (s *Store) Set(key string, value interface{}) error {
	return s.s.Set(key, value)
}

// Delete removes a KV pair for a given key
func (s *Store) Delete(key string) error {
	return s.s.Delete(key)
}

// Count return number of items in the store
func (s *Store) Count() int {
	return s.s.Count()
}

// Close shuts imdgo down. Must be called in order to release all the resources
func (s *Store) Close() error {
	return s.s.Close()
}

// validateMembers checks the validity of members IP addresses
func validateMembers(members []string) error {
	for _, m := range members {
		if net.ParseIP(m) == nil {
			return fmt.Errorf("invalid member address: %s", m)
		}
	}
	return nil
}

// getHostAddr resolves current host IP address
func getHostAddr(members []string) string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	log.Printf("IMDGO: hostname: %s", hostname)

	addresses, err := net.LookupIP(hostname)
	if err != nil {
		panic(err)
	}
	log.Printf("IMDGO: host addresses: %v", addresses)

	var addr string
	for _, v := range addresses {
		for _, m := range members {
			if v.String() == strings.Split(m, ":")[0] {
				addr = m
				break
			}
		}
		if addr != "" {
			break
		}
	}

	if addr == "" {
		panic("can't find host address")
	}
	return addr
}
