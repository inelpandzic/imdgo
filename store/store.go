// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string      `json:"op,omitempty"`
	Key   string      `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	nodeID   string
	raftDir  string
	raftBind string // addr can be with the port, 1.1.1.1:3333
	members  []string
	inmem    bool

	mu sync.RWMutex
	m  map[string]interface{} // The key-value store for the system.

	raft   *raft.Raft   // The consensus mechanism
	server *http.Server // server for sending writes to the leader
	logger *zap.Logger
}

// New returns a new Store.
func New(raftDir, raftBind string, members []string) *Store {
	logger, _ := zap.NewDevelopment() // TODO: use proper logger here

	return &Store{
		nodeID:   nodeID(raftBind),
		m:        make(map[string]interface{}),
		inmem:    true,
		raftDir:  raftDir,
		raftBind: raftBind,
		members:  members,
		logger:   logger,
		server:   newServer(raftBind),
	}
}

// Open opens the store. If and there are no existing peers, meaning there is not a formed cluster,
// then this node becomes the first node, and therefore leader, of the cluster.
// nodeID should be the server identifier for this node.
// Call Close() to shut everything down.
func (s *Store) Open() error {
	nodeID := nodeID(s.raftBind)

	s.logger.Debug("opening the store", zap.String("node", nodeID))
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	addr, err := net.ResolveTCPAddr("tcp", s.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	snapshots, err := raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %w", err)
	}

	var logStore raft.LogStore
	var stableStore raft.StableStore
	if s.inmem {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
		if err != nil {
			return fmt.Errorf("new bolt store: %w", err)
		}
		logStore = boltDB
		stableStore = boltDB
	}

	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %w", err)
	}
	s.raft = ra

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	leader := s.raft.Leader()
	if leader == "" && len(configFuture.Configuration().Servers) == 0 {
		servers := getServers(s.members)
		configuration := raft.Configuration{
			Servers: servers,
		}

		s.logger.Debug("bootstrapping the cluster", zap.Any("members", servers))
		ra.BootstrapCluster(configuration)
	} else {
		s.logger.Sugar().Debugf("joining to existing cluster on leader: %s", leader)
		if err := s.join(nodeID, string(leader), configFuture); err != nil {
			return fmt.Errorf("failed to join to existing cluester: %w", err)
		}
	}

	if err := s.srvStart(); err != nil {
		return err
	}

	return nil
}

// join joins a node, identified by nodeID, through the current leader at addr, to the existing cluster.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) join(nodeID, addr string, configFuture raft.ConfigurationFuture) error {
	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s on leader %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	return nil
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (interface{}, error) {
	s.logger.Sugar().Debugf("get operation: key:%s", key)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[key], nil
}

// Set sets the value for the given key.
func (s *Store) Set(key string, value interface{}) error {
	s.logger.Sugar().Debugf("set operation: key:%s, val:%s", key, value)

	if s.raft.State() != raft.Leader {
		l := stripPort(string(s.raft.Leader()))
		s.logger.Sugar().Debugf("not leader, forwarding request to %s", l)
		return writeOnLeader(l, key, value)
	}

	c := &command{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	s.logger.Sugar().Debugf("delete operation: key:%s", key)

	if s.raft.State() != raft.Leader {
		l := stripPort(string(s.raft.Leader()))
		s.logger.Sugar().Debugf("not leader, forwarding request to %s", l)
		return deleteOnLeader(l, key)
	}

	c := &command{
		Op:  "delete",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return f.Error()
}

func (s *Store) Close() error {
	s.logger.Info("shutting down imdgo server")
	s.server.Close()

	s.logger.Info("shutting down imdgo raft")
	return s.raft.Shutdown().Error()
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	f.logger.Debug("applying fsm", zap.Any("log", l))

	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {

		f.logger.Sugar().Panicf("failed to unmarshal command: %s", err.Error())
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Debug("FSM snapshot")

	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]interface{})
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	f.logger.Debug("FSM restoring to a prev state")

	o := make(map[string]interface{})
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *fsm) applySet(key string, value interface{}) interface{} {
	f.logger.Sugar().Debugf("FSM applying set: key:%s, val:%s", key, value)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.logger.Sugar().Debugf("FSM applying delete: key:%s", key)

	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

type fsmSnapshot struct {
	store map[string]interface{}
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}

func getServers(members []string) []raft.Server {
	var servers []raft.Server

	for _, m := range members {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(nodeID(m)),
			Address: raft.ServerAddress(m),
		})
	}

	return servers
}

func nodeID(bindAddr string) string {
	return "node-" + bindAddr + "-edon"
}
