package store

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/orcaman/concurrent-map"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	srvPort             = 6801
	raftPort            = 6701
)

// command will be applied by raft FSM
type command struct {
	Op    string      `json:"op,omitempty"`
	Key   string      `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
}

// S is an in-memory key-value store, where all changes are made via Raft consensus.
type S struct {
	nodeID   string
	raftDir  string
	raftBind string   // IP address with raft port
	members  []string // IP addresses  with raft port
	inmem    bool

	m cmap.ConcurrentMap
	raft   *raft.Raft   // the consensus mechanism
	server *http.Server // server for sending writes to the leader

	logger hclog.Logger
}

// New returns a new in-memory Store.
func New(raftDir, hostAddr string, members []string, logger hclog.Logger) *S {
    if logger == nil {
        o := hclog.DefaultOptions
        o.JSONFormat = false
        o.Level = hclog.Info
        logger = hclog.New(o)
    }

	return &S{
		nodeID:   nodeID(hostAddr),
		m:        cmap.New(),
		inmem:    true,
		raftDir:  raftDir,
		raftBind: fmt.Sprintf("%s:%d", hostAddr, raftPort),
		members:  members,
		logger:   logger,
		server:   newServer(hostAddr),
	}
}

// Open opens the store. If and there are no existing peers, meaning there is not a formed cluster,
// then this node becomes the first node, and therefore leader, of the cluster.
func (s *S) Open() error {
	s.logger.Debug("opening the store", "node", s.nodeID)

	config := raft.DefaultConfig()
    config.Logger = s.logger

	config.LocalID = raft.ServerID(s.nodeID)

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

	// bootstrap the cluster or join the existing one
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

		s.logger.Debug("bootstrapping the cluster", "members", servers)
		ra.BootstrapCluster(configuration)
	} else {
		s.logger.Debug(fmt.Sprintf("joining to existing cluster on leader: %s", leader))
		if err := s.join(s.nodeID, string(leader), configFuture); err != nil {
			return fmt.Errorf("failed to join to existing cluester: %w", err)
		}
	}

	if err := s.startServer(); err != nil {
		return err
	}

    s.logger.Info("IMDGO store started", "members", s.members)
	return nil
}

// join joins a node, identified by nodeID, through the current leader at addr, to the existing cluster.
// The node must be ready to respond to Raft communications at that address.
func (s *S) join(nodeID, addr string, configFuture raft.ConfigurationFuture) error {
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
func (s *S) Get(key string) (interface{}, bool) {
	s.logger.Debug("get operation:", "key", key)
	return s.m.Get(key)
}

// Set sets the value for the given key. If it gets called on a follower node
// the request will be forwarded to the leader.
func (s *S) Set(key string, value interface{}) error {
	s.logger.Debug("set operation: ", "key", key, "value", value)

	if s.raft.State() != raft.Leader {
		l := stripPort(string(s.raft.Leader()))
		s.logger.Debug(fmt.Sprintf("forwarding the request to leader at: %s", l))
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

// Delete deletes the given key. If it gets called on a follower node
// the request will be forwarded to the leader.
func (s *S) Delete(key string) error {
	s.logger.Debug("delete operation", "key", key)

	if s.raft.State() != raft.Leader {
		l := stripPort(string(s.raft.Leader()))
		s.logger.Debug(fmt.Sprintf("forwarding the request to leader at: %s", l))
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

func (s *S) Count() int {
	return s.m.Count()
}

// Close shuts down raft and store http server
func (s *S) Close() error {
	s.logger.Info("shutting down imdgo server")
	s.server.Close()

	s.logger.Info("shutting down imdgo raft")
	return s.raft.Shutdown().Error()
}

// getServers returns configured raft servers
func getServers(members []string) []raft.Server {
	var servers []raft.Server

	for _, m := range members {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(nodeID(m)),
			Address: raft.ServerAddress(fmt.Sprintf("%s:%d", m, raftPort)),
		})
	}

	return servers
}

// nodeID generates node ID.
func nodeID(hostAddr string) string {
	return "node-" + hostAddr + "-edon"
}
