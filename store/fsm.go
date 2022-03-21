package store

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	cmap "github.com/orcaman/concurrent-map"
	"go.uber.org/zap"
)

type fsm S

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
	for item := range f.m.IterBuffered() {
		o[item.Key] = item.Val
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

	// Set the state from the snapshot, no lock required according to Hashicorp docs.
	cm := cmap.New()
	cm.MSet(o)
	f.m = cm

	return nil
}

func (f *fsm) applySet(key string, value interface{}) interface{} {
	f.logger.Sugar().Debugf("FSM applying set: key:%s, val:%s", key, value)
	f.m.Set(key, value)
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.logger.Sugar().Debugf("FSM applying delete: key:%s", key)

	f.m.Remove(key)
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
