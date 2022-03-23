package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// newServer returns http.Server used for sending write operations
// to the leader as well as supporting imdgo management API
func newServer(hostAddr string) *http.Server {
	return &http.Server{
		Addr:         fmt.Sprintf("%s:%d", hostAddr, srvPort),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 90 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
}

// startServer will start up http server
func (s *S) startServer() error {
	s.logger.Sugar().Debugf("setting up handler and starting store server: %s", s.server.Addr)

	mux := http.NewServeMux()
	mux.HandleFunc("/imdgo/key", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Sugar().Debugf("got a write request from node %s", r.RemoteAddr)

		switch r.Method {
		case "POST":
			// Read the value from the POST body.
			m := map[string]string{}
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer r.Body.Close()
			
			for k, v := range m {
				if err := s.Set(k, v); err != nil {
					s.logger.Error("failed to store", zap.Error(err))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
		case "DELETE":
			buf := new(bytes.Buffer)
			buf.ReadFrom(r.Body)
			defer r.Body.Close()

			k := buf.String()
			if k == "" {
				s.logger.Error("failed to delete, missing key")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if err := s.Delete(k); err != nil {
				s.logger.Error("failed to delete", zap.Error(err))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	})


	s.server.Handler = mux

	go func() {
		err := s.server.ListenAndServe()
		if err != nil {
			if err != http.ErrServerClosed {
				s.logger.Panic("HTTP serve failed: %s", zap.Error(err))
			}
		}
	}()

	return nil
}

// writeOnLeader will send a set operation to the leader from a follower node
func writeOnLeader(leaderAddr string, key string, value interface{}) error {
	b, err := json.Marshal(map[string]interface{}{key: value})
	if err != nil {
		return err
	}

	e := fmt.Sprintf("http://%s:%d/imdgo/key", leaderAddr, srvPort)
	resp, err := http.Post(e, "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil

}

// deleteOnLeader will send a delete operation to the leader from a follower node
func deleteOnLeader(leaderAddr string, key string) error {
	e := fmt.Sprintf("http://%s:%d/imdgo/key", leaderAddr, srvPort)
	req, err := http.NewRequest(http.MethodDelete, e, strings.NewReader(key))
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

// stripPort returns only IP address without the trailing port number
func stripPort(a string) string {
	return strings.Split(a, ":")[0]
}
