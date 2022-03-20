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

const srvPort = 9999

func newServer(addr string) *http.Server {
	return &http.Server{
		Addr:         fmt.Sprintf("%s:%d", stripPort(addr), srvPort),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 90 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
}

func (s *Store) srvStart() error {
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
			for k, v := range m {
				if err := s.Set(k, v); err != nil {
					s.logger.Error("failed to store", zap.Error(err))
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
		case "DELETE":
			k := ""
			parts := strings.Split(r.URL.Path, "/")

			s.logger.Sugar().Debugf("AAAA PARTS = %v", parts)

			if len(parts) == 4 {
				k =  parts[3]
			}

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

func deleteOnLeader(leaderAddr string, key string) error {
	e := fmt.Sprintf("http://%s:%d/imdgo/key/%s", leaderAddr, srvPort, key)
	req, err := http.NewRequest(http.MethodDelete, e, nil)
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

func stripPort(a string) string {
	return strings.Split(a, ":")[0]
}
