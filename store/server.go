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

const port = 9999

func (s *Store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	return
}

func srvStart(addr string, store *Store) error {
	a := fmt.Sprintf("%s:%d", addr, port)
	store.logger.Sugar().Debugf("starting store server: %s", a)

	store.server = &http.Server{
		Addr:         a,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 90 * time.Second,
		IdleTimeout:  120 * time.Second,
		Handler:      store,
	}

	go func() {
		err := store.server.ListenAndServe()
		if err != nil {
			if err != http.ErrServerClosed {
				store.logger.Panic("HTTP serve failed: %s", zap.Error(err))
			}
		}
	}()

	return nil
}

func writeOnLeader(leaderAddr string, key, value string) error {
	b, err := json.Marshal(map[string]string{key: value})
	if err != nil {
		return err
	}

	resp, err := http.Post(fmt.Sprintf("http://%s:%d/set", leaderAddr, port), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil

}

func stripPort(a string) string {
	return strings.Split(a, ":")[0]
}
