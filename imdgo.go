package imdgo

import (
	"github.com/inelpandzic/imdgo/store"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
)

const port = "12000"

type Config struct {
	Members []string
}

type Cache struct {
	s *store.Store
}

func New(conf *Config) (*Cache, error) {
	ex, err := os.Executable()
	if err != nil {
		return nil, err
	}

	raftBind := getHostAddr(conf.Members)
	s := store.New(filepath.Dir(ex), raftBind, conf.Members)

	err = s.Open()
	if err != nil {
		return nil, err
	}

	c := &Cache{
		s: s,
	}

	return c, nil
}

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

func (c *Cache) Get(key string) (string, error){
	return c.s.Get(key)
}

func (c *Cache) Set(key, value string) error{
	return c.s.Set(key, value)
}

func (c *Cache) Delete(key string) error{
	return c.s.Delete(key)
}