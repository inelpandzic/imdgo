# IMDGO - In-Memory Data Grid in Go

Or simple - a light-weight distributed in-memory key-value store inspired primarily by Hazelcast.

IMDGO is built on top of Raft consensus protocol ([Hashicorp's implementation](https://github.com/hashicorp/raft)) which makes it a CP system in terms
of the CAP theorem. 

Currently, it is something like a POC and features like partitioning, key TTL and store management API are coming soon in the upcoming releases.

## Quickstart

```go
// set the members of your application cluster
c := &imdgo.Config{Members: []string{"192.168.10.1:12000", "192.168.10.2:12000"}}
store, err := imdgo.New(c)
if err != nil {
    panic(err)
}

key := "my-key"

// You set the value on one node or application instance
err := store.Set(key, "and my value which can be anything, not just string")
if err != nil {
    log.Error(err))
}

// And you can get it on some other application instance
val, err := store.Get(key)
if err != nil {
    log.Error(err))
}

fmt.Println(val)
```

## Credits

Credit goes to [otoolep](https://github.com/otoolep) and his [hraft](https://github.com/otoolep/hraftd) for helping me get started quickly
with Hashicorp's Raft lib. There is code from hraft in imdgo, I don't change something that is good for the time being.
