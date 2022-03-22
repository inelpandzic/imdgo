# IMDGO - In-Memory Data Grid in Go

Or simply - a light-weight distributed in-memory key-value store inspired primarily by Hazelcast.

IMDGO is built on top of Raft consensus protocol ([Hashicorp's implementation](https://github.com/hashicorp/raft)) which makes it a CP system in terms
of the CAP theorem. 

Because of the leader based replication with Raft, every write and delete operation go to the leader (in case request hits a follower node)
and then are replicated to follower.
Currently, these requests are forwarded via plain old HTTP with JSON, but it will be done with MessagePack or gRPC.

It uses [orcaman/concurrent-map](https://github.com/orcaman/concurrent-map) as an underlining map to reduce locking and contention as much as possible.

The project is work in progress, initial release is coming in the upcoming days.

Features like data partitioning (currently every node has all the data), item TTL and store management API are coming soon in the upcoming releases.

## Quickstart

Import the package:

```go
import (
    "github.com/inelpandzic/imdgo"
)
```
```shell
go get "github.com/inelpandzic/imdgo"
```

### Usage

```go
// set the members of your application cluster
c := &imdgo.Config{Members: []string{"192.168.10.1", "192.168.10.2", "192.168.10.3"}}
store, err := imdgo.New(c)
if err != nil {
    panic(err)
}

key := "my-key"

// You can set the value on one node or application instance
err := store.Set(key, "and my value which can be anything, not just string")
if err != nil {
    log.Error(err))
}

// And then you can get it on some other application instance, safely replicated
if val, ok := m.Get(key); ok {
    fmt.Println(val)
}
```

For now, imdgo runs with only default preconfigured ports `6701` and `6801` which needs to be available.

## Credits

Credit goes to [otoolep](https://github.com/otoolep) and his [hraft](https://github.com/otoolep/hraftd) for helping me get started quickly
with Hashicorp's Raft lib. There is code from hraft in imdgo, I don't change something that is good for the time being.
