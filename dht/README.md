# DHT Discovery for rqlite

This package provides DHT-based discovery for rqlite, allowing nodes to discover each other through the BitTorrent Distributed Hash Table network.

## How it works

DHT discovery works by having each rqlite node:

1. Use a shared key (configurable, defaults to "rqlite")
2. Hash this key to create a BitTorrent infohash
3. Announce its presence in the DHT network with this infohash
4. Periodically query the DHT for other peers using the same infohash

This allows nodes to find each other without requiring DNS or a centralized service like Consul or etcd.

## Configuration

Configuration is via a JSON file:

```json
{
    "key": "my-rqlite-cluster",
    "port": 4001,
    "bootstrap": [
        "router.bittorrent.com:6881",
        "router.utorrent.com:6881", 
        "dht.transmissionbt.com:6881"
    ],
    "announce_every": "30s",
    "query_every": "10s"
}
```

The configuration options are:

- `key`: A string shared by all nodes in the cluster (default: "rqlite")
- `port`: The HTTP port that rqlite listens on (default: 4001)
- `bootstrap`: DHT bootstrap nodes (default: standard BitTorrent bootstrap nodes)
- `announce_every`: How often to announce to the DHT (default: 20s)
- `query_every`: How often to query for peers (default: 7s)

## Environment Variables

- `RQLITE_DISCO_DHT_HOSTS`: A comma-separated list of host:port pairs to use for discovery. When set, the DHT discovery is bypassed, and these addresses are used directly.

## Example Usage

With rqlite:

```bash
rqlited -disco-mode dht -disco-config /path/to/dht.json /path/to/data/dir
```

In code:

```go
import "github.com/rqlite/rqlite-disco-clients/dht"

// Create with default settings
client, err := dht.New(nil)

// Or with a config
client, err := dht.New(&dht.Config{
    Key: "my-cluster",
    Port: 4001,
})

// Get peer addresses
addresses, err := client.Lookup()
```

## Security Considerations

- DHT discovery does not provide authentication
- Use a unique key for your cluster, not the default
- Consider using rqlite's authentication and TLS features
- For production use, restrict incoming UDP connections to trusted networks