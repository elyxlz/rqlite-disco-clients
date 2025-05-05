package dht

import "time"

const (
	// exampleConfig is an example of how the DHT config file
	// should be structured. In this example 'rqlite-cluster'
	// is the key shared by nodes in the same rqlite cluster.
	// All nodes looking to join the same cluster must use the
	// same key. Port is the HTTP[S] port those nodes will be 
	// listening on.
	exampleConfig = `
{
	"key": "rqlite-cluster",
	"port": 4001,
	"bootstrap": [
		"router.bittorrent.com:6881",
		"router.utorrent.com:6881",
		"dht.transmissionbt.com:6881"
	],
	"announce_every": "30s",
	"query_every": "10s"
}
`
)

// Config is the configuration for a DHT disco client.
type Config struct {
	// Key is the shared string used to identify cluster nodes in the DHT.
	Key string `json:"key,omitempty"`

	// Port is the HTTP port that nodes are listening on.
	Port int `json:"port,omitempty"`

	// Bootstrap servers for DHT network.
	Bootstrap []string `json:"bootstrap,omitempty"`

	// AnnounceEvery controls how often nodes announce to DHT.
	AnnounceEvery time.Duration `json:"announce_every,omitempty"`

	// QueryEvery controls how often nodes query for peers.
	QueryEvery time.Duration `json:"query_every,omitempty"`
}