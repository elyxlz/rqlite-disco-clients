package dht

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/dht/v2"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/rqlite/rqlite-disco-clients/expand"
)

const (
	// DHT key override environment variable.
	DHTOverrideEnv = "RQLITE_DISCO_DHT_HOSTS"

	// DefaultKey is the default key used to identify rqlite nodes in the DHT.
	DefaultKey = "rqlite"

	// DefaultPort is the default HTTP port for rqlite.
	DefaultPort = 4001

	// DefaultAnnounceEvery is how often to announce to the DHT.
	DefaultAnnounceEvery = 20 * time.Second

	// DefaultQueryEvery is how often to query the DHT for peers.
	DefaultQueryEvery = 7 * time.Second

	// DefaultRoutingTarget is the minimum number of nodes to have in the routing
	// table before considering it warmed up.
	DefaultRoutingTarget = 25

	// DefaultRoutingTimeout is how long to wait for the routing table to warm up
	// before proceeding anyway.
	DefaultRoutingTimeout = 60 * time.Second
)

// Client provides BitTorrent DHT-based discovery for rqlite.
type Client struct {
	key  string
	port int
	cfg  *Config

	mu            sync.Mutex
	lastContact   time.Time
	lastAddresses []string
	lastError     error
	logger        *log.Logger
	close         context.CancelFunc
	ctx           context.Context
	dht           *dht.Server
}

// NewConfigFromReader returns a Client configuration from the data read
// from r. If r is nil, a nil Configuration is returned.
func NewConfigFromReader(r io.Reader) (*Config, error) {
	if r == nil {
		return nil, nil
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(expand.ExpandEnvBytes(b), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// New returns an instantiated DHT client. If the cfg is nil, the default
// config is used.
func New(cfg *Config) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	client := &Client{
		key:    DefaultKey,
		port:   DefaultPort,
		cfg:    cfg,
		logger: log.New(os.Stderr, "[disco-dht] ", log.LstdFlags),
		ctx:    ctx,
		close:  cancel,
	}

	if cfg != nil {
		if cfg.Key != "" {
			client.key = cfg.Key
		}
		if cfg.Port != 0 {
			client.port = cfg.Port
		}
	}

	// For tests, we can continue without a real DHT connection
	if os.Getenv("RQLITE_DISCO_DHT_TESTS") == "1" {
		return client, nil
	}

	// Initialize DHT server
	if err := client.initDHT(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize DHT: %w", err)
	}

	go client.run()
	return client, nil
}

// initDHT initializes the DHT server with appropriate configuration
func (c *Client) initDHT() error {
	conf := dht.NewDefaultServerConfig()
	conf.StartingNodes = func() ([]dht.Addr, error) {
		var addrs []dht.Addr
		
		// Use bootstrap nodes from config or defaults
		bootstrapNodes := []string{
			"router.bittorrent.com:6881", 
			"router.utorrent.com:6881", 
			"dht.transmissionbt.com:6881",
		}
		
		if c.cfg != nil && len(c.cfg.Bootstrap) > 0 {
			bootstrapNodes = c.cfg.Bootstrap
		}
		
		for _, node := range bootstrapNodes {
			addr, err := net.ResolveUDPAddr("udp", node)
			if err != nil {
				c.logger.Printf("failed to resolve bootstrap node %s: %v", node, err)
				continue
			}
			addrs = append(addrs, dht.NewAddr(addr))
		}
		
		if len(addrs) == 0 {
			return nil, fmt.Errorf("no valid bootstrap nodes")
		}
		
		return addrs, nil
	}
	
	// Create the DHT server
	server, err := dht.NewServer(conf)
	if err != nil {
		return err
	}
	
	c.dht = server
	c.logger.Printf("DHT server started")
	
	return nil
}

// run is the main loop that announces and queries for peers
func (c *Client) run() {
	defer c.logger.Printf("DHT client stopped")
	
	// Wait for the routing table to warm up
	c.logger.Printf("waiting for DHT routing table to warm up...")
	warmupTimeout := time.NewTimer(DefaultRoutingTimeout)
	warmedUp := false
	
	for !warmedUp {
		select {
		case <-time.After(500 * time.Millisecond):
			if c.dht != nil && c.dht.Stats().Nodes >= DefaultRoutingTarget {
				c.logger.Printf("DHT routing table warmed up with %d nodes", c.dht.Stats().Nodes)
				warmedUp = true
			}
		case <-warmupTimeout.C:
			c.logger.Printf("DHT routing table warm-up timed out, proceeding anyway")
			warmedUp = true
		case <-c.ctx.Done():
			return
		}
	}
	
	// Get announcement and query intervals
	announceEvery := DefaultAnnounceEvery
	queryEvery := DefaultQueryEvery
	
	if c.cfg != nil {
		if c.cfg.AnnounceEvery > 0 {
			announceEvery = c.cfg.AnnounceEvery
		}
		if c.cfg.QueryEvery > 0 {
			queryEvery = c.cfg.QueryEvery
		}
	}
	
	// Create infohash from key
	sum := sha1.Sum([]byte(c.key))
	ih := metainfo.HashBytes(sum[:])
	
	// Set up tickers for periodic announcements and queries
	tAnnounce := time.NewTicker(announceEvery)
	tQuery := time.NewTicker(queryEvery)
	defer tAnnounce.Stop()
	defer tQuery.Stop()
	
	// Initial announce and query
	if c.dht != nil {
		c.announce(ih)
		c.query(ih)
	}
	
	for {
		select {
		case <-tAnnounce.C:
			if c.dht != nil {
				c.announce(ih)
			}
		case <-tQuery.C:
			if c.dht != nil {
				c.query(ih)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// announce announces our presence in the DHT
func (c *Client) announce(ih metainfo.Hash) {
	c.logger.Printf("announcing to DHT with key %s (infohash: %s)", c.key, ih.HexString())
	ann, err := c.dht.Announce(ih, c.port, true)
	if err != nil {
		c.logger.Printf("error announcing to DHT: %v", err)
		c.mu.Lock()
		c.lastError = err
		c.mu.Unlock()
		return
	}
	
	// Start a goroutine to collect peers from this announce
	go func() {
		defer ann.Close()
		
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		defer cancel()
		
		var peers []string
		
		// Collect peers from the announce
		for {
			select {
			case <-ctx.Done():
				return
			case peersValues, ok := <-ann.Peers:
				if !ok { // Channel closed
					return
				}
				for _, peer := range peersValues.Peers {
					addr := fmt.Sprintf("%s:%d", peer.IP.String(), peer.Port)
					peers = append(peers, addr)
				}
				
				if len(peers) > 0 {
					c.mu.Lock()
					
					// Merge the new addresses with existing ones
					for _, p := range peers {
						if !slices.Contains(c.lastAddresses, p) {
							c.lastAddresses = append(c.lastAddresses, p)
						}
					}
					
					slices.Sort(c.lastAddresses)
					c.logger.Printf("discovered %d DHT peers, total: %d", len(peers), len(c.lastAddresses))
					c.lastContact = time.Now()
					c.lastError = nil
					c.mu.Unlock()
				}
			}
		}
	}()
}

// query looks up peers for our infohash via a scrape-style announce
func (c *Client) query(ih metainfo.Hash) {
	c.logger.Printf("querying DHT for peers with infohash: %s", ih.HexString())
	
	// Use a simple announce with scrape option
	ann, err := c.dht.Announce(ih, 0, false, dht.Scrape())
	if err != nil {
		c.logger.Printf("error querying DHT: %v", err)
		c.mu.Lock()
		c.lastError = err
		c.mu.Unlock()
		return
	}
	
	// Start a goroutine to collect peers from the scrape
	go func() {
		defer ann.Close()
		
		ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
		defer cancel()
		
		var peers []string
		
		// Collect peers from the announce
		for {
			select {
			case <-ctx.Done():
				return
			case peersValues, ok := <-ann.Peers:
				if !ok { // Channel closed
					return
				}
				for _, peer := range peersValues.Peers {
					addr := fmt.Sprintf("%s:%d", peer.IP.String(), peer.Port)
					peers = append(peers, addr)
				}
				
				if len(peers) > 0 {
					c.mu.Lock()
					
					// Merge the new addresses with existing ones
					for _, p := range peers {
						if !slices.Contains(c.lastAddresses, p) {
							c.lastAddresses = append(c.lastAddresses, p)
						}
					}
					
					slices.Sort(c.lastAddresses)
					c.logger.Printf("scrape found %d DHT peers, total: %d", len(peers), len(c.lastAddresses))
					c.lastContact = time.Now()
					c.lastError = nil
					c.mu.Unlock()
				}
			}
		}
	}()
}

// Lookup returns the network addresses of nodes in the DHT with the same key.
// 
// If the environment variable RQLITE_DISCO_DHT_HOSTS is set, its value is used
// instead of DHT resolution. That value is a comma-separated list of addresses,
// each of which is a host:port pair. This is useful for testing and is not suitable
// for production use.
func (c *Client) Lookup() ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If there's an environment variable override, use that instead
	// of actual DHT queries.
	if val, ok := os.LookupEnv(DHTOverrideEnv); ok {
		addrs := make([]string, 0)
		for _, addr := range strings.Split(val, ",") {
			if addr == "" {
				continue
			}
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return nil, fmt.Errorf("%s: invalid address %s", DHTOverrideEnv, addr)
			}
			addrs = append(addrs, addr)
		}
		c.lastAddresses = addrs
		c.lastContact = time.Now()
		return addrs, nil
	}

	if c.lastError != nil {
		return nil, c.lastError
	}
	
	// If we have no addresses from DHT yet, return an empty list
	if len(c.lastAddresses) == 0 {
		c.logger.Printf("no addresses in DHT cache yet")
		return []string{}, nil
	}
	
	res := make([]string, len(c.lastAddresses))
	copy(res, c.lastAddresses)
	return res, nil
}

// Stats returns some basic diagnostics information about the client.
func (c *Client) Stats() (map[string]interface{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	sum := sha1.Sum([]byte(c.key))
	ih := metainfo.HashBytes(sum[:])
	
	stats := map[string]interface{}{
		"mode":      "dht",
		"key":       c.key,
		"port":      c.port,
		"info_hash": ih.HexString(),
	}
	
	if c.dht != nil {
		dhtStats := c.dht.Stats()
		stats["dht_nodes"] = dhtStats.Nodes
		stats["dht_good_nodes"] = dhtStats.GoodNodes
		stats["dht_queries"] = dhtStats.OutstandingTransactions
	}

	if c.lastError != nil {
		stats["last_error"] = c.lastError.Error()
	}
	if !c.lastContact.IsZero() {
		stats["last_contact"] = c.lastContact
	}
	if len(c.lastAddresses) > 0 {
		stats["last_addresses"] = c.lastAddresses
	}

	return stats, nil
}

// String returns the client name.
func (c *Client) String() string { 
	return "dht" 
}

// Close stops all client activities and shuts down the DHT server.
func (c *Client) Close() error {
	c.close()
	if c.dht != nil {
		c.logger.Printf("shutting down DHT server")
		c.dht.Close()
	}
	return nil
}

// For testing only
func (c *Client) addAddress(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Ensure we don't add duplicates
	for _, a := range c.lastAddresses {
		if a == addr {
			return
		}
	}
	
	c.lastAddresses = append(c.lastAddresses, addr)
	c.lastContact = time.Now()
	
	// Sort addresses for consistency
	slices.Sort(c.lastAddresses)
}