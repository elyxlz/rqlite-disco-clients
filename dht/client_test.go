package dht

import (
	"os"
	"strings"
	"testing"
)

func setup() {
	// Set the test environment variable to avoid actual DHT connections during tests
	os.Setenv("RQLITE_DISCO_DHT_TESTS", "1")
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func Test_ClientOverride(t *testing.T) {
	os.Setenv(DHTOverrideEnv, "10.0.0.1:4002,10.0.0.2:4002")
	defer os.Unsetenv(DHTOverrideEnv)

	c, err := New(&Config{Key: "unit-test", Port: 4002})
	if err != nil {
		t.Fatalf("failed to create client: %s", err)
	}
	defer c.Close()
	
	addrs, err := c.Lookup()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(addrs) != 2 || addrs[0] != "10.0.0.1:4002" || addrs[1] != "10.0.0.2:4002" {
		t.Fatalf("wrong peers: %v", addrs)
	}
}

func Test_EmptyAddressList(t *testing.T) {
	c, err := New(&Config{Key: "unit-test", Port: 4002})
	if err != nil {
		t.Fatalf("failed to create client: %s", err)
	}
	defer c.Close()

	// No addresses will have been discovered in the short time since starting
	addrs, err := c.Lookup()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	
	// In the implementation we return an empty array, not nil
	if addrs == nil {
		t.Fatalf("expected empty array, got nil")
	}
	
	if len(addrs) != 0 {
		t.Fatalf("expected empty addresses, got: %v", addrs)
	}
}

func Test_Config(t *testing.T) {
	cfg, err := NewConfigFromReader(strings.NewReader(`{"key":"test-key", "port": 1234}`))
	if err != nil {
		t.Fatalf("failed to read config: %s", err)
	}
	
	if cfg.Key != "test-key" {
		t.Errorf("expected key 'test-key', got '%s'", cfg.Key)
	}
	
	if cfg.Port != 1234 {
		t.Errorf("expected port 1234, got %d", cfg.Port)
	}
}

func Test_Stats(t *testing.T) {
	c, err := New(&Config{Key: "unit-test", Port: 4002})
	if err != nil {
		t.Fatalf("failed to create client: %s", err)
	}
	defer c.Close()
	
	c.addAddress("1.2.3.4:4002")
	
	stats, err := c.Stats()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	
	if stats["mode"] != "dht" {
		t.Errorf("wrong mode: %v", stats["mode"])
	}
	
	if stats["key"] != "unit-test" {
		t.Errorf("wrong key: %v", stats["key"])
	}
	
	if stats["port"] != 4002 {
		t.Errorf("wrong port: %v", stats["port"])
	}
	
	addrs, ok := stats["last_addresses"].([]string)
	if !ok || len(addrs) != 1 || addrs[0] != "1.2.3.4:4002" {
		t.Errorf("wrong last_addresses: %v", stats["last_addresses"])
	}
}

func Test_String(t *testing.T) {
	c, err := New(&Config{Key: "unit-test", Port: 4002})
	if err != nil {
		t.Fatalf("failed to create client: %s", err)
	}
	defer c.Close()
	
	if c.String() != "dht" {
		t.Errorf("wrong string: %s", c.String())
	}
}