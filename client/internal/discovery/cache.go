package discovery

import "sync"

// Cache remembers each peer's last working address. It is safe for concurrent use.
type Cache struct {
	mu sync.RWMutex
	m  map[string]string
}

// NewCache returns an empty address cache.
func NewCache() *Cache {
	return &Cache{m: make(map[string]string)}
}

// Get returns the cached address for nodeID, if any.
func (c *Cache) Get(nodeID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	addr, ok := c.m[nodeID]
	return addr, ok
}

// Put records addr as nodeID's last working address.
func (c *Cache) Put(nodeID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[nodeID] = addr
}

// Remove forgets nodeID's cached address.
func (c *Cache) Remove(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, nodeID)
}
