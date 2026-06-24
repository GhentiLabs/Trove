package discovery

import "testing"

func TestCache(t *testing.T) {
	c := NewCache()
	if _, ok := c.Get("peer"); ok {
		t.Fatal("empty cache returned a hit")
	}
	c.Put("peer", "198.51.100.4:22000")
	if addr, ok := c.Get("peer"); !ok || addr != "198.51.100.4:22000" {
		t.Fatalf("Get = %q, %v", addr, ok)
	}
	c.Put("peer", "198.51.100.5:22000")
	if addr, _ := c.Get("peer"); addr != "198.51.100.5:22000" {
		t.Fatalf("Put did not overwrite: %q", addr)
	}
	c.Remove("peer")
	if _, ok := c.Get("peer"); ok {
		t.Fatal("Remove left an entry")
	}
}
