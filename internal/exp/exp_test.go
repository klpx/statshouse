package exp

import (
	"encoding/binary"
	"testing"
	"unsafe"
)

const (
	nTags = 16
)

type Key struct {
	Tags  [nTags]int32
	Slice []byte
}

type Item struct {
	Key Key
	// Pre-computed string key to avoid allocations
	stringKey string

	SomeData [1000]byte
}

// newItem creates a new Item with pre-computed string key
func newItem(key Key) *Item {
	item := &Item{Key: key}
	return item
}

// requiredBufferSize calculates the required size for the key buffer
func requiredBufferSize(key *Key) int {
	return nTags*4 + len(key.Slice)
}

// GetKey returns the pre-computed string key
func (mi *Item) GetKey() string {
	return mi.stringKey
}

// MapCache provides a way to look up Items by Key
type MapCache struct {
	items   map[string]*Item
	tmpHeap []byte
}

// NewMapCache creates a new MapCache
func NewMapCache() *MapCache {
	return &MapCache{
		items: make(map[string]*Item),
	}
}

// getTempBuffer returns a buffer suitable for temporary key computation
func (mc *MapCache) getTempBuffer(size int) []byte {
	if cap(mc.tmpHeap) >= size {
		mc.tmpHeap = mc.tmpHeap[:size]
		return mc.tmpHeap
	}
	mc.tmpHeap = make([]byte, size)
	return mc.tmpHeap
}

// Get returns an existing item or creates a new one if it doesn't exist
func (mc *MapCache) Get(key Key) *Item {
	size := requiredBufferSize(&key)
	buf := mc.getTempBuffer(size)

	// Compute key string using appropriate buffer
	for i := 0; i < nTags; i++ {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(key.Tags[i]))
	}
	copy(buf[nTags*4:], key.Slice)
	keyStr := unsafe.String(unsafe.SliceData(buf), size)

	// Look up or create item
	item, ok := mc.items[keyStr]
	if !ok {
		item = newItem(key)
		mc.items[keyStr] = item
	}
	return item
}

func BenchmarkMapCacheMultiItem(b *testing.B) {
	b.Run("small-keys", func(b *testing.B) {
		cache := NewMapCache()
		keys := make([]Key, 1000)
		for i := 0; i < 1000; i++ {
			keys[i] = Key{[nTags]int32{1, 2, 3}, []byte("some_data")}
		}

		// Pre-warm the cache
		for _, key := range keys {
			_ = cache.Get(key)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, key := range keys {
				_ = cache.Get(key)
			}
		}
	})

	b.Run("large-keys", func(b *testing.B) {
		cache := NewMapCache()
		keys := make([]Key, 1000)
		largeSlice := make([]byte, 1000) // slice larger than inlineKeySize
		for i := 0; i < 1000; i++ {
			keys[i] = Key{[nTags]int32{1, 2, 3}, largeSlice}
		}

		// Pre-warm the cache
		for _, key := range keys {
			_ = cache.Get(key)
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, key := range keys {
				_ = cache.Get(key)
			}
		}
	})
}
