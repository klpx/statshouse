package exp

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/cornelk/hashmap"
	"github.com/dolthub/swiss"
)

const (
	nTags = 16
)

type Key struct {
	Tags  [nTags]int32
	Slice []byte
}

type Item struct {
	Key       Key
	stringKey string
	SomeData  [1000]byte
}

// Base implementation shared across different map types
func newItem(key Key) *Item {
	return &Item{Key: key}
}

func requiredBufferSize(key *Key) int {
	return nTags*4 + len(key.Slice)
}

func computeKeyString(buf []byte, key *Key) string {
	for i := 0; i < nTags; i++ {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(key.Tags[i]))
	}
	copy(buf[nTags*4:], key.Slice)
	return unsafe.String(unsafe.SliceData(buf), len(buf))
}

// Standard map implementation
type StdMapCache struct {
	items   map[string]*Item
	tmpHeap []byte
}

func NewStdMapCache() *StdMapCache {
	return &StdMapCache{
		items: make(map[string]*Item),
	}
}

func (mc *StdMapCache) getTempBuffer(size int) []byte {
	if cap(mc.tmpHeap) >= size {
		mc.tmpHeap = mc.tmpHeap[:size]
		return mc.tmpHeap
	}
	mc.tmpHeap = make([]byte, size)
	return mc.tmpHeap
}

func (mc *StdMapCache) Get(key Key) *Item {
	size := requiredBufferSize(&key)
	buf := mc.getTempBuffer(size)
	keyStr := computeKeyString(buf, &key)

	item, ok := mc.items[keyStr]
	if !ok {
		item = newItem(key)
		mc.items[keyStr] = item
	}
	return item
}

// Swiss map implementation
type SwissMapCache struct {
	items   *swiss.Map[string, *Item]
	tmpHeap []byte
}

func NewSwissMapCache() *SwissMapCache {
	return &SwissMapCache{
		items: swiss.NewMap[string, *Item](16), // initial size hint
	}
}

func (mc *SwissMapCache) getTempBuffer(size int) []byte {
	if cap(mc.tmpHeap) >= size {
		mc.tmpHeap = mc.tmpHeap[:size]
		return mc.tmpHeap
	}
	mc.tmpHeap = make([]byte, size)
	return mc.tmpHeap
}

func (mc *SwissMapCache) Get(key Key) *Item {
	size := requiredBufferSize(&key)
	buf := mc.getTempBuffer(size)
	keyStr := computeKeyString(buf, &key)

	item, ok := mc.items.Get(keyStr)
	if !ok {
		item = newItem(key)
		mc.items.Put(keyStr, item)
	}
	return item
}

// Cornelk hashmap implementation
type HashMapCache struct {
	items   *hashmap.Map[string, *Item]
	tmpHeap []byte
}

func NewHashMapCache() *HashMapCache {
	return &HashMapCache{
		items: hashmap.New[string, *Item](),
	}
}

func (mc *HashMapCache) getTempBuffer(size int) []byte {
	if cap(mc.tmpHeap) >= size {
		mc.tmpHeap = mc.tmpHeap[:size]
		return mc.tmpHeap
	}
	mc.tmpHeap = make([]byte, size)
	return mc.tmpHeap
}

func (mc *HashMapCache) Get(key Key) *Item {
	size := requiredBufferSize(&key)
	buf := mc.getTempBuffer(size)
	keyStr := computeKeyString(buf, &key)

	item, ok := mc.items.Get(keyStr)
	if !ok {
		item = newItem(key)
		mc.items.Set(keyStr, item)
	}
	return item
}

func BenchmarkMapImplementations(b *testing.B) {
	benchCases := []struct {
		name     string
		keySize  int
		numItems int
	}{
		{"small-keys-1k", 10, 1_000},
		{"large-keys-1k", 1000, 1_000},
		{"small-keys-10k", 10, 10_000},
		{"large-keys-10k", 1000, 10_000},
	}

	for _, bc := range benchCases {
		// Generate test data
		keys := make([]Key, bc.numItems)
		dataSlice := make([]byte, bc.keySize)
		for i := 0; i < bc.numItems; i++ {
			keys[i] = Key{[nTags]int32{1, 2, 3}, dataSlice}
		}

		b.Run("std-map-"+bc.name, func(b *testing.B) {
			cache := NewStdMapCache()
			// Pre-warm
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

		b.Run("swiss-map-"+bc.name, func(b *testing.B) {
			cache := NewSwissMapCache()
			// Pre-warm
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

		b.Run("hashmap-"+bc.name, func(b *testing.B) {
			cache := NewHashMapCache()
			// Pre-warm
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
}
