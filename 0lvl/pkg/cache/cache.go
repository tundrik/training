package cache

//https://github.com/golang/go/issues/9477
//https://discuss.dgraph.io/t/the-state-of-caching-in-go-dgraph-blog/4157/18
//https://github.com/VictoriaMetrics/fastcache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
	xxhash "github.com/cespare/xxhash/v2"
)


const bucketsCount = 512

const chunkSize = 64 * 1024

const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

const maxBucketSize uint64 = 1 << bucketSizeBits


// Используйте Cache.UpdateStats для получения свежей статистики из кеша.
type Stats struct {
	GetCalls uint64
	SetCalls uint64

    // Misses — количество промахов в кэше.
	Misses uint64

    // Collisions — количество коллизий кэша.
    //
    // Обычно количество коллизий должно быть близко к нулю.
    // Большое количество коллизий указывает на проблемы с кешем.
	Collisions uint64
	Сorruptions uint64

	EntriesCount uint64
	AllocBytes uint64
	MaxBytes uint64
}

func (s *Stats) Reset() {
	*s = Stats{}
}

// Кэш — это быстрый потокобезопасный кеш в памяти, оптимизированный для большого количества
// записей.
//
// Он оказывает гораздо меньшее влияние на сборщик мусора по сравнению с простым `map[string][]byte`.
//
// Параллельные горутины могут вызывать любые методы Cache в одном и том же экземпляре Cache.
//
// Вызовите Reset, когда кеш больше не нужен. Это возвращает выделенную память.
type Cache struct {
	buckets [bucketsCount]bucket
}

// Если maxBytes меньше 32 МБ, то минимальная емкость кэша составляет 32 МБ.
func New(maxBytes uint64) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes)
	}
	var c Cache
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	for i := range c.buckets[:] {
		err := c.buckets[i].Init(maxBucketBytes); if err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// Сохраненная запись может быть удалена в любой момент либо из-за 
// переполнения кэша или из-за маловероятной коллизии хешей.
//
// (k, v) записи, общий размер которых превышает 64 КБ, не сохраняются в кеше.
func (c *Cache) Set(k, v []byte) error {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Set(k, v, h)
}

// Get добавляет значение по ключу k в dst и возвращает результат.
//
// Get выделяет новый фрагмент байта для возвращаемого значения, если dst равен нулю.
func (c *Cache) Get(dst, k []byte) []byte {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	dst, _ = c.buckets[idx].Get(dst, k, h, true)
	return dst
}

// HasGet работает идентично Get, но также возвращает, существует ли данный ключ в кеше.
// Этот метод позволяет дифференцировать
// сохраненное нулевое/пустое значение по сравнению с несуществующим значением.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Get(dst, k, h, true)
}

// Has возвращает true, если запись для данного ключа k существует в кеше.
func (c *Cache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_, ok := c.buckets[idx].Get(nil, k, h, false)
	return ok
}

// Del удаляет значение для данного k из кеша.
func (c *Cache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Del(h)
}

// Reset удаляет все элементы из кэша.
func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
}

// UpdateStats добавляет статистику кэша в s.
//
// Вызов s.Reset перед вызовом UpdateStats, если s используется повторно.
func (c *Cache) UpdateStats(s *Stats) {
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s)
	}
}

type bucket struct {
	mu sync.RWMutex

    // chunks — это кольцевой буфер с закодированными парами (k, v).
    // Он состоит из блоков по 64 КБ.
	chunks [][]byte

	// m сопоставляет hash(k) с idx пары (k, v) в chunks.
	m map[uint64]uint64

	// idx указывает на фрагменты для записи следующей пары (k, v).
	idx uint64

	// gen — генерация фрагментов.
	gen uint64

	getCalls    uint64
	setCalls    uint64
	misses      uint64
	collisions  uint64
	corruptions uint64
}

func (b *bucket) Init(maxBytes uint64) error {
	if maxBytes == 0 {
		return fmt.Errorf("maxBytes cannot be zero")
	}
	if maxBytes >= maxBucketSize {
		return fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize)
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	b.Reset()
	return nil
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	atomic.StoreUint64(&b.corruptions, 0)
	b.mu.Unlock()
}

func (b *bucket) cleanLocked() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	for k, v := range bm {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
			continue
		}
		delete(bm, k)
	}
}

func (b *bucket) UpdateStats(s *Stats) {
	s.GetCalls += atomic.LoadUint64(&b.getCalls)
	s.SetCalls += atomic.LoadUint64(&b.setCalls)
	s.Misses += atomic.LoadUint64(&b.misses)
	s.Collisions += atomic.LoadUint64(&b.collisions)
	s.Сorruptions += atomic.LoadUint64(&b.corruptions)

	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	bytesSize := uint64(0)
	for _, chunk := range b.chunks {
		bytesSize += uint64(cap(chunk))
	}
	s.AllocBytes += bytesSize
	s.MaxBytes += uint64(len(b.chunks)) * chunkSize
	b.mu.RUnlock()
}

func (b *bucket) Set(k, v []byte, h uint64) error {
	atomic.AddUint64(&b.setCalls, 1)
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
        // Слишком большой ключ или значение — его длину невозможно закодировать
        // с 2 байтами (см. ниже). Пропустить запись.
		return fmt.Errorf("set max len k or v")
	}
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	if kvLen >= chunkSize {
		return fmt.Errorf("chunk max 64KB; len k and v: %d", kvLen)
	}

	chunks := b.chunks
	needClean := false
	b.mu.Lock()
	idx := b.idx
	idxNew := idx + kvLen
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	if chunkIdxNew > chunkIdx {
		if chunkIdxNew >= uint64(len(chunks)) {
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			needClean = true
		} else {
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}
	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0]
	}
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	chunks[chunkIdx] = chunk
	b.m[h] = idx | (b.gen << bucketSizeBits)
	b.idx = idxNew
	if needClean {
		b.cleanLocked()
	}
	b.mu.Unlock()
	return nil
}

func (b *bucket) Get(dst, k []byte, h uint64, returnDst bool) ([]byte, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	found := false
	chunks := b.chunks
	b.mu.RLock()
	v := b.m[h]
	bGen := b.gen & ((1 << genSizeBits) - 1)
	if v > 0 {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == maxGen && bGen == 1 && idx >= b.idx {
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// Corrupted data. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize
			if idx+4 >= chunkSize {
				// Corrupted data. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				// Corrupted data. Just skip it.
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			// https://github.com/VictoriaMetrics/fastcache/issues/59
			if string(k) == string(chunk[idx:idx+keyLen]) {
				idx += keyLen
				if returnDst {
					dst = append(dst, chunk[idx:idx+valLen]...)
				}
				found = true
			} else {
				atomic.AddUint64(&b.collisions, 1)
			}
		}
	}
end:
	b.mu.RUnlock()
	if !found {
		atomic.AddUint64(&b.misses, 1)
	}
	return dst, found
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}

const chunksPerAlloc = 1024

var (
	freeChunks     []*[chunkSize]byte
	freeChunksLock sync.Mutex
)

func getChunk() []byte {
	freeChunksLock.Lock()
	if len(freeChunks) == 0 {
        // Выделяем внекучную память, чтобы GOGC не учитывал размер кэша.
        // Это должно уменьшить потерю свободной памяти.
		data, err := unix.Mmap(-1, 0, chunkSize*chunksPerAlloc, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", chunkSize*chunksPerAlloc, err))
		}
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			freeChunks = append(freeChunks, p)
			data = data[chunkSize:]
		}
	}
	n := len(freeChunks) - 1
	p := freeChunks[n]
	freeChunks[n] = nil
	freeChunks = freeChunks[:n]
	freeChunksLock.Unlock()
	return p[:]
}

func putChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	freeChunksLock.Lock()
	freeChunks = append(freeChunks, p)
	freeChunksLock.Unlock()
}