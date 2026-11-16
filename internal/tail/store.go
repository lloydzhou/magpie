package tail

// Block storage: one key per block, value = parent hash || content.
//
//	blk:{block_hash_hex} -> parent_hex(32 ASCII) || content(<= BlockSize)
//
// Because block_hash = blake2b(parent_hash || block)[:16], the chain tail's hash
// is equivalent to "the entry point of the whole prefix": given a tip and a
// block count, the gateway walks backwards to the first block.
//
// This is exactly vLLM's hash rule; only the placement differs. vLLM's inference
// process holds the token sequence, so it can recompute the chain forward from
// block 0 and needs only a hash -> block_id table. A gateway by definition does
// not have the prefix (it only receives (L, tip) plus the tail), so it must walk
// backwards and the parent pointer has to actually be stored. Those 32 bytes per
// block are the price of not seeing the full text, not redundancy.
//
// Read order: ReadPrefix returns head -> tail. A storage layer that returns the
// walk order (tail first) would not error anywhere; it would just splice the
// context backwards further up.

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// ErrMissingPrefix means the chain behind a tip cannot be fully walked: a block
// was evicted, the store was cleared, or the process restarted.
//
// It is recoverable: the server answers 409 with X-Tail-Retransmit so the client
// resends from 0. It is never a reason to assemble a prefix with a hole in it.
var ErrMissingPrefix = errors.New("tail: prefix missing")

// Store is the chained block store.
type Store interface {
	// PutChain writes blocks in order; the first block's parent is parent and
	// every following block's parent is the previous block's id.
	PutChain(ctx context.Context, parent BlockID, ids []BlockID, contents [][]byte) error

	// ReadPrefix walks n blocks back from tip and returns their contents
	// head -> tail. A missing block anywhere yields ErrMissingPrefix.
	ReadPrefix(ctx context.Context, tip []byte, n int) ([][]byte, error)

	// Clear drops every block this store holds.
	Clear(ctx context.Context) error
}

// NewMemoryStore returns an in-process chain store. maxBytes caps the bytes kept
// (0 means no cap); eviction is least-recently-used, and a partial chain simply
// reads back as ErrMissingPrefix, which is the same state a TTL expiry
// produces in the Redis layout.
func NewMemoryStore(maxBytes int64) Store {
	return &MemoryStore{
		maxBytes: maxBytes,
		lru:      list.New(),
		items:    make(map[BlockID]*list.Element),
	}
}

// MemoryStore is the in-process Store. A single magpie process needs nothing
// more: the credential table lives in the client's memory and the block cache
// here. The Redis backend in the reference is a multi-process concern.
type MemoryStore struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	lru      *list.List // front = most recently used
	items    map[BlockID]*list.Element
}

type memBlock struct {
	id     BlockID
	parent BlockID
	data   []byte
}

// PutChain implements Store.
func (s *MemoryStore) PutChain(ctx context.Context, parent BlockID, ids []BlockID, contents [][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) != len(contents) {
		return errors.New("tail: PutChain: ids and contents differ in length")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tip := parent
	for i, id := range ids {
		data := contents[i]
		if elem, ok := s.items[id]; ok {
			// Re-writing the same id: the chain position is identical by
			// construction, only the data is refreshed.
			entry := elem.Value.(*memBlock)
			s.curBytes += int64(len(data)) - int64(len(entry.data))
			entry.data = data
			entry.parent = tip
			s.lru.MoveToFront(elem)
		} else {
			elem := s.lru.PushFront(&memBlock{id: id, parent: tip, data: data})
			s.items[id] = elem
			s.curBytes += int64(len(data))
		}
		tip = id
	}
	s.evictLocked()
	return nil
}

// evictLocked drops least-recently-used blocks until the byte budget is met.
func (s *MemoryStore) evictLocked() {
	if s.maxBytes <= 0 {
		return
	}
	for s.curBytes > s.maxBytes && s.lru.Len() > 0 {
		elem := s.lru.Back()
		entry := elem.Value.(*memBlock)
		s.lru.Remove(elem)
		delete(s.items, entry.id)
		s.curBytes -= int64(len(entry.data))
	}
}

// ReadPrefix implements Store.
//
// The returned slices are the stored bytes, not copies; callers must not modify
// them. They are concatenated straight into the request text.
func (s *MemoryStore) ReadPrefix(ctx context.Context, tip []byte, n int) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n <= 0 {
		return [][]byte{}, nil
	}
	if len(tip) != BlockIDSize {
		return nil, ErrMissingPrefix
	}
	var cur BlockID
	copy(cur[:], tip)

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		elem, ok := s.items[cur]
		if !ok {
			return nil, ErrMissingPrefix
		}
		entry := elem.Value.(*memBlock)
		s.lru.MoveToFront(elem)
		out = append(out, entry.data)
		cur = entry.parent
	}
	// The walk produced tail -> head; hand back head -> tail.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// Clear implements Store.
func (s *MemoryStore) Clear(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[BlockID]*list.Element)
	s.lru.Init()
	s.curBytes = 0
	return nil
}

// Len is the number of blocks held. Test and diagnostics use only.
func (s *MemoryStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// Bytes is the number of content bytes held. Test and diagnostics use only.
func (s *MemoryStore) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.curBytes
}
