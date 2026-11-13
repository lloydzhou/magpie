package tail

// Chain store behaviour: the read order, the missing-block contract and
// eviction.

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestStoreReadRoundtripPreservesOrder(t *testing.T) {
	store := NewMemoryStore(0)
	text := corpus(400)
	blocks := TextBlocks(BlockID{}, text)
	if len(blocks) < 3 {
		t.Fatalf("%d blocks, want at least 3", len(blocks))
	}

	ids := make([]BlockID, len(blocks))
	contents := make([][]byte, len(blocks))
	for i, blk := range blocks {
		ids[i] = blk.ID
		contents[i] = blk.Data
	}
	if err := store.PutChain(context.Background(), BlockID{}, ids, contents); err != nil {
		t.Fatalf("PutChain: %v", err)
	}

	tip := ids[len(ids)-1]
	got, err := store.ReadPrefix(context.Background(), tip[:], len(ids))
	if err != nil {
		t.Fatalf("ReadPrefix: %v", err)
	}
	if len(got) != len(ids) {
		t.Fatalf("%d contents, want %d", len(got), len(ids))
	}
	for i := range ids {
		if !bytes.Equal(got[i], contents[i]) {
			t.Fatalf("content %d is out of order or corrupted", i)
		}
	}
	// The whole prefix has to concatenate back into the original text prefix; a
	// reversed return order would splice the context backwards and never error.
	if joined := bytes.Join(got, nil); !bytes.Equal(joined, text[:len(ids)*BlockSize]) {
		t.Fatal("concatenated prefix does not match the original text")
	}
}

// TestStoreRootParentIsZeroSentinel pins the storage form of the root: parent_hex
// is 32 ASCII zeros, so walking one block past the head must fail rather than
// read an unrelated block.
func TestStoreRootParentIsZeroSentinel(t *testing.T) {
	store := NewMemoryStore(0)
	blocks := TextBlocks(BlockID{}, corpus(300))
	ids := make([]BlockID, len(blocks))
	contents := make([][]byte, len(blocks))
	for i, blk := range blocks {
		ids[i] = blk.ID
		contents[i] = blk.Data
	}
	if err := store.PutChain(context.Background(), BlockID{}, ids, contents); err != nil {
		t.Fatalf("PutChain: %v", err)
	}
	tip := ids[len(ids)-1]

	if _, err := store.ReadPrefix(context.Background(), tip[:], len(ids)+1); !errors.Is(err, ErrMissingPrefix) {
		t.Fatalf("reading past the root: err = %v, want ErrMissingPrefix", err)
	}
}

// TestStoreMissingMiddleBlockIsPrefixMissing models an evicted middle block:
// the chain has a hole and must read as missing rather than splice around it.
func TestStoreMissingMiddleBlockIsPrefixMissing(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(0)
	blocks := TextBlocks(BlockID{}, corpus(300))
	if len(blocks) < 3 {
		t.Fatalf("%d blocks, want at least 3", len(blocks))
	}
	ids, contents := idsOf(blocks), dataOf(blocks)

	// Write the head, then the tail chained under the absent middle block.
	if err := store.PutChain(ctx, BlockID{}, ids[:1], contents[:1]); err != nil {
		t.Fatalf("PutChain head: %v", err)
	}
	if err := store.PutChain(ctx, ids[1], ids[2:], contents[2:]); err != nil {
		t.Fatalf("PutChain tail: %v", err)
	}

	tip := ids[len(ids)-1]
	if _, err := store.ReadPrefix(ctx, tip[:], len(ids)); !errors.Is(err, ErrMissingPrefix) {
		t.Fatalf("err = %v, want ErrMissingPrefix", err)
	}
	// The blocks that do exist still resolve on their own.
	if _, err := store.ReadPrefix(ctx, tip[:], len(ids)-2); err != nil {
		t.Fatalf("resolvable tail: %v", err)
	}
}

func TestStoreUnknownTipIsPrefixMissing(t *testing.T) {
	store := NewMemoryStore(0)
	unknown := Sum(nil, []byte("never stored"))
	if _, err := store.ReadPrefix(context.Background(), unknown[:], 1); !errors.Is(err, ErrMissingPrefix) {
		t.Fatalf("err = %v, want ErrMissingPrefix", err)
	}
}

func TestStorePartialLengthAndEmpty(t *testing.T) {
	store := NewMemoryStore(0)
	blocks := TextBlocks(BlockID{}, corpus(300))
	ids := make([]BlockID, len(blocks))
	contents := make([][]byte, len(blocks))
	for i, blk := range blocks {
		ids[i] = blk.ID
		contents[i] = blk.Data
	}
	if err := store.PutChain(context.Background(), BlockID{}, ids, contents); err != nil {
		t.Fatalf("PutChain: %v", err)
	}
	tip := ids[len(ids)-1]

	got, err := store.ReadPrefix(context.Background(), tip[:], 2)
	if err != nil {
		t.Fatalf("ReadPrefix: %v", err)
	}
	if len(got) != 2 || !bytes.Equal(got[0], contents[len(contents)-2]) ||
		!bytes.Equal(got[1], contents[len(contents)-1]) {
		t.Fatal("partial read did not return the last two blocks in head->tail order")
	}

	empty, err := store.ReadPrefix(context.Background(), tip[:], 0)
	if err != nil {
		t.Fatalf("ReadPrefix(0): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ReadPrefix(0) returned %d contents", len(empty))
	}
}

// TestStoreEvictionReadsAsPrefixMissing: an evicted head is exactly the state a
// TTL expiry produces in the Redis layout, and must surface as a missing prefix
// rather than a short one.
func TestStoreEvictionReadsAsPrefixMissing(t *testing.T) {
	blocks := TextBlocks(BlockID{}, corpus(400))
	ids := make([]BlockID, len(blocks))
	contents := make([][]byte, len(blocks))
	for i, blk := range blocks {
		ids[i] = blk.ID
		contents[i] = blk.Data
	}
	// A budget of two blocks evicts everything older.
	store := NewMemoryStore(2 * BlockSize)
	if err := store.PutChain(context.Background(), BlockID{}, ids, contents); err != nil {
		t.Fatalf("PutChain: %v", err)
	}
	if got := store.(*MemoryStore).Len(); got != 2 {
		t.Fatalf("store holds %d blocks, want 2", got)
	}
	tip := ids[len(ids)-1]
	if _, err := store.ReadPrefix(context.Background(), tip[:], len(ids)); !errors.Is(err, ErrMissingPrefix) {
		t.Fatalf("err = %v, want ErrMissingPrefix", err)
	}
	if _, err := store.ReadPrefix(context.Background(), tip[:], 2); err != nil {
		t.Fatalf("the two surviving blocks must still resolve: %v", err)
	}
}

// TestStoreClearDropsEverything models the "store was cleared / process
// restarted" case.
func TestStoreClearDropsEverything(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(0)
	blocks := TextBlocks(BlockID{}, corpus(300))
	tip := blocks[len(blocks)-1].ID
	if err := store.PutChain(ctx, BlockID{}, idsOf(blocks), dataOf(blocks)); err != nil {
		t.Fatalf("PutChain: %v", err)
	}
	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := store.ReadPrefix(ctx, tip[:], len(blocks)); !errors.Is(err, ErrMissingPrefix) {
		t.Fatalf("err = %v, want ErrMissingPrefix", err)
	}
	if got := store.(*MemoryStore).Len(); got != 0 {
		t.Fatalf("store still holds %d blocks", got)
	}
}

func TestStoreRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewMemoryStore(0)
	blocks := TextBlocks(BlockID{}, corpus(300))
	if err := store.PutChain(ctx, BlockID{}, idsOf(blocks), dataOf(blocks)); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutChain err = %v, want context.Canceled", err)
	}
	if _, err := store.ReadPrefix(ctx, blocks[0].ID[:], 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadPrefix err = %v, want context.Canceled", err)
	}
}

func idsOf(blocks []Block) []BlockID {
	ids := make([]BlockID, len(blocks))
	for i, blk := range blocks {
		ids[i] = blk.ID
	}
	return ids
}

func dataOf(blocks []Block) [][]byte {
	out := make([][]byte, len(blocks))
	for i, blk := range blocks {
		out[i] = blk.Data
	}
	return out
}
