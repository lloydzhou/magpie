// Package tail implements the tail transport (protocol v3): a request body is
// rendered into a stable one-dimensional text stream, split into fixed-size
// blocks, and only the bytes after a prefix the peer already holds are sent on
// the wire. This file owns the block cutting and the block hash chain.
//
// The scheme follows vLLM Automatic Prefix Caching:
//
//		Block 1: |<--- block -->|
//		Block 2: |<---- prefix ---->| |<--- block -->|
//
//	 1. Fixed-size blocks; boundaries depend on position only, never on content.
//	 2. block_hash = hash(parent_hash || block). The parent hash makes identical
//	    content under different prefixes get different ids -- that *is* the
//	    prefix semantics.
//	 3. Only full blocks enter the chain/cache.
//
// The granularity is bytes, not tokens, because the gateway side has no
// tokenizer. Block boundaries must be globally fixed: the same prefix has to
// cut at the same offsets in a 1 KB request and a 1 MB request, otherwise not a
// single block matches across turns.
package tail

import (
	"fmt"

	"golang.org/x/crypto/blake2b"
)

const (
	// BlockSize is the fixed block length in bytes.
	//
	// Why not vLLM's 16 tokens (~48 B for CJK): resolving a prefix walks one hop
	// per block from chain tail to root, so hops = context_len / block_size.
	// 829 KB at 512 B is 1,774 hops; at 48 B it is 17,706 hops (10x read cost)
	// and all it buys is resending ~400 B less tail per turn. Granularity is
	// set by walk cost, not by token size.
	BlockSize = 512

	// BlockIDSize is the block hash length: 128 bit. The reference requires a
	// cryptographically strong id; 16 bytes makes collisions negligible.
	BlockIDSize = 16
)

// BlockID is the hash of one full block, chained over its parent.
type BlockID [BlockIDSize]byte

// String renders the id the same way Python's bytes.hex() does; this is the
// form used on the wire (X-Tail-Have) and in storage keys.
func (id BlockID) String() string { return fmt.Sprintf("%x", id[:]) }

// IsZero reports whether the id is the all-zero sentinel used as chain root
// parent.
func (id BlockID) IsZero() bool { return id == BlockID{} }

// chainParentBytes is what the hash chain feeds before a block.
//
// The chain root's parent is *empty*, not sixteen zero bytes: the reference
// starts with parent = b"". The all-zero id is only the sentinel the storage
// layout stores for a root (parent_hex = "0" * 32), so the two forms must not be
// conflated or every root hash would differ.
func (id BlockID) chainParentBytes() []byte {
	if id.IsZero() {
		return nil
	}
	return id[:]
}

// Block is one full block plus the parent hash it was chained under.
//
// Parent is carried on the block (rather than being a parameter of PutChain)
// because the storage layout is "value = parent_hex || content": the parent
// pointer is part of the stored record, and the gateway can only walk the chain
// backwards from a tip, so the pointer has to be persisted per block.
type Block struct {
	ID     BlockID
	Parent BlockID
	Data   []byte
}

// Sum computes blake2b-128(parent || block), byte-identical to the Python
// reference's
//
//	hashlib.new("blake2b", digest_size=16); h.update(parent); h.update(block)
func Sum(parent, block []byte) BlockID {
	// blake2b.New only rejects sizes outside [1,64]; the constant is valid, so
	// an error here is a programming bug, not a runtime condition.
	h, err := blake2b.New(BlockIDSize, nil)
	if err != nil {
		panic("tail: blake2b: " + err.Error())
	}
	_, _ = h.Write(parent)
	_, _ = h.Write(block)
	var id BlockID
	h.Sum(id[:0])
	return id
}

// Blocks splits text into fixed-size full blocks starting from a zero root
// parent, and returns the short trailing bytes that are *not* part of the chain.
//
// Only full blocks enter the chain; the tail's hash would change on the next
// append, so caching it would be wrong.
func Blocks(text []byte) (full []Block, tail []byte) {
	var parent BlockID
	for len(text) >= BlockSize {
		data := text[:BlockSize]
		id := Sum(parent.chainParentBytes(), data)
		full = append(full, Block{ID: id, Parent: parent, Data: data})
		parent = id
		text = text[BlockSize:]
	}
	if len(text) > 0 {
		tail = text
	}
	return full, tail
}

// TextBlocks splits text into full blocks chained under an existing parent.
// It is the cold-start/test helper: the gateway uses it to pre-seed a chain.
func TextBlocks(parent BlockID, text []byte) []Block {
	var out []Block
	for len(text) >= BlockSize {
		data := text[:BlockSize]
		id := Sum(parent.chainParentBytes(), data)
		out = append(out, Block{ID: id, Parent: parent, Data: data})
		parent = id
		text = text[BlockSize:]
	}
	return out
}

// BlockHasher is a streaming block cutter. It keeps only the current partial
// block, so memory is O(BlockSize) regardless of request size while producing
// exactly the same boundaries and hashes as Blocks.
type BlockHasher struct {
	blockSize int
	buf       []byte
	parent    BlockID
}

// NewBlockHasher returns a hasher rooted at parent. The zero parent means "cut
// from the beginning"; a gateway resuming behind a stored prefix passes that
// prefix's tip so new hashes are naturally chained under it.
func NewBlockHasher(parent BlockID) *BlockHasher {
	return &BlockHasher{blockSize: BlockSize, parent: parent}
}

// Feed consumes p and emits every block that became full.
func (h *BlockHasher) Feed(p []byte) []Block {
	h.buf = append(h.buf, p...)
	var out []Block
	for len(h.buf) >= h.blockSize {
		data := make([]byte, h.blockSize)
		copy(data, h.buf[:h.blockSize])
		h.buf = h.buf[h.blockSize:]
		id := Sum(h.parent.chainParentBytes(), data)
		out = append(out, Block{ID: id, Parent: h.parent, Data: data})
		h.parent = id
	}
	return out
}

// Pending returns the bytes fed but not yet part of a full block, and resets
// the buffer.
//
// The client needs this: after finding a matching prefix boundary those bytes
// were already swallowed by Feed and must be handed to the body explicitly,
// otherwise that stretch of the tail is silently dropped.
func (h *BlockHasher) Pending() []byte {
	if len(h.buf) == 0 {
		return nil
	}
	out := h.buf
	h.buf = nil
	return out
}

// Flush emits the short trailing block, if any. Its hash is *not* chained into
// subsequent blocks, so callers must not cache it as reusable.
func (h *BlockHasher) Flush() []byte {
	out := h.buf
	h.buf = nil
	return out
}
