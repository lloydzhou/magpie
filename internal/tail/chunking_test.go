package tail

// Block cutting and hash-chain behaviour. These are the properties the whole
// cache depends on: if the boundaries or the chain shift between two runs, no
// block matches and every request is a full resend.

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// corpus builds a deterministic, non-periodic text long enough to span several
// blocks. Periodic content would hide boundary bugs.
func corpus(repeat int) []byte {
	words := []string{"分析", "数据", "结果", "模型", "输入", "输出", "评估", "指标", "样本",
		"训练", "推理", "上下文", "缓存", "传输", "协议", "分块", "网关", "客户端", "服务端"}
	var b bytes.Buffer
	for i := 0; i < repeat; i++ {
		b.WriteString(words[i%len(words)])
		if i%3 == 0 {
			b.WriteString("，")
		}
	}
	return b.Bytes()
}

func TestBlocksCoversInputExactly(t *testing.T) {
	text := corpus(400)
	full, tail := Blocks(text)
	if len(full) == 0 {
		t.Fatal("expected several full blocks")
	}
	var joined []byte
	for _, blk := range full {
		if len(blk.Data) != BlockSize {
			t.Fatalf("block length %d, want %d", len(blk.Data), BlockSize)
		}
		joined = append(joined, blk.Data...)
	}
	joined = append(joined, tail...)
	if !bytes.Equal(joined, text) {
		t.Fatal("blocks plus tail do not reconstruct the text")
	}
	if len(tail) >= BlockSize {
		t.Fatalf("tail is %d bytes, must be shorter than a block", len(tail))
	}
}

func TestBlocksEmptyInput(t *testing.T) {
	full, tail := Blocks(nil)
	if len(full) != 0 || len(tail) != 0 {
		t.Fatalf("empty input produced %d blocks and %d tail bytes", len(full), len(tail))
	}
}

// TestRootBlockParentIsEmptyBytes pins the reference's root rule: the chain
// starts at an empty parent (b""), not at sixteen zero bytes.
func TestRootBlockParentIsEmptyBytes(t *testing.T) {
	block := bytes.Repeat([]byte("a"), BlockSize)
	full, _ := Blocks(block)
	if len(full) != 1 {
		t.Fatalf("%d blocks, want 1", len(full))
	}
	if got := full[0].ID; got != Sum(nil, block) {
		t.Fatalf("root hash %s does not match blake2b(\"\" || block)", got)
	}
	if got := full[0].ID; got == Sum(make([]byte, BlockIDSize), block) {
		t.Fatal("root parent was hashed as 16 zero bytes; the reference uses an empty parent")
	}
	if !full[0].Parent.IsZero() {
		t.Fatalf("root parent id %s, want the zero sentinel", full[0].Parent)
	}
}

// TestChainedHashDependsOnParent is the prefix semantics: identical content
// under a different prefix must get a different id.
func TestChainedHashDependsOnParent(t *testing.T) {
	block := bytes.Repeat([]byte("x"), BlockSize)
	parentA := Sum(nil, []byte("prefix A"))
	parentB := Sum(nil, []byte("prefix B"))
	if Sum(parentA[:], block) == Sum(parentB[:], block) {
		t.Fatal("same block under different parents hashed the same")
	}
}

// TestHashesStableAcrossRounds checks that rendering the same request twice
// yields the same block ids -- the precondition for a second-round hit.
func TestHashesStableAcrossRounds(t *testing.T) {
	text := corpus(400)
	first, _ := Blocks(text)
	second, _ := Blocks(append([]byte(nil), text...))
	if len(first) != len(second) {
		t.Fatalf("%d vs %d blocks", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("block %d changed id between rounds", i)
		}
	}
}

// TestAppendOnlyKeepsPrefixIDs is what fixed-size blocks buy: appending text only
// adds blocks, it never renumbers the ones before it.
func TestAppendOnlyKeepsPrefixIDs(t *testing.T) {
	base := corpus(300)
	grown := append(append([]byte(nil), base...), corpus(220)...)
	before, _ := Blocks(base)
	after, _ := Blocks(grown)
	// The last block of `before` may not be full, so compare the full blocks.
	fullBefore := len(base) / BlockSize
	if fullBefore < 2 {
		t.Fatalf("need at least 2 full blocks, got %d", fullBefore)
	}
	for i := 0; i < fullBefore; i++ {
		if before[i].ID != after[i].ID {
			t.Fatalf("prefix block %d changed id after an append", i)
		}
	}
}

// TestHasherMatchesOneShot keeps the streaming cutter and the one-shot cutter on
// the same boundaries: client and gateway must agree or nothing ever matches.
func TestHasherMatchesOneShot(t *testing.T) {
	text := corpus(700)
	once, _ := Blocks(text)
	for _, step := range []int{1, 7, 256, 4096, len(text)} {
		hasher := NewBlockHasher(BlockID{})
		var streaming []Block
		for i := 0; i < len(text); i += step {
			end := i + step
			if end > len(text) {
				end = len(text)
			}
			streaming = append(streaming, hasher.Feed(text[i:end])...)
		}
		if len(streaming) != len(once) {
			t.Fatalf("step %d: %d blocks, want %d", step, len(streaming), len(once))
		}
		for i := range once {
			if streaming[i].ID != once[i].ID {
				t.Fatalf("step %d: block %d differs from the one-shot cut", step, i)
			}
		}
	}
}

// TestHasherPendingHandsBackPartialBlock guards the tail the client must send:
// bytes already swallowed by Feed have to come back out.
func TestHasherPendingHandsBackPartialBlock(t *testing.T) {
	hasher := NewBlockHasher(BlockID{})
	text := corpus(200)
	blocks := hasher.Feed(text)
	pending := hasher.Pending()
	var joined []byte
	for _, blk := range blocks {
		joined = append(joined, blk.Data...)
	}
	joined = append(joined, pending...)
	if !bytes.Equal(joined, text) {
		t.Fatal("blocks plus pending do not reconstruct the text")
	}
	if len(pending) != len(text)%BlockSize {
		t.Fatalf("pending %d bytes, want %d", len(pending), len(text)%BlockSize)
	}
}

// TestTextBlocksChainsUnderParent checks the cold-start helper, which the
// gateway uses to seed a chain.
func TestTextBlocksChainsUnderParent(t *testing.T) {
	parent := Sum(nil, []byte("seed"))
	text := corpus(300)
	blocks := TextBlocks(parent, text)
	if len(blocks) < 2 {
		t.Fatalf("%d blocks, want at least 2", len(blocks))
	}
	if blocks[0].Parent != parent {
		t.Fatal("first block is not chained under the given parent")
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i].Parent != blocks[i-1].ID {
			t.Fatalf("block %d is not chained under block %d", i, i-1)
		}
	}
}

// TestBlockIDString is the hex form used in the wire header and storage keys.
func TestBlockIDString(t *testing.T) {
	id := Sum(nil, []byte("hello"))
	if got, want := id.String(), hex.EncodeToString(id[:]); got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	if len(id.String()) != 32 {
		t.Fatalf("hex length %d, want 32", len(id.String()))
	}
}
