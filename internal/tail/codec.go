package tail

// Codec: prefix credentials on the client, chain walking on the server.
//
// Division of labour (the essential difference from v2):
//
//	client                        server
//	------                        ------
//	render text                   cut blocks, hash, write store
//	verify "the first L bytes     walk the chain from (L, tip)
//	  are unchanged" (local)      and return the prefix
//	send the bytes after L        chain the new blocks in
//
// The client produces no hash that is ever stored -- its hashes are only used
// for local comparison. So v2's "gateway trusts the client's block hashes, which
// can be poisoned" gap does not exist structurally.

import (
	"context"
	"net/http"
	"sync"
)

const (
	// OutputChunk is the size of one tail slice handed to the block cutter. It
	// only affects how often the hasher runs, not the semantics.
	OutputChunk = 16 * 1024

	// BlockBatch limits how many blocks one PutChain call writes, keeping a
	// single store call bounded.
	BlockBatch = 256
)

// Credential is one claim that "the peer holds length bytes ending at tip".
type Credential struct {
	Length int
	Tip    BlockID
}

// Prepared is one request ready to go on the wire.
type Prepared struct {
	// Session is the hash of the text's first block. It is the client's
	// credential-table key and never goes on the wire.
	Session []byte
	// HaveLen is the prefix length (a multiple of BlockSize) the peer is
	// believed to hold; HaveTip is that prefix's chain tail.
	HaveLen int
	HaveTip []byte
	// Body is what to send: the rendered text from HaveLen onwards.
	Body []byte
	// ReusedBytes is the part of the text the peer already has; FullBytes is
	// the whole rendered text.
	ReusedBytes int
	FullBytes   int
}

// Client is the outbound side: render, self-check the prefix, emit the tail.
//
// Its only local state is "session key -> [(L, tip), ...]": 8+16 bytes per
// credential, linear in sessions and independent of context length. v2's
// set-of-known-block-hashes (131 B per block) is gone.
type Client struct {
	mu             sync.Mutex
	maxCredentials int
	creds          map[BlockID][]Credential
}

// NewClient returns a client with an empty credential table.
func NewClient() *Client {
	return &Client{maxCredentials: 4, creds: make(map[BlockID][]Credential)}
}

// Prepare renders the request body and produces the tail to send.
//
// payload is the final JSON body the upstream would have received. On a
// credential hit Body contains only the bytes after HaveLen; with no usable
// credential HaveLen is 0 and Body is the whole text.
func (c *Client) Prepare(payload []byte) (*Prepared, error) {
	text, err := RenderJSON(payload)
	if err != nil {
		return nil, err
	}
	return c.PrepareText(text), nil
}

// PrepareText is Prepare for an already-rendered text.
func (c *Client) PrepareText(text []byte) *Prepared {
	blocks, _ := Blocks(text)

	// The first block's hash is the session key: content-addressed, so the
	// client never has to invent an id.
	var session BlockID
	if len(blocks) > 0 {
		session = blocks[0].ID
	}

	c.mu.Lock()
	candidates := append([]Credential(nil), c.creds[session]...)
	c.mu.Unlock()

	// Find the longest prefix whose boundary hash the peer is known to hold.
	// Only a match *on a block boundary* proves the whole prefix is identical.
	bestLen := 0
	var bestTip BlockID
	for i, blk := range blocks {
		pos := (i + 1) * BlockSize
		for _, cred := range candidates {
			if cred.Length == pos && cred.Tip == blk.ID {
				if pos > bestLen {
					bestLen, bestTip = pos, blk.ID
				}
				break
			}
		}
	}

	p := &Prepared{
		HaveLen:     bestLen,
		Body:        text[bestLen:],
		ReusedBytes: bestLen,
		FullBytes:   len(text),
	}
	if len(blocks) > 0 {
		p.Session = session[:]
	}
	if bestLen > 0 {
		p.HaveTip = bestTip[:]
	}
	return p
}

// UpdateFromHeaders records the server-confirmed prefix from a response
// (X-Tail-Have). The server is always authoritative.
func (c *Client) UpdateFromHeaders(session []byte, h http.Header) {
	length, tip := DecodeHave(h.Get(HeaderHave))
	if length > 0 {
		c.SetCredential(session, length, tip)
	}
}

// SetCredential records a prefix the peer holds.
//
// Several credentials per session are kept on purpose: concurrent or
// out-of-order responses would otherwise make the next request fall back to a
// full resend.
func (c *Client) SetCredential(session []byte, length int, tip []byte) {
	if len(session) != BlockIDSize || length <= 0 || len(tip) != BlockIDSize {
		return
	}
	var key BlockID
	copy(key[:], session)
	var tipID BlockID
	copy(tipID[:], tip)

	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.creds[key]
	kept := make([]Credential, 0, len(list)+1)
	kept = append(kept, Credential{Length: length, Tip: tipID})
	for _, cred := range list {
		if cred.Length == length {
			continue // one credential per length: the newest wins
		}
		kept = append(kept, cred)
	}
	if len(kept) > c.maxCredentials {
		kept = kept[:c.maxCredentials]
	}
	c.creds[key] = kept
}

// Credentials returns the credentials held for a session, newest first.
func (c *Client) Credentials(session []byte) []Credential {
	if len(session) != BlockIDSize {
		return nil
	}
	var key BlockID
	copy(key[:], session)
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Credential(nil), c.creds[key]...)
}

// Invalidate drops credentials, so the next round resends from 0. An empty
// session drops every session's credentials.
func (c *Client) Invalidate(session []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(session) == 0 {
		c.creds = make(map[BlockID][]Credential)
		return
	}
	var key BlockID
	copy(key[:], session)
	delete(c.creds, key)
}

// Reset drops every credential. A 409 names only "the prefix this round asked
// for", not which session failed, so the safe reaction is to forget everything
// and resend in full.
func (c *Client) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creds = make(map[BlockID][]Credential)
}

// Sessions is the number of distinct sessions tracked.
func (c *Client) Sessions() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.creds)
}

// CredentialBytes is the local state size (plain data, no container overhead).
func (c *Client) CredentialBytes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for _, list := range c.creds {
		for range list {
			total += 8 + BlockIDSize
		}
	}
	return total
}

// Reader is the server side: it stitches the stored prefix and the request's
// tail into one text stream, and chains the new blocks back into the store.
//
// Block cutting and hashing happen only here -- this is the single authority for
// block hashes.
type Reader struct {
	store     Store
	haveLen   int
	haveTip   BlockID
	batch     int
	prefixLen int // prefix blocks times BlockSize (haveLen rounded down)

	prefixBlocks int
	newBlocks    int
	prefixBytes  int
	tailBytes    int

	tip         BlockID // chain tail; the next round's credential
	chainParent BlockID
	pending     []Block
}

// NewReader prepares to decode one request. haveLen is rounded down to a block
// boundary; without a matching tip the prefix cannot be walked and ReadPrefix
// reports ErrMissingPrefix.
func NewReader(store Store, haveLen int, haveTip []byte) *Reader {
	r := &Reader{store: store, batch: BlockBatch}
	if haveLen > 0 {
		r.prefixLen = haveLen - haveLen%BlockSize
	}
	if len(haveTip) == BlockIDSize {
		copy(r.haveTip[:], haveTip)
	}
	r.prefixBlocks = r.prefixLen / BlockSize
	if r.prefixBlocks > 0 {
		r.tip = r.haveTip
	}
	r.chainParent = r.tip
	return r
}

// ReadPrefix resolves the stored prefix into one byte slice.
//
// It fails with ErrMissingPrefix rather than returning a short prefix: a prefix
// with a hole would splice unrelated text into the request.
func (r *Reader) ReadPrefix(ctx context.Context) ([]byte, error) {
	if r.prefixBlocks == 0 {
		return nil, nil
	}
	contents, err := r.store.ReadPrefix(ctx, r.haveTip[:], r.prefixBlocks)
	if err != nil {
		return nil, err
	}
	if len(contents) != r.prefixBlocks {
		return nil, ErrMissingPrefix
	}
	total := 0
	for _, piece := range contents {
		total += len(piece)
	}
	buf := make([]byte, 0, total)
	for _, piece := range contents {
		r.prefixBytes += len(piece)
		buf = append(buf, piece...)
	}
	return buf, nil
}

// Text returns the complete rendered text: the stored prefix followed by the
// request tail, chaining the new blocks into the store on the way.
//
// Writing the blocks here is what makes the next round hit; a short trailing
// block is emitted but not stored, because appending more text would change it.
func (r *Reader) Text(ctx context.Context, tail []byte) ([]byte, error) {
	prefix, err := r.ReadPrefix(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(prefix)+len(tail))
	out = append(out, prefix...)

	hasher := NewBlockHasher(r.tip)
	for len(tail) > 0 {
		n := len(tail)
		if n > OutputChunk {
			n = OutputChunk
		}
		piece := tail[:n]
		tail = tail[n:]
		r.tailBytes += len(piece)
		for _, blk := range hasher.Feed(piece) {
			out = append(out, blk.Data...)
			r.pending = append(r.pending, blk)
			r.newBlocks++
			if len(r.pending) >= r.batch {
				if err := r.flushPending(ctx); err != nil {
					return nil, err
				}
			}
		}
	}
	if rest := hasher.Flush(); len(rest) > 0 {
		out = append(out, rest...)
	}
	if err := r.flushPending(ctx); err != nil {
		return nil, err
	}
	r.tip = r.chainParent
	return out, nil
}

// flushPending writes the queued blocks as one chain extension.
func (r *Reader) flushPending(ctx context.Context) error {
	if len(r.pending) == 0 {
		return nil
	}
	ids := make([]BlockID, len(r.pending))
	contents := make([][]byte, len(r.pending))
	for i, blk := range r.pending {
		ids[i] = blk.ID
		contents[i] = blk.Data
	}
	if err := r.store.PutChain(ctx, r.chainParent, ids, contents); err != nil {
		return err
	}
	r.chainParent = ids[len(ids)-1]
	r.pending = r.pending[:0]
	return nil
}

// Tip is the chain tail after the current text, or nil when there is no full
// block at all.
func (r *Reader) Tip() []byte {
	if r.tip.IsZero() {
		return nil
	}
	tip := r.tip
	return tip[:]
}

// PrefixLen is how many bytes of the text full blocks cover: the value to send
// back as the next round's X-Tail-Have length.
func (r *Reader) PrefixLen() int { return (r.prefixBlocks + r.newBlocks) * BlockSize }

// PrefixBlocks is how many blocks came from the store.
func (r *Reader) PrefixBlocks() int { return r.prefixBlocks }

// NewBlocks is how many new blocks this request produced.
func (r *Reader) NewBlocks() int { return r.newBlocks }

// PrefixBytes is how many text bytes came from the store.
func (r *Reader) PrefixBytes() int { return r.prefixBytes }

// TailBytes is how many bytes the request carried.
func (r *Reader) TailBytes() int { return r.tailBytes }

// HitRatio is the reused share of the text, 0..1.
func (r *Reader) HitRatio() float64 {
	total := r.prefixBytes + r.tailBytes
	if total == 0 {
		return 0
	}
	return float64(r.prefixBytes) / float64(total)
}
