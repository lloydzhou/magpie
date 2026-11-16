package tail

// Server: inbound side. It turns (prefix credential, tail bytes) back into the
// request body the client originally sent, and chains the new blocks so the next
// round can hit.

import (
	"context"
	"errors"
	"fmt"
)

// Server decodes tail requests against one block store.
type Server struct {
	store Store
}

// NewServer returns a server backed by st.
func NewServer(st Store) *Server {
	return &Server{store: st}
}

// Decoded is one reconstructed request.
type Decoded struct {
	// Payload is the complete request body JSON, ready to hand to the normal
	// request path. It always unmarshals as JSON.
	Payload []byte
	// FullBytes is the length of the rendered text, ReusedBytes the part that
	// came from the store.
	FullBytes   int
	ReusedBytes int
	// HaveLen and HaveTip are next round's credential: how many bytes full
	// blocks cover and the hash of the last full block.
	HaveLen int
	HaveTip []byte
	// HitRatio is the reused share of the text; Hit says the store served a
	// prefix at all.
	HitRatio float64
	Hit      bool
}

// Decode reconstructs the request body from a tail request.
//
// A prefix that cannot be walked fails with ErrMissingPrefix (the caller answers
// 409 + X-Tail-Retransmit); a half payload is never returned.
func (s *Server) Decode(ctx context.Context, haveLen int, haveTip, body []byte) (*Decoded, error) {
	reader := NewReader(s.store, haveLen, haveTip)
	text, err := reader.Text(ctx, body)
	if err != nil {
		if errors.Is(err, ErrMissingPrefix) {
			return nil, fmt.Errorf("tail: decode: tip=%x blocks=%d: %w",
				haveTip, reader.PrefixBlocks(), ErrMissingPrefix)
		}
		return nil, fmt.Errorf("tail: decode: %w", err)
	}

	payload, err := ParseJSON(text)
	if err != nil {
		return nil, fmt.Errorf("tail: decode: %w", err)
	}

	decoded := &Decoded{
		Payload:     payload,
		FullBytes:   len(text),
		ReusedBytes: reader.PrefixBytes(),
		HaveLen:     reader.PrefixLen(),
		HaveTip:     reader.Tip(),
		HitRatio:    reader.HitRatio(),
		Hit:         reader.PrefixBlocks() > 0,
	}
	return decoded, nil
}

// HaveHeader is the X-Tail-Have value to send back with this response.
func (d *Decoded) HaveHeader() string { return EncodeHave(d.HaveLen, d.HaveTip) }

// CacheStatus is the X-Tail-Status value for this response.
func (d *Decoded) CacheStatus() string {
	if d.Hit {
		return StatusHit
	}
	return StatusMiss
}

// HitHeader is the X-Tail-Hit value for this response, e.g. "0.9700".
func (d *Decoded) HitHeader() string { return fmt.Sprintf("%.4f", d.HitRatio) }
