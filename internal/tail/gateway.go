package tail

// Wire protocol: headers, cache status and the X-Tail-Have credential.
//
// Request:
//
//	POST /v1/chat/completions
//	Content-Type: application/x-tail-v3
//	X-Tail-Have: <L>;<32 hex>       # optional: prefix the client believes the
//	                                # peer already holds
//	body: the raw bytes after byte L of the rendered text
//
// Response:
//
//	X-Tail-Version: 3
//	X-Tail-Have: <L'>;<32 hex>      # next round's credential; the server is
//	                                # always authoritative
//	X-Tail-Status: hit|miss|degraded
//	X-Tail-Hit: 0.97
//	(409 + X-Tail-Retransmit: 1 when the prefix is gone)
//
// The session key (the hash of the text's first block) never goes on the wire: it
// is only the client's credential-table key and is recomputed from content
// before sending. The server does not need it -- (L, tip) is self-contained:
// walk the chain from tip, fail -> 409.

import (
	"encoding/hex"
	"strconv"
	"strings"
)

const (
	// ContentType marks a request body as tail-encoded.
	ContentType = "application/x-tail-v3"

	// Version is the wire version (incompatible with v2).
	Version = "3"

	HeaderVersion = "X-Tail-Version"
	// HeaderBlock is the handshake's block size. Two implementations that
	// disagree on it never hit -- the client names a boundary the peer's chain
	// does not have -- so it is advertised rather than assumed.
	HeaderBlock      = "X-Tail-Block"
	HeaderStatus     = "X-Tail-Status"
	HeaderHit        = "X-Tail-Hit"
	HeaderRetransmit = "X-Tail-Retransmit"
	HeaderHave       = "X-Tail-Have"

	// StatusHit, StatusMiss and StatusDegraded are the X-Tail-Status values.
	StatusHit      = "hit"
	StatusMiss     = "miss"
	StatusDegraded = "degraded"

	// RetransmitValue is the X-Tail-Retransmit value that asks for a resend.
	RetransmitValue = "1"
)

// EncodeHave renders the X-Tail-Have value "<bytes>;<tip hex>".
//
// An empty string means "no prefix to offer": length 0, or no tip. Sending a
// bare length would make the peer read a prefix it cannot walk.
func EncodeHave(length int, tip []byte) string {
	if length <= 0 || len(tip) == 0 {
		return ""
	}
	return strconv.Itoa(length) + ";" + hex.EncodeToString(tip)
}

// DecodeHave parses X-Tail-Have. Missing or malformed values mean "start from
// 0": the tail the client sends is always a suffix of the full text, so a
// dropped credential costs a full resend, never a wrong prefix.
func DecodeHave(v string) (length int, tip []byte) {
	if v == "" {
		return 0, nil
	}
	semi := strings.IndexByte(v, ';')
	if semi < 0 {
		return 0, nil
	}
	length, err := strconv.Atoi(v[:semi])
	if err != nil {
		return 0, nil
	}
	tip, err = hex.DecodeString(v[semi+1:])
	if err != nil {
		return 0, nil
	}
	if length <= 0 || len(tip) != BlockIDSize {
		return 0, nil
	}
	return length, tip
}

// IsContentType reports whether a Content-Type header selects the tail body.
func IsContentType(v string) bool {
	return strings.Contains(v, ContentType)
}
