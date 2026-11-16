package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yetone/magpie/internal/lazyre"
	"github.com/yetone/magpie/internal/provider"
	"github.com/yetone/magpie/internal/tail"
)

// The tail transport, at the gateway's two edges.
//
// An agent resends the whole conversation every turn: the same bytes go up
// again, turn after turn, and on a home uplink that upload is the wait before
// the first token. The tail transport sends only what the other end doesn't
// hold yet. The client renders the request body into a stable text stream, cuts
// it into fixed blocks and hashes them into a chain (internal/tail), then sends
// the bytes after the longest prefix it knows the far end holds. The prefix is
// named by two headers: how many bytes it covers, and the id of its last block.
//
// Both ends are magpies, each behind a switch of its own: MAGPIE_TAIL turns this
// gateway's ingress into something that puts such a request back together, and
// provider.Provider.Tail makes the egress send one. Because the body stops being
// JSON, the egress has to know the far end takes it before the first one goes
// out, so it asks: an OPTIONS on the very URL it would POST to (tailProbe). A
// far end that does not say so is never sent a tail, and a far end that lies
// costs one plain retry, not a broken turn. Nothing else in the gateway
// changes — what tailIngress hands on is the JSON body every later stage was
// already written for, and the far end answers an ordinary chat completions
// request, so protocol translation, routing and the provider's own fixups all
// behave as they did.

// TailEnv is the gateway's switch for the tail transport, in the shape
// MAGPIE_ADDR and MAGPIE_ALLOW_IPS have: the gateway a deployment runs behind a
// container is configured by the environment it starts with, not by a file
// somebody edits. "1", "on", "yes" and "true" turn it on; anything else, or
// nothing, leaves it off and every request goes whole.
const TailEnv = "MAGPIE_TAIL"

const (
	// tailCacheBytes bounds the block cache. A gateway serves many
	// conversations, and the blocks of the ones nobody asks about any more
	// are worth less than the memory: 256 MB holds roughly 480 sessions the
	// size of a large Codex turn.
	tailCacheBytes = 256 << 20

	// tailProbeTimeout bounds the handshake. It runs inside a request, so it is
	// shorter than any turn: a far end that cannot answer it in this long is
	// treated as not taking the transport, which is always the safe answer.
	tailProbeTimeout = 3 * time.Second

	// tailProbeAgain is how long "it doesn't take it" is believed before it is
	// asked again. Long enough not to pay a probe per turn, short enough that a
	// far end upgraded at noon is used before the afternoon is over.
	tailProbeAgain = 5 * time.Minute
)

// tailIngress puts a request's body back together when it came over the tail
// transport, and hands every other body on untouched.
//
// It runs before anything reads the body, and what it returns is the JSON the
// rest of the gateway already expects, so every stage after it is unaware the
// request travelled in pieces.
func (s *Server) tailIngress(w http.ResponseWriter, r *http.Request, from provider.Protocol, body []byte) ([]byte, bool) {
	if from != provider.Chat || !tailOn() ||
		!tail.IsContentType(r.Header.Get("Content-Type")) {
		return body, true
	}
	haveLen, haveTip := tail.DecodeHave(r.Header.Get(tail.HeaderHave))
	decoded, err := s.tailServer().Decode(r.Context(), haveLen, haveTip, body)
	if err != nil {
		if errors.Is(err, tail.ErrMissingPrefix) {
			// The blocks the client's prefix names aren't here: the cache
			// was cleared, the gateway restarted, or another gateway took
			// the session over. The client is told to send the whole
			// request once more, rather than magpie answering a body with
			// a hole in it — a hole in the middle of a prompt is still
			// valid JSON, and would be answered as a different
			// conversation.
			w.Header().Set(tail.HeaderVersion, tail.Version)
			w.Header().Set(tail.HeaderRetransmit, "1")
			writeError(w, from, http.StatusConflict,
				"tail prefix missing: send the whole request again")
			return nil, false
		}
		writeError(w, from, http.StatusBadRequest, "tail: "+err.Error())
		return nil, false
	}
	// The next turn's credential rides back on this turn's reply: a client
	// keeps it, and one turn later names everything up to it.
	w.Header().Set(tail.HeaderVersion, tail.Version)
	w.Header().Set(tail.HeaderHave, tail.EncodeHave(decoded.HaveLen, decoded.HaveTip))
	if decoded.ReusedBytes > 0 {
		w.Header().Set(tail.HeaderHit, "1")
	}
	return decoded.Payload, true
}

// tailOn reports whether this gateway takes a request over the tail transport.
func tailOn() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(TailEnv))) {
	case "1", "on", "yes", "true":
		return true
	}
	return false
}

// tailServer is the block cache, built once: what the gateway can put a later
// request back together from.
func (s *Server) tailServer() *tail.Server {
	s.tailOnce.Do(func() {
		s.tailSrv = tail.NewServer(tail.NewMemoryStore(tailCacheBytes))
	})
	return s.tailSrv
}

// tailClientFor is the credential table of the conversations a provider
// answers, one per provider: a prefix a client may name means something only to
// the endpoint that gave it.
func (s *Server) tailClientFor(p provider.Provider) *tail.Client {
	if v, ok := s.tailClients.Load(p.ID); ok {
		return v.(*tail.Client)
	}
	c := tail.NewClient()
	actual, _ := s.tailClients.LoadOrStore(p.ID, c)
	return actual.(*tail.Client)
}

// tailHandshake answers OPTIONS on the chat endpoints: what they take, and
// whether this gateway takes the tail transport.
//
// It is how the other end of the transport finds out, before it sends the first
// request, that its body will be put back together here. Answering it costs
// nothing and never touches the store, so a client may ask as often as it likes.
// A gateway with the switch off answers too, without X-Tail-Version: "this is
// JSON only" is an answer worth having.
func (s *Server) tailHandshake(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "POST, OPTIONS")
	if tailOn() {
		w.Header().Set(tail.HeaderVersion, tail.Version)
		w.Header().Set(tail.HeaderBlock, strconv.Itoa(tail.BlockSize))
	}
	w.WriteHeader(http.StatusNoContent)
}

// tailCap is what this gateway believes about one provider's far end.
//
// Zero state is "not asked yet": the first request over a tail=on provider pays
// one OPTIONS for the answer, and every turn after it is free. A "no" is asked
// again after tailProbeAgain, so a far end upgraded later is picked up without
// restarting anything.
type tailCap struct {
	mu    sync.Mutex // one probe at a time per provider, not one per process
	state atomic.Int32
	again atomic.Int64 // unix nano before which a "no" is not asked again
}

const (
	tailCapUnknown int32 = iota
	tailCapTakes
	tailCapDoesNot
)

func (c *tailCap) takes() bool { return c.state.Load() == tailCapTakes }

// tailProbe asks a far end whether it takes the tail transport: an OPTIONS on
// the exact URL this gateway would POST to, so no path anybody has to implement
// is invented for it.
//
// Only an answer that names the version and the block size it speaks counts.
// Anything else — a 405 from a server without OPTIONS, a 404, a proxy answering
// the request itself, a version or block size that differs, no answer at all —
// is "it doesn't take it", which sends the request whole, as it went before.
func (s *Server) tailProbe(ctx context.Context, p provider.Provider) bool {
	url := strings.TrimRight(p.Base(provider.Chat), "/") + "/chat/completions"
	ctx, cancel := context.WithTimeout(ctx, tailProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodOptions, url, nil)
	if err != nil {
		return false
	}
	res, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		return false
	}
	if strings.TrimSpace(res.Header.Get(tail.HeaderVersion)) != tail.Version {
		return false
	}
	if got := strings.TrimSpace(res.Header.Get(tail.HeaderBlock)); got != strconv.Itoa(tail.BlockSize) {
		return false
	}
	return true
}

// tailCapable answers whether a provider's far end takes the tail transport,
// asking it the first time and whenever a "no" has expired.
//
// The answer is per provider, in this process: a prefix a client may name means
// something only to the endpoint that answered.
func (s *Server) tailCapable(ctx context.Context, p provider.Provider) bool {
	if !p.Tail {
		return false
	}
	v, _ := s.tailCaps.LoadOrStore(p.ID, &tailCap{})
	c := v.(*tailCap)
	switch c.state.Load() {
	case tailCapTakes:
		return true
	case tailCapDoesNot:
		if time.Now().UnixNano() < c.again.Load() {
			return false
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// asked again under the lock: the turn that waited reads the answer the
	// first one got instead of asking twice
	switch c.state.Load() {
	case tailCapTakes:
		return true
	case tailCapDoesNot:
		if time.Now().UnixNano() < c.again.Load() {
			return false
		}
	}
	if s.tailProbe(ctx, p) {
		c.state.Store(tailCapTakes)
		return true
	}
	c.state.Store(tailCapDoesNot)
	c.again.Store(time.Now().Add(tailProbeAgain).UnixNano())
	return false
}

// tailForget marks a provider's far end as not taking the transport, for a far
// end that said it did and then answered as if it didn't (it was replaced, or
// something between the two answered the handshake).
func (s *Server) tailForget(p provider.Provider) {
	v, _ := s.tailCaps.LoadOrStore(p.ID, &tailCap{})
	c := v.(*tailCap)
	c.state.Store(tailCapDoesNot)
	c.again.Store(time.Now().Add(tailProbeAgain).UnixNano())
}

// tailNotJSON is what a far end says when it was handed a tail body but read a
// JSON one. It is the only way to recognise "this endpoint doesn't take it" from
// an ordinary refusal of the request, which must not cost a second request.
var tailNotJSON = lazyre.New(`(?i)(invalid|expected|malformed|unexpected|parse[d]?|decode)[^.:;]{0,60}(json|content[- ]?type|media[- ]?type)`)

// tailFallback asks the whole request once more when a far end that said it
// takes the transport answered as if it didn't.
//
// The handshake is what keeps this rare: it only happens when the far end
// changed under a running gateway, or when something between the two answered
// the OPTIONS itself. The reply is read first — a plain refusal of the request
// (a content policy, a bad tool pair) passes through untouched, so a payload
// error never costs a second request and never disables the transport.
func (s *Server) tailFallback(ctx context.Context, p provider.Provider, to provider.Protocol,
	path string, body []byte, in http.Header, res *http.Response) (*http.Response, error) {
	if to != provider.Chat || !p.Tail || res.StatusCode < 400 {
		return res, nil
	}
	v, ok := s.tailCaps.Load(p.ID)
	if !ok || !v.(*tailCap).takes() {
		return res, nil
	}
	b, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	res.Body.Close()
	res.Body = io.NopCloser(bytes.NewReader(b))
	switch res.StatusCode {
	case http.StatusUnsupportedMediaType:
		// nothing else means "not this content type"
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		if !tailNotJSON.Match(b) {
			return res, nil
		}
	default:
		return res, nil
	}
	s.tailForget(p)
	plain := p
	plain.Tail = false
	return s.forwardOnce(ctx, plain, to, path, body, in)
}

// tailOut rewrites req to carry only the bytes the far end doesn't hold, for a
// provider whose requests go over the tail transport. It returns nil — and
// leaves req exactly as it was — for every request that isn't one: another
// protocol, a provider not so marked, an encoded body, or a body the codec
// can't render.
func (s *Server) tailOut(req *http.Request, p provider.Provider, to provider.Protocol, body []byte) *tailSent {
	if to != provider.Chat || !p.Tail || req.Header.Get("Content-Encoding") != "" {
		return nil
	}
	// a body that isn't JSON is only safe where the far end said it takes one:
	// asked before the first tail goes out, never assumed (tailProbe)
	if !s.tailCapable(req.Context(), p) {
		return nil
	}
	cl := s.tailClientFor(p)
	prepared, err := cl.Prepare(body)
	if err != nil {
		// A body the codec can't render goes as it always did: the
		// transport is an optimisation, and never a new way to fail.
		return nil
	}
	provider.SetBody(req, prepared.Body)
	req.Header.Set("Content-Type", tail.ContentType)
	if have := tail.EncodeHave(prepared.HaveLen, prepared.HaveTip); have != "" {
		req.Header.Set(tail.HeaderHave, have)
	} else {
		req.Header.Del(tail.HeaderHave)
	}
	return &tailSent{cl: cl, prepared: prepared}
}

// tailSent is one request on its way over the tail transport: the credential
// table that made it, and what it made.
type tailSent struct {
	cl       *tail.Client
	prepared *tail.Prepared
}

// sent is the bytes that go on the wire, for the ledger's own count.
func (t *tailSent) sent() int { return len(t.prepared.Body) }

// settle keeps the credential a successful reply carries.
func (t *tailSent) settle(res *http.Response) {
	if res == nil || res.StatusCode >= 400 {
		return
	}
	t.cl.UpdateFromHeaders(t.prepared.Session, res.Header)
}

// tailRetransmit asks for the whole request again when the far end says it
// holds none of the prefix this turn named. The credential table starts over
// with it: what the far end answered before is no longer true, and the next turn
// learns the new prefix from the reply to this one.
//
// The retry is one, and it is asked of the same provider: a 409 is the
// transport's own recovery, not a provider that failed, so it must not move on
// to a fallback or rest a key.
func (s *Server) tailRetransmit(ctx context.Context, p provider.Provider, to provider.Protocol,
	path string, body []byte, in http.Header, res *http.Response) (*http.Response, error) {
	if to != provider.Chat || !p.Tail || res.StatusCode != http.StatusConflict ||
		res.Header.Get(tail.HeaderRetransmit) == "" {
		return res, nil
	}
	res.Body.Close()
	s.tailClientFor(p).Reset()
	return s.forwardOnce(ctx, p, to, path, body, in)
}
