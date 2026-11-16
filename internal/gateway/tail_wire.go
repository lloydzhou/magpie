package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
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
// Both ends are magpies, and a saved byte takes both: one has to put a request
// back together (tailIngress), the other has to send one. The switch is the
// built-in plugin tail's, in the window's Plugins › Built-in list
// (plugins/tail): on, this gateway does both, off, every request goes whole.
// Because the body stops being JSON, the egress has to know the far end takes
// it before the first one goes out, so it asks: an OPTIONS on the very URL it
// would POST to (tailProbeOnce). Nothing is configured per provider — what a
// far end takes is what it says it takes. A far end that doesn't say so is
// never sent a tail, and a far end that lies costs one plain retry, not a
// broken turn. Nothing else in the gateway changes — what tailIngress hands on
// is the JSON body every later stage was already written for, and the far end
// answers an ordinary chat completions request, so protocol translation,
// routing and the provider's own fixups all behave as they did.

// tailIngress puts a request's body back together when it came over the tail
// transport, and hands every other body on untouched.
//
// It runs before anything reads the body, and what it returns is the JSON the
// rest of the gateway already expects, so every stage after it is unaware the
// request travelled in pieces.
func (s *Server) tailIngress(w http.ResponseWriter, r *http.Request, from provider.Protocol, body []byte) ([]byte, bool) {
	if from != provider.Chat || !tail.Served() ||
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

// tailServer is the block cache: what the gateway can put a later request back
// together from. It is built on first use, with the size the panel serves, and
// rebuilt when that size changes.
func (s *Server) tailServer() *tail.Server {
	cache := tail.Settings().Cache
	s.tailMu.Lock()
	defer s.tailMu.Unlock()
	if s.tailSrv == nil || s.tailCache != cache {
		s.tailSrv = tail.NewServer(tail.NewMemoryStore(cache))
		s.tailCache = cache
	}
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
	if tail.Served() {
		w.Header().Set(tail.HeaderVersion, tail.Version)
		w.Header().Set(tail.HeaderBlock, strconv.Itoa(tail.BlockSize))
	}
	w.WriteHeader(http.StatusNoContent)
}

// tailCap is what this gateway believes about one provider's far end.
//
// Both answers go stale: a "no" is asked again sooner, because an endpoint that
// was upgraded is worth finding out about, and a "yes" later, because one that
// was replaced is caught by its own refusal anyway (tailFallback).
type tailCap struct {
	state  atomic.Int32
	until  atomic.Int64 // unix nano: when this answer stops being believed
	asking atomic.Bool  // the background question is in flight; one is enough
}

const (
	tailCapUnknown int32 = iota
	tailCapTakes
	tailCapDoesNot
)

// takes reports whether the far end is believed to take the transport.
func (c *tailCap) takes() bool {
	return c.state.Load() == tailCapTakes && time.Now().UnixNano() < c.until.Load()
}

// refuses reports whether a "no" is still fresh. An unknown answer is not one:
// it is a question to ask, not an answer to cache.
func (c *tailCap) refuses() bool {
	return c.state.Load() == tailCapDoesNot && time.Now().UnixNano() < c.until.Load()
}

// set records one answer to the handshake.
func (c *tailCap) set(takes bool) {
	cfg := tail.Settings()
	if takes {
		c.state.Store(tailCapTakes)
		c.until.Store(time.Now().Add(cfg.CapFresh).UnixNano())
		return
	}
	c.state.Store(tailCapDoesNot)
	c.until.Store(time.Now().Add(cfg.CapAgain).UnixNano())
}

// tailProbeOnce asks a far end whether it takes the tail transport: an OPTIONS
// on the exact URL this gateway would POST to, so no path anybody has to
// implement is invented for it.
//
// It returns two things on purpose. takes is what the far end said; answered is
// whether it said anything at all. A server that never answers the OPTIONS is not
// the same as one that answers "no": the first is asked again, the second is
// believed for a while.
//
// Anything the far end does answer with counts as "no" unless it names the
// version and the block size it speaks — every way a server can be unable to
// take this transport: a 405 or a 501 from one that serves POST alone, a 404 from
// one that routes nothing else, a 401 behind something that wants a sign-in, a
// 200 from a proxy that never asked the origin, and a header that names another
// version or another block size. Being wrong in this direction costs the saving;
// being wrong in the other costs the turn.
func (s *Server) tailProbeOnce(ctx context.Context, p provider.Provider) (takes, answered bool) {
	url := strings.TrimRight(p.Base(provider.Chat), "/") + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodOptions, url, nil)
	if err != nil {
		return false, false
	}
	req.Header.Set("Accept", "*/*")
	res, err := s.client.Do(req)
	if err != nil {
		return false, false
	}
	defer res.Body.Close()
	// the body is never read: the answer is in the headers, and a big one (an
	// HTML error page) is not worth waiting for
	if res.StatusCode >= 400 {
		return false, true
	}
	if strings.TrimSpace(res.Header.Get(tail.HeaderVersion)) != tail.Version {
		return false, true
	}
	if strings.TrimSpace(res.Header.Get(tail.HeaderBlock)) != strconv.Itoa(tail.BlockSize) {
		return false, true
	}
	return true, true
}

// tailCapable answers whether a provider's far end takes the tail transport.
//
// It never asks on the request's way. An answer that isn't known yet means this
// turn goes whole — plain JSON, the request as it went before this transport
// existed — and the question is asked in the background: one OPTIONS, bounded,
// whose answer is then good for the settings' CapFresh or CapAgain. So the handshake
// costs no turn any latency, ever, and the only thing a fresh conversation pays
// is that its first turn is a full one, which is what it costs anyway.
//
// Listing models starts the question (tailAskAll), which is the one thing an
// agent does before its first turn: in practice the answer is there before the
// turn is, and the first turn already goes as a tail.
//
// The answer is per provider, in this process: a prefix a client may name means
// something only to the endpoint that answered.
func (s *Server) tailCapable(p provider.Provider) bool {
	if !tail.Served() {
		return false
	}
	v, _ := s.tailCaps.LoadOrStore(p.ID, &tailCap{})
	c := v.(*tailCap)
	if c.takes() {
		return true
	}
	if c.refuses() {
		return false
	}
	// unknown: this turn goes whole, and the question is asked off its way
	s.tailAskLater(p, c)
	return false
}

// tailAskLater asks the far end in the background, once: the turn that found the
// answer unknown started the question and went on without it.
func (s *Server) tailAskLater(p provider.Provider, c *tailCap) {
	if !c.asking.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.asking.Store(false)
		// not the request's context: the turn that started this is over
		ctx, cancel := context.WithTimeout(context.Background(), tail.Settings().Probe)
		defer cancel()
		takes, answered := s.tailProbeOnce(ctx, p)
		if !answered {
			// nothing at all before the timeout: a no, so the next turns
			// don't each wait one out
			takes = false
		}
		c.set(takes)
	}()
}

// tailAskAll asks every far end the tail transport is switched on for, off a
// request's way.
//
// It runs while the model list is being built, which is the one thing an agent
// does before its first turn: the answer is ready by then, without a turn ever
// waiting on it and without a probe per turn.
func (s *Server) tailAskAll() {
	if !tail.Served() {
		return
	}
	for _, p := range provider.All() {
		if !p.On() {
			continue
		}
		v, _ := s.tailCaps.LoadOrStore(p.ID, &tailCap{})
		c := v.(*tailCap)
		if !c.takes() {
			s.tailAskLater(p, c)
		}
	}
}

// tailForget marks a provider's far end as not taking the transport, for a far
// end that said it did and then answered as if it didn't (it was replaced, or
// something between the two answered the handshake). It is asked again after
// the settings' CapAgain.
func (s *Server) tailForget(p provider.Provider) {
	v, _ := s.tailCaps.LoadOrStore(p.ID, &tailCap{})
	c := v.(*tailCap)
	c.state.Store(tailCapDoesNot)
	c.until.Store(time.Now().Add(tail.Settings().CapAgain).UnixNano())
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
	if to != provider.Chat || !tail.Served() || res.StatusCode < 400 {
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
	return s.forwardOnce(tailWhole(ctx), p, to, path, body, in)
}

// tailOut rewrites req to carry only the bytes the far end doesn't hold. It
// returns nil — and leaves req exactly as it was — for every request that isn't
// one: another protocol, the transport switched off, the whole-request retry of
// tailFallback, an encoded body, or a body the codec can't render.
func (s *Server) tailOut(req *http.Request, p provider.Provider, to provider.Protocol, body []byte) *tailSent {
	if to != provider.Chat || !tail.Served() || tailWholeBy(req.Context()) ||
		req.Header.Get("Content-Encoding") != "" {
		return nil
	}
	// a body that isn't JSON is only safe where the far end said it takes one:
	// asked before the first tail goes out, never assumed (tailProbe)
	if !s.tailCapable(p) {
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
	if to != provider.Chat || !tail.Served() || res.StatusCode != http.StatusConflict ||
		res.Header.Get(tail.HeaderRetransmit) == "" {
		return res, nil
	}
	res.Body.Close()
	s.tailClientFor(p).Reset()
	return s.forwardOnce(ctx, p, to, path, body, in)
}

// tailWholeRetry marks a request that must go whole even though the transport is
// on: tailFallback's one retry, after a far end that said it takes a tail
// answered as if it didn't.
type tailWholeRetryKey struct{}

// tailWhole is ctx for a request that goes whole.
func tailWhole(ctx context.Context) context.Context {
	return context.WithValue(ctx, tailWholeRetryKey{}, true)
}

// tailWholeBy is whether ctx is one.
func tailWholeBy(ctx context.Context) bool {
	whole, _ := ctx.Value(tailWholeRetryKey{}).(bool)
	return whole
}
