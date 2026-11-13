package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/yetone/magpie/internal/provider"
	"github.com/yetone/magpie/internal/tail"
)

// tailTurn is one agent turn: a long system prompt, a few tool definitions and
// the history so far — the shape whose upload the tail transport is for.
func tailTurn(turns int) []map[string]any {
	msgs := []map[string]any{
		{"role": "system", "content": strings.Repeat("你是助手，先给结论再给依据。", 200)},
	}
	for range turns {
		msgs = append(msgs,
			map[string]any{"role": "user", "content": strings.Repeat("分析这一步的输入与输出。", 40)},
			map[string]any{"role": "assistant", "content": strings.Repeat("结论与依据如下。", 60)},
			map[string]any{"role": "tool", "tool_call_id": "c", "content": strings.Repeat("命令输出。", 80)},
		)
	}
	return msgs
}

func tailPayload(t *testing.T, model string, msgs []map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"model": model, "messages": msgs, "stream": true,
		"tools": []map[string]any{{
			"type": "function",
			"function": map[string]any{
				"name": "run", "description": strings.Repeat("跑一条命令。", 40),
				"parameters": map[string]any{"type": "object"},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// tailUpstream answers Chat as a short SSE reply, keeps what it was sent, and —
// when asked to speak the tail transport itself — puts a tail back together the
// way the far end of a real one does.
type tailUpstream struct {
	mu       sync.Mutex
	types    []string
	bodies   []string
	decoded  []string
	srv      *tail.Server
	refuse   bool // answer the first request that names a prefix with a 409
	refused  int
	lost     int // prefixes the store truly did not hold (a restart, not a test flag)
	requests int
}

func (u *tailUpstream) last(t *testing.T) (contentType, body, decoded string) {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.bodies) == 0 {
		t.Fatal("nothing sent upstream")
	}
	i := len(u.bodies) - 1
	return u.types[i], u.bodies[i], u.decoded[i]
}

func (u *tailUpstream) count() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.requests
}

func setRefusing(u *tailUpstream, on bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.refuse = on
}

func newTailUpstream(t *testing.T) (*tailUpstream, string) {
	t.Helper()
	u := &tailUpstream{srv: tail.NewServer(tail.NewMemoryStore(0))}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		ct := r.Header.Get("Content-Type")
		u.mu.Lock()
		u.requests++
		u.mu.Unlock()

		decoded := string(b)
		if tail.IsContentType(ct) {
			haveLen, haveTip := tail.DecodeHave(r.Header.Get(tail.HeaderHave))
			u.mu.Lock()
			refuse := u.refuse && u.refused == 0
			u.mu.Unlock()
			if refuse && haveLen > 0 {
				u.mu.Lock()
				u.refused++
				u.mu.Unlock()
				w.Header().Set(tail.HeaderRetransmit, "1")
				w.WriteHeader(http.StatusConflict)
				return
			}
			u.mu.Lock()
			tailSrv := u.srv
			u.mu.Unlock()
			dec, err := tailSrv.Decode(r.Context(), haveLen, haveTip, b)
			if err != nil {
				u.mu.Lock()
				u.lost++
				u.mu.Unlock()
				w.Header().Set(tail.HeaderRetransmit, "1")
				w.WriteHeader(http.StatusConflict)
				return
			}
			w.Header().Set(tail.HeaderHave, tail.EncodeHave(dec.HaveLen, dec.HaveTip))
			decoded = string(dec.Payload)
		}
		u.mu.Lock()
		u.types = append(u.types, ct)
		u.bodies = append(u.bodies, string(b))
		u.decoded = append(u.decoded, decoded)
		u.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chatSSE("ok", "", ""))
	}))
	t.Cleanup(srv.Close)
	return u, srv.URL
}

// tailAsk sends one turn the way the client of the tail transport sends it.
func tailAsk(t *testing.T, s *Server, prep *tail.Prepared) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(prep.Body)))
	req.Header.Set("Content-Type", tail.ContentType)
	if have := tail.EncodeHave(prep.HaveLen, prep.HaveTip); have != "" {
		req.Header.Set(tail.HeaderHave, have)
	}
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// tailSaves sets a provider up at url and marks it as the tail's far end.
func tailRegisters(t *testing.T, url string, marked bool) {
	t.Helper()
	p := provider.Provider{ID: "peer", Name: "Peer", Key: "k", Chat: url + "/v1",
		Models: []string{"m"}, Tail: marked}
	if err := provider.Save(p); err != nil {
		t.Fatal(err)
	}
}

// A request that carries only the bytes the gateway doesn't hold is put back
// together there: the vendor's model is asked the same conversation, and the
// next turn names the prefix this one left.
func TestTailTurnReachesTheVendorWhole(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	t.Setenv(TailEnv, "1")
	u, url := newTailUpstream(t)
	tailRegisters(t, url, false)

	srv := New()
	cl := tail.NewClient()
	turn := tailPayload(t, "peer/m", tailTurn(1))
	prep, err := cl.Prepare(turn)
	if err != nil {
		t.Fatal(err)
	}
	rec := tailAsk(t, srv, prep)
	if rec.Code != 200 {
		t.Fatalf("first turn: %d %s", rec.Code, rec.Body.String())
	}
	if _, _, got := u.last(t); gjson.Get(got, "messages.0.content").String() == "" {
		t.Fatalf("the vendor's model was asked nothing: %s", got)
	}
	cl.UpdateFromHeaders(prep.Session, rec.Header())

	// a second turn: the same conversation, one message longer
	msgs := tailTurn(1)
	msgs = append(msgs, map[string]any{"role": "user", "content": "继续下一步。"})
	longer := tailPayload(t, "peer/m", msgs)
	prep2, err := cl.Prepare(longer)
	if err != nil {
		t.Fatal(err)
	}
	if prep2.HaveLen == 0 || prep2.ReusedBytes == 0 {
		t.Fatalf("second turn named no prefix: have %d reused %d", prep2.HaveLen, prep2.ReusedBytes)
	}
	if len(prep2.Body) > len(longer)/4 {
		t.Fatalf("second turn went %d of %d bytes", len(prep2.Body), len(longer))
	}
	rec2 := tailAsk(t, srv, prep2)
	if rec2.Code != 200 {
		t.Fatalf("second turn: %d %s", rec2.Code, rec2.Body.String())
	}
	_, _, got := u.last(t)
	if want := len(msgs); len(gjson.Get(got, "messages").Array()) != want {
		t.Fatalf("vendor's model got %d messages, want %d: %s",
			len(gjson.Get(got, "messages").Array()), want, got)
	}
	if !strings.Contains(got, "继续下一步。") {
		t.Fatalf("the new turn is missing from what was sent: %s", got)
	}
}

// Without the gateway's switch, a body that is only a tail is not JSON and is
// turned away as any other body that isn't JSON is: the feature is the switch.
func TestTailTurnWithoutTheSwitchIsNoJSON(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	u, url := newTailUpstream(t)
	tailRegisters(t, url, false)

	srv := New()
	cl := tail.NewClient()
	turn := tailPayload(t, "peer/m", tailTurn(1))
	prep, err := cl.Prepare(turn)
	if err != nil {
		t.Fatal(err)
	}
	if rec := tailAsk(t, srv, prep); rec.Code == 200 {
		t.Fatal("a tail was served with the switch off")
	}
	if u.count() != 0 {
		t.Fatal("a tail was forwarded upstream with the switch off")
	}
}

// A provider marked tail=on sends only the bytes the far end doesn't hold: the
// second turn of a conversation goes up as its own new bytes, and the far end
// puts the same request back together.
func TestTailProviderSendsOnlyTheTail(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	u, url := newTailUpstream(t)
	tailRegisters(t, url, true)

	srv := New()
	msgs := tailTurn(2)
	first := tailPayload(t, "peer/m", msgs)
	if rec := chatAsk(t, srv, string(first)); rec.Code != 200 {
		t.Fatalf("first turn: %d %s", rec.Code, rec.Body.String())
	}
	ct, body, decoded := u.last(t)
	if !tail.IsContentType(ct) {
		t.Fatalf("first turn went as %q, not the tail transport", ct)
	}
	if len(body) < len(first)*9/10 {
		t.Fatalf("first turn went as %d of %d bytes: a cold tail sends the whole request", len(body), len(first))
	}
	if gjson.Get(decoded, "messages").Array() == nil {
		t.Fatalf("what the far end put together isn't the request: %s", decoded)
	}

	// the next turn: the conversation one message longer
	msgs = append(msgs, map[string]any{"role": "user", "content": "继续。"})
	longer := tailPayload(t, "peer/m", msgs)
	if rec := chatAsk(t, srv, string(longer)); rec.Code != 200 {
		t.Fatalf("second turn: %d %s", rec.Code, rec.Body.String())
	}
	ct, body, decoded = u.last(t)
	if !tail.IsContentType(ct) {
		t.Fatalf("second turn went as %q, not the tail transport", ct)
	}
	if len(body) >= len(longer)/2 {
		t.Fatalf("second turn went %d of %d bytes", len(body), len(longer))
	}
	if !strings.Contains(decoded, "继续。") {
		t.Fatalf("the far end put together a request without this turn: %s", decoded)
	}
}

// turnMessages reads the messages out of a marshaled request.
func turnMessages(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	var payload struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	return payload.Messages
}

// chatAsk asks the gateway for a Chat turn as an agent does.
func chatAsk(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+Token)
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// A far end that lost the prefix says so once, and the whole request goes
// again: the turn costs what it would have cost without the transport, and the
// agent never sees the 409.
func TestTailFarEndThatLostThePrefixGetsTheWholeRequest(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	u, url := newTailUpstream(t)
	setRefusing(u, true)
	tailRegisters(t, url, true)

	srv := New()
	msgs := tailTurn(2)
	if rec := chatAsk(t, srv, string(tailPayload(t, "peer/m", msgs))); rec.Code != 200 {
		t.Fatalf("first turn: %d %s", rec.Code, rec.Body.String())
	}
	if _, body, _ := u.last(t); len(body) == 0 {
		t.Fatal("nothing was sent upstream")
	}

	// the prefix the first turn left is gone at the far end
	u.mu.Lock()
	u.srv = tail.NewServer(tail.NewMemoryStore(0))
	u.mu.Unlock()
	msgs = append(msgs, map[string]any{"role": "user", "content": "继续。"})
	rec := chatAsk(t, srv, string(tailPayload(t, "peer/m", msgs)))
	if rec.Code != 200 {
		t.Fatalf("second turn: %d %s", rec.Code, rec.Body.String())
	}
	u.mu.Lock()
	refused := u.refused
	u.mu.Unlock()
	if refused != 1 {
		t.Fatalf("the far end was asked a prefix it lost: %d refusals, want 1", refused)
	}
	ct, _, decoded := u.last(t)
	if !tail.IsContentType(ct) {
		t.Fatalf("the retry went as %q", ct)
	}
	if !strings.Contains(decoded, "继续。") {
		t.Fatalf("the retry wasn't the whole request: %s", decoded)
	}
}

// tailSessionTurn is one conversation's history at a given length. The opening
// line differs per session, so each conversation is its own tail session.
func tailSessionTurn(session, turns int) []map[string]any {
	msgs := []map[string]any{
		{"role": "system", "content": strings.Repeat(fmt.Sprintf("会话%d的系统提示。", session), 120)},
	}
	for i := 0; i < turns; i++ {
		// A tool answer belongs to a tool call: a tool message with no
		// tool_calls above it is an orphan, and the gateway's fixups turn one
		// into a plain user message — which would change the very sequence this
		// test compares. So each turn is a well-formed tool round.
		callID := fmt.Sprintf("call_%d_%d", session, i)
		msgs = append(msgs,
			map[string]any{"role": "user",
				"content": fmt.Sprintf("会话%d第%d问。", session, i+1) + strings.Repeat("输入。", 30)},
			map[string]any{"role": "assistant",
				"content": fmt.Sprintf("会话%d第%d答。", session, i+1) + strings.Repeat("输出。", 30),
				"tool_calls": []map[string]any{{
					"id": callID, "type": "function",
					"function": map[string]any{"name": "run", "arguments": "{}"},
				}}},
			map[string]any{"role": "tool", "tool_call_id": callID,
				"content": strings.Repeat("命令输出。", 40)},
		)
	}
	return msgs
}

// canonicalMessages is the comparison form of a message list: the sequence of
// roles and contents, and nothing else.
//
// Not the whole JSON: by the time the vendor sees a request, the gateway has
// applied the provider's own fixups, which may add fields. The question this
// test asks is only whether one conversation's messages were ever answered as
// another's, and that is decided by the role/content sequence.
func canonicalMessages(t *testing.T, msgs []map[string]any) string {
	t.Helper()
	var b strings.Builder
	for _, m := range msgs {
		fmt.Fprintf(&b, "%v\x00%v\x01", m["role"], m["content"])
	}
	return b.String()
}

// TestTailParallelSessionsDoNotMix: an agent opens several conversations at
// once, and their requests are in flight together.
//
// The credential key is the body's own first block, not a name the client had to
// invent, so two conversations cannot share one: the request the far end puts
// back together is always exactly one conversation at one of its lengths, never
// a prefix borrowed from another. If this mixed, the saving would look better
// than it is — one conversation claiming another's prefix as its own.
func TestTailParallelSessionsDoNotMix(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	u, url := newTailUpstream(t)
	tailRegisters(t, url, true)

	const sessions, turns = 4, 3
	srv := New()

	// Every payload is built before anything is sent: the far end must see
	// exactly these histories, in any order.
	type pending struct {
		body string
		want string
	}
	queue := make([]pending, 0, sessions*turns)
	for session := 0; session < sessions; session++ {
		for turn := 1; turn <= turns; turn++ {
			msgs := tailSessionTurn(session, turn)
			queue = append(queue, pending{
				body: string(tailPayload(t, "peer/m", msgs)),
				want: canonicalMessages(t, msgs),
			})
		}
	}

	codes := make([]int, len(queue))
	var wg sync.WaitGroup
	for i := range queue {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/v1/chat/completions",
				strings.NewReader(queue[i].body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+Token)
			srv.Handler().ServeHTTP(rec, req)
			codes[i] = rec.Code
		}(i)
	}
	wg.Wait()

	for i, code := range codes {
		if code != 200 {
			t.Fatalf("request %d: %d", i, code)
		}
	}
	u.mu.Lock()
	decoded := append([]string(nil), u.decoded...)
	types := append([]string(nil), u.types...)
	u.mu.Unlock()
	if len(decoded) != len(queue) {
		t.Fatalf("the far end was asked %d requests, want %d", len(decoded), len(queue))
	}
	// and they really went over the transport, or this proves nothing
	for i, ct := range types {
		if !tail.IsContentType(ct) {
			t.Fatalf("request %d went as %q, not the tail transport", i, ct)
		}
	}

	// Each history the test built must appear exactly once among what the far
	// end put back together — a bijection, not merely "no garbage".
	seen := make(map[string]int, len(decoded))
	for _, d := range decoded {
		var payload struct {
			Messages []map[string]any `json:"messages"`
		}
		if err := json.Unmarshal([]byte(d), &payload); err != nil {
			t.Fatalf("the far end put together something that isn't a request: %s", d)
		}
		seen[canonicalMessages(t, payload.Messages)]++
	}
	for _, p := range queue {
		switch seen[p.want] {
		case 1:
			delete(seen, p.want)
		case 0:
			t.Fatalf("a conversation's history never reached the far end: %q", p.want[:120])
		default:
			t.Fatalf("a conversation's history reached the far end %d times", seen[p.want])
		}
	}
	if len(seen) != 0 {
		t.Fatalf("%d histories reached the far end that no session sent", len(seen))
	}
}

// TestTailRecoversAfterTheFarEndStartsWithNothing: the far end's block cache is
// memory, so a restart under a conversation leaves the client naming a prefix
// nobody holds.
//
// One turn pays for that: the refused tail and then the whole request. The turn
// after it must be cheap again — a transport that stayed expensive after a
// restart would be worse than not having it.
func TestTailRecoversAfterTheFarEndStartsWithNothing(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	u, url := newTailUpstream(t)
	tailRegisters(t, url, true)

	srv := New()
	msgs := tailSessionTurn(0, 1)
	if rec := chatAsk(t, srv, string(tailPayload(t, "peer/m", msgs))); rec.Code != 200 {
		t.Fatalf("first turn: %d %s", rec.Code, rec.Body.String())
	}

	// the far end comes back with an empty cache, as a restarted process would
	u.mu.Lock()
	u.srv = tail.NewServer(tail.NewMemoryStore(0))
	u.mu.Unlock()

	msgs = tailSessionTurn(0, 2)
	second := tailPayload(t, "peer/m", msgs)
	if rec := chatAsk(t, srv, string(second)); rec.Code != 200 {
		t.Fatalf("turn after the restart: %d %s", rec.Code, rec.Body.String())
	}
	u.mu.Lock()
	lost := u.lost
	u.mu.Unlock()
	if lost != 1 {
		t.Fatalf("the far end refused %d prefixes, want exactly 1: one restart costs one round", lost)
	}
	if _, body, decoded := u.last(t); len(body) < len(second)*9/10 {
		t.Fatalf("the turn after a restart went as %d of %d bytes: it must be the whole request",
			len(body), len(second))
	} else if !strings.Contains(decoded, "会话0第2问") {
		t.Fatalf("the resent request is missing this turn: %s", decoded)
	}

	// and the next turn is cheap again
	msgs = tailSessionTurn(0, 3)
	third := tailPayload(t, "peer/m", msgs)
	if rec := chatAsk(t, srv, string(third)); rec.Code != 200 {
		t.Fatalf("third turn: %d %s", rec.Code, rec.Body.String())
	}
	u.mu.Lock()
	lost = u.lost
	u.mu.Unlock()
	if lost != 1 {
		t.Fatalf("%d prefixes refused in total: recovery must not cost a second round", lost)
	}
	if _, body, decoded := u.last(t); len(body) >= len(third)/2 {
		t.Fatalf("the turn after the recovery went as %d of %d bytes: it did not recover",
			len(body), len(third))
	} else if !strings.Contains(decoded, "会话0第3问") {
		t.Fatalf("the recovered turn is missing its own messages: %s", decoded)
	}
}
