package tail

// End-to-end behaviour of the two halves: what the client puts on the wire, what
// the server reconstructs from it, and how the credential round trip makes the
// second request cheap.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// requestPayload builds a realistic OpenAI Chat Completions body.
func requestPayload(turns int) []byte {
	messages := []map[string]any{
		{"role": "system", "content": string(corpus(120))},
		{"role": "assistant", "content": "好的，我已经理解了上下文。"},
	}
	for i := 0; i < turns; i++ {
		messages = append(messages,
			map[string]any{"role": "user", "content": "问题" + string(rune('A'+i)) + "：" + string(corpus(30))},
			map[string]any{"role": "assistant", "content": "回答" + string(rune('A'+i)) + "：" + string(corpus(40))},
		)
	}
	payload := map[string]any{
		"model":       "deepseek-chat",
		"temperature": 0.7,
		"max_tokens":  1024,
		"messages":    messages,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return raw
}

// serve runs one full round trip and feeds the response credential back into the
// client, which is what the HTTP wiring does with X-Tail-Have.
func serve(t *testing.T, client *Client, server *Server, payload []byte) (*Prepared, *Decoded) {
	t.Helper()
	prepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	decoded, err := server.Decode(context.Background(), prepared.HaveLen, prepared.HaveTip, prepared.Body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	h := http.Header{}
	h.Set(HeaderHave, decoded.HaveHeader())
	client.UpdateFromHeaders(prepared.Session, h)
	return prepared, decoded
}

// TestColdThenHit is the core promise: the second identical request puts only
// the short tail on the wire.
func TestColdThenHit(t *testing.T) {
	store := NewMemoryStore(0)
	server := NewServer(store)
	client := NewClient()
	payload := requestPayload(20)

	first, decoded1 := serve(t, client, server, payload)
	if first.HaveLen != 0 {
		t.Fatalf("cold request claimed a %d byte prefix", first.HaveLen)
	}
	if len(first.Body) != first.FullBytes {
		t.Fatalf("cold body is %d bytes, want the whole %d", len(first.Body), first.FullBytes)
	}
	if first.FullBytes < 4*BlockSize {
		t.Fatalf("payload only renders to %d bytes; need several blocks", first.FullBytes)
	}

	mem := store.(*MemoryStore)
	blocksAfterCold := mem.Len()
	second, decoded2 := serve(t, client, server, payload)
	if second.HaveLen == 0 {
		t.Fatal("second request did not reuse the prefix")
	}
	if second.HaveLen%BlockSize != 0 {
		t.Fatalf("HaveLen %d is not a multiple of %d", second.HaveLen, BlockSize)
	}
	if len(second.HaveTip) != BlockIDSize {
		t.Fatalf("HaveTip is %d bytes, want %d", len(second.HaveTip), BlockIDSize)
	}
	if len(second.Body) != second.FullBytes-second.HaveLen {
		t.Fatalf("body %d bytes, want FullBytes-HaveLen = %d",
			len(second.Body), second.FullBytes-second.HaveLen)
	}
	if len(second.Body) >= BlockSize {
		t.Fatalf("second body is %d bytes; the tail should be under one block", len(second.Body))
	}
	if len(second.Body)*10 >= len(first.Body) {
		t.Fatalf("second body %d not an order of magnitude below %d", len(second.Body), len(first.Body))
	}
	if !decoded2.Hit || decoded2.HitRatio < 0.9 {
		t.Fatalf("hit=%v ratio=%.3f, want hit above 0.9", decoded2.Hit, decoded2.HitRatio)
	}
	// The same content must not be written twice: the second round adds no block.
	if after := mem.Len(); after != blocksAfterCold {
		t.Fatalf("second round wrote blocks: %d -> %d", blocksAfterCold, after)
	}
	if decoded1.Hit {
		t.Fatal("the cold round reported a cache hit")
	}
}

// TestDecodedPayloadMatchesOriginal is the gateway's hard requirement: the
// reconstructed body must be valid JSON and carry the same request.
func TestDecodedPayloadMatchesOriginal(t *testing.T) {
	for _, turns := range []int{0, 3, 20} {
		server := NewServer(NewMemoryStore(0))
		client := NewClient()
		payload := requestPayload(turns)

		prepared, err := client.Prepare(payload)
		if err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		decoded, err := server.Decode(context.Background(), prepared.HaveLen, prepared.HaveTip, prepared.Body)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		var want, got any
		if err := json.Unmarshal(payload, &want); err != nil {
			t.Fatalf("original payload: %v", err)
		}
		if err := json.Unmarshal(decoded.Payload, &got); err != nil {
			t.Fatalf("decoded payload is not JSON: %v", err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("turns=%d: reconstructed payload differs\n got: %s", turns, decoded.Payload)
		}
	}
}

// TestReusedPrefixIsExact checks that the bytes the server serves from the store
// are the same bytes the client skipped.
func TestReusedPrefixIsExact(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	payload := requestPayload(12)

	first, _ := serve(t, client, server, payload)
	second, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if second.HaveLen != first.HaveLen && first.HaveLen != 0 {
		t.Fatalf("credential changed between rounds: %d -> %d", first.HaveLen, second.HaveLen)
	}
	full, err := RenderJSON(payload)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	if !bytes.Equal(second.Body, full[second.HaveLen:]) {
		t.Fatal("body is not exactly the text after HaveLen")
	}
}

// TestAppendOnlyExtendsPrefix: a new turn only adds blocks.
func TestAppendOnlyExtendsPrefix(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()

	payload := requestPayload(6)
	first, decoded1 := serve(t, client, server, payload)
	if first.HaveLen != 0 {
		t.Fatalf("cold request had a prefix of %d", first.HaveLen)
	}
	savedLen := decoded1.HaveLen

	appended := append([]byte(nil), payload...)
	var body map[string]any
	if err := json.Unmarshal(appended, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	body["messages"] = append(body["messages"].([]any),
		map[string]any{"role": "user", "content": "追加的提问：" + string(corpus(60))})
	appended, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	second, decoded2 := serve(t, client, server, appended)
	if second.HaveLen != savedLen {
		t.Fatalf("have len %d, want the previous prefix %d", second.HaveLen, savedLen)
	}
	if decoded2.HaveLen <= savedLen {
		t.Fatalf("prefix did not grow: %d -> %d", savedLen, decoded2.HaveLen)
	}
	var want, got any
	_ = json.Unmarshal(appended, &want)
	if err := json.Unmarshal(decoded2.Payload, &got); err != nil {
		t.Fatalf("decoded payload: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("reconstructed appended payload differs")
	}
}

// TestEditedMiddleMessageFallsBackToFull: a changed prefix must be resent from 0,
// never appended to the stored one.
func TestEditedMiddleMessageFallsBackToFull(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	payload := requestPayload(8)
	serve(t, client, server, payload)

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	messages := body["messages"].([]any)
	messages[3].(map[string]any)["content"] = "被改掉的回答：" + string(corpus(80))
	edited, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	prepared, decoded := serve(t, client, server, edited)
	if prepared.HaveLen != 0 {
		t.Fatalf("edited middle kept a %d byte prefix", prepared.HaveLen)
	}
	if len(prepared.Body) != prepared.FullBytes {
		t.Fatal("edited request did not resend the whole text")
	}
	if decoded.Hit {
		t.Fatal("store reported a hit for a changed prefix")
	}
	var want, got any
	_ = json.Unmarshal(edited, &want)
	if err := json.Unmarshal(decoded.Payload, &got); err != nil {
		t.Fatalf("decoded payload: %v", err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("reconstructed edited payload differs")
	}
}

// TestChangedParamBreaksPrefix: top-level parameters sit at the very front of the
// text, so changing one invalidates everything.
func TestChangedParamBreaksPrefix(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	payload := requestPayload(8)
	serve(t, client, server, payload)

	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	body["temperature"] = 0.1
	changed, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	prepared, err := client.Prepare(changed)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if prepared.HaveLen != 0 {
		t.Fatalf("param change kept a %d byte prefix", prepared.HaveLen)
	}
}

// TestSessionKeyIsContentAddressed: the credential key is the first block's hash,
// so equal openings share it without the client inventing an id.
func TestSessionKeyIsContentAddressed(t *testing.T) {
	payload := requestPayload(3)
	a, err := NewClient().Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	b, err := NewClient().Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if !bytes.Equal(a.Session, b.Session) {
		t.Fatal("same opening produced different session keys")
	}
	if len(a.Session) != BlockIDSize {
		t.Fatalf("session key is %d bytes, want %d", len(a.Session), BlockIDSize)
	}

	// A different opening -- a different model renders at byte 0 -- is a
	// different session.
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	body["model"] = "another-model"
	changed, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	c, err := NewClient().Prepare(changed)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if bytes.Equal(a.Session, c.Session) {
		t.Fatal("a different opening shared the session key")
	}
}

// TestCredentialsAreBounded: concurrent responses can name several prefixes, but
// the table must not grow without limit.
func TestCredentialsAreBounded(t *testing.T) {
	client := NewClient()
	session := bytes.Repeat([]byte("s"), BlockIDSize)
	for i := 0; i < 10; i++ {
		client.SetCredential(session, BlockSize*(i+1), bytes.Repeat([]byte{byte(i)}, BlockIDSize))
	}
	if got := len(client.Credentials(session)); got != 4 {
		t.Fatalf("%d credentials kept, want the cap of 4", got)
	}
	// A repeated length replaces the older claim instead of piling up.
	client.SetCredential(session, BlockSize*10, bytes.Repeat([]byte{9}, BlockIDSize))
	if got := len(client.Credentials(session)); got != 4 {
		t.Fatalf("%d credentials after a duplicate length", got)
	}
	latest := client.Credentials(session)[0]
	if latest.Length != BlockSize*10 {
		t.Fatalf("newest credential is length %d, want %d", latest.Length, BlockSize*10)
	}
}

// TestInvalidateAndResetDropCredentials covers both the per-session and the
// whole-table drop the gateway needs after a 409.
func TestInvalidateAndResetDropCredentials(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	payload := requestPayload(6)
	serve(t, client, server, payload)
	if client.Sessions() != 1 {
		t.Fatalf("%d sessions, want 1", client.Sessions())
	}

	prepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	client.Invalidate(prepared.Session)
	if client.Sessions() != 0 {
		t.Fatalf("%d sessions after invalidate, want 0", client.Sessions())
	}
	if got, err := client.Prepare(payload); err != nil || got.HaveLen != 0 {
		t.Fatalf("after invalidate: have len %d, err %v", got.HaveLen, err)
	}

	serve(t, client, server, payload)
	if client.Sessions() != 1 {
		t.Fatalf("%d sessions after re-seeding, want 1", client.Sessions())
	}
	client.Reset()
	if client.Sessions() != 0 {
		t.Fatalf("%d sessions after reset, want 0", client.Sessions())
	}
	if got, err := client.Prepare(payload); err != nil || got.HaveLen != 0 {
		t.Fatalf("after reset: have len %d, err %v", got.HaveLen, err)
	}
}

// TestStaleCredentialYieldsMissingPrefix: a cleared store must surface a
// retransmit signal, never a spliced payload.
func TestStaleCredentialYieldsMissingPrefix(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(0)
	server := NewServer(store)
	client := NewClient()
	payload := requestPayload(10)

	serve(t, client, server, payload)
	prepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if prepared.HaveLen == 0 {
		t.Fatal("expected a credential hit before clearing the store")
	}

	if err := store.Clear(ctx); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := server.Decode(ctx, prepared.HaveLen, prepared.HaveTip, prepared.Body); !errors.Is(err, ErrMissingPrefix) {
		t.Fatalf("err = %v, want ErrMissingPrefix", err)
	}
}

// TestDecodeWithoutCredentialIgnoresTip: a bogus have header must degrade to a
// full resend, not to a wrong prefix.
func TestDecodeWithoutCredentialIgnoresTip(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	payload := requestPayload(2)
	text, err := RenderJSON(payload)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	decoded, err := server.Decode(context.Background(), 0, nil, text)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(decoded.Payload, mustMarshal(t, payload)) {
		t.Fatalf("payload mismatch: %s", decoded.Payload)
	}
	if decoded.HaveLen%BlockSize != 0 {
		t.Fatalf("HaveLen %d is not block aligned", decoded.HaveLen)
	}
}

// TestHaveHeaderRoundtrip covers the credential encoding end to end.
func TestHaveHeaderRoundtrip(t *testing.T) {
	tip := Sum(nil, []byte("tip"))
	encoded := EncodeHave(1536, tip[:])
	if want := "1536;" + tip.String(); encoded != want {
		t.Fatalf("EncodeHave = %q, want %q", encoded, want)
	}
	length, gotTip := DecodeHave(encoded)
	if length != 1536 || !bytes.Equal(gotTip, tip[:]) {
		t.Fatalf("DecodeHave = (%d, %x), want (1536, %x)", length, gotTip, tip[:])
	}

	if got := EncodeHave(0, nil); got != "" {
		t.Fatalf("EncodeHave(0, nil) = %q, want empty", got)
	}
	if got := EncodeHave(0, tip[:]); got != "" {
		t.Fatalf("EncodeHave(0, tip) = %q, want empty", got)
	}
	if got := EncodeHave(512, nil); got != "" {
		t.Fatalf("EncodeHave(512, nil) = %q, want empty", got)
	}
	for _, bad := range []string{"", "garbage", "1536;short", "notanumber;" + tip.String(),
		"0;" + tip.String(), "-512;" + tip.String(), "1536;" + tip.String() + "ff"} {
		if length, gotTip := DecodeHave(bad); length != 0 || gotTip != nil {
			t.Fatalf("DecodeHave(%q) = (%d, %x), want (0, nil)", bad, length, gotTip)
		}
	}
}

func TestIsContentType(t *testing.T) {
	if !IsContentType(ContentType) {
		t.Fatalf("IsContentType(%q) = false", ContentType)
	}
	if !IsContentType("application/x-tail-v3; charset=utf-8") {
		t.Fatal("content type with parameters was not recognised")
	}
	if IsContentType("application/json") {
		t.Fatal("application/json was accepted")
	}
	if IsContentType("") {
		t.Fatal("empty content type was accepted")
	}
	if Version != "3" {
		t.Fatalf("Version = %q, want \"3\"", Version)
	}
}

func TestDecodedHeaders(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	payload := requestPayload(4)
	prepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	decoded, err := server.Decode(context.Background(), prepared.HaveLen, prepared.HaveTip, prepared.Body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := decoded.HaveHeader(); got != EncodeHave(decoded.HaveLen, decoded.HaveTip) {
		t.Fatalf("HaveHeader = %q, want EncodeHave of the fields", got)
	}
	if length, tip := DecodeHave(decoded.HaveHeader()); length != decoded.HaveLen ||
		!bytes.Equal(tip, decoded.HaveTip) {
		t.Fatal("HaveHeader does not round trip through DecodeHave")
	}
	if got := decoded.CacheStatus(); got != StatusMiss {
		t.Fatalf("cold decode status = %q, want %q", got, StatusMiss)
	}
	if decoded.HaveLen <= 0 || decoded.HaveLen%BlockSize != 0 {
		t.Fatalf("HaveLen %d is not a positive block multiple", decoded.HaveLen)
	}
	if len(decoded.HaveTip) != BlockIDSize {
		t.Fatalf("HaveTip is %d bytes", len(decoded.HaveTip))
	}
	if got := decoded.HitHeader(); got != "0.0000" {
		t.Fatalf("cold HitHeader = %q, want \"0.0000\"", got)
	}

	// A warm round reports a hit and a reused share.
	_, err = client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	h := http.Header{}
	h.Set(HeaderHave, decoded.HaveHeader())
	client.UpdateFromHeaders(prepared.Session, h)
	warmPrepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	warm, err := server.Decode(context.Background(), warmPrepared.HaveLen, warmPrepared.HaveTip, warmPrepared.Body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := warm.CacheStatus(); got != StatusHit {
		t.Fatalf("warm status = %q, want %q", got, StatusHit)
	}
	if got := warm.HitHeader(); got == "0.0000" {
		t.Fatal("warm HitHeader is zero")
	}
}

// TestUpdateFromHeadersIgnoresJunk keeps a malformed response header from
// poisoning the credential table.
func TestUpdateFromHeadersIgnoresJunk(t *testing.T) {
	client := NewClient()
	session := bytes.Repeat([]byte("k"), BlockIDSize)
	for _, value := range []string{"", "junk", "512;zz", "notalength;" + Sum(nil, []byte("x")).String()} {
		h := http.Header{}
		h.Set(HeaderHave, value)
		client.UpdateFromHeaders(session, h)
		if got := len(client.Credentials(session)); got != 0 {
			t.Fatalf("header %q created %d credentials", value, got)
		}
	}
}

// TestEmptyPayloadRendersNothing: a request with no messages is legal and must
// round trip through the whole path without a block at all.
func TestEmptyPayloadRendersNothing(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	payload := []byte(`{"model":"m","messages":[]}`)

	prepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if prepared.FullBytes != len(`<|model|>"m"`) {
		t.Fatalf("rendered %d bytes: %q", prepared.FullBytes, prepared.Body)
	}
	if len(prepared.Session) != 0 || len(prepared.HaveTip) != 0 {
		t.Fatal("a sub-block text must not produce a session key or tip")
	}
	decoded, err := server.Decode(context.Background(), 0, nil, prepared.Body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.HaveLen != 0 || decoded.HaveTip != nil {
		t.Fatalf("HaveLen/HaveTip = %d/%x, want 0/nil", decoded.HaveLen, decoded.HaveTip)
	}
	if got := decoded.HaveHeader(); got != "" {
		t.Fatalf("HaveHeader = %q, want empty", got)
	}
	if !bytes.Equal(decoded.Payload, mustMarshal(t, payload)) {
		t.Fatalf("payload = %s", decoded.Payload)
	}
}

// mustMarshal canonicalises a payload the way the transport does, for comparing
// reconstructed bodies.
func mustMarshal(t *testing.T, raw []byte) []byte {
	t.Helper()
	text, err := RenderJSON(raw)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	payload, err := Parse(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	out, err := MarshalPayload(payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	return out
}

// TestFullHitSendsEmptyBody is the边界 the cache is built for: when the whole text
// is full blocks, a repeat request puts zero bytes of body on the wire.
func TestFullHitSendsEmptyBody(t *testing.T) {
	// Find content padding that makes the rendered text an exact multiple of the
	// block size; that is the only case where the reuse covers everything.
	var payload []byte
	for pad := 0; pad < 4*BlockSize; pad++ {
		candidate, err := json.Marshal(map[string]any{
			"model":    "m",
			"messages": []any{map[string]any{"role": "user", "content": strings.Repeat("x", pad)}},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		text, err := RenderJSON(candidate)
		if err != nil {
			t.Fatalf("RenderJSON: %v", err)
		}
		if len(text) > 0 && len(text)%BlockSize == 0 {
			payload = candidate
			break
		}
	}
	if payload == nil {
		t.Fatal("no padding produced a block-aligned text")
	}

	server := NewServer(NewMemoryStore(0))
	client := NewClient()
	first, _ := serve(t, client, server, payload)
	second, decoded := serve(t, client, server, payload)
	if second.HaveLen != first.FullBytes {
		t.Fatalf("HaveLen %d, want the whole %d", second.HaveLen, first.FullBytes)
	}
	if len(second.Body) != 0 {
		t.Fatalf("body is %d bytes, want empty", len(second.Body))
	}
	if decoded.ReusedBytes != decoded.FullBytes {
		t.Fatalf("reused %d of %d bytes", decoded.ReusedBytes, decoded.FullBytes)
	}
	if decoded.HitRatio != 1 {
		t.Fatalf("hit ratio %.3f, want 1", decoded.HitRatio)
	}
	if !bytes.Equal(decoded.Payload, mustMarshal(t, payload)) {
		t.Fatal("reconstructed payload differs")
	}
}

// TestChainLongerThanBatch exercises the batched store write: a text past
// BlockBatch blocks must be written in several PutChain calls and still resolve
// as one chain.
func TestChainLongerThanBatch(t *testing.T) {
	content := strings.Repeat("20000 ", BlockBatch*BlockSize/6+512)
	payload, err := json.Marshal(map[string]any{
		"model":    "m",
		"messages": []any{map[string]any{"role": "user", "content": content}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	server := NewServer(NewMemoryStore(0))
	client := NewClient()

	prepared, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if prepared.FullBytes/BlockSize <= BlockBatch {
		t.Fatalf("text covers %d blocks; need more than one batch of %d",
			prepared.FullBytes/BlockSize, BlockBatch)
	}
	decoded, err := server.Decode(context.Background(), prepared.HaveLen, prepared.HaveTip, prepared.Body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.HaveLen/BlockSize <= BlockBatch {
		t.Fatalf("stored %d blocks, want more than one batch", decoded.HaveLen/BlockSize)
	}
	if !bytes.Equal(decoded.Payload, mustMarshal(t, payload)) {
		t.Fatal("reconstructed payload differs")
	}

	// The chain written across batches must resolve on the next round.
	h := http.Header{}
	h.Set(HeaderHave, decoded.HaveHeader())
	client.UpdateFromHeaders(prepared.Session, h)
	second, err := client.Prepare(payload)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if second.HaveLen == 0 || len(second.Body) >= BlockSize {
		t.Fatalf("second round: have %d, body %d bytes", second.HaveLen, len(second.Body))
	}
}

// TestRenderAcceptsGoNativeTypes: callers may build a payload from Go values
// instead of decoding JSON, and the output must not depend on that choice.
func TestRenderAcceptsGoNativeTypes(t *testing.T) {
	native := map[string]any{
		"model": "m",
		"messages": []map[string]any{
			{"role": "user", "content": "你好", "meta": map[string]string{"k": "v"}},
		},
		"stop": []string{"\n\n"},
		"n":    float32(1.5),
		"seed": uint32(7),
	}
	got := string(Render(native))
	want := `<|model|>"m"<|n|>1.5<|stop|>["\n\n"]<|seed|>7<|user|>"你好"<|extra|>{"meta":{"k":"v"}}`
	if got != want {
		t.Fatalf("native payload rendered\n got: %s\nwant: %s", got, want)
	}
}

// TestOutOfOrderRepliesKeepTheLongestPrefix: several requests of one
// conversation can be in flight at once, and a later request may be answered
// before an earlier one.
//
// The credential table keeps more than one entry per session for exactly this:
// with a single entry, the reply that happens to arrive last wins — and if that
// is the shortest one, the next turn still "works", still hits, and still saves,
// while naming a prefix far shorter than it could. Nothing errors; the saving
// just drains away. So this test pins both halves: the longest prefix wins, and
// a one-entry table would indeed have lost it.
func TestOutOfOrderRepliesKeepTheLongestPrefix(t *testing.T) {
	server := NewServer(NewMemoryStore(0))
	short, mid, long := requestPayload(4), requestPayload(6), requestPayload(8)

	// Three requests of one conversation, all prepared before any reply came
	// back: the state of a client with several requests in flight.
	c := NewClient()
	prepShort, err := c.Prepare(short)
	if err != nil {
		t.Fatalf("Prepare short: %v", err)
	}
	prepMid, err := c.Prepare(mid)
	if err != nil {
		t.Fatalf("Prepare mid: %v", err)
	}
	if !bytes.Equal(prepShort.Session, prepMid.Session) {
		t.Fatal("one conversation ended up with two session keys")
	}
	for _, p := range []*Prepared{prepShort, prepMid} {
		if p.HaveLen != 0 {
			t.Fatalf("a request prepared before any reply claimed a %d byte prefix", p.HaveLen)
		}
	}

	decShort, err := server.Decode(context.Background(), prepShort.HaveLen, prepShort.HaveTip, prepShort.Body)
	if err != nil {
		t.Fatalf("Decode short: %v", err)
	}
	decMid, err := server.Decode(context.Background(), prepMid.HaveLen, prepMid.HaveTip, prepMid.Body)
	if err != nil {
		t.Fatalf("Decode mid: %v", err)
	}
	if decMid.HaveLen <= decShort.HaveLen {
		t.Fatalf("the longer conversation left %d bytes, the shorter %d",
			decMid.HaveLen, decShort.HaveLen)
	}

	// The replies come back the wrong way round: the long one first, the short
	// one last.
	replies := []struct {
		session []byte
		decoded *Decoded
	}{
		{prepMid.Session, decMid},
		{prepShort.Session, decShort},
	}
	for _, r := range replies {
		h := http.Header{}
		h.Set(HeaderHave, r.decoded.HaveHeader())
		c.UpdateFromHeaders(r.session, h)
	}

	next, err := c.Prepare(long)
	if err != nil {
		t.Fatalf("Prepare long: %v", err)
	}
	if next.HaveLen != decMid.HaveLen {
		t.Fatalf("after out-of-order replies the next turn named %d bytes, want the longest %d",
			next.HaveLen, decMid.HaveLen)
	}

	// The same replies into a table that keeps one entry per session: the last
	// reply to arrive hides the longer one, and the turn costs what that miss
	// costs. This is what maxCredentials is buying.
	single := NewClient()
	single.maxCredentials = 1
	for _, r := range replies {
		h := http.Header{}
		h.Set(HeaderHave, r.decoded.HaveHeader())
		single.UpdateFromHeaders(r.session, h)
	}
	nextSingle, err := single.Prepare(long)
	if err != nil {
		t.Fatalf("Prepare long (single): %v", err)
	}
	if nextSingle.HaveLen != decShort.HaveLen {
		t.Fatalf("a one-entry table named %d bytes, want the shortest %d",
			nextSingle.HaveLen, decShort.HaveLen)
	}
	if len(nextSingle.Body) <= len(next.Body) {
		t.Fatalf("a one-entry table sent %d bytes and a four-entry table %d: keeping more "+
			"credentials must save", len(nextSingle.Body), len(next.Body))
	}
}
