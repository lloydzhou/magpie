package tail

// Parser and encoder edge cases.
//
// The streaming parser is the part of the reference that has historically broken
// silently: a JSON string-state bug makes lenient unit tests pass while real
// requests with quotes, backslashes or tag-shaped content round trip wrongly.
// These tests feed every byte boundary and hostile content on purpose.

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

// renderForTest renders without failing the test early.
func renderForTest(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	out, err := RenderErr(payload)
	if err != nil {
		t.Fatalf("RenderErr: %v", err)
	}
	return out
}

// TestParseAtEveryByteBoundary checks that feeding the text in one-byte pieces
// gives the same payload as feeding it at once.
func TestParseAtEveryByteBoundary(t *testing.T) {
	payload := map[string]any{
		"model": "deepseek-chat",
		"messages": []any{
			map[string]any{"role": "system", "content": string(corpus(40))},
			map[string]any{"role": "user", "content": "他说\"你好\"，然后走了\\"},
			map[string]any{"role": "tool", "tool_call_id": "c1", "content": "12:00"},
		},
		"temperature": 0.7,
	}
	text := renderForTest(t, payload)

	want, err := Parse(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	for _, step := range []int{1, 2, 3, 7, 64, len(text)} {
		parser := NewParser()
		var events []Event
		for i := 0; i < len(text); i += step {
			end := i + step
			if end > len(text) {
				end = len(text)
			}
			got, err := parser.Feed(text[i:end])
			if err != nil {
				t.Fatalf("step %d: Feed: %v", step, err)
			}
			events = append(events, got...)
		}
		rest, err := parser.Finish()
		if err != nil {
			t.Fatalf("step %d: Finish: %v", step, err)
		}
		events = append(events, rest...)

		var messages []any
		params := map[string]any{}
		for _, ev := range events {
			switch ev.Kind {
			case EventMessageEnd:
				messages = append(messages, ev.Message)
			case EventParam:
				params[ev.Name] = ev.Value
			}
		}
		if len(messages) != len(want["messages"].([]any)) {
			t.Fatalf("step %d: %d messages, want %d", step, len(messages), len(want["messages"].([]any)))
		}
		if !reflect.DeepEqual(messages, want["messages"]) {
			t.Fatalf("step %d: messages differ from the buffered parse", step)
		}
	}
}

// TestParseSplitAfterBackslash is the定向 edge case: content ends with a
// backslash, so the next message's opening quote sits right after an escape
// character. A parser that loses the escape state across feeds reads it as a
// string boundary and mis-parses everything after it.
func TestParseSplitAfterBackslash(t *testing.T) {
	payload := map[string]any{
		"model": "m",
		"messages": []any{
			map[string]any{"role": "user", "content": "a\\"},
			map[string]any{"role": "assistant", "content": "他说\"你好\"然后走了"},
			map[string]any{"role": "user", "content": "再次 \"引号\" 与 \\ 反斜杠"},
		},
	}
	text := renderForTest(t, payload)
	want, err := Parse(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	for cut := 0; cut <= len(text); cut++ {
		parser := NewParser()
		var events []Event
		for _, piece := range [][]byte{text[:cut], text[cut:]} {
			got, err := parser.Feed(piece)
			if err != nil {
				t.Fatalf("cut %d: Feed: %v", cut, err)
			}
			events = append(events, got...)
		}
		rest, err := parser.Finish()
		if err != nil {
			t.Fatalf("cut %d: Finish: %v", cut, err)
		}
		events = append(events, rest...)

		var messages []any
		for _, ev := range events {
			if ev.Kind == EventMessageEnd {
				messages = append(messages, ev.Message)
			}
		}
		if !reflect.DeepEqual(messages, want["messages"]) {
			t.Fatalf("cut %d: parse result changed with the split point", cut)
		}
	}
}

// TestHostileContentRoundTrips: tag-shaped text, quotes, backslashes, newlines and
// control characters inside content must survive, because they live in JSON
// strings and the scanner knows it.
func TestHostileContentRoundTrips(t *testing.T) {
	hostile := []string{
		"<|user|>", "<|end|>", "<|tool_calls|>", `"引号"`, `\`,
		"\n", "<|model|>", `a"b`, `中\"文`, "🚀", "\x00\x1e\x1f\x7f",
	}
	for _, content := range hostile {
		payload := map[string]any{
			"model":    "m",
			"messages": []any{map[string]any{"role": "user", "content": content}},
		}
		text := renderForTest(t, payload)
		back, err := Parse(text)
		if err != nil {
			t.Fatalf("content %q: Parse: %v", content, err)
		}
		messages := back["messages"].([]any)
		if got := messages[0].(map[string]any)["content"]; got != content {
			t.Fatalf("content %q came back as %v", content, got)
		}
	}
}

// TestUnknownTagStaysContent: an unknown <|xxx|> in the body must not be read as
// structure.
func TestUnknownTagStaysContent(t *testing.T) {
	payload := map[string]any{
		"model": "m",
		"messages": []any{
			map[string]any{"role": "user", "content": "写一个 <|unknown|> 标记"},
		},
	}
	back, err := Parse(renderForTest(t, payload))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	content := back["messages"].([]any)[0].(map[string]any)["content"]
	if content != "写一个 <|unknown|> 标记" {
		t.Fatalf("content = %v", content)
	}
}

// TestPlainTextHasNoMessages: text without any role tag is not an error, it just
// produces no messages.
func TestPlainTextHasNoMessages(t *testing.T) {
	payload, err := Parse([]byte("just some text"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty", payload)
	}
	events, err := NewParser().Finish()
	if err != nil || len(events) != 0 {
		t.Fatalf("Finish on an empty parser = %v, %v", events, err)
	}
}

// TestContentStatesRoundTrip covers OpenAI's three content states: absent, null
// and empty string. All three produce no body text, so only <|extra|> can carry
// the difference.
func TestContentStatesRoundTrip(t *testing.T) {
	raw := []byte(`{"model":"m","messages":[
		{"role":"user"},
		{"role":"assistant","content":null},
		{"role":"user","content":""},
		{"role":"user","content":"x","custom":{"b":2,"a":[1,2]}}]}`)
	text, err := RenderJSON(raw)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	back, err := Parse(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	messages := back["messages"].([]any)

	if _, has := messages[0].(map[string]any)["content"]; has {
		t.Fatal("a message with no content key gained one")
	}
	if v, has := messages[1].(map[string]any)["content"]; !has || v != nil {
		t.Fatalf("null content came back as %v (present=%v)", v, has)
	}
	if v := messages[2].(map[string]any)["content"]; v != "" {
		t.Fatalf("empty content came back as %v", v)
	}
	if v := messages[3].(map[string]any)["custom"]; !reflect.DeepEqual(v,
		map[string]any{"a": []any{json.Number("1"), json.Number("2")}, "b": json.Number("2")}) {
		t.Fatalf("unknown field came back as %v", v)
	}
}

// TestToolCallsArgumentsRestoredAsString: arguments is a JSON string per the
// OpenAI shape, so a dict-form arguments value must be re-encoded.
func TestToolCallsArgumentsRestoredAsString(t *testing.T) {
	raw := []byte(`{"model":"m","messages":[{"role":"assistant","content":null,
		"tool_calls":[{"id":"c1","type":"function",
		"function":{"name":"get_time","arguments":{"tz":"UTC","n":2}}}]}]}`)
	text, err := RenderJSON(raw)
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	back, err := Parse(text)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	calls := back["messages"].([]any)[0].(map[string]any)["tool_calls"].([]any)
	fn := calls[0].(map[string]any)["function"].(map[string]any)
	if fn["name"] != "get_time" {
		t.Fatalf("function name = %v", fn["name"])
	}
	args, ok := fn["arguments"].(string)
	if !ok {
		t.Fatalf("arguments is %T, want a JSON string", fn["arguments"])
	}
	if args != `{"n":2,"tz":"UTC"}` {
		t.Fatalf("arguments = %q, want the sorted compact JSON", args)
	}
}

// TestRenderIsDeterministic: key order in the Go map must not affect the text.
func TestRenderIsDeterministic(t *testing.T) {
	a := map[string]any{
		"model": "m", "temperature": 0.7, "max_tokens": 100,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	}
	b := map[string]any{
		"messages":   []any{map[string]any{"content": "hi", "role": "user"}},
		"max_tokens": 100, "model": "m", "temperature": 0.7,
	}
	if got, want := renderForTest(t, a), renderForTest(t, b); !bytes.Equal(got, want) {
		t.Fatalf("map iteration order leaked into the text\n%q\n%q", got, want)
	}
}

// TestRenderNumbersMatchPython pins the int/float distinction and Python's float
// repr thresholds. A single differing byte here changes a block hash.
func TestRenderNumbersMatchPython(t *testing.T) {
	cases := []struct {
		value string
		want  string
	}{
		{"100", "100"},
		{"-0", "0"},
		{"1e0", "1.0"},
		{"1.0", "1.0"},
		{"0.7", "0.7"},
		{"1e15", "1000000000000000.0"},
		{"1e16", "1e+16"},
		{"1e-4", "0.0001"},
		{"1e-5", "1e-05"},
		{"1e21", "1e+21"},
		{"-0.0", "-0.0"},
		{"5e-324", "5e-324"},
		{"1.5e300", "1.5e+300"},
		{"123456789012345678901234567890", "123456789012345678901234567890"},
		{"3.141592653589793", "3.141592653589793"},
	}
	for _, tc := range cases {
		payload := map[string]any{"model": "m", "seed": json.Number(tc.value)}
		text := string(renderForTest(t, payload))
		want := `<|model|>"m"<|seed|>` + tc.want
		if text != want {
			t.Fatalf("number %s rendered %q, want %q", tc.value, text, want)
		}
	}

	// Go-native numeric types follow the Python value they represent.
	if got := string(Render(map[string]any{"seed": 1e15})); got != `<|seed|>1000000000000000.0` {
		t.Fatalf("float64 1e15 rendered %q", got)
	}
	if got := string(Render(map[string]any{"seed": int64(100)})); got != `<|seed|>100` {
		t.Fatalf("int64 100 rendered %q", got)
	}
	if got := string(Render(map[string]any{"seed": math.Inf(1)})); got != `<|seed|>Infinity` {
		t.Fatalf("+Inf rendered %q", got)
	}
}

// TestRenderEscapesLikePython pins the string escaping rules with ensure_ascii
// disabled: only quotes, backslashes and C0 controls are escaped.
func TestRenderEscapesLikePython(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a\"b", `"a\"b"`},
		{`a\b`, `"a\\b"`},
		{"a\nb", `"a\nb"`},
		{"a\tb\r\b\f", `"a\tb\r\b\f"`},
		{"\x00\x1f", `"\u0000\u001f"`},
		{"\x7f", "\"\x7f\""}, // DEL is not escaped
		{"中文🚀", `"中文🚀"`},
		{"\u2028", "\"\u2028\""},
	}
	for _, tc := range cases {
		got := string(renderForTest(t, map[string]any{"model": tc.in}))
		want := "<|model|>" + tc.want
		if got != want {
			t.Fatalf("escaping %q gave %q, want %q", tc.in, got, want)
		}
	}
}

// TestMarshalPayloadSortsKeys: the encoder must sort keys at every level so both
// implementations agree.
func TestMarshalPayloadSortsKeys(t *testing.T) {
	payload := map[string]any{
		"b": 1, "a": map[string]any{"z": 1, "y": 2},
		"c": []any{map[string]any{"q": 1, "p": 2}},
	}
	got, err := MarshalPayload(payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	want := `{"a":{"y":2,"z":1},"b":1,"c":[{"p":2,"q":1}]}`
	if string(got) != want {
		t.Fatalf("MarshalPayload = %s, want %s", got, want)
	}
}
