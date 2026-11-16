package tail

// Renderer: OpenAI Chat Completions request body -> one-dimensional text.
//
// The text is *not* JSON. It is a harmony-style tag stream, the same shape the
// reference implementation produces:
//
//	<|model|>deepseek-chat<|temperature|>0.7<|system|>"你是助手。"<|user|>"你好"
//
// Message boundaries are expressed by the next role tag, so no separator is
// needed. Everything after a role tag up to the next tag is JSON, which is why
// the body needs no escaping of its own: a literal `<|user|>` inside content
// sits inside a JSON string and the parser skips it.
//
// Determinism is a hard requirement: the same request body must render to
// byte-identical text, otherwise block contents change, block hashes change and
// the whole cache misses. Hence the fixed PARAM_ORDER, sorted object keys and
// compact separators.
//
// The format version is intentionally *not* in the text; it travels in the
// X-Tail-Version header. Putting it in the body would land it on the first
// block, so a version bump would invalidate everything.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const (
	// tagOpen and tagClose bracket a structural tag: <|name|>.
	tagOpen  = "<|"
	tagClose = "|>"
)

// roleTags open a new message when the parser meets one.
var roleTags = map[string]bool{
	"system": true, "user": true, "assistant": true,
	"tool": true, "developer": true, "function": true,
}

// fieldTags are tags that appear *inside* a message and scope the text after
// them to one field.
var fieldTags = map[string]bool{
	"tool_calls": true, "tool_call_id": true, "name": true, "extra": true,
}

// paramOrder is the fixed top-level parameter order. The order is part of the
// wire format: dict insertion order comes from the client and differs between
// SDK versions, so it cannot be used.
var paramOrder = []string{
	"model",
	"max_tokens",
	"max_completion_tokens",
	"temperature",
	"top_p",
	"n",
	"stream",
	"stream_options",
	"stop",
	"presence_penalty",
	"frequency_penalty",
	"logit_bias",
	"logprobs",
	"top_logprobs",
	"seed",
	"response_format",
	"tool_choice",
	"parallel_tool_calls",
	"user",
	"reasoning_effort",
	"service_tier",
}

// allTags is every known tag name; the parser only treats <|name|> as
// structure when name is in here, so an unknown <|xxx|> stays content.
var allTags = func() map[string]bool {
	m := map[string]bool{"tools": true}
	for n := range roleTags {
		m[n] = true
	}
	for n := range fieldTags {
		m[n] = true
	}
	for _, n := range paramOrder {
		m[n] = true
	}
	return m
}()

// missingContent and other sentinels mark states that a plain value cannot:
// OpenAI distinguishes "content absent", "content null" and "content empty",
// and all three produce no body text, so the difference is carried in the
// <|extra|> object.
var (
	sentinelMissing = map[string]any{"__missing__": true}
	sentinelContent = "__content__"
)

// tag renders name as <|name|>.
func tag(name string) string { return tagOpen + name + tagClose }

// Render renders a request body into the transport text.
//
// Numbers must be passed as json.Number (decode the wire JSON with
// Decoder.UseNumber, or use RenderJSON) to be byte-identical with Python, which
// distinguishes int 100 from float 100.0 while a Go float64 does not. A value
// that cannot be encoded is written as null; use RenderErr when that matters.
func Render(payload map[string]any) []byte {
	out, _ := RenderErr(payload)
	return out
}

// RenderErr is Render with error reporting for values JSON cannot represent.
func RenderErr(payload map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	if err := renderInto(&buf, payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// RenderJSON renders raw request JSON. It decodes with UseNumber so the output
// matches Python byte for byte, including int-vs-float.
func RenderJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("tail: render: %w", err)
	}
	return RenderErr(payload)
}

func renderInto(dst *bytes.Buffer, payload map[string]any) error {
	writeParam := func(name string, value any) error {
		dst.WriteString(tag(name))
		return encodePyJSON(dst, value)
	}

	for _, key := range paramOrder {
		v, ok := payload[key]
		if !ok || v == nil {
			continue
		}
		if err := writeParam(key, v); err != nil {
			return err
		}
	}
	if tools, ok := payload["tools"]; ok && pyTruthy(tools) {
		if err := writeParam("tools", tools); err != nil {
			return err
		}
	}

	messages, _ := asSlice(payload["messages"])
	for _, raw := range messages {
		msg, ok := asDict(raw)
		if !ok {
			continue
		}
		if err := messageInto(dst, msg); err != nil {
			return err
		}
	}
	return nil
}

// messageInto writes one message: the role tag, then its JSON-encoded content
// and fields.
func messageInto(dst *bytes.Buffer, msg map[string]any) error {
	role := "user"
	if r, ok := msg["role"].(string); ok && r != "" {
		role = r
	}
	dst.WriteString(tag(role))

	content := contentToText(msg["content"])
	if content != "" {
		// Content travels as a JSON string, not as raw text: json encoding gives
		// full escaping, so a literal <|user|> inside the body cannot be read as
		// structure.
		if err := encodePyJSON(dst, content); err != nil {
			return err
		}
	} else if raw, ok := msg["content"]; ok && raw != nil {
		if err := encodePyJSON(dst, ""); err != nil { // explicit empty string
			return err
		}
	}

	if tc, ok := msg["tool_calls"]; ok && pyTruthy(tc) {
		dst.WriteString(tag("tool_calls"))
		if err := encodePyJSON(dst, tc); err != nil {
			return err
		}
	}
	if tcid, ok := msg["tool_call_id"]; ok && pyTruthy(tcid) {
		dst.WriteString(tag("tool_call_id"))
		if err := encodePyJSON(dst, pyStr(tcid)); err != nil {
			return err
		}
	}
	if name, ok := msg["name"]; ok && pyTruthy(name) {
		dst.WriteString(tag("name"))
		if err := encodePyJSON(dst, pyStr(name)); err != nil {
			return err
		}
	}

	// Unknown fields are appended deterministically so nothing is dropped
	// silently. The "did the message have a content key, and what was it" fact
	// rides along here too: absent / null / "" all produce no body text and
	// cannot be told apart from the body alone.
	extra := make(map[string]any)
	for k, v := range msg {
		switch k {
		case "role", "content", "tool_calls", "tool_call_id", "name":
			continue
		}
		extra[k] = v
	}
	if raw, ok := msg["content"]; ok && contentToText(raw) == "" {
		if _, exists := extra[sentinelContent]; !exists {
			extra[sentinelContent] = raw
		}
	} else if _, ok := msg["content"]; !ok {
		if _, exists := extra[sentinelContent]; !exists {
			extra[sentinelContent] = sentinelMissing
		}
	}
	if len(extra) > 0 {
		dst.WriteString(tag("extra"))
		if err := encodePyJSON(dst, extra); err != nil {
			return err
		}
	}
	return nil
}

// contentToText normalises message.content to a string. OpenAI allows a plain
// string or a list of parts; image parts have no text form, so their URL is
// kept behind an <|image|> marker for the parser to recognise.
func contentToText(content any) string {
	switch c := content.(type) {
	case nil:
		return ""
	case string:
		return c
	case []any:
		var b strings.Builder
		for _, rawPart := range c {
			part, ok := asDict(rawPart)
			if !ok {
				b.WriteString(pyStr(rawPart))
				continue
			}
			ptype, _ := part["type"].(string)
			switch ptype {
			case "text":
				b.WriteString(pyOrEmpty(part["text"]))
			case "image_url":
				url := part["image_url"]
				if m, ok := asDict(url); ok {
					url = m["url"]
				}
				b.WriteString("<|image|>" + pyOrEmpty(url))
			default:
				var buf bytes.Buffer
				_ = encodePyJSON(&buf, part) // unknown part: keep it serialised
				b.Write(buf.Bytes())
			}
		}
		return b.String()
	default:
		var buf bytes.Buffer
		_ = encodePyJSON(&buf, content)
		return buf.String()
	}
}

// ---------------------------------------------------------------------------
// Python-compatible JSON encoder
// ---------------------------------------------------------------------------

// encodePyJSON writes v exactly as Python's
//
//	json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
//
// does. Byte identity with the reference matters more than speed here: a single
// differing byte changes a block hash and forfeits the cache.
func encodePyJSON(dst *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		dst.WriteString("null")
	case bool:
		if x {
			dst.WriteString("true")
		} else {
			dst.WriteString("false")
		}
	case string:
		encodePyString(dst, x)
	case json.Number:
		return encodePyNumber(dst, x)
	case float64:
		dst.WriteString(formatPyFloat(x))
	case float32:
		dst.WriteString(formatPyFloat(float64(x)))
	case int:
		dst.WriteString(strconv.FormatInt(int64(x), 10))
	case int8:
		dst.WriteString(strconv.FormatInt(int64(x), 10))
	case int16:
		dst.WriteString(strconv.FormatInt(int64(x), 10))
	case int32:
		dst.WriteString(strconv.FormatInt(int64(x), 10))
	case int64:
		dst.WriteString(strconv.FormatInt(x, 10))
	case uint:
		dst.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint8:
		dst.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint16:
		dst.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint32:
		dst.WriteString(strconv.FormatUint(uint64(x), 10))
	case uint64:
		dst.WriteString(strconv.FormatUint(x, 10))
	case []any:
		return encodePyArray(dst, x)
	case map[string]any:
		return encodePyObject(dst, x)
	default:
		return encodePyJSONReflect(dst, v)
	}
	return nil
}

func encodePyJSONReflect(dst *bytes.Buffer, v any) error {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			dst.WriteString("null")
			return nil
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.String:
		encodePyString(dst, rv.String())
		return nil
	case reflect.Bool:
		if rv.Bool() {
			dst.WriteString("true")
		} else {
			dst.WriteString("false")
		}
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dst.WriteString(strconv.FormatInt(rv.Int(), 10))
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dst.WriteString(strconv.FormatUint(rv.Uint(), 10))
		return nil
	case reflect.Float32, reflect.Float64:
		dst.WriteString(formatPyFloat(rv.Float()))
		return nil
	case reflect.Slice, reflect.Array:
		items := make([]any, rv.Len())
		for i := range items {
			items[i] = rv.Index(i).Interface()
		}
		return encodePyArray(dst, items)
	case reflect.Map:
		obj := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key, ok := pyJSONKey(iter.Key())
			if !ok {
				return fmt.Errorf("tail: encode: unsupported map key type %s", rv.Type().Key())
			}
			obj[key] = iter.Value().Interface()
		}
		return encodePyObject(dst, obj)
	default:
		return fmt.Errorf("tail: encode: unsupported type %T", v)
	}
}

// pyJSONKey converts a non-string map key the way Python's encoder does: numbers
// become their decimal form, true/false/null become those words.
func pyJSONKey(k reflect.Value) (string, bool) {
	for k.Kind() == reflect.Interface {
		if k.IsNil() {
			return "null", true
		}
		k = k.Elem()
	}
	switch k.Kind() {
	case reflect.String:
		return k.String(), true
	case reflect.Bool:
		if k.Bool() {
			return "true", true
		}
		return "false", true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(k.Uint(), 10), true
	case reflect.Float32, reflect.Float64:
		return formatPyFloat(k.Float()), true
	default:
		return "", false
	}
}

func encodePyArray(dst *bytes.Buffer, items []any) error {
	dst.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			dst.WriteByte(',')
		}
		if err := encodePyJSON(dst, item); err != nil {
			return err
		}
	}
	dst.WriteByte(']')
	return nil
}

func encodePyObject(dst *bytes.Buffer, obj map[string]any) error {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	// Python's sort_keys=True sorts by code point; UTF-8 byte order is the same
	// order for valid UTF-8, so sort.Strings matches.
	sort.Strings(keys)
	dst.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			dst.WriteByte(',')
		}
		encodePyString(dst, k)
		dst.WriteByte(':')
		if err := encodePyJSON(dst, obj[k]); err != nil {
			return err
		}
	}
	dst.WriteByte('}')
	return nil
}

// encodePyString escapes a string the way Python's C encoder does with
// ensure_ascii=False: only the two quote characters, the five short escapes and
// the remaining C0 controls are touched. DEL (0x7f) and all non-ASCII text stay
// raw.
func encodePyString(dst *bytes.Buffer, s string) {
	dst.WriteByte('"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		if start < i {
			dst.WriteString(s[start:i])
		}
		switch c {
		case '"':
			dst.WriteString(`\"`)
		case '\\':
			dst.WriteString(`\\`)
		case '\b':
			dst.WriteString(`\b`)
		case '\f':
			dst.WriteString(`\f`)
		case '\n':
			dst.WriteString(`\n`)
		case '\r':
			dst.WriteString(`\r`)
		case '\t':
			dst.WriteString(`\t`)
		default:
			// Remaining C0 controls: \u00XX with lowercase hex, 4 digits.
			const hexDigits = "0123456789abcdef"
			dst.WriteString(`\u00`)
			dst.WriteByte(hexDigits[c>>4])
			dst.WriteByte(hexDigits[c&0xf])
		}
		start = i + 1
	}
	if start < len(s) {
		dst.WriteString(s[start:])
	}
	dst.WriteByte('"')
}

// encodePyNumber writes a json.Number with the same normalisation Python's
// json.loads + json.dumps round trip applies: an integer literal becomes a
// canonical integer, anything with a fraction or exponent becomes a float and
// is formatted with Python's float repr.
func encodePyNumber(dst *bytes.Buffer, n json.Number) error {
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		// Python int: arbitrary precision, canonical decimal. "-0" is 0.
		i, ok := new(bigIntParser).parse(s)
		if !ok {
			return fmt.Errorf("tail: encode: bad number %q", s)
		}
		dst.WriteString(i)
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// Overflow is a legal JSON float literal in Python (it becomes inf), so
		// mirror json.loads rather than failing.
		if strings.HasPrefix(s, "-") {
			dst.WriteString("-Infinity")
		} else {
			dst.WriteString("Infinity")
		}
		return nil
	}
	dst.WriteString(formatPyFloat(f))
	return nil
}

// bigIntParser normalises an integer literal without pulling in math/big: strip
// a leading sign, drop a redundant leading minus on zero, keep the digits.
type bigIntParser struct{}

func (bigIntParser) parse(s string) (string, bool) {
	if s == "" {
		return "", false
	}
	neg := false
	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}
	if s == "" {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return "", false
		}
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0", true // "-0" and "00" are 0 to Python
	}
	if neg {
		return "-" + s, true
	}
	return s, true
}

// formatPyFloat formats a float the way Python's repr() does.
//
// Python switches to exponential notation when the decimal point position
// (number = 0.d1d2... x 10^decpt) is decpt <= -4 or decpt > 16, and it always
// keeps a ".0" on integral values -- neither of which Go's %g does.
func formatPyFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}

	s := strconv.FormatFloat(f, 'e', -1, 64) // shortest round-trip: "d.ddde±dd"
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	epos := strings.IndexByte(s, 'e')
	mantissa, expPart := s[:epos], s[epos+1:]
	exp, _ := strconv.Atoi(expPart)
	digits := strings.Replace(mantissa, ".", "", 1)
	decpt := exp + 1

	var out string
	if decpt <= -4 || decpt > 16 {
		out = digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		esign := '+'
		e := decpt - 1
		if e < 0 {
			esign = '-'
			e = -e
		}
		// Python pads the exponent to at least two digits.
		out += "e" + string(esign) + fmt.Sprintf("%02d", e)
	} else {
		switch {
		case decpt <= 0:
			out = "0." + strings.Repeat("0", -decpt) + digits
		case decpt >= len(digits):
			out = digits + strings.Repeat("0", decpt-len(digits)) + ".0"
		default:
			out = digits[:decpt] + "." + digits[decpt:]
		}
	}
	if neg {
		out = "-" + out
	}
	return out
}

// ---------------------------------------------------------------------------
// Python value helpers
// ---------------------------------------------------------------------------

// pyTruthy reports Python truthiness for the values a decoded JSON body can
// hold. The renderer uses `if x:` tests, and Go has no equivalent.
func pyTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return true
		}
		return f != 0
	case float64:
		return x != 0
	case float32:
		return x != 0
	case int:
		return x != 0
	case int64:
		return x != 0
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Pointer, reflect.Interface:
			return !rv.IsNil()
		case reflect.Slice, reflect.Array, reflect.Map:
			return rv.Len() > 0
		}
		return true
	}
}

// pyStr mirrors Python's str(v) for the scalar cases the renderer needs.
func pyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	case json.Number:
		return string(x)
	default:
		return fmt.Sprint(v)
	}
}

// pyOrEmpty is Python's `v or ""`.
func pyOrEmpty(v any) string {
	if !pyTruthy(v) {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return pyStr(v)
}

// asDict accepts the map forms a decoded body can take.
func asDict(v any) (map[string]any, bool) {
	switch x := v.(type) {
	case map[string]any:
		return x, true
	case nil:
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map && rv.Type().Key().Kind() == reflect.String {
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			out[iter.Key().String()] = iter.Value().Interface()
		}
		return out, true
	}
	return nil, false
}

// asSlice accepts the slice forms a decoded body can take.
func asSlice(v any) ([]any, bool) {
	switch x := v.(type) {
	case []any:
		return x, true
	case nil:
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = rv.Index(i).Interface()
		}
		return out, true
	}
	return nil, false
}
