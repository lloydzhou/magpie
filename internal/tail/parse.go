package tail

// Parser: transport text -> request body.
//
// The inverse of Render, streaming in both directions: bytes go in as they
// arrive, events come out as soon as a message (or a top-level parameter) is
// complete. Nothing ever needs the whole object assembled first.
//
// Tag detection is JSON-aware. Content and field values are carried as JSON
// strings, so a `<|user|>` *inside* a JSON string is content, not structure.
// The parser tracks string/escape state while scanning for "<|", which is why
// no extra escape syntax is needed: JSON already provides it.
//
// A "<|name|>" whose name is not in allTags is kept as content, so text that
// happens to contain a tag-shaped word survives the round trip.

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// EventKind is the type of a parser event.
type EventKind int

const (
	// EventParam is a top-level parameter: Name plus decoded Value.
	EventParam EventKind = iota
	// EventMessageStart opens a message; Role holds the role name.
	EventMessageStart
	// EventMessageEnd carries a completed message in Message.
	EventMessageEnd
)

// Event is one parser output. Which fields are set depends on Kind.
//
// Body text is not an event: the parser accumulates it into the message it
// belongs to and only reports completed messages and parameters, exactly like the
// reference parser.
type Event struct {
	Kind    EventKind
	Name    string         // EventParam: parameter name
	Value   any            // EventParam: decoded JSON value
	Role    string         // EventMessageStart / EventMessageEnd
	Message map[string]any // EventMessageEnd: the reconstructed message
}

const roleParam = "__param__"

var tagOpenBytes = []byte(tagOpen)
var tagCloseBytes = []byte(tagClose)

// Parser decodes transport text incrementally. Call Feed for every byte slice
// and Finish once at the end.
type Parser struct {
	buf []byte

	// JSON string state, advanced incrementally so each byte is scanned once:
	// inStr is "inside a JSON string", escaped is "previous byte was a
	// backslash that has not been paired yet", scan is how far into buf that
	// state is valid.
	inStr   bool
	escaped bool
	scan    int

	curRole    string // "" = no open message; roleParam = collecting a parameter
	paramName  string
	paramValue []byte
	parts      []byte            // body text of the current message
	fields     map[string]string // field tag -> text following it
	inField    string            // which field body text belongs to ("" = content)
}

// NewParser returns a parser with no state.
func NewParser() *Parser { return &Parser{fields: make(map[string]string)} }

// Feed consumes a byte slice and returns every event that became complete.
//
// Any byte boundary is legal, including the middle of a multi-byte UTF-8
// character: the scanner works on bytes and only tags, quotes and backslashes
// are significant, all of them ASCII.
func (p *Parser) Feed(piece []byte) ([]Event, error) {
	if len(piece) > 0 {
		p.buf = append(p.buf, piece...)
	}
	return p.drain(false)
}

// Finish flushes the remaining buffer and closes the open message.
func (p *Parser) Finish() ([]Event, error) {
	events, err := p.drain(true)
	if err != nil {
		return events, err
	}
	last, err := p.flushMessage()
	if err != nil {
		return events, err
	}
	return append(events, last...), nil
}

// drain cuts the buffer into "body text" and "tags" and handles each.
func (p *Parser) drain(final bool) ([]Event, error) {
	var out []Event
	buf := p.buf
	n := len(buf)
	pos := 0

	for pos < n {
		openAt := p.findTagStart(buf, pos)
		if openAt < 0 {
			// No tag left. Unless this is the final drain, hold back the last
			// byte so a tag split across two Feeds is not mistaken for content.
			if final {
				p.appendText(buf[pos:])
				pos = n
			} else {
				end := n - 1
				if end < pos {
					end = pos
				}
				p.appendText(buf[pos:end])
				pos = end
			}
			break
		}

		closeRel := bytes.Index(buf[openAt+len(tagOpen):], tagCloseBytes)
		if closeRel < 0 {
			// Incomplete tag: keep it for the next Feed.
			if final {
				p.appendText(buf[pos:])
				pos = n
			} else {
				p.appendText(buf[pos:openAt])
				pos = openAt
			}
			break
		}
		closeAt := openAt + len(tagOpen) + closeRel
		name := string(buf[openAt+len(tagOpen) : closeAt])

		if !allTags[name] {
			// Not a known tag: keep it together with its brackets as content.
			p.appendText(buf[pos : closeAt+len(tagClose)])
			pos = closeAt + len(tagClose)
			continue
		}

		// Whatever came before the tag belongs to the previous message.
		p.appendText(buf[pos:openAt])
		pos = closeAt + len(tagClose)

		switch {
		case roleTags[name]:
			flushed, err := p.flushMessage()
			if err != nil {
				return out, err
			}
			out = append(out, flushed...)
			p.curRole = name
			out = append(out, Event{Kind: EventMessageStart, Role: name})
		case name == "tools" || paramOrderSet[name]:
			flushed, err := p.flushMessage()
			if err != nil {
				return out, err
			}
			out = append(out, flushed...)
			p.curRole = roleParam
			p.paramName = name
			p.paramValue = nil
		default:
			// A field tag inside a message: the text after it is that field's.
			p.fields[name] = ""
			p.inField = name
		}
	}

	// Drop the consumed prefix. The slice reuses the array, and the append in
	// Feed only copies the still-live bytes once the capacity runs out.
	p.buf = buf[pos:]
	if p.scan >= pos {
		p.scan -= pos
	} else {
		p.scan = 0
	}
	return out, nil
}

// findTagStart returns the index of the next *effective* tag opening at or after
// pos, or -1.
//
// A "<|" inside a JSON string (or straddling the scan boundary) is content, so
// the string state is consulted while searching. That state is advanced
// incrementally: each byte is only ever inspected once.
func (p *Parser) findTagStart(buf []byte, pos int) int {
	for {
		rel := bytes.Index(buf[pos:], tagOpenBytes)
		if rel < 0 {
			p.advanceScan(buf, len(buf))
			return -1
		}
		openAt := pos + rel
		if p.advanceScan(buf, openAt) && !p.inStr {
			return openAt
		}
		pos = openAt + len(tagOpen)
		if p.scan < pos {
			p.scan = pos
		}
	}
}

// advanceScan moves the JSON string state forward to upto and reports whether
// the scanner is outside a string afterwards.
//
// The backslash state deliberately carries across calls: a string ending in a
// backslash split between two Feeds must not treat the next quote as a
// boundary. Character-at-a-time state is the correctness basis here; only the
// iteration style (slicing, not indexing) may be optimised.
func (p *Parser) advanceScan(buf []byte, upto int) bool {
	if upto > p.scan {
		inStr, escaped := p.inStr, p.escaped
		for _, ch := range buf[p.scan:upto] {
			if escaped {
				escaped = false
			} else if ch == '\\' {
				escaped = true
			} else if ch == '"' {
				inStr = !inStr
			}
		}
		p.inStr, p.escaped = inStr, escaped
		p.scan = upto
	}
	return !p.inStr
}

// appendText routes text to the parameter value, the current field or the
// message body, depending on state.
func (p *Parser) appendText(b []byte) {
	if len(b) == 0 {
		return
	}
	if p.curRole == roleParam {
		p.paramValue = append(p.paramValue, b...)
		return
	}
	if p.curRole == "" {
		// Preamble before any message/parameter: dropped, like the reference.
		return
	}
	if p.inField != "" {
		p.fields[p.inField] += string(b)
		return
	}
	p.parts = append(p.parts, b...)
}

// flushMessage closes the open message or parameter and returns the resulting
// event, if any.
func (p *Parser) flushMessage() ([]Event, error) {
	if p.curRole == "" {
		return nil, nil
	}
	role := p.curRole
	paramName := p.paramName
	paramValue := p.paramValue
	parts := p.parts
	fields := p.fields

	p.curRole = ""
	p.paramName = ""
	p.paramValue = nil
	p.parts = nil
	p.fields = make(map[string]string)
	p.inField = ""

	if role == roleParam {
		value, ok := pyLoads(paramValue)
		if !ok {
			// Same as the reference: an unparsable parameter value yields no
			// event instead of failing the stream.
			return nil, nil
		}
		return []Event{{Kind: EventParam, Name: paramName, Value: value}}, nil
	}

	msg, err := buildMessage(role, parts, fields)
	if err != nil {
		return nil, err
	}
	return []Event{{Kind: EventMessageEnd, Role: role, Message: msg}}, nil
}

// buildMessage assembles one message from its body text and field texts.
func buildMessage(role string, parts []byte, fields map[string]string) (map[string]any, error) {
	msg := map[string]any{"role": role}

	if len(parts) > 0 {
		if v, ok := pyLoads(parts); ok {
			msg["content"] = v
		} else {
			msg["content"] = string(parts) // tolerate non-JSON content
		}
	}

	if raw, ok := fields["tool_calls"]; ok {
		calls, err := parseToolCalls(raw)
		if err != nil {
			return nil, err
		}
		msg["tool_calls"] = calls
	}
	if raw, ok := fields["tool_call_id"]; ok {
		if v, ok := pyLoads([]byte(raw)); ok {
			msg["tool_call_id"] = v
		} else {
			msg["tool_call_id"] = raw
		}
	}
	if raw, ok := fields["name"]; ok {
		if v, ok := pyLoads([]byte(raw)); ok {
			msg["name"] = v
		} else {
			msg["name"] = raw
		}
	}
	if raw, ok := fields["extra"]; ok {
		extra := map[string]any{}
		if v, ok := pyLoads([]byte(raw)); ok {
			if m, isDict := asDict(v); isDict {
				extra = m
			}
		}
		// __content__ records whether the original message had a content key
		// and what it was: absent / null / "" all produce no body text, so the
		// three can only be told apart here.
		saved, hadContent := extra[sentinelContent]
		delete(extra, sentinelContent)
		for k, v := range extra {
			msg[k] = v
		}
		if hadContent {
			if _, exists := msg["content"]; !exists && !isMissingSentinel(saved) {
				msg["content"] = saved
			}
		}
	}
	return msg, nil
}

// parseToolCalls decodes the deterministic JSON under <|tool_calls|>.
//
// arguments is restored to a string (the shape OpenAI specifies) so the
// reconstructed request matches the client's original one.
func parseToolCalls(raw string) (any, error) {
	var calls any
	if err := pyUnmarshal([]byte(raw), &calls); err != nil {
		return nil, fmt.Errorf("tail: parse tool_calls: %w", err)
	}
	items, ok := asSlice(calls)
	if !ok {
		return calls, nil
	}
	for _, rawCall := range items {
		call, ok := asDict(rawCall)
		if !ok {
			continue
		}
		fn, ok := asDict(call["function"])
		if !ok {
			continue
		}
		if _, isString := fn["arguments"].(string); isString {
			continue
		}
		arg := fn["arguments"]
		if !pyTruthy(arg) {
			arg = map[string]any{}
		}
		var buf bytes.Buffer
		if err := encodePyJSON(&buf, arg); err != nil {
			return nil, err
		}
		fn["arguments"] = buf.String()
	}
	return calls, nil
}

// Parse decodes a complete transport text into a request body.
//
// Numbers come back as json.Number so that marshalling them again is
// byte-identical with Python (see MarshalPayload); a plain json.Marshal of the
// result agrees for strings and integers but can format large floats
// differently.
func Parse(text []byte) (map[string]any, error) {
	parser := NewParser()
	events, err := parser.Feed(text)
	if err != nil {
		return nil, err
	}
	rest, err := parser.Finish()
	if err != nil {
		return nil, err
	}
	events = append(events, rest...)

	payload := make(map[string]any)
	var messages []any
	for _, ev := range events {
		switch ev.Kind {
		case EventMessageEnd:
			messages = append(messages, ev.Message)
		case EventParam:
			payload[ev.Name] = ev.Value
		}
	}
	if len(messages) > 0 {
		payload["messages"] = messages
	}
	return payload, nil
}

// ParseJSON decodes transport text straight into request-body JSON.
//
// This is the inbound half of the round trip: the gateway reconstructs the
// request the client originally sent.
func ParseJSON(text []byte) ([]byte, error) {
	payload, err := Parse(text)
	if err != nil {
		return nil, err
	}
	return MarshalPayload(payload)
}

// MarshalPayload encodes a request body with Python's json.dumps settings
// (sorted keys, compact separators, no ASCII escaping) so both sides produce
// the same bytes.
func MarshalPayload(payload map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	if err := encodePyJSON(&buf, payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// pyLoads decodes JSON with numbers kept as json.Number. ok=false mirrors
// Python's ValueError.
func pyLoads(raw []byte) (any, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var v any
	if err := pyUnmarshal(raw, &v); err != nil {
		return nil, false
	}
	return v, true
}

func pyUnmarshal(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}

// isMissingSentinel reports whether v is the {"__missing__": true} marker that
// records "the message had no content key".
func isMissingSentinel(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	b, ok := m["__missing__"].(bool)
	return ok && b
}

// paramOrderSet is paramOrder as a set for the parser.
var paramOrderSet = func() map[string]bool {
	m := make(map[string]bool, len(paramOrder))
	for _, n := range paramOrder {
		m[n] = true
	}
	return m
}()
