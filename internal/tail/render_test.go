package tail

// Golden-vector tests against the Python reference implementation.
//
// The vectors in testdata/render_vectors.json were produced by the reference
// renderer (tail/transport/render.py + chunking.py). They are the contract
// between the two implementations: if a byte differs, the two sides cannot
// interoperate, because block boundaries and every block hash would differ too.
//
// The generator is deliberately kept out of this repo; regenerating the vectors
// means running the Python reference.

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type vectorFile struct {
	BlockSize   int          `json:"block_size"`
	BlockIDSize int          `json:"block_id_size"`
	Cases       []vectorCase `json:"cases"`
}

type vectorCase struct {
	Name    string   `json:"name"`
	Payload string   `json:"payload"`
	Text    string   `json:"text"`
	TextLen int      `json:"text_len"`
	Blocks  []string `json:"blocks"`
	Parsed  string   `json:"parsed"`
}

func loadVectors(t *testing.T, path string) vectorFile {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	var file vectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("parse testdata: %v", err)
	}
	if len(file.Cases) == 0 {
		t.Fatal("testdata has no cases")
	}
	return file
}

// vectorFiles are the golden corpora: hand-picked format features plus a
// randomized sweep. Both were produced by the Python reference.
var vectorFiles = []string{"testdata/render_vectors.json", "testdata/random_vectors.json"}

// TestRenderMatchesPython checks the rendered text byte for byte.
func TestRenderMatchesPython(t *testing.T) {
	for _, path := range vectorFiles {
		file := loadVectors(t, path)
		if file.BlockSize != BlockSize || file.BlockIDSize != BlockIDSize {
			t.Fatalf("%s: constants %d/%d, want %d/%d",
				path, file.BlockSize, file.BlockIDSize, BlockSize, BlockIDSize)
		}
		for _, tc := range file.Cases {
			tc := tc
			t.Run(path+"/"+tc.Name, func(t *testing.T) {
				got, err := RenderJSON([]byte(tc.Payload))
				if err != nil {
					t.Fatalf("RenderJSON: %v", err)
				}
				if len(got) != tc.TextLen {
					t.Fatalf("text length %d, want %d", len(got), tc.TextLen)
				}
				if string(got) != tc.Text {
					gotSnippet, wantSnippet := firstDiff(got, []byte(tc.Text))
					t.Fatalf("text mismatch\n got: %q\nwant: %q", gotSnippet, wantSnippet)
				}
			})
		}
	}
}

// TestBlockHashesMatchPython checks that the fixed-size cutting and the hash
// chain agree with the reference, which is what makes a block reusable across
// implementations.
func TestBlockHashesMatchPython(t *testing.T) {
	for _, path := range vectorFiles {
		file := loadVectors(t, path)
		for _, tc := range file.Cases {
			tc := tc
			t.Run(path+"/"+tc.Name, func(t *testing.T) {
				text, err := RenderJSON([]byte(tc.Payload))
				if err != nil {
					t.Fatalf("RenderJSON: %v", err)
				}
				blocks, _ := Blocks(text)
				if len(blocks) != len(tc.Blocks) {
					t.Fatalf("%d blocks, want %d", len(blocks), len(tc.Blocks))
				}
				for i, blk := range blocks {
					if got := blk.ID.String(); got != tc.Blocks[i] {
						t.Fatalf("block %d hash %s, want %s", i, got, tc.Blocks[i])
					}
				}
			})
		}
	}
}

// TestParseMatchesPython checks the text -> payload path: the reconstructed body
// must marshal to exactly what the reference produces, and must be plain JSON.
func TestParseMatchesPython(t *testing.T) {
	for _, path := range vectorFiles {
		file := loadVectors(t, path)
		for _, tc := range file.Cases {
			tc := tc
			t.Run(path+"/"+tc.Name, func(t *testing.T) {
				payload, err := Parse([]byte(tc.Text))
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				got, err := MarshalPayload(payload)
				if err != nil {
					t.Fatalf("MarshalPayload: %v", err)
				}
				if string(got) != tc.Parsed {
					gotSnippet, wantSnippet := firstDiff(got, []byte(tc.Parsed))
					t.Fatalf("payload mismatch\n got %q\nwant %q", gotSnippet, wantSnippet)
				}
				// The gateway hands Payload straight to the normal request path,
				// so it has to unmarshal as ordinary JSON.
				var v any
				if err := json.Unmarshal(got, &v); err != nil {
					t.Fatalf("payload is not valid JSON: %v", err)
				}
			})
		}
	}
}

// firstDiff trims a mismatch down to the first differing byte; whole texts are
// too long to be readable in a failure message.
func firstDiff(got, want []byte) (string, string) {
	const window = 60
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			lo := i - window
			if lo < 0 {
				lo = 0
			}
			hi := i + window
			if hi > len(got) {
				hi = len(got)
			}
			wlo, whi := lo, i+window
			if whi > len(want) {
				whi = len(want)
			}
			return string(got[lo:hi]), string(want[wlo:whi])
		}
	}
	return string(got), string(want)
}

// TestBlockIDHexMatchesPython keeps the hex form of the ids aligned: the wire
// header and the storage key both use it.
func TestBlockIDHexMatchesPython(t *testing.T) {
	id := Sum(nil, []byte("x"))
	want := "442a44457137672b3218c1007dc8f76a"
	if got := hex.EncodeToString(id[:]); got != want {
		t.Fatalf("blake2b-128 = %s, want %s", got, want)
	}
}

// numbersVector pins the encoder against Python's json.dumps output for numbers
// and strings, including float repr thresholds, denormals, huge integers and
// every escaping case. These are primitives the rendered text is built from, so a
// divergence here is a divergence in every block hash.
type numbersVector struct {
	BlockSize   int         `json:"block_size"`
	BlockIDSize int         `json:"block_id_size"`
	Numbers     [][2]string `json:"numbers"`
	Strings     [][2]string `json:"strings"`
	Nested      [2]string   `json:"nested"`
}

func loadNumbers(t *testing.T) numbersVector {
	t.Helper()
	raw, err := os.ReadFile("testdata/numbers.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	var vec numbersVector
	if err := json.Unmarshal(raw, &vec); err != nil {
		t.Fatalf("parse testdata: %v", err)
	}
	if len(vec.Numbers) == 0 || len(vec.Strings) == 0 {
		t.Fatal("testdata is empty")
	}
	return vec
}

func TestEncodeNumbersMatchPython(t *testing.T) {
	vec := loadNumbers(t)
	for _, tc := range vec.Numbers {
		literal, want := tc[0], tc[1]
		got, err := MarshalPayload(map[string]any{"v": json.Number(literal)})
		if err != nil {
			t.Fatalf("MarshalPayload(%s): %v", literal, err)
		}
		if wantJSON := `{"v":` + want + `}`; string(got) != wantJSON {
			t.Fatalf("number %s encoded %s, want %s", literal, got, wantJSON)
		}
	}
}

func TestEncodeStringsMatchPython(t *testing.T) {
	vec := loadNumbers(t)
	for _, tc := range vec.Strings {
		in, want := tc[0], tc[1]
		got, err := MarshalPayload(map[string]any{"v": in})
		if err != nil {
			t.Fatalf("MarshalPayload(%q): %v", in, err)
		}
		if wantJSON := `{"v":` + want + `}`; string(got) != wantJSON {
			t.Fatalf("string %q encoded %s, want %s", in, got, wantJSON)
		}
	}
}

func TestEncodeNestedMatchesPython(t *testing.T) {
	vec := loadNumbers(t)
	payload, err := decodeDocument(vec.Nested[0])
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	got, err := MarshalPayload(payload)
	if err != nil {
		t.Fatalf("MarshalPayload: %v", err)
	}
	if string(got) != vec.Nested[1] {
		t.Fatalf("nested encoded %s, want %s", got, vec.Nested[1])
	}
}

// decodeDocument decodes raw JSON with numbers kept as json.Number.
func decodeDocument(raw string) (map[string]any, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var payload map[string]any
	if err := dec.Decode(&payload); err != nil {
		return nil, err
	}
	return payload, nil
}
