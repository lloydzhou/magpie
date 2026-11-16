package tail

import (
	"testing"
	"time"

	"github.com/yetone/magpie/internal/pluginrt"
	tailwire "github.com/yetone/magpie/internal/tail"
)

// The panel exists: the built-in plugin `tail` is registered, off in a preset
// that leaves it out rather than on at any price, with the four numbers the
// transport runs with.
func TestThePanelIsRegistered(t *testing.T) {
	for _, m := range pluginrt.Registered() {
		if m.ID != "tail" {
			continue
		}
		if m.Essential {
			t.Fatal("the transport is on in the Essentials preset: it is not essential")
		}
		s := m.Schema()
		if s == nil {
			t.Fatal("the panel has no settings to draw")
		}
		for _, key := range []string{"capFreshMin", "capAgainMin", "probeSec", "cacheMB"} {
			if _, ok := s.Properties[key]; !ok {
				t.Fatalf("the panel has no %s", key)
			}
		}
		return
	}
	t.Fatal("no built-in plugin tail: nothing turns the transport on")
}

// The panel's numbers are what the transport gets, and a value the file doesn't
// hold leaves the transport's default standing.
func TestConfigOfReadsThePanel(t *testing.T) {
	got := configOf(map[string]any{
		"capFreshMin": float64(5), // json numbers read back as float64
		"capAgainMin": int64(2),
		"probeSec":    1,
		"cacheMB":     64,
	})
	want := tailwire.Config{
		CapFresh: 5 * time.Minute,
		CapAgain: 2 * time.Minute,
		Probe:    time.Second,
		Cache:    64 << 20,
	}
	if got != want {
		t.Fatalf("configOf = %+v, want %+v", got, want)
	}
	if got := configOf(nil); got != tailwire.Defaults {
		t.Fatalf("with nothing set: %+v, want the defaults %+v", got, tailwire.Defaults)
	}
	// a value that isn't a positive number is not one to run with
	if got := configOf(map[string]any{"cacheMB": 0, "probeSec": "x"}); got != tailwire.Defaults {
		t.Fatalf("with nonsense set: %+v, want the defaults", got)
	}
}
