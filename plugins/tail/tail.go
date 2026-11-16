// Package tail is the tail transport's switch: the one place a user turns it on,
// in the window's Plugins › Built-in list, and the four numbers it runs with.
//
// On, this magpie does both halves of the transport: it puts a request that came
// in as a tail back together, and it sends only the bytes a far end doesn't hold
// to the far ends that say they take one — which they say by answering the
// OPTIONS handshake (docs/subsystems/tail-transport.md). Nothing is configured
// per provider: what a far end takes is what it says it takes, and a far end
// that says nothing is sent whole requests. Off, every request goes whole, in
// both directions.
//
// The switch is the plugin's own: it serves internal/tail's point on Apply and
// stops it on dispose, so a flip is seen by the next request.
package tail

import (
	"time"

	"github.com/yetone/magpie/internal/pluginrt"
	tailwire "github.com/yetone/magpie/internal/tail"
)

// Settings are the panel's numbers. They take effect when the transport is
// turned on again, or magpie starts: the plugin reads them on Apply.
type Settings struct {
	CapFreshMin int `json:"capFreshMin" title:"Believe a yes for" description:"Minutes a far end that takes the transport is believed before it is asked again" min:"1" max:"1440"`
	CapAgainMin int `json:"capAgainMin" title:"Believe a no for" description:"Minutes a far end that doesn't is believed before it is asked again; shorter, so one upgraded today is used today" min:"1" max:"1440"`
	ProbeSec    int `json:"probeSec" title:"Give the handshake" description:"Seconds the OPTIONS handshake is given; it is asked off a turn's way, so no request ever waits on it" min:"1" max:"60"`
	CacheMB     int `json:"cacheMB" title:"Block cache" description:"Megabytes of blocks kept to put later requests back together from" min:"16" max:"8192"`
}

func init() {
	pluginrt.Register(pluginrt.Manifest{
		ID: "tail", API: "^1", Requires: []string{"gateway"}, Essential: false,
		Name: "Tail transport",
		Sub:  "Send only the bytes the far end doesn't hold: the same conversation, turn after turn, costs the upload once",
		Settings: Settings{
			CapFreshMin: 30,
			CapAgainMin: 5,
			ProbeSec:    3,
			CacheMB:     256,
		},
		Apply: func(c *pluginrt.Ctx) error {
			c.OnDispose(tailwire.Serve(configOf(c.Config)))
			return nil
		},
	})
}

// configOf reads the composed settings, and the transport's defaults for any the
// file doesn't hold.
func configOf(m map[string]any) tailwire.Config {
	cfg := tailwire.Defaults
	if v := minutes(m, "capFreshMin", cfg.CapFresh); v > 0 {
		cfg.CapFresh = time.Duration(v) * time.Minute
	}
	if v := minutes(m, "capAgainMin", cfg.CapAgain); v > 0 {
		cfg.CapAgain = time.Duration(v) * time.Minute
	}
	if v := seconds(m, "probeSec", cfg.Probe); v > 0 {
		cfg.Probe = time.Duration(v) * time.Second
	}
	if v := megabytes(m, "cacheMB", cfg.Cache); v > 0 {
		cfg.Cache = v
	}
	return cfg
}

// minutes is m's key as whole minutes, or d's when it isn't a positive number.
func minutes(m map[string]any, key string, d time.Duration) int {
	return whole(m[key], int(d.Minutes()))
}

// seconds is m's key as whole seconds, or d's when it isn't a positive number.
func seconds(m map[string]any, key string, d time.Duration) int {
	return whole(m[key], int(d.Seconds()))
}

// megabytes is m's key in bytes, or d's when it isn't a positive number.
func megabytes(m map[string]any, key string, d int64) int64 {
	return int64(whole(m[key], int(d>>20))) << 20
}

// whole is v as a positive integer, or d.
func whole(v any, d int) int {
	switch n := v.(type) {
	case int:
		if n > 0 {
			return n
		}
	case int64:
		if n > 0 {
			return int(n)
		}
	case float64:
		if n >= 1 {
			return int(n)
		}
	}
	return d
}
