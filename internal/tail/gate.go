package tail

// The transport's switch and its settings, as the built-in plugin `tail`
// serves them (plugins/tail, docs/subsystems/tail-transport.md).
//
// The gateway asks this point rather than an environment variable, the way it
// asks gwtool and gwbudget for theirs: the panel is where a user turns the
// transport on and off, and there is no other switch. Off is what a process
// nothing has served in gets — a CLI, a TUI, or a gateway whose plugin is off —
// and off sends and takes every request whole.
//
// The plugin's Apply serves it, and its dispose stops it, so a flipped switch
// is seen by the next request: nothing is cached here.

import (
	"sync"
	"sync/atomic"
	"time"
)

// Config is what the panel's settings say. The durations and the size are read
// where they are used, so a config served with another cache size rebuilds the
// block cache.
type Config struct {
	// CapFresh is how long "this far end takes the transport" is believed.
	CapFresh time.Duration
	// CapAgain is how long "it doesn't" is believed: shorter, because an
	// endpoint upgraded this morning should be used this afternoon.
	CapAgain time.Duration
	// Probe bounds the handshake asked off a turn's way.
	Probe time.Duration
	// Cache is how many bytes the block cache may hold.
	Cache int64
}

// Defaults are the settings a panel nobody has touched serves: the values the
// transport was measured with.
var Defaults = Config{
	CapFresh: 30 * time.Minute,
	CapAgain: 5 * time.Minute,
	Probe:    3 * time.Second,
	Cache:    256 << 20,
}

var current atomic.Pointer[Config]

// Serve turns the transport on with cfg until stop: this gateway takes tail
// requests, and sends tails to the far ends that say they take them. stop turns
// it off, as the plugin's dispose does.
//
// Off leaves nothing behind: every request goes whole. The block cache lives on
// until one of another size is asked for, so turning the transport off and on
// again doesn't cost a conversation the prefix the cache still holds.
func Serve(cfg Config) (stop func()) {
	current.Store(&cfg)
	var once sync.Once
	return func() {
		once.Do(func() { current.Store(nil) })
	}
}

// Served is whether the transport is on.
func Served() bool { return current.Load() != nil }

// Settings are the settings in force: the panel's, or Defaults while nothing
// serves this point.
func Settings() Config {
	if c := current.Load(); c != nil {
		return *c
	}
	return Defaults
}
