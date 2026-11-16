# Tail transport

A long agent turn resends the whole conversation every time, and on a home
uplink that upload is most of the wait before the first token. The tail
transport sends only the bytes the far end doesn't hold yet: a request body is
rendered into a stable text stream, cut into fixed blocks and hashed into a
chain, so a client that knows the far end's prefix can name it and upload only
the bytes after it. Both ends are magpies, and at each of them the built-in
plugin `tail` is the switch: on, that magpie takes such a request apart and sends
one to the far ends that say they take it.

## Responsibilities

- **Block chain** (`internal/tail`, source of truth for the wire format): render
  a Chat request body into a one-dimensional text stream (`render.go`), parse it
  back (`parse.go`), cut it into 512 B blocks and hash them into a chain
  (`chunking.go`), keep the client's credential table and the server's
  resolution (`codec.go`), and hold blocks (`store.go`).
- **Ingress** (`gateway.tailIngress`, `internal/gateway/tail_wire.go`): put a
  request whose body is only a tail back together before anything reads it.
- **Egress** (`gateway.tailOut`, `gateway.tailRetransmit`): send a request as the
  tail of its own body to a far end that answered the handshake.
- **Switch and settings** (`plugins/tail`, `internal/tail/gate.go`): the
  window's Plugins › Built-in row that turns both halves on, and the four
  numbers they run with.
- **Out of scope**: no protocol translation, routing or provider fixups change;
  no block cache shared across processes (each gateway's is its own).

## The switch

One switch, in the window's Plugins › Built-in list, for both halves: the
built-in plugin `tail` (`plugins/tail`, off in the Essentials preset, on
otherwise like every built-in). On, this magpie puts a tail request back together
*and* sends tails to the far ends that say they take them. Off, every request
goes whole in both directions, exactly as before this transport existed.

There is nothing to configure per provider. What a far end takes is what it says
it takes: the handshake below, asked off every turn's way and believed for a
while. A provider's URL stays the far end's ordinary Chat base (`…/v1`), and a
far end that says nothing gets byte-identical requests to what magpie sent before.

The same panel holds the four numbers, from the plugin's `Settings` struct
([`plugins/tail/tail.go`](../../plugins/tail/tail.go)):

| Setting | Default | What it is |
| --- | --- | --- |
| `capFreshMin` | 30 | minutes a "yes" to the handshake is believed |
| `capAgainMin` | 5 | minutes a "no" is believed, then asked again |
| `probeSec` | 3 | seconds the handshake is given |
| `cacheMB` | 256 | megabytes the block cache may hold |

They are read on the plugin's `Apply`, so an edited value applies when the
transport is turned off and on again, or magpie starts. The switch follows
plugins.yml like every other built-in's (`pluginrt.KeepFresh` reconciles within a
second or two); the point itself caches nothing — `tail.Served()` reads the state
`Apply` and dispose leave — so the request after that sees it.

## Wire format

`internal/tail` is the source of truth; this is what a reader of a capture needs.

- Request: `Content-Type: application/x-tail-v3`, optional
  `X-Tail-Have: <length>;<tip_hex>` — the prefix the client knows the far end
  holds: how many bytes it covers (a multiple of `BlockSize`) and the 16-byte id
  of its last block, hex.
- Body: the rendered text from that offset on. With no `X-Tail-Have` it is the
  whole request, so a cold client and a client whose prefix was lost use the
  same path.
- Reply: `X-Tail-Version`, `X-Tail-Have` (the prefix this turn leaves behind, for
  the next one), `X-Tail-Hit: 1` when the request named a prefix at all.
- `409` with `X-Tail-Retransmit: 1`: the named prefix isn't there. The client
  resends the whole request once.

Block ids are `blake2b(parent_id || block)[:16]`, the chain root's parent being
no bytes at all. Only full blocks are stored, so a turn is charged one block of
re-alignment plus its own new bytes.

## The handshake, and why the body may stop being JSON

Every other request magpie sends is a JSON body. A tail request is not: it is the
rendered text from some offset on. So before one goes out, the egress has to know
the far end takes it:

```
OPTIONS {the URL the request would POST to}
  -> 204, Allow: POST, OPTIONS
     X-Tail-Version: 3
     X-Tail-Block: 512
```

`OPTIONS` on the path the request itself uses, rather than a new endpoint: there
is nothing new for a far end to implement. The block size is in the answer
because two implementations that disagree on it never hit — the client would name
a boundary the peer's chain does not have — so it is advertised instead of
assumed. A gateway with the transport off answers too, without `X-Tail-Version`:
"JSON only" is an answer worth having, and it is how a far end upgraded later is
found within `capAgainMin`.

**It is never asked on a turn's way.** Three states, and only one of them sends a
tail:

| State | What a turn does | How it is left |
| --- | --- | --- |
| unknown | **goes whole — plain JSON**, exactly as before this transport existed | a background `OPTIONS` is started, once, and this turn does not wait for it |
| takes | the tail transport | the answer is believed for `capFreshMin` (30 min) |
| does not | **goes whole, always** | believed for `capAgainMin` (5 min), then asked again |

So the handshake costs a turn no latency at all, and costs a fresh conversation
nothing it wasn't already paying: its first turn is a full one either way. The
question is started early anyway — listing models does it (`tailAskAll`), which is
the one thing an agent does before its first turn — so in practice the answer is
there before the turn is, and the first turn already goes as a tail.

Two consequences worth stating plainly:

- **Deployment order stops mattering.** The near end may be upgraded (or turned
  on) before the far end is; it asks, is told no, and sends whole requests until
  the answer changes. The old failure mode was a first request that wasn't JSON
  meeting a server that parsed JSON: a 400/415 per turn, counted as an upstream
  failure — or, worse, a 200 from something that read nothing.
- **A lying far end costs one retry, not a broken turn.** If it answered the
  handshake and then refused a tail as a body it couldn't read (it was replaced
  under a running gateway), the request goes again as plain JSON, the transport
  is left off, and it is asked again after a few minutes. A refusal of the
  *request* — a content policy, a bad tool pair — is not mistaken for that: the
  reply is read first, and only a refusal that names JSON or the content type
  triggers it.

## Runtime paths

Ingress, one Chat turn:

1. `Server.handle` reads the body (`requestBody`), then calls
   `Server.tailIngress` before `requestModel`.
2. `tailIngress` checks the gateway switch and the content type, decodes
   `X-Tail-Have`, and asks `tail.Server.Decode` to walk the chain from the tip
   and rebuild the text. It stores the blocks this turn adds.
3. Missing blocks return `ErrMissingPrefix` → `409` + `X-Tail-Retransmit: 1`.
   Anything else is a `400`.
4. The rebuilt text is parsed to JSON, the prefix this turn leaves is written to
   the reply's `X-Tail-Have`, and that JSON body continues through the gateway
   as if it had just arrived: model routing, translation and the provider's
   fixups all see an ordinary request.

Egress, one Chat turn:

1. `Server.forwardOnce` finalizes the body, then calls `Server.tailOut`. The
   transport off, another protocol, the whole-request retry of `tailFallback`, or
   an encoded body leaves the request untouched. So does a far end that has not answered the handshake:
   `tailOut` asks `tailCapable` first, which answers from cache and never probes
   on the request's way: an unknown answer means this turn goes whole, and the
   question is asked in the background.
2. `tailOut` asks the provider's credential table for a `Prepared` request, sets
   the tail as the request body, `application/x-tail-v3` as the content type and
   the prefix in `X-Tail-Have`. What the ledger counts as sent is the tail's
   bytes, not the request it stands for.
3. A successful reply's `X-Tail-Have` becomes the credential for the next turn.
4. `Server.forward` intercepts `409` + `X-Tail-Retransmit` before
   `PassesErrors` and before any routing fallback: it drops the provider's
   credentials (`tail.Client.Reset`) and asks `forwardOnce` once more, which
   now sends the whole request. A 409 is the transport's own recovery, so it
   never moves a request to another provider or rests a key.

## State

- **Capability**: per provider, in memory (`Server.tailCaps`), the far end's
  answer to the handshake and when that answer goes stale. Not persisted: a
  restarted gateway asks again, which is one OPTIONS per provider, off any
  turn's way.
- **Client credential table**: per provider, in memory (`Server.tailClients`),
  `session → [(length, tip)]`. A session is the body's own content, so it needs
  no bookkeeping across restarts: a fresh gateway simply misses and gets the
  whole request.
- **Block cache**: one per gateway (`Server.tailSrv`, `cacheMB` budget, rebuilt
  when that size changes), holding
  `parent → content` and a chain walkable from a tip. It is not shared between
  gateway processes; a session that moves to another gateway gets one 409 and
  one full upload, then is cached there.

## Failure and compatibility

- The switch off: no behavior change anywhere. A body that isn't JSON is
  rejected exactly as before, and nothing is probed.
- A far end that doesn't answer the handshake is never sent a tail, so its turns
  are byte-identical to what magpie sent before this transport existed.
- A far end that lost the prefix costs one 409 and one full upload; the agent
  never sees it.
- A body the codec can't render is sent whole: the transport never turns a
  request into an error.
- `Decoded.Payload` is normalized JSON (keys sorted, Python float repr), so it
  may differ byte-for-byte from what the client sent while standing for the same
  request. Anything that needs the original bytes uses `tail.MarshalPayload`.
- The tail request's `Content-Type` is not rewritten as text
  (`jsonBody` in `body_text.go` excludes it): rewriting a byte would move every
  block boundary after it.

## Verification

```sh
go test -tags nogui ./internal/tail/                     # the wire format
go test -tags nogui -run TestTail ./internal/gateway/    # both edges
```

`internal/tail/testdata` holds golden vectors generated from the Python
reference implementation: rendered text, every block's id, and the parsed
payload must match byte for byte, since two implementations that disagree on a
block boundary never hit.

`internal/gateway/tail_wire_test.go` covers: a tail turn reaching the vendor
whole, the switch being the only configuration there is (a panel turned on
serving a tail and sending one, with nothing set on the provider), sending
only its new bytes, a far end that lost the prefix being asked for the whole
request exactly once, four conversations in flight together arriving as twelve
un-mixed histories, and a far end that came back empty being recovered from in
one turn. `internal/tail/codec_test.go` pins the credential table under
out-of-order replies (and shows what a one-entry table would have lost).

For the two ends together, run two of the built binary with the plugin on, one
with a provider at the inference service and one with a provider pointed at the
first, and compare what a TCP counter between them sees against the same turns
sent with the plugin off.

The conditions a real deployment runs in — several sessions at once, replies
arriving out of order, the gateway restarted under a conversation — are checked
by the tail repository's `tools/hardcases.py`, which drives two real binaries and
counts bytes on the wire between them:

```sh
python -m tools.hardcases --magpie /tmp/magpie-tail --check all --sessions 4 --rounds 3
```

## Concurrent sessions, out-of-order replies, restarts

Three things that do not change a single byte of the wire format but can make the
numbers lie or the saving quietly disappear.

**Several sessions at once.** Credentials are keyed by the body's own first
block (`Prepared.Session`), so two conversations cannot share one: the key is
content, not a name. A parallel burst of four sessions × three turns arrives at
the inference service as twelve complete, un-mixed histories.

**Replies out of order.** `tail.Client` keeps four credentials per session
(`maxCredentials`), and `Prepare` picks the *longest* prefix they prove, not the
last one written. This matters because a later request in a conversation can be
answered before an earlier one: with a single credential, the late short reply
overwrites the long one, and the next turn still "works" — still hits, still
saves — while claiming a prefix that is far shorter than it could. Nothing
errors; the saving just drains away. Measured cold on one conversation, four
requests in flight with the shortest one answered last:

These are the numbers those two tests pin (bytes on a real wire, two processes,
measured by the tail repository's `tools/hardcases.py`):

| Credentials kept | Next turn up | Full text |
| --- | ---: | ---: |
| 4 (`maxCredentials`) | 20,495 B | 130,609 B |
| 1 | 80,910 B | 130,609 B |

So the entry a single-credential table keeps is not just "one turn stale": it is
whichever turn happened to answer last.

**A restart under a conversation.** The block cache is memory, so a restarted
gateway holds none of the prefixes its clients believe in. The first turn after
a restart therefore pays for the failed attempt *and* the whole request — and
then recovers:

| Turn after restart | Upload | Full text |
| ---: | ---: | ---: |
| 1 | 106,475 B | 90,709 B |
| 2 | 15,658 B | 105,805 B |
| 3 | 15,923 B | 120,883 B |
| 4 | 15,762 B | 136,048 B |

The first row is larger than the full text because it holds both: the 15,766 B
tail sent on the stale credential (refused with `409`) and the 90,709 B resend.
That is the whole cost of a restart — one wasted round, no error at the agent.

## What it is worth, by turn kind

The saving is prefix-shaped, so it is not one number. Over a trace with the
events a real agent produces (appends, a compaction, a branch, a tool added
mid-conversation, two sessions), 20 turns of ~40 KB system prompt and ~24 KB a
turn, 25 Mbit/s uplink, two magpies and a throttle between them:

| Turn kind | Turns | Plain up | Tail up | Plain upload | Tail upload | Saving |
| --- | --- | --- | --- | --- | --- | --- |
| append | 16 | 3.27 MB | 0.69 MB | 1,337 ms | 253 ms | 4.7x |
| branch | 2 | 0.37 MB | 0.23 MB | 153 ms | 90 ms | 1.7x |
| new tools | 1 | 0.18 MB | 0.18 MB | 73 ms | 74 ms | 1.0x |
| compaction | 1 | 0.12 MB | 0.12 MB | 49 ms | 50 ms | 1.0x |
| total | 20 | 3.85 MB | 1.18 MB | 1,612 ms | 466 ms | 3.3x |

The rows that matter as much as the total are the ones at 1.0x: a compaction or a
reordered history is un-savable by construction, since the prefix itself changed,
and that turn costs what it always did. So the transport pays on the turns a
conversation is growing — which is most of them — and pays nothing on the ones
that rewrite it.

The same trace at 10 Mbit/s takes plain 3.95 s against tail 0.90 s, and the plain
upload time rises by the ratio the bandwidth fell by: the throttle measures the
upload, not just the clock.
