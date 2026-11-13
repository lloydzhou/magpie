# Tail transport

A long agent turn resends the whole conversation every time, and on a home
uplink that upload is most of the wait before the first token. The tail
transport sends only the bytes the far end doesn't hold yet: a request body is
rendered into a stable text stream, cut into fixed blocks and hashed into a
chain, so a client that knows the far end's prefix can name it and upload only
the bytes after it. Both ends are magpies, each behind a switch of its own.

## Responsibilities

- **Block chain** (`internal/tail`, source of truth for the wire format): render
  a Chat request body into a one-dimensional text stream (`render.go`), parse it
  back (`parse.go`), cut it into 512 B blocks and hash them into a chain
  (`chunking.go`), keep the client's credential table and the server's
  resolution (`codec.go`), and hold blocks (`store.go`).
- **Ingress** (`gateway.tailIngress`, `internal/gateway/tail_wire.go`): put a
  request whose body is only a tail back together before anything reads it.
- **Egress** (`gateway.tailOut`, `gateway.tailRetransmit`): send a request to a
  provider marked `tail=on` as the tail of its own body.
- **Out of scope**: no protocol translation, routing or provider fixups change;
  no block cache shared across processes (each gateway's is its own).

## Switches

| Switch | Where | Meaning |
| --- | --- | --- |
| `MAGPIE_TAIL=1` | `gateway.tailOn` (`gateway.TailEnv`) | This gateway puts a tail request back together: a request whose `Content-Type` is `application/x-tail-v3` is answered, and the reply carries the prefix the next turn may name. Off, the default, such a body is not JSON and is turned away as any other body that isn't. It is read from the environment for the same reason `MAGPIE_ADDR` and `MAGPIE_ALLOW_IPS` are: the gateway this transport is for starts in a container, where nobody edits `settings.json`. |
| `tail=on` on a provider | `provider.Provider.Tail`, set with `magpie provider add … tail=on` / `magpie provider set <id> tail=on` | This provider's Chat requests go over the tail transport. For a provider that is another magpie with its own `tail` switch on. |

The provider's URL is the far end's ordinary Chat base (`…/v1`); nothing about
the URL says "tail". A provider without the switch talks to the same URL as it
always did, which is what makes an A/B a change of provider and nothing else.

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

1. `Server.forwardOnce` finalizes the body, then calls `Server.tailOut`. A
   provider without `tail=on`, another protocol, or an encoded body leaves the
   request untouched.
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

- **Client credential table**: per provider, in memory (`Server.tailClients`),
  `session → [(length, tip)]`. A session is the body's own content, so it needs
  no bookkeeping across restarts: a fresh gateway simply misses and gets the
  whole request.
- **Block cache**: one per gateway (`Server.tailSrv`, 256 MB budget), holding
  `parent → content` and a chain walkable from a tip. It is not shared between
  gateway processes; a session that moves to another gateway gets one 409 and
  one full upload, then is cached there.

## Failure and compatibility

- Both switches off (the default): no behavior change anywhere. A body that
  isn't JSON is rejected exactly as before.
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
whole, the switch being what serves one, a provider marked `tail=on` sending
only its new bytes, and a far end that lost the prefix being asked for the whole
request exactly once.

For the two ends together, run two of the built binary — one with `MAGPIE_TAIL=1`
and a provider at the inference service, one with a `tail=on` provider pointed at
the first — and compare what a TCP counter between them sees against the same
turns sent as plain JSON.
