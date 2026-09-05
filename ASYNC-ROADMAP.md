# Linux::Event::Async Status and Roadmap

## Current status

Linux::Event::Async 0.002 is the first stable release target. The earlier
0.001_001 version was a developer release, so 0.002 is the next valid stable
version in Perl version ordering.

Linux::Event 0.110 supplies the released public resource hierarchy and the
versioned ordered-byte consumer ABI required by this distribution. Async 0.002
now covers the first useful coroutine path from connection acceptance through
framed I/O and monotonic timing:

- Future::AsyncAwait integration;
- `Linux::Event::Async::Future` for `async sub` results and cold waits;
- `Linux::Event::Async::Stream` application `ready`;
- Stream output `drain` at the normal backpressure low-water transition;
- one persistent native Stream Awaitable for framed `recv`;
- `Linux::Event::Async::Listener` with persistent pull-based `accept`;
- `Linux::Event::Async::Timer` with persistent `wait` and coalesced expiration
  counts;
- wait-local cancellation for receive, accept, and Timer waits;
- bounded native Stream prefetch; and
- reentrant close/lifetime safety across the consumer ABI.

## Architectural boundary

Linux::Event remains a callback-first reactor. It owns epoll dispatch, resource
lifecycle, socket acquisition, accept4, native framing, TLS, ordered-byte I/O,
backpressure, monotonic timer scheduling, process/kernel resources, and the
versioned native consumer ABI.

Linux::Event::Async owns Future::AsyncAwait integration, coroutine-facing
operation state, cancellation propagation, async-sub result Futures, persistent
Awaitables where repeated operation cost warrants them, and adapters over
Linux::Event public resources.

The dependency direction remains one way:

```text
Linux::Event::Async -> Linux::Event
Linux::Event::Async -> Future::AsyncAwait

Linux::Event -X-> Linux::Event::Async
Linux::Event -X-> Future::AsyncAwait
```

Core callback users must not pay a Future::AsyncAwait dependency or dispatch tax
for Async support.

## Operation design rule

Not every resource should become an Awaitable and not every operation should use
the same implementation pattern.

Use `Linux::Event::Async::Future` for comparatively cold one-shot transitions
when a small object allocation is not on a demonstrated hot path. Use persistent
resource-owned Awaitables for repeated operations when per-event allocation or
dispatch overhead matters.

Current examples:

```text
cold / one-shot
    Stream ready
    Stream drain
        -> Linux::Event::Async::Future

hot / repeated
    Stream recv
    Listener accept
    Timer wait
        -> persistent per-resource Awaitable
```

A reusable Awaitable is an optimization with lifecycle cost. It should be earned
by operation frequency and measurement rather than applied mechanically.

## Stream model

A concrete Async Stream remains a protocol subclass:

```perl
package MyProtocol;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";
```

The subclass is where Linux::Event resolves and caches framing, TLS policy,
socket policy, `stream_options`, and reusable lifecycle behavior.

### Application readiness

`ready` returns a Future resolving with the same Stream. Outbound readiness
covers resolver completion, connect, and optional TLS handshake/verification.
The normal core `on_ready` callback remains supported and runs before readiness
Future waiters resume.

Cancellation is waiter-local. Closing before readiness fails waiters; closing
reentrantly from `on_ready` is historical success rather than retroactive
connection failure.

### Output drain

`drain` returns a Future. An unblocked Stream completes immediately. A blocked
Stream resolves when the current high-water period crosses the configured low
watermark; it does not promise an empty output queue.

`on_drain` is reserved by Async Stream as the stable bridge from core's exact
backpressure transition to pending drain Futures. Cancellation is waiter-local.
Close or Stream failure fails pending drain waiters.

### Framed receive

Each Stream owns one provider context and one persistent native receive
Awaitable. No Future or Awaitable is allocated per message.

One receive may be pending at a time. Clean EOF resolves as `undef`; typed core
errors propagate; receive cancellation neither closes the Stream nor consumes
the next message.

During one native input drain Async may retain up to 64 additional messages and
approximately 256 KiB of payload, with one complete-frame byte-boundary
overshoot. When the bound is reached, consumer delivery pauses and normal
Linux::Event backpressure resumes.

The native provider must continue to obey consumer ABI v1 retain/release rules
around callback-capable host operations so resumed coroutines may close or
destroy the Stream reentrantly.

## Listener accept model

`Linux::Event::Async::Listener` owns one persistent Awaitable and exposes:

```perl
my $stream = await $listener->accept;
```

`on_accept` and `on_error` are reserved as the bridge from core Listener events
to the Awaitable. The returned object is the exact configured `stream_class`.

Acceptance is paused while no wait is armed. Wait cancellation pauses acceptance
but does not close the Listener. For 0.002 the Listener fixes
`max_accept_per_tick => 1` and level-triggered operation. This prevents core's
normal accept batching from accepting additional sockets behind an already
completed pull-style Awaitable.

A future native accept provider may add bounded prefetch only if benchmarking
shows that one accept per readiness turn materially limits realistic server
workloads.

## Timer model

`Linux::Event::Async::Timer` subclasses the public
`Linux::Event::Kernel::Timer` and reserves `on_timer` as its scheduler bridge.
Each Timer owns one persistent Awaitable:

```perl
my $count = await $timer->wait;
```

The result is the represented expiration count. Linux::Event's fixed-rate
recurrence and missed-period coalescing remain intact.

If a recurring Timer expires while no wait is armed, counts accumulate in one
scalar. This preserves elapsed recurring periods without an unbounded tick
queue.

`cancel_wait` and async-sub cancellation affect only the current wait. The
underlying recurring Timer remains active. Core `$timer->cancel` remains
terminal and fails a pending wait. One-shot final expiration remains consumable
once; a subsequent wait fails even when attempted reentrantly from the resumed
continuation.

## Future model

`Linux::Event::Async::Future` represents an async computation or cold one-shot
operation. It implements the Future::AsyncAwait Awaitable protocol but is not a
subclass or complete implementation of the CPAN `Future` API.

`AWAIT_WAIT` follows the Linux::Event Loop associated with the operation that the
async sub is currently awaiting. Sequential operations may move between Loops,
including through nested async subs.

## Performance invariants

The stable design should preserve these constraints:

1. No per-message Future or Awaitable allocation on Stream receive.
2. No per-accept Future or Awaitable allocation on Listener accept.
3. No per-tick Future or Awaitable allocation on recurring Timer wait.
4. Cold waits do not gain specialized persistent state without measurement.
5. Native framing remains in Linux::Event.
6. Stream framing, TLS, socket policy, and tuning remain class policy.
7. Async buffering and prefetch remain bounded.
8. Pull-style resources must not silently discard events accepted behind a
   completed Awaitable.
9. Lifecycle correctness under cancellation, close, EOF, failure, reentrancy,
   and destruction takes precedence over speculative batching wins.

Benchmark changes to hot paths must be paired with correctness tests.

## 0.002 release gates

The 0.002 candidate should satisfy all of the following:

- minimum dependency remains released Linux::Event 0.110;
- vendored consumer ABI header matches the supported core ABI;
- build/test/disttest pass on Perl 5.36 and current supported Perl against
  Linux::Event 0.110;
- a separate integration lane passes against current Linux::Event main;
- Future::AsyncAwait alternate-Awaitable conformance passes;
- Stream readiness, drain, receive, cancellation, EOF, prefetch, reentrancy,
  global destruction, and stress tests pass;
- Listener accept covers repeated acceptance, cancellation/retry, close/error,
  backlog preservation, and Async Stream handoff;
- Timer covers one-shot, recurring, coalesced expirations, reschedule,
  wait-local cancellation, underlying Timer cancellation, and reentrant terminal
  waits;
- `make disttest` includes every public module and regression file;
- CPAN metadata declares every runtime/test dependency and every public module;
- POD, README, Changes, LICENSE, and metadata describe the actual release tree.

## Next implementation priorities

The next candidate capabilities are:

1. datagram receive and output suspension;
2. process completion and process pipe I/O;
3. signal delivery;
4. eventfd events;
5. Pipe and TTY input/output suspension points;
6. resolver completion where a standalone operation is useful; and
7. generic fd readable/writable readiness as a low-level escape hatch.

For each operation decide before implementation:

- Is it cold/one-shot or hot/repeated?
- Can it use a stable public callback/lifecycle boundary?
- Does a persistent Awaitable materially improve the hot path?
- What exactly does cancellation cancel: the wait, the resource operation, or
  the resource itself?
- Which Loop drives `AWAIT_WAIT`?
- What happens if a continuation closes/destroys the resource reentrantly?
- Is buffering or prefetch required, and if so what is the hard bound?
- Can the feature be implemented without changing callback users in core?

## Deferred higher-level work

Structured concurrency, task groups, cancellation scopes, HTTP, WebSocket, and
application frameworks belong above this primitive layer. They may use Async,
but they should not force Future semantics back into Linux::Event core.

## Non-goals

The project does not currently aim to replace Linux::Event's reactor with a
scheduler, make core depend on Future::AsyncAwait, expose private
Future::AsyncAwait continuation internals, support unbounded event queues, hide
Stream framing/TLS/tuning inside generic operation objects, or claim that
async/await is inherently faster than callbacks.

The purpose is ergonomic coroutine syntax while preserving the performance and
policy advantages of Linux::Event's native callback/resource architecture.
