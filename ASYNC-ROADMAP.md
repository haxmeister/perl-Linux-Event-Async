# Linux::Event::Async Status and Roadmap

## Current status

Linux::Event::Async 0.002 is the first stable release target. The earlier
0.001_001 version was a developer release, so 0.002 is the next valid stable
version in Perl version ordering.

Linux::Event 0.110 supplies the released public resource hierarchy and the
versioned ordered-byte consumer ABI required by this distribution. Async 0.002
now covers useful coroutine paths across stream sockets, datagram sockets,
listeners, monotonic timers, pidfd process completion, signalfd subscriptions,
and eventfd notification:

- Future::AsyncAwait integration;
- `Linux::Event::Async::Future` for `async sub` results and cold waits;
- `Linux::Event::Async::Stream` application `ready`, output `drain`, and one
  persistent native Awaitable for framed `recv`;
- `Linux::Event::Async::Listener` with persistent pull-based `accept`;
- `Linux::Event::Async::Dgram` with Future-based `ready`/`drain` and persistent
  pull-based packet `recv`;
- `Linux::Event::Async::Timer` with persistent `wait` and coalesced expiration
  counts;
- `Linux::Event::Async::Process` with Future-based process completion `wait`;
- `Linux::Event::Async::Signal` with persistent `wait` and bounded per-signal
  idle coalescing;
- `Linux::Event::Async::Event` with persistent eventfd `wait` and scalar idle
  coalescing;
- wait-local cancellation where cancellation means abandoning a suspension;
- bounded native Stream prefetch; and
- reentrant close/lifetime safety across the consumer ABI.

## Architectural boundary

Linux::Event remains a callback-first reactor. It owns epoll dispatch, resource
lifecycle, socket acquisition, accept4, recvmsg/sendmsg, native framing, TLS,
ordered-byte I/O, packet I/O, backpressure, monotonic timer scheduling, pidfd
process lifecycle, process pipe I/O, signalfd ownership/fan-out, eventfd
signaling, kernel resources, and the versioned native consumer ABI.

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
    Dgram ready
    Dgram drain
    Process wait
        -> Linux::Event::Async::Future

hot / repeated
    Stream recv
    Listener accept
    Dgram recv
    Timer wait
    Signal wait
    Event wait
        -> persistent per-resource Awaitable
```

A reusable Awaitable is an optimization with lifecycle cost. It should be earned
by operation frequency and measurement rather than applied mechanically.

## Bounded-state rule

Async must not hide unbounded event queues behind coroutine syntax.

The current resources use four different bounds appropriate to their kernel/core
semantics:

1. Stream may retain up to 64 additional framed messages and approximately
   256 KiB of payload, with one complete-frame byte-boundary overshoot.
2. Listener and Datagram pause pull delivery so excess work remains in the
   kernel backlog/receive queue.
3. Timer and Event coalesce idle counts into one scalar.
4. Signal cannot pause one subscriber inside shared signalfd fan-out, so it
   retains at most one pending entry per subscribed signal number and accumulates
   counts per number.

The resource's real semantics determine the bound; Async does not impose one
universal queue abstraction.

## Stream model

A concrete Async Stream remains a protocol subclass:

```perl
package MyProtocol;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";
```

The subclass is where Linux::Event resolves and caches framing, TLS policy,
socket policy, `stream_options`, and reusable lifecycle behavior.

`ready` returns a Future resolving with the same Stream. Outbound readiness
covers resolver completion, connect, and optional TLS handshake/verification.
`drain` returns a Future resolving at the core blocked-to-low-water transition.
Neither cold operation receives specialized persistent state.

Each Stream owns one provider context and one persistent native receive
Awaitable. One receive may be pending at a time. Clean EOF resolves as `undef`;
typed core errors propagate; receive cancellation neither closes the Stream nor
consumes the next message.

The native provider must obey consumer ABI v1 retain/release rules around
callback-capable host operations because resumed coroutines may close or destroy
the Stream reentrantly.

## Listener model

`Linux::Event::Async::Listener` owns one persistent Awaitable:

```perl
my $stream = await $listener->accept;
```

Acceptance is paused while no wait is armed. Wait cancellation pauses acceptance
but does not close the Listener. Version 0.002 fixes `max_accept_per_tick => 1`
and level-triggered operation so additional connections remain in the kernel
backlog rather than being accepted behind an already completed Awaitable.

## Datagram model

`Linux::Event::Async::Dgram` keeps packet boundaries and peer identity intact:

```perl
my ($payload, $peer) = await $socket->recv;
```

`ready` and `drain` are cold Futures. `recv` uses one persistent Awaitable.
Version 0.002 fixes `max_datagrams_per_tick => 1` and `edge_triggered => 0`, and
pauses read interest when no receive is armed. Additional packets remain in the
kernel receive queue.

Receive cancellation pauses only the wait. `send` remains the core atomic
Datagram operation; `drain` waits for the blocked output period to cross the
configured low watermark.

## Timer model

`Linux::Event::Async::Timer` subclasses `Linux::Event::Kernel::Timer` and
reserves `on_timer` as its scheduler bridge:

```perl
my $count = await $timer->wait;
```

Each Timer owns one persistent Awaitable. Fixed-rate recurrence and core
missed-period coalescing remain intact. If a recurring Timer expires while no
wait is armed, represented counts accumulate in one scalar.

`cancel_wait` affects only the current suspension. Core `$timer->cancel` remains
terminal and fails a pending wait.

## Process model

`Linux::Event::Async::Process` reserves `on_exit` as its completion bridge:

```perl
await $process->wait;
```

Process completion is cold, so each caller receives a normal Async Future.
Multiple Futures may observe the same exit. Cancellation is waiter-local and
never signals, kills, detaches, or otherwise changes Process lifetime.

Core reaps pidfd status and drains remaining stdout/stderr bytes before invoking
`on_exit`, so wait completion means status accessors and final output callbacks
are settled.

Version 0.002 retains stdout/stderr delivery on Linux::Event's native callback
path. `write_stdin`, `close_stdin`, and stdin backpressure also remain core
operations. Pull-style Process pipe suspension is deferred until it has a
bounded buffering or explicit one-read fairness contract.

## Signal model

`Linux::Event::Async::Signal` reserves `on_signal` as the bridge from the shared
signalfd service:

```perl
my ($number, $count) = await $signal->wait;
```

Signal delivery is repeated and uses one persistent Awaitable. A subscription
cannot pause only itself inside shared signalfd fan-out, so notifications that
arrive without an armed wait are retained in bounded coalesced state: one entry
per subscribed signal number plus its accumulated delivered count.

`cancel_wait` is wait-local. Core `$signal->cancel` remains terminal and discards
unconsumed Async counts. Signal-mask ownership, per-thread masking requirements,
and one-Loop ownership remain core semantics.

## Event model

`Linux::Event::Async::Event` reserves `on_event` as the eventfd bridge:

```perl
my $count = await $event->wait;
```

Event delivery is repeated and uses one persistent Awaitable. If core drains the
eventfd with no armed wait, delivered counts accumulate in one scalar for the
next wait. Event remains a notification primitive rather than a payload queue.

`cancel_wait` is wait-local. Core `$event->cancel` remains terminal. Core's
thread/fork signaling boundary is retained: cloned worker handles may signal but
do not own the Loop or Async wait state.

## Future model

`Linux::Event::Async::Future` represents an async computation or cold one-shot
operation. It implements the Future::AsyncAwait Awaitable protocol but is not a
subclass or complete implementation of CPAN `Future`.

`AWAIT_WAIT` follows the Linux::Event Loop associated with the operation the
async sub is currently awaiting. Sequential operations may move between Loops,
including through nested async subs.

## Performance invariants

The stable design should preserve these constraints:

1. No per-message Future or Awaitable allocation on Stream receive.
2. No per-accept Future or Awaitable allocation on Listener accept.
3. No per-packet Future or Awaitable allocation on Datagram receive.
4. No per-tick Future or Awaitable allocation on recurring Timer wait.
5. No per-delivery Future or Awaitable allocation on Signal/Event wait.
6. Cold waits, including Process exit, do not gain specialized persistent state
   without measurement.
7. Native framing remains in Linux::Event.
8. Stream framing, TLS, socket policy, and tuning remain class policy.
9. Datagram packet boundaries and socket policy remain core behavior.
10. Process reaping, status, signaling, and pipe I/O remain core behavior.
11. Signal masks/fan-out and Event signaling ownership remain core behavior.
12. Async buffering, prefetch, and idle retention remain explicitly bounded.
13. Pull-style resources must not silently discard work removed behind a
    completed Awaitable.
14. Lifecycle correctness under cancellation, close, EOF, failure, reentrancy,
    and destruction takes precedence over speculative batching wins.

Benchmark changes to hot paths must be paired with correctness tests.

## 0.002 release gates

The 0.002 candidate should satisfy all of the following:

- minimum dependency remains released Linux::Event 0.110;
- vendored consumer ABI header matches the supported core ABI;
- build/test/disttest pass on Perl 5.36 and 5.44 against Linux::Event 0.110;
- a separate integration lane passes against current Linux::Event main;
- Future::AsyncAwait alternate-Awaitable conformance passes;
- Stream readiness, drain, receive, cancellation, EOF, prefetch, reentrancy,
  global destruction, and stress tests pass;
- Listener repeated acceptance, cancellation/retry, close/error, backlog
  preservation, and Async Stream handoff pass;
- Datagram readiness, ordered receive, kernel-queue preservation,
  cancellation/retry, error routing, close, and real output backpressure pass;
- Timer one-shot, recurrence, coalescing, reschedule, wait-local cancellation,
  core cancellation, and reentrant terminal waits pass;
- Process normal exit, detached attachment, multiple/cancelled waiters, final
  stdout/stderr drain ordering, reentrant historical wait, signal termination,
  and callback policy pass;
- Signal repeated delivery, scalar/list results, idle per-signal coalescing,
  wait-local cancellation, terminal cancellation, and reentrant cancellation
  pass;
- Event repeated delivery, idle count accumulation, detached pre-attach signal,
  wait-local cancellation, terminal cancellation, and reentrant cancellation
  pass;
- `make disttest` includes every public module and regression file;
- distribution metadata declares every runtime/test dependency and public
  module; and
- POD, README, Changes, LICENSE, and metadata describe the actual release tree.

## Next implementation priorities

The remaining candidate primitive work is:

1. Process stdout/stderr and stdin-drain suspension after a bounded pipe-I/O
   contract is designed;
2. Pipe and TTY input/output suspension points, preferably by sharing ordered-byte
   machinery rather than duplicating Stream logic;
3. standalone resolver completion where a distinct operation is useful; and
4. generic fd readable/writable readiness as a low-level escape hatch.

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
