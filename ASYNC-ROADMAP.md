# Linux::Event::Async Status and Roadmap

## Current status

Linux::Event::Async 0.002 is the first stable release target. The earlier
0.001_001 version was a developer release, so 0.002 is the next valid stable
version in Perl version ordering.

The architectural prerequisite that originally blocked this distribution is no
longer pending. Linux::Event 0.110 ships the public versioned ordered-byte
consumer ABI and the public `Linux::Event::IO::Sock::Stream` hierarchy used by
Linux::Event::Async.

The implemented 0.002 surface is intentionally narrow:

- Future::AsyncAwait integration;
- `Linux::Event::Async::Future` for `async sub` results;
- `Linux::Event::Async::Stream` for framed `SOCK_STREAM` receive;
- one persistent native receive Awaitable per Stream;
- receive cancellation, EOF, failure, and reentrant close handling; and
- bounded native prefetch for consecutive receives.

The old implementation staging plan is complete. This document now records the
constraints that must remain true and the direction for work after 0.002.

## Architectural boundary

Linux::Event remains a callback-first reactor. It owns:

- epoll dispatch;
- ordered-byte I/O;
- socket acquisition and lifecycle;
- native framers;
- TLS;
- buffering and output queues;
- backpressure and fairness;
- deadlines and timeouts; and
- the versioned native consumer extension ABI.

Linux::Event::Async owns:

- Future::AsyncAwait integration;
- coroutine-facing receive state;
- the persistent Stream receive Awaitable;
- async-sub result Futures;
- cancellation propagation between those Futures and awaited operations; and
- future async operation adapters built on Linux::Event primitives.

The dependency direction stays one way:

```text
Linux::Event::Async -> Linux::Event
Linux::Event::Async -> Future::AsyncAwait

Linux::Event -X-> Linux::Event::Async
Linux::Event -X-> Future::AsyncAwait
```

Linux::Event core must never need Future knowledge to support this distribution.

## Public extension boundary

Linux::Event::Async must consume only supported Linux::Event public surfaces:

- `Linux::Event::IO::Sock::Stream` as the public stream-socket base;
- `Linux::Event::Framer` for native framing and consumer declaration; and
- ordered-byte consumer ABI v1 for native message delivery.

The Async distribution vendors the ABI v1 header used to compile its provider.
That header must stay synchronized with the ABI contract shipped by the minimum
supported Linux::Event release.

Async must not link directly against private Linux::Event XS symbols or treat
historical private implementation package names as application APIs.

## Stream receive model

A concrete Async Stream is a protocol subclass:

```perl
package MyProtocol;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";
```

The subclass is where Linux::Event resolves and caches protocol policy:

- native framing;
- TLS policy;
- socket policy;
- `stream_options`; and
- lifecycle behavior.

The Async consumer replaces framed callback delivery. `on_message`,
`on_messages`, and `message_batch_size` cannot be combined with the native
Async consumer.

### Receive state

Each Stream owns one provider context and one persistent Awaitable view.

The normal sequence is:

```text
recv arms reusable receive context
    -> Linux::Event reads and frames input
    -> native consumer receives a complete message
    -> receive state becomes ready
    -> Future::AsyncAwait continuation resumes
    -> AWAIT_GET consumes the result
    -> coroutine may immediately arm the next receive
```

A second receive cannot overlap a pending generation, and a ready generation
must be consumed before the next generation begins.

### Bounded prefetch

During one native framed-input drain, Async may retain up to 64 additional
messages and approximately 256 KiB of payload. The byte threshold permits one
complete-frame overshoot.

The prefetch exists to allow consecutive receives to complete without one
coroutine suspension per already-buffered frame. It is deliberately bounded.
When the bound is reached, consumer delivery pauses and normal Linux::Event
backpressure resumes.

An unbounded decoded-message queue is not part of the design.

### Terminal and cancellation semantics

The stable receive contract is:

- one pending receive per Stream;
- ordered framed delivery;
- clean EOF resolves as `undef`;
- I/O, framing, close, detach, and read-side lifecycle failures throw typed
  Linux::Event errors;
- receive cancellation does not close the Stream;
- receive cancellation does not consume the next message;
- async-sub Future cancellation propagates to the receive currently awaited;
- terminal events remain ordered after already prefetched messages; and
- Stream close or destruction cannot leave a dangling provider or continuation.

## Native lifetime rule

Provider-owned frames that call back into Linux::Event through the consumer host
API must obey ABI v1 retain/release ownership.

Before a callback-capable host call such as resume or pause, Async retains the
host. The matching release is the provider frame's final host/context action.
The release may destroy both the host state and the provider context.

This rule is required because resume, pause, cancellation callbacks, or resumed
coroutines can reentrantly close or destroy the Stream.

Tests must continue to cover reentrant close from provider-controlled callback
paths rather than relying only on stress or global-destruction tests.

## Future model

`Linux::Event::Async::Future` represents an entire asynchronous computation,
not one Stream message.

It implements the Future::AsyncAwait Awaitable protocol and native result,
failure, readiness, cancellation, and cancellation-chain state. It is not a
subclass of the CPAN `Future` distribution and is not intended to reproduce that
module's complete API.

`AWAIT_WAIT` follows the Linux::Event Loop associated with the operation the
async sub is currently awaiting. Loop association is resolved again between
dispatches so sequential awaits may move between Loops.

## Performance invariants

The first stable release establishes several design constraints that should not
be casually traded away:

1. No per-message Future allocation on Stream receive.
2. No per-message Awaitable allocation on Stream receive.
3. Native framing remains in Linux::Event.
4. Linux::Event callback users pay no Future::AsyncAwait dependency or dispatch
   tax for Async support.
5. Async buffering remains bounded.
6. Framing, TLS, and Stream tuning stay class policy rather than being rebuilt
   around each receive operation.
7. Any optimization must preserve lifecycle correctness under reentrant close,
   cancellation, EOF, detach, and destruction.

Benchmark results are evidence for these choices, not API guarantees. Changes to
the receive path should be evaluated with the repository benchmark harnesses on
multiple payload sizes and should always be paired with correctness tests.

## 0.002 release gates

A 0.002 release candidate should satisfy all of the following:

- Linux::Event minimum dependency is a released version containing consumer ABI
  v1; currently 0.110;
- the vendored ABI header matches the supported core ABI;
- the distribution builds and tests on Perl 5.36 and the current supported Perl;
- the Future::AsyncAwait alternative-Awaitable conformance tests pass;
- Stream receive, cancellation, EOF, prefetch, reentrancy, global destruction,
  and stress tests pass;
- reentrant Stream close is covered under the host retain/release rule;
- `make disttest` passes in a clean distribution tree;
- current Linux::Event main remains integration-compatible;
- CPAN metadata declares every runtime and test dependency;
- public modules carry one stable distribution version; and
- POD, README, Changes, LICENSE, and metadata describe the released architecture
  rather than an unreleased development branch.

## Work after 0.002

The next useful layer is not a new event loop abstraction. It is a small set of
awaitable operations over capabilities Linux::Event already owns.

Candidate priorities are:

1. connection readiness and connection failure;
2. listener accept;
3. output drain and write-side completion;
4. timers;
5. process completion;
6. resolver completion; and
7. other kernel-event operations where an Awaitable materially improves
   application structure.

Each operation should answer the same questions before becoming public:

- Can it reuse stable native or object-owned state rather than allocate an
  avoidable wrapper on every operation?
- What owns cancellation?
- Which Loop drives `AWAIT_WAIT`?
- What happens under close or destruction while a continuation is running?
- Does exposing it in Async require any change to callback users in core?
- Can the feature be implemented through a public Linux::Event boundary?

## Deferred higher-level work

Structured concurrency, task groups, cancellation scopes, protocol-specific
clients, HTTP, WebSocket, and application frameworks belong above the 0.002
primitive layer.

They may eventually use Linux::Event::Async, but they should not force Future or
protocol semantics back into Linux::Event core.

## Non-goals

The project does not currently aim to:

- replace Linux::Event's reactor with a scheduler;
- make Linux::Event depend on Future::AsyncAwait;
- make every Linux::Event object automatically Awaitable;
- support multiple simultaneous readers on one Stream;
- expose private Future::AsyncAwait continuation internals;
- provide an unbounded message queue;
- hide Stream framing, TLS, or tuning policy inside generic operation objects;
  or
- claim that async/await is inherently faster than callbacks.

The purpose is to provide ergonomic coroutine syntax while preserving the
performance and policy advantages of Linux::Event's native callback and Stream
architecture.
