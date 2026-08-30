# Linux::Event::Async Roadmap

## Purpose

`Linux::Event::Async` will provide Future::AsyncAwait-compatible async/await
support above the callback-first Linux::Event reactor.

The project must preserve the architectural boundary established by benchmark
evidence:

```text
Linux::Event
    callback/reactor core
    Stream lifecycle and transports
    native framing
    generic native message-consumer capability

Linux::Event::Async
    reusable receive Awaitable
    Future::AsyncAwait integration
    async-sub result Future
    higher-level async operations
```

Linux::Event remains independently useful and must not depend on Future,
Future::AsyncAwait, or Linux::Event::Async.

## Confirmed decisions

1. Linux::Event remains callback/reactor-first.
2. Async/await is a separate distribution layered above Linux::Event.
3. A received message must not require allocating and completing a new Future.
4. A Stream uses one reusable receive Awaitable state.
5. One conventional Future is still required for the result of an entire
   `async sub`.
6. Native framing should be able to complete the receive Awaitable without
   first invoking a public Perl `on_message` callback.
7. Callback delivery and pull/Awaitable delivery are mutually exclusive for
   one Stream descriptor.
8. Cancellation of a pending receive does not close the Stream or consume the
   next message.
9. Only one receive may be active on a Stream.
10. The core extension boundary must be generic and versioned; it must not
    expose Future::AsyncAwait-specific semantics.
11. Native framing is optional. The async design must remain functional for
    raw Streams and Perl framing, although the fastest path uses native framing.
12. Performance claims must identify framing mode, read policy, watcher
    dispatch, producer design, and Awaitable implementation.

## Benchmark evidence

These benchmarks used AF_UNIX socketpairs, epoll, a forked producer released by
a timing barrier, stock Future::AsyncAwait 0.71, position-rotated samples, and
matched read sizes.

### Stable terminology

- **Stream Awaitable**: a reusable Awaitable completed through a normal public
  Stream callback.
- **Direct Awaitable**: a reusable Awaitable completed without passing through
  that public callback.

With Perl framing, Direct Awaitable used a bare `watch_fd` receiver. With
native framing, the native framer belongs to Stream, so Direct Awaitable used
Stream's embedded native receive state. This implementation difference must be
stated whenever the tables are cited.

### Perl delimiter framing

The comparison used Perl `index`/`substr` framing and the same bounded
one-read policy for both implementations.

| Payload | Stream Awaitable | Direct Awaitable | Stream retained |
|---:|---:|---:|---:|
| 2,500 B | 170,711 msg/s | 202,432 msg/s | 84.3% |
| 35,000 B | 57,213 msg/s | 68,231 msg/s | 83.9% |
| 200,000 B | 15,060 msg/s | 17,832 msg/s | 84.5% |

This established that the Stream abstraction itself imposed approximately 16%
overhead in the matched test. Earlier approximately 2:1 results had mixed
main's drain-until-EAGAIN policy with the direct receiver's bounded one-read
policy and must not be cited as intrinsic Stream overhead.

### Native delimiter framing

The comparison used main-style Perl watcher dispatch and a read budget equal to
one read. The native callback result is included as the delivery ceiling.

| Payload | Native callback | Stream Awaitable | Direct Awaitable | Stream retained |
|---:|---:|---:|---:|---:|
| 2,500 B | 924,262 msg/s | 170,677 msg/s | 533,964 msg/s | 32.0% |
| 35,000 B | 119,449 msg/s | 86,258 msg/s | 120,209 msg/s | 71.8% |
| 200,000 B | 20,948 msg/s | 22,780 msg/s | 24,061 msg/s | 94.7% |

Native framing changed the tradeoff. The public Perl `on_message` entry before
coroutine resumption dominated small-message Stream Awaitable performance.
Direct native completion was more than three times faster at 2.5 KB, 39% faster
at 35 KB, and approximately 6% faster at 200 KB.

The target architecture is therefore a Direct Awaitable built through a generic
native consumer capability, not a Future-first reactor.

## Distribution responsibilities

### Linux::Event

Linux::Event owns:

- epoll dispatch;
- watcher and Stream lifecycle;
- transports and TLS;
- native and raw input buffering;
- native framing;
- backpressure;
- read fairness;
- a generic native message-consumer extension ABI;
- ownership-safe delivery of message, EOF, and failure events;
- introspection counters for the generic consumer path.

Linux::Event does not own:

- Future::AsyncAwait integration;
- `AWAIT_*` methods;
- async/await syntax;
- general Future state;
- coroutine-result objects;
- Future chaining or Future cancellation policy;
- public methods named `recv_native` or `recv_embedded`.

### Linux::Event::Async

Linux::Event::Async owns:

- `Linux::Event::Async`;
- `Linux::Event::Async::Stream`;
- `Linux::Event::Async::Future`;
- the native consumer implementation;
- reusable receive state;
- Future::AsyncAwait's Awaitable protocol;
- receive cancellation semantics;
- top-level `AWAIT_WAIT`;
- async operations added after Stream receive is complete;
- Awaitable conformance tests;
- async-specific benchmarks and documentation.

The distribution dependency is one-way:

```text
Linux::Event::Async -> Linux::Event
Linux::Event::Async -> Future::AsyncAwait

Linux::Event -X-> Linux::Event::Async
Linux::Event -X-> Future::AsyncAwait
```

## Proposed Linux::Event native consumer ABI

The initial design should use a versioned function table rather than linking an
external distribution directly against private XS symbols.

A conceptual provider interface is:

```c
typedef struct les_consumer_api_v1 {
    void *(*create)(pTHX_ SV *stream);
    int   (*message)(pTHX_ void *context, SV *message);
    void  (*eof)(pTHX_ void *context);
    void  (*fail)(pTHX_ void *context, SV *error);
    void  (*destroy)(pTHX_ void *context);
} les_consumer_api_v1;
```

The exact ABI is not yet frozen. Before implementation it must define:

- structure size and ABI version;
- interpreter/thread ownership;
- message SV ownership transfer;
- context lifetime;
- Stream lifetime while a continuation is active;
- reentrancy during `message`;
- EOF and failure ordering;
- cancellation and detachment;
- return codes such as continue, pause, close, and error;
- safe behavior after ithread cloning;
- behavior when an extension was built against an incompatible core.

Stream state should need only the provider table and one provider-owned context:

```c
const les_consumer_api_v1 *consumer_api;
void *consumer_context;
```

Linux::Event::Async should own the Awaitable fields in that context rather than
placing Future-like state in Linux::Event core.

## Descriptor and delivery model

The native consumer should participate in the cached Stream descriptor system,
similar to native framer and TLS declarations.

A descriptor selects exactly one framed delivery mode:

```text
on_message
on_messages
native consumer
```

A native consumer must not be combined with `on_message` or `on_messages`.

The desired receive sequence is:

```text
recv arms reusable consumer context
    -> native parser produces one message
    -> native consumer marks the receive ready
    -> consumer invokes the FAA continuation
    -> AWAIT_GET takes the result and resets the context
    -> the coroutine may immediately arm the next receive
```

The implementation must detach or otherwise stabilize the old receive state
before invoking the continuation. The resumed coroutine may issue another
receive before the original native delivery call returns.

## Buffering and backpressure

The common pending-receive path should transfer the completed message directly
into the consumer context without copying payload bytes.

When no receive is armed, the preferred design is to retain input in Stream's
native buffer and stop parsing rather than eagerly creating an unbounded Perl
message queue. Read interest may be paused to allow kernel backpressure.

When a receive is armed again:

1. resume parsing existing native input;
2. complete one receive;
3. stop again unless reentrant coroutine execution armed another receive.

A bounded ready-message slot may be used if required, but unlimited pre-decoded
message queuing is not the default design.

## Read fairness and watcher dispatch

The core work should include or preserve a configurable
`read_budget_bytes`. Benchmark comparisons demonstrated that
read-drain policy materially changes results. Zero may retain unlimited-drain
semantics; production defaults require a separate evidence-based decision.

Direct native Stream watcher dispatch is independent of async/await and may be
included if regression testing confirms it benefits Stream without weakening
the public watcher contract.

Neither read fairness nor native watcher dispatch should be presented as an
async-only optimization.

## Linux::Event::Async API direction

Application subclasses should inherit explicitly:

```perl
package MyProtocol;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";
```

Application code should use:

```perl
my $message = await $stream->recv;
```

`recv` arms one reusable receive and returns the Stream's Awaitable view.
Whether that view is literally the same blessed Stream scalar or a stable,
allocation-free XS view must be decided by conformance and benchmark evidence.
A separately allocated operation per message is not acceptable for the primary
fast path.

Required Awaitable behavior includes:

- `AWAIT_IS_READY`;
- `AWAIT_IS_CANCELLED`;
- `AWAIT_ON_READY`;
- `AWAIT_GET`;
- `AWAIT_CLONE`;
- `AWAIT_CHAIN_CANCEL`;
- `AWAIT_ON_CANCEL`;
- `AWAIT_WAIT`.

`AWAIT_CLONE` returns `Linux::Event::Async::Future`, not another Stream.
That Future represents the whole async sub.

Repeated or stale access must not observe a later receive. If returning the
Stream itself cannot satisfy Future::AsyncAwait's conformance expectations,
the design must use a generation-safe view and remeasure its cost.

## Receive semantics

The initial contract is:

- one pending receive per Stream;
- ordered message delivery;
- clean EOF resolves as `undef`;
- I/O, framing, timeout, explicit-close, and transport failures throw typed
  Linux::Event errors;
- cancellation releases only the pending receive;
- cancellation does not close the Stream;
- cancellation does not consume the next message;
- bytes arriving after cancellation remain available;
- callback and Awaitable delivery cannot be mixed;
- reentrant next-receive registration is supported;
- Stream close, detach, transition, TLS shutdown, and destruction cannot leave
  a continuation or consumer context dangling.

## Main-distribution implementation phase

The next work begins in the Linux::Event repository, not this distribution.

### Phase 1: design and branch preparation

1. Start from the current Linux::Event `main` branch.
2. Create a dedicated feature branch for the native consumer ABI.
3. Preserve unrelated experimental Future-first work outside this branch.
4. Record a pre-change callback and native-framing performance baseline.
5. Freeze the v1 ownership and lifetime rules before exposing the ABI.

### Phase 2: generic core capability

1. Add versioned consumer declarations to Stream descriptors.
2. Add provider table and context pointers to native Stream state.
3. Route framed messages to either callbacks or the native consumer.
4. Implement consumer-controlled pause/continue behavior.
5. Propagate EOF, framing failure, I/O failure, timeout, close, detach, and
   destruction.
6. Add safe reentrancy around consumer invocation.
7. Add or preserve bounded read fairness.
8. Evaluate direct native Stream watcher dispatch independently.
9. Add introspection counters for consumer calls, pauses, buffered input,
   ownership transfers, and failures.

### Phase 3: core-only test provider

Linux::Event tests must include a small fake native consumer that has no
Future::AsyncAwait dependency. It should prove:

- descriptor registration;
- message ordering;
- direct ownership transfer;
- immediate and delayed consumption;
- pause and resume;
- clean EOF;
- typed failures;
- cancellation/detachment equivalent cleanup;
- Stream destruction;
- reentrant consumer behavior;
- callback/consumer exclusivity;
- incompatible ABI rejection;
- ithread clone safety or explicit clone rejection.

### Phase 4: regression testing

Before any main-branch merge:

1. Run the complete Linux::Event test suite.
2. Run callback delivery benchmarks before and after the change.
3. Test raw `on_data`, `on_message`, and `on_messages`.
4. Test every built-in/native-plugin framer.
5. Test plain, TLS, accepted, connected, transitioned, paused, and closing
   Streams.
6. Test read budgets of one read, bounded multi-read, and unlimited drain.
7. Confirm no callback-mode allocation or dispatch regression when no consumer
   is configured.
8. Inspect callback entries, epoll events, reads, bytes copied, queue depth,
   native buffer depth, and consumer invocations.
9. Run sanitizers or equivalent native lifetime diagnostics where practical.
10. Run `git diff --check` and a clean distribution build.

The benchmark acceptance matrix retains the established payloads:

```text
2,500 bytes
35,000 bytes
200,000 bytes
```

Report median message rate, payload throughput, CPU time per message, p50/p99
latency, read syscalls, epoll events, Perl callback entries, continuation
invocations, allocations, and copied bytes where instrumentable.

### Phase 5: merge and release gate

The Linux::Event core change is ready only when:

- ordinary callback semantics are unchanged;
- callback throughput has no material regression;
- the fake consumer passes lifecycle and ownership tests;
- the ABI is documented and versioned;
- unsupported ABI versions fail clearly;
- the full test suite passes;
- the native consumer benchmark demonstrates that the extension boundary
  preserves the Direct Awaitable performance opportunity.

Only after that core capability is merged and released should
Linux::Event::Async implementation begin.

## Linux::Event::Async implementation phase

After the required Linux::Event release:

1. Create the distribution skeleton.
2. Declare the minimum compatible Linux::Event ABI/version.
3. Implement the provider-owned reusable receive context.
4. Implement `Linux::Event::Async::Stream`.
5. Implement `Linux::Event::Async::Future`.
6. Integrate Future::AsyncAwait without patching FAA.
7. Run the official alternative-Awaitable conformance suite.
8. Add pending, immediate-ready, cancellation, EOF, failure, close, detach,
   reentrancy, destruction, and repeated-use tests.
9. Reproduce both Perl-framing and native-framing benchmark matrices.
10. Document callback versus async tradeoffs honestly.
11. Add timers, process completion, connect, accept, drain, resolver, or other
    async operations only after Stream receive is stable.

## Non-goals for the first release

The first release does not attempt:

- multiple concurrent readers on one Stream;
- a general scheduler replacement;
- structured concurrency;
- an FAA-private continuation ABI;
- universal awaitability of ordinary Linux::Event objects;
- making Linux::Event depend on Future;
- batching every operation behind one abstraction;
- eliminating the documented FAA CODE-reference continuation boundary.

## Immediate next work

The next work session is specifically:

> Implement the generic native consumer capability in the Linux::Event main
> codebase on an appropriate feature branch, then perform comprehensive
> callback and Stream regression testing.

Do not begin the Linux::Event::Async implementation until that main-distribution
work has passed its regression and benchmark gates.
