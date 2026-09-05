# Linux::Event::Async

`Linux::Event::Async` adds `async`/`await` syntax and Awaitable operations to
[`Linux::Event`](https://github.com/haxmeister/perl-linux-event) without making
the core reactor depend on Future or Future::AsyncAwait.

Version 0.002 is the first stable release. It provides:

- `Linux::Event::Async::Future` for `async sub` results and cold one-shot waits;
- `Linux::Event::Async::Stream` with `ready`, `drain`, and persistent framed
  `recv`;
- `Linux::Event::Async::Listener` with persistent pull-based `accept`; and
- `Linux::Event::Async::Timer` with persistent `wait` and coalesced expiration
  counts.

The design keeps Linux::Event callback-first underneath. Async is a language
surface over the existing epoll, socket, TLS, framing, backpressure, and timer
machinery rather than a second event loop.

## Requirements

- Linux
- Perl 5.36 or newer
- Linux::Event 0.110 or newer
- Future::AsyncAwait 0.71 or newer

Linux::Event 0.110 is the minimum because it contains the public resource
hierarchy and the ordered-byte consumer ABI used by the optimized Stream receive
path.

## Complete socket example

Define the protocol once as a Stream subclass:

```perl
package LineStream;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";

sub stream_options ($class) {
    return (
        read_size         => 65_536,
        read_budget_bytes => 262_144,
        max_buffer        => 8_388_608,
    );
}
```

Create an Async Listener and consume connections with ordinary
Future::AsyncAwait syntax:

```perl
use Linux::Event::Async;
use Linux::Event::Loop;

my $loop = Linux::Event::Loop->new;
my $listener = Linux::Event::Async::Listener->new(
    loop         => $loop,
    stream_class => 'LineStream',
    host         => '127.0.0.1',
    port         => 9999,
);

async sub serve ($listener) {
    while (1) {
        my $stream = await $listener->accept;
        await $stream->ready;

        while (defined(my $message = await $stream->recv)) {
            $stream->send($message);
            await $stream->drain if $stream->is_write_blocked;
        }
    }
}

my $task = serve($listener);
$task->AWAIT_WAIT;
```

`use Linux::Event::Async` imports `async` and `await` through
Future::AsyncAwait and configures `Linux::Event::Async::Future` as the result
Future for `async sub` declarations.

## Why Stream remains subclass-based

Subclassing is a deliberate performance and composition feature, not merely a
callback convention. Linux::Event resolves protocol policy once per class and
can cache the resulting descriptor for every connection.

A Stream subclass is where you define:

- native framing and wire format;
- TLS identity, verification, ALPN, and role policy;
- socket policy;
- `stream_options` read, fairness, buffer, output, and timeout tuning; and
- reusable lifecycle behavior.

TLS and framing compose directly:

```perl
package SecureLineStream;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";
use Linux::Event::TLS
    verify => 1,
    alpn   => ['my-protocol'];
```

Async changes where application code suspends. It does not rebuild framing,
TLS, buffering, or socket policy around each `await`.

## Stream tuning

The complete `stream_options` set applicable to a framed Async Stream is:

```perl
sub stream_options ($class) {
    return (
        read_size          =>    65_536,
        read_budget_bytes  =>         0,
        high_watermark     => 1_048_576,
        low_watermark      =>   262_144,
        max_pending_bytes  =>         0,
        max_buffer         => 8_388_608,
        idle_timeout       =>         0,
        read_timeout       =>         0,
        write_timeout      =>         0,
    );
}
```

Those are Linux::Event's defaults. `read_budget_bytes => 0` means no byte
fairness budget, `max_pending_bytes => 0` means no explicit hard queued-output
limit, and timeout values are seconds with zero meaning disabled.

`read_batch_bytes` belongs to raw callback Streams and does not apply to a
framed Async Stream. `message_batch_size` belongs to framed callback batching
and cannot be combined with the Async native consumer.

## Connection readiness

```perl
my $stream = SecureLineStream->connect(
    loop => $loop,
    host => 'example.com',
    port => 443,
);

await $stream->ready;
```

`ready` returns a `Linux::Event::Async::Future` resolving with the same Stream.
For outbound TCP it covers hostname resolution and connection establishment. For
TLS it additionally covers handshake, certificate verification, and negotiated
transport state such as ALPN.

Multiple callers may wait for the same one-shot readiness transition. Cancelling
one readiness Future cancels only that wait and does not close the Stream.
Connection, resolver, transport, TLS, timeout, and setup failures propagate as
`Linux::Event::Error` objects.

A directly adopted plain connected socket is already ready. A detached outbound
Stream must be added to a Loop before a pending readiness Future can be awaited.

## Framed receive

```perl
my $message = await $stream->recv;
```

`recv` uses one persistent native Awaitable per Stream. There is no per-message
Future or Awaitable allocation.

The receive contract is:

- one pending receive per Stream;
- ordered framed delivery;
- clean EOF resolves to `undef`;
- I/O, framing, close, detach, and read-side failures throw typed
  `Linux::Event::Error` values;
- cancelling a receive does not close the Stream;
- cancelling a receive does not consume the next message; and
- cancellation of an async-sub Future propagates to its currently awaited
  receive.

Within one framed-input drain, Async may retain up to 64 additional messages and
approximately 256 KiB of payload. The byte threshold allows one complete-frame
overshoot. Once the bound is reached, consumer delivery pauses and normal
Linux::Event backpressure resumes.

`on_message`, `on_messages`, and `message_batch_size` are incompatible with the
Async native receive consumer.

## Output backpressure

```perl
$stream->send($payload);
await $stream->drain if $stream->is_write_blocked;
```

`drain` returns a `Linux::Event::Async::Future`. If the Stream is not blocked it
is already complete. Otherwise it resolves when the current high-water blocked
period ends as queued output falls through the configured low watermark.

`drain` does **not** mean `pending_bytes == 0`; it means the application may
resume producing output according to Linux::Event's backpressure policy.

`on_drain` is reserved by `Linux::Event::Async::Stream` as the bridge from the
core blocked-to-low-water transition to pending drain Futures. Application
backpressure logic belongs after `await $stream->drain`.

Cancelling one drain Future cancels only that waiter. It neither closes the
Stream nor discards queued output. Stream close or failure completes pending
drain waits with an error.

## Pull-based Listener accept

```perl
my $stream = await $listener->accept;
```

`Linux::Event::Async::Listener` owns one persistent Awaitable for repeated
accepts. The returned object is the exact configured `stream_class`; no generic
connection wrapper is added.

Acceptance is paused while no wait is armed. Cancelling an accept wait pauses
acceptance but does not close the listening socket. This keeps excess incoming
connections in the kernel backlog rather than building an unbounded queue of
accepted Stream objects.

For 0.002, Async Listener fixes `max_accept_per_tick => 1` and uses
level-triggered acceptance. This prevents the core's normal accept batching from
accepting a tail of connections behind an already completed pull-style
Awaitable. A future native implementation may add bounded accept prefetch if
measurement justifies it.

`on_accept` and `on_error` are reserved by `Linux::Event::Async::Listener` as
its Awaitable bridge.

## Awaitable Timer

```perl
my $timer = Linux::Event::Async::Timer->new(
    loop  => $loop,
    every => 1,
);

while (1) {
    my $expirations = await $timer->wait;
    say "tick x$expirations";
}
```

`Linux::Event::Async::Timer` retains the public
`Linux::Event::Kernel::Timer` scheduler and one persistent Awaitable per Timer.
Repeated recurring waits therefore do not allocate one Future or Awaitable per
tick.

The normal Timer schedules remain available:

```perl
after => $seconds
at    => $monotonic_seconds
every => $seconds
```

Recurring timers remain fixed-rate. If the Loop is late, Linux::Event coalesces
missed periods; `wait` returns that represented expiration count. If expirations
occur while no wait is armed, Async accumulates the count in one scalar rather
than creating an unbounded tick queue.

`cancel_wait` cancels only the pending suspension. The underlying recurring
Timer remains active. Core `$timer->cancel` remains terminal and fails a pending
wait. `on_timer` is reserved by `Linux::Event::Async::Timer` as its Awaitable
bridge.

## Future model

`Linux::Event::Async::Future` represents an asynchronous computation or a cold
one-shot wait such as Stream readiness or drain. It is intentionally separate
from persistent hot-operation Awaitables.

`AWAIT_WAIT` drives one `run_once(-1)` at a time on the Linux::Event Loop
associated with the operation currently awaited by the Future. Sequential
awaits may therefore move between Loops.

The native Future implements the Future::AsyncAwait Awaitable protocol and has
convenience methods including `done`, `fail`, `get`, `cancel`, `is_ready`,
`is_cancelled`, `on_ready`, and `on_cancel`. It is not a subclass or complete
replacement for the CPAN `Future` distribution.

## Performance model

The operation type determines the suspension mechanism:

```text
cold transition
    Stream ready / Stream drain
        -> Linux::Event::Async::Future
        -> Future::AsyncAwait continuation

hot repeated operation
    Stream recv / Listener accept / Timer wait
        -> persistent per-resource Awaitable
        -> Future::AsyncAwait continuation
```

The rule is pragmatic: do not build specialized reusable state for a cold event
unless measurement justifies it, and do not accept avoidable per-event
allocation on a demonstrated hot path.

Linux::Event callback users pay no Future::AsyncAwait dependency or dispatch tax
for this distribution.

## 0.002 scope

The first stable release includes Future::AsyncAwait integration, Stream
readiness/drain/framed receive, pull-based Listener accept, and awaitable Timer
expiration. Datagram operations, process completion, signals, eventfd events,
Pipe/TTY operations, resolver operations, and generic fd readiness remain future
work.

See `ASYNC-ROADMAP.md` for the architecture constraints and extension priorities.

## Installation

From CPAN:

```sh
cpanm Linux::Event::Async
```

From a checkout:

```sh
perl Makefile.PL
make
make test
make install
```

## License

Linux::Event::Async is free software distributed under the same terms as Perl 5
itself. See `LICENSE`.
