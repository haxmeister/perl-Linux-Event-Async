# Linux::Event::Async

`Linux::Event::Async` adds `async`/`await` syntax and native Awaitable objects to
[`Linux::Event`](https://github.com/haxmeister/perl-linux-event) without making
the core reactor depend on Future or Future::AsyncAwait.

Version 0.002 is the first stable release. It focuses on the hot framed-stream
receive path. An Async Stream owns one persistent native receive Awaitable;
receiving a message does not allocate a new Future or Awaitable for that
message.

## Requirements

- Linux
- Perl 5.36 or newer
- Linux::Event 0.110 or newer
- Future::AsyncAwait 0.71 or newer

Linux::Event 0.110 is the first supported core release because it contains the
public native ordered-byte consumer ABI used by this distribution.

## Basic use

Define the wire protocol once as a Stream subclass:

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

Then use ordinary Future::AsyncAwait syntax:

```perl
use Linux::Event::Async;

async sub consume ($stream) {
    while (defined(my $message = await $stream->recv)) {
        say $message;
    }
}

my $task = consume($stream);
$task->AWAIT_WAIT;
```

`use Linux::Event::Async` imports `async` and `await` through
Future::AsyncAwait and configures `Linux::Event::Async::Future` as the result
Future for `async sub` declarations.

## Why the Stream remains a subclass

The subclass is not just callback organization. It is where Linux::Event can
resolve and cache protocol policy once:

- the native framer and wire format;
- TLS identity, verification, ALPN, and role policy;
- `stream_options` read, fairness, buffer, output, and timeout tuning;
- socket policy; and
- reusable lifecycle behavior.

For example, TLS and framing compose directly:

```perl
package SecureLineStream;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";
use Linux::Event::TLS
    verify => 1,
    alpn   => ['my-protocol'];
```

The resulting class still uses the normal Linux::Event connection, TLS,
backpressure, output queue, timeout, and framing machinery. Async changes the
framed receive surface, not the underlying transport architecture.

## Receive contract

`recv` arms one receive and returns the Stream's persistent Awaitable view.
Only one receive may be pending at a time, and a ready result must be consumed
before the next receive generation is started.

```perl
my $message = await $stream->recv;
```

The contract is:

- framed messages are delivered in order;
- clean EOF resolves to `undef`;
- I/O, framing, close, detach, and read-side failures throw Linux::Event typed
  errors;
- cancelling a receive does not close the Stream;
- cancelling a receive does not consume the next message;
- cancelling an async-sub Future propagates to the receive it is currently
  awaiting; and
- reentrant coroutine execution may arm the next receive safely.

The Async native consumer is the framed message delivery mechanism. An Async
Stream subclass cannot also define `on_message` or `on_messages`, and it cannot
use `message_batch_size`.

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

Those values are the Linux::Event defaults. `read_budget_bytes => 0` means no
byte fairness budget, `max_pending_bytes => 0` means no explicit hard queued
output limit, and the three timeout values are seconds with zero meaning
disabled.

`read_batch_bytes` belongs to raw callback Streams and does not apply here.
`message_batch_size` belongs to callback message batching and is incompatible
with the Async native consumer.

## Bounded prefetch

Within one native framed-input drain, Async may retain up to 64 additional
messages and approximately 256 KiB of payload before pausing native delivery.
The byte boundary allows one complete-frame overshoot.

This bounded prefetch allows consecutive receives to complete immediately when
messages are already available, while preventing an unbounded decoded-message
queue. Once the bound is reached, Linux::Event's normal pause and kernel
backpressure behavior takes over.

## Future model

`Linux::Event::Async::Future` represents an entire asynchronous computation. It
is deliberately separate from the persistent Stream receive Awaitable.

`AWAIT_WAIT` drives one `run_once(-1)` operation at a time on the Linux::Event
Loop associated with the operation currently awaited by the Future. Calling
`$loop->run` directly retains the core Linux::Event contract: it continues until
`stop` is called and does not stop just because one Future became ready.

The native Future implements the Future::AsyncAwait Awaitable protocol and has
convenience methods such as `done`, `fail`, `get`, `cancel`, `is_ready`,
`is_cancelled`, `on_ready`, and `on_cancel`. It is not a subclass of the CPAN
`Future` distribution and should not be treated as a drop-in implementation of
that module's complete API.

## Performance model

The design is intentionally callback-first underneath and async at the language
surface:

```text
Linux::Event epoll / Stream / native framer
        -> versioned native consumer ABI
        -> persistent receive Awaitable
        -> Future::AsyncAwait continuation
```

The primary receive path avoids per-message Future allocation and per-message
Awaitable allocation. Linux::Event remains free to optimize framing, buffering,
fairness, TLS, and watcher dispatch independently of this distribution.

Benchmark scripts are kept in the repository for development comparison; their
results are evidence for implementation choices, not a performance guarantee
for arbitrary applications or hardware.

## 0.002 scope

The first stable release provides:

- Future::AsyncAwait integration;
- `Linux::Event::Async::Future`;
- `Linux::Event::Async::Stream`;
- framed `recv` with persistent native Awaitable state;
- receive cancellation and clean EOF semantics;
- bounded native prefetch; and
- Linux::Event consumer ABI v1 lifetime handling, including reentrant close.

Awaitable accept, drain, timers, process completion, resolver operations, and
other higher-level operations are future work rather than hidden 0.002 APIs.

See `ASYNC-ROADMAP.md` for the current architecture constraints and next
extension priorities.

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

Linux::Event::Async is free software distributed under the same terms as Perl
5 itself. See `LICENSE`.
