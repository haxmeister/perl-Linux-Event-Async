# Linux::Event::Async

`Linux::Event::Async` adds `async`/`await` syntax and Awaitable operations to
[`Linux::Event`](https://github.com/haxmeister/perl-linux-event) without making
the core reactor depend on Future or Future::AsyncAwait.

Version 0.002 is the first stable release. It provides:

- `Linux::Event::Async::Future` for `async sub` results and cold one-shot waits;
- `Linux::Event::Async::Stream` with `ready`, `drain`, and persistent framed
  `recv`;
- `Linux::Event::Async::Listener` with persistent pull-based `accept`;
- `Linux::Event::Async::Dgram` with `ready`, `drain`, and persistent pull-based
  packet `recv`;
- `Linux::Event::Async::Timer` with persistent `wait` and coalesced expiration
  counts;
- `Linux::Event::Async::Process` with Future-based process completion `wait`;
- `Linux::Event::Async::Signal` with persistent signalfd `wait`; and
- `Linux::Event::Async::Event` with persistent eventfd `wait`.

The design keeps Linux::Event callback-first underneath. Async is a language
surface over the existing epoll, socket, TLS, framing, datagram, backpressure,
timer, pidfd, signalfd, and eventfd machinery rather than a second event loop.

## Requirements

- Linux
- Perl 5.36 or newer
- Linux::Event 0.110 or newer
- Future::AsyncAwait 0.71 or newer

Linux::Event 0.110 is the minimum because it contains the public resource
hierarchy and the ordered-byte consumer ABI used by the optimized Stream receive
path.

## Complete stream-socket example

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

## Stream readiness, receive, and output

```perl
my $stream = SecureLineStream->connect(
    loop => $loop,
    host => 'example.com',
    port => 443,
);

await $stream->ready;
my $message = await $stream->recv;
$stream->send($reply);
await $stream->drain if $stream->is_write_blocked;
```

`ready` is a cold `Linux::Event::Async::Future`. Outbound readiness includes
hostname resolution and TCP connection establishment; with TLS it also includes
handshake and verification. Multiple readiness waiters may observe the same
one-shot transition, and cancelling one waiter does not close the Stream.

`recv` uses one persistent native Awaitable per Stream. There is no per-message
Future or Awaitable allocation. Only one receive may be pending. Clean EOF
resolves to `undef`. Cancelling a receive does not close the Stream or consume
the next message.

Within one framed-input drain, Async may retain up to 64 additional messages and
approximately 256 KiB of payload. The byte threshold allows one complete-frame
overshoot. Once bounded prefetch fills, normal Linux::Event consumer pause and
backpressure resume.

`on_message`, `on_messages`, and `message_batch_size` are incompatible with the
Async native receive consumer.

`drain` is another cold Future. It resolves when an active high-water blocked
period crosses the configured low watermark. It does **not** promise
`pending_bytes == 0`. Cancelling one drain Future affects only that waiter.
`on_drain` is reserved by Async Stream as the exact core-to-Future bridge.

## Pull-based Listener accept

```perl
my $stream = await $listener->accept;
```

`Linux::Event::Async::Listener` owns one persistent Awaitable for repeated
accepts. The returned object is the exact configured `stream_class`.

Acceptance is paused while no wait is armed. Cancelling an accept wait pauses
acceptance but does not close the Listener. Version 0.002 fixes
`max_accept_per_tick => 1` and uses level-triggered acceptance so excess
connections remain in the kernel backlog instead of being accepted behind an
already completed pull-style Awaitable.

## Awaitable Datagram I/O

Define Datagram policy once on a subclass:

```perl
package PacketSocket;
use parent 'Linux::Event::Async::Dgram';

sub datagram_options ($class) {
    return (
        max_datagram_size => 65_535,
        receive_buffer    => 1_048_576,
    );
}
```

Then receive one kernel packet at a time:

```perl
my $socket = PacketSocket->new(
    loop => $loop,
    host => '127.0.0.1',
    port => 9999,
);

await $socket->ready;

while (1) {
    my ($payload, $peer) = await $socket->recv;
    my $ok = $socket->send($payload, to => $peer);
    await $socket->drain if defined($ok) && !$ok;
}
```

`recv` uses one persistent Awaitable per Datagram socket. List context returns
payload and `Linux::Event::Address`; scalar context returns payload.

Async Datagram receive is pull-based. Version 0.002 therefore fixes
`max_datagrams_per_tick => 1` and `edge_triggered => 0`. When no receive is
pending, read interest is paused and additional packets remain in the kernel
receive queue.

The complete effective Async `datagram_options` surface is:

```perl
sub datagram_options ($class) {
    return (
        max_datagram_size      =>     65_535,
        max_datagrams_per_tick =>          1,
        edge_triggered         =>          0,
        high_watermark         =>  1_048_576,
        low_watermark          =>    262_144,
        max_pending_bytes      =>          0,
        max_pending_datagrams  =>          0,
        reuseaddr              =>          0,
        reuseport              =>          0,
        broadcast              =>          0,
        v6only                 =>      undef,
        send_buffer            =>      undef,
        receive_buffer         =>      undef,
    );
}
```

Atomic packet boundaries, connected/unconnected UDP, Unix-domain datagrams,
adopted sockets, peer addresses, queue limits, socket policy, and output
backpressure remain core behavior. `ready` and `drain` are cold Futures;
`on_datagram` and `on_drain` are reserved Async bridges.

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
The normal `after`, `at`, and `every` schedules remain available.

Recurring timers remain fixed-rate. Linux::Event coalesces missed periods and
`wait` returns the represented expiration count. Expirations occurring while no
wait is armed accumulate in one scalar rather than an unbounded tick queue.

`cancel_wait` cancels only the suspension. The recurring Timer remains active.
Core `$timer->cancel` remains terminal and fails a pending wait.

## Awaitable Process completion

```perl
package WorkerProcess;
use parent 'Linux::Event::Async::Process';

sub on_stdout ($process, $bytes) {
    print $bytes;
}

package main;
my $process = WorkerProcess->spawn(
    loop    => $loop,
    command => [$^X, '-e', 'print "done\\n"'],
    stdout  => 'pipe',
);

await $process->wait;
say $process->exit_code;
```

Process completion is a cold one-shot transition, so `wait` returns a normal
Async Future. Core reaps the pid and drains remaining stdout/stderr bytes before
the reserved `on_exit` bridge completes waiters. Exit status and final output
callbacks are therefore stable when `await $process->wait` resumes.

Multiple callers may wait for the same exit. Cancelling one wait does not
signal, kill, detach, or otherwise alter the Process.

The complete `process_options` tuning surface remains available:

```perl
sub process_options ($class) {
    return (
        read_size            =>    65_536,
        max_reads_per_tick   =>         64,
        stdin_high_watermark =>  1_048_576,
        stdin_low_watermark  =>    262_144,
        max_pending_stdin    =>          0,
    );
}
```

Version 0.002 leaves stdout/stderr on Linux::Event's established callback-drain
path and retains `write_stdin`, `close_stdin`, and `pending_stdin_bytes` as core
operations. A pull-style Process pipe API is deferred until it has an explicit
bounded buffering or one-read fairness contract.

## Awaitable Signal delivery

```perl
use POSIX qw(SIGINT SIGTERM);

my $signal = Linux::Event::Async::Signal->new(
    loop    => $loop,
    signals => [SIGINT, SIGTERM],
);

my ($number, $count) = await $signal->wait;
```

`Linux::Event::Async::Signal` retains Linux::Event's shared signalfd ownership
and one persistent Awaitable per subscription. List context returns signal
number and aggregated signalfd record count; scalar context returns the signal
number.

A subscription cannot pause only its own fan-out while remaining in the shared
signalfd service. If notifications arrive while no wait is armed, Async stores
at most one pending entry per subscribed signal number and accumulates the count
for that number. The memory bound is therefore the fixed subscription set, not
the number of delivered notifications.

`cancel_wait` is wait-local. Core `$signal->cancel` remains terminal and fails a
pending wait. Signal-mask ownership, per-thread masking, and one-Loop-per-signal
rules remain exactly the core rules. `on_signal` is reserved by Async Signal.

## Awaitable eventfd notification

```perl
my $event = Linux::Event::Async::Event->new(loop => $loop);

# Producer side: publish payload to its real channel first.
$event->signal;

my $count = await $event->wait;
```

`Linux::Event::Async::Event` adapts the public eventfd-backed Event primitive
with one persistent Awaitable. If the Loop drains eventfd while no wait is armed,
Async accumulates the delivered count in one scalar. No per-notification object
or queue entry is allocated.

The eventfd is still a notification mechanism, not a payload channel. Put work
in the application queue, shared structure, pipe, socket, or other IPC channel
before signaling; use that payload channel as the source of truth.

`cancel_wait` is wait-local. Core `$event->cancel` remains terminal and fails a
pending wait. The core thread/fork boundary is preserved: cloned worker handles
may signal the eventfd but do not own the Loop or Async wait state. `on_event` is
reserved by Async Event.

## Future and performance model

`Linux::Event::Async::Future` represents an asynchronous computation or a cold
one-shot operation. It implements the Future::AsyncAwait Awaitable protocol but
is not a subclass or complete replacement for the CPAN `Future` distribution.

`AWAIT_WAIT` drives the Linux::Event Loop associated with the operation currently
awaited by the Future. Sequential awaits may therefore move between Loops.

The operation type determines the suspension mechanism:

```text
cold transition
    Stream ready / Stream drain
    Dgram ready / Dgram drain
    Process wait
        -> Linux::Event::Async::Future

hot repeated operation
    Stream recv
    Listener accept
    Dgram recv
    Timer wait
    Signal wait
    Event wait
        -> persistent per-resource Awaitable
```

The rule is pragmatic: do not build specialized reusable state for a cold event
unless measurement justifies it, and do not accept avoidable per-event
allocation on a demonstrated hot path.

Linux::Event callback users pay no Future::AsyncAwait dependency or dispatch tax
for this distribution.

## 0.002 scope

The first stable release includes Future::AsyncAwait integration, Stream
readiness/drain/framed receive, pull-based Listener accept, pull-based Datagram
receive/output backpressure, awaitable Timer expiration, awaitable Process
completion, bounded awaitable Signal delivery, and awaitable eventfd
notification.

Pull-style Process pipe I/O, Pipe/TTY operations, standalone resolver operations,
and generic fd readiness remain future work.

See `ASYNC-ROADMAP.md` for architecture constraints and extension priorities.

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
