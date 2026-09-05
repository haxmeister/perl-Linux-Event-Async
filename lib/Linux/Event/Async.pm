package Linux::Event::Async;
use v5.36;
use strict;
use warnings;

our $VERSION = '0.002';

use Future::AsyncAwait 0.71 ();
use Linux::Event::Async::Future ();
use Linux::Event::Async::Stream ();
use Linux::Event::Async::Listener ();
use Linux::Event::Async::Timer ();
use Linux::Event::Async::Dgram ();
use Linux::Event::Async::Process ();
use Linux::Event::Async::Signal ();
use Linux::Event::Async::Event ();

sub import {
    @_ = ('Future::AsyncAwait', future_class => 'Linux::Event::Async::Future');
    goto &Future::AsyncAwait::import;
}

1;

__END__

=head1 NAME

Linux::Event::Async - async/await integration for Linux::Event

=head1 SYNOPSIS

  package LineStream;
  use parent 'Linux::Event::Async::Stream';
  use Linux::Event::Framer 'Delimiter', "\n";

  package main;
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

=head1 DESCRIPTION

C<Linux::Event::Async> adds Future::AsyncAwait syntax and Awaitable operations
to the callback-first L<Linux::Event> reactor. It is a separate distribution so
Linux::Event itself remains independent of Future and Future::AsyncAwait.

Importing this module installs C<async> and C<await> through
L<Future::AsyncAwait> and selects L<Linux::Event::Async::Future> as the Future
class used for C<async sub> results.

Version 0.002 deliberately uses two suspension models. Cold lifecycle or
backpressure transitions such as Stream/Dgram C<ready>, Stream/Dgram C<drain>,
and Process C<wait> return ordinary L<Linux::Event::Async::Future> objects. Hot
repeated operations such as framed Stream C<recv>, Dgram C<recv>, Listener
C<accept>, Timer C<wait>, Signal C<wait>, and Event C<wait> use persistent
per-resource Awaitables so steady-state loops avoid one Future or Awaitable
allocation per event.

=head1 ARCHITECTURE

Linux::Event continues to own epoll dispatch, socket lifecycle, bind/listen and
accept4, datagram packet I/O, TLS, framing, buffering, backpressure, fairness,
monotonic timer scheduling, pidfd process lifecycle, asynchronous process pipes,
signalfd delivery, eventfd signaling, and the versioned native consumer ABI.
Linux::Event::Async owns coroutine-facing suspension state,
Future::AsyncAwait integration, cancellation semantics, async-sub result
Futures, and operation adapters over Linux::Event resources.

The dependency direction is intentionally one way:

  Linux::Event::Async -> Linux::Event
  Linux::Event::Async -> Future::AsyncAwait

Linux::Event does not depend on this distribution.

=head1 STREAM MODEL

Applications normally define a protocol class derived from
L<Linux::Event::Async::Stream> and declare one of Linux::Event's built-in native
framers with L<Linux::Event::Framer>. That subclass retains the normal
Linux::Event Stream policy surface, including C<stream_options>, socket policy,
and declarative L<Linux::Event::TLS> configuration.

C<< await $stream->ready >> waits for application readiness. For TLS that means
after handshake and verification, not merely after TCP connection.
C<< await $stream->recv >> receives one framed message through the persistent
native receive Awaitable. C<< await $stream->drain >> waits for an active
high-water backpressure period to clear through the configured low watermark.

Only one receive may be pending on a Stream. Clean EOF resolves to C<undef>.
Cancelling a receive does not close the Stream and does not consume the next
message. Stream lifecycle failures are delivered as the typed errors supplied
by Linux::Event.

See L<Linux::Event::Async::Stream> for the complete readiness, drain, receive,
cancellation, framing, TLS, and tuning contract.

=head1 LISTENER MODEL

L<Linux::Event::Async::Listener> provides C<< await $listener->accept >> and
returns the exact C<stream_class> object constructed by the Linux::Event core.
One persistent Listener Awaitable is reused across an accept loop.

The Async Listener is pull-based: acceptance is paused while no accept is
pending, and cancellation pauses rather than closes the listening socket. The
0.002 implementation uses one level-triggered kernel accept per armed wait so
additional connections remain in the kernel backlog instead of being accepted
and discarded behind a completed Awaitable.

Use an L<Linux::Event::Async::Stream> subclass as C<stream_class> for a complete
coroutine path from accept through application readiness, framed receive, and
backpressured output.

=head1 DATAGRAM MODEL

L<Linux::Event::Async::Dgram> adapts the public
L<Linux::Event::IO::Sock::Dgram> API. C<< await $socket->recv >> returns one
packet, with the peer address as a second result in list context, and reuses one
persistent Awaitable across receives.

Datagram receive is pull-based. Async fixes receive fairness to one
level-triggered packet per armed wait so packets beyond the completed Awaitable
remain in the kernel receive queue rather than being removed by core batching.
C<ready> and output C<drain> remain ordinary Future waits.

Bound UDP, connected UDP, Unix-domain datagrams, adopted sockets, atomic packet
boundaries, peer addresses, queue limits, socket policy, and output backpressure
remain Linux::Event core behavior.

=head1 TIMER MODEL

L<Linux::Event::Async::Timer> adapts the public monotonic
L<Linux::Event::Kernel::Timer> scheduler. C<< await $timer->wait >> returns the
number of expirations represented by that timer event and reuses one persistent
Awaitable across recurring waits.

Recurring timers retain Linux::Event's fixed-rate and coalescing semantics. If
expirations occur while no wait is armed, the represented count is accumulated
in one scalar rather than an unbounded queue. Cancelling a pending Timer wait
cancels only that suspension; the underlying recurring Timer continues until
its normal C<cancel> is called.

=head1 PROCESS MODEL

L<Linux::Event::Async::Process> adapts the public pidfd-backed
L<Linux::Event::Kernel::Process> lifecycle. C<< await $process->wait >> resolves
with the same Process after normal core exit completion.

Core reaps the pid and drains remaining stdout/stderr pipe data before the
reserved C<on_exit> bridge runs, so Process status fields and final output
callbacks are settled before the Future resumes. Multiple callers may wait for
the same exit. Cancelling one wait is waiter-local and never sends a signal or
terminates the child.

Version 0.002 intentionally leaves stdout/stderr delivery on the core callback
path and retains C<write_stdin>, C<close_stdin>, and stdin backpressure as core
operations. It does not impose a partially specified pull-buffering model on
Process pipe I/O merely to add an C<await> spelling.

=head1 SIGNAL MODEL

L<Linux::Event::Async::Signal> adapts the public shared-signalfd
L<Linux::Event::Kernel::Signal> subscription. C<< await $signal->wait >> returns
the delivered signal number, and in list context also returns the aggregated
signalfd record count.

A Signal subscription cannot pause only its own fan-out while remaining attached
to the shared signalfd. Notifications arriving without an armed wait are
therefore coalesced in bounded state: at most one pending entry per subscribed
signal number, with counts accumulated into that entry. No unbounded event queue
is created.

C<cancel_wait> affects only the suspension. Core C<< $signal->cancel >> remains
the terminal subscription operation and fails a pending wait.

=head1 EVENT MODEL

L<Linux::Event::Async::Event> adapts the public eventfd-backed
L<Linux::Event::Kernel::Event> primitive. C<< await $event->wait >> returns the
counter value drained for the notification and reuses one persistent Awaitable.

If the Loop drains eventfd while no wait is armed, Async accumulates the count
in one scalar for the next wait rather than allocating per-notification queue
entries. The Event still carries notification only; application payloads belong
in the queue, pipe, socket, shared structure, or other IPC channel published
before C<signal>.

C<cancel_wait> is wait-local. Core C<< $event->cancel >> remains terminal.
Core thread and fork signaling rules remain unchanged; cloned worker handles may
signal but do not own Async wait state.

=head1 DRIVING ASYNC WORK

Calling an C<async sub> starts it immediately and returns a
L<Linux::Event::Async::Future>. C<AWAIT_WAIT> drives the Linux::Event Loop
associated with the operation currently awaited by that Future until it is
ready.

C<< $loop->run >> retains the Linux::Event core contract and continues until
C<stop> is called. Completion of an async Future does not implicitly stop a
shared Loop because unrelated watchers or tasks may still be active.

=head1 REQUIREMENTS

Linux::Event::Async requires Linux, Perl 5.36 or newer, Linux::Event 0.110 or
newer, and Future::AsyncAwait 0.71 or newer.

=head1 FIRST RELEASE SCOPE

Version 0.002 provides:

=over 4

=item *

L<Linux::Event::Async::Future> for C<async sub> results and cold one-shot waits;

=item *

L<Linux::Event::Async::Stream> application C<ready>, output C<drain>, and
persistent framed C<recv>;

=item *

L<Linux::Event::Async::Listener> persistent C<accept>;

=item *

L<Linux::Event::Async::Dgram> application C<ready>, output C<drain>, and
persistent packet C<recv>;

=item *

L<Linux::Event::Async::Timer> persistent C<wait> with coalesced expiration
counts;

=item *

L<Linux::Event::Async::Process> Future-based process completion C<wait> after
pidfd reaping and final stdout/stderr drain;

=item *

L<Linux::Event::Async::Signal> persistent C<wait> over shared signalfd delivery
with bounded idle coalescing; and

=item *

L<Linux::Event::Async::Event> persistent C<wait> over eventfd counter delivery.

=back

Pull-style process pipe operations, Pipe/TTY operations, standalone resolver
operations, and generic fd readiness remain future extensions rather than hidden
0.002 APIs.

=head1 SEE ALSO

L<Linux::Event::Async::Listener>, L<Linux::Event::Async::Stream>,
L<Linux::Event::Async::Dgram>, L<Linux::Event::Async::Timer>,
L<Linux::Event::Async::Process>, L<Linux::Event::Async::Signal>,
L<Linux::Event::Async::Event>, L<Linux::Event::Async::Future>,
L<Linux::Event>, L<Future::AsyncAwait>

=cut
