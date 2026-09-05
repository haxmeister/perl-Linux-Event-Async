package Linux::Event::Async;
use v5.36;
use strict;
use warnings;

our $VERSION = '0.002';

use Future::AsyncAwait 0.71 ();
use Linux::Event::Async::Future ();
use Linux::Event::Async::Stream ();

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
  my $stream = LineStream->connect(
      loop => $loop,
      host => '127.0.0.1',
      port => 9999,
  );

  async sub consume ($stream) {
      await $stream->ready;
      while (defined(my $message = await $stream->recv)) {
          say $message;
      }
  }

  my $task = consume($stream);
  $task->AWAIT_WAIT;

=head1 DESCRIPTION

C<Linux::Event::Async> adds Future::AsyncAwait syntax and Awaitable operations
to the callback-first L<Linux::Event> reactor. It is a separate distribution so
Linux::Event itself remains independent of Future and Future::AsyncAwait.

Importing this module installs C<async> and C<await> through
L<Future::AsyncAwait> and selects L<Linux::Event::Async::Future> as the Future
class used for C<async sub> results.

The first stable release covers two Stream suspension patterns. Application
readiness is a cold one-shot lifecycle operation and C<< $stream->ready >>
returns an ordinary L<Linux::Event::Async::Future>. Framed receive is a hot
repeated operation, so L<Linux::Event::Async::Stream> owns one persistent native
receive Awaitable; C<recv> does not allocate one Future or Awaitable per
message.

=head1 ARCHITECTURE

Linux::Event continues to own epoll dispatch, socket lifecycle, TLS, framing,
buffering, backpressure, fairness, deadlines, and the versioned native consumer
ABI. Linux::Event::Async owns coroutine-facing suspension state,
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
Linux::Event stream policy surface, including C<stream_options>, socket policy,
and declarative L<Linux::Event::TLS> configuration.

For an outbound connection, C<< await $stream->ready >> waits until the Stream
is application-ready. For TLS that means after handshake and verification, not
merely after TCP connection. The normal C<on_ready> lifecycle callback remains
available and runs before readiness Future waiters resume.

Only one receive may be pending on a Stream. Clean EOF resolves to C<undef>.
Cancelling a pending receive does not close the Stream and does not consume the
next message. I/O, framing, and lifecycle failures are delivered as the typed
errors supplied by Linux::Event.

See L<Linux::Event::Async::Stream> for the complete readiness, receive,
cancellation, framing, TLS, and tuning contract.

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

Version 0.002 provides the async-sub Future implementation, awaitable Stream
application readiness, and framed C<SOCK_STREAM> receive. Additional awaitable
operations such as listener accept, output drain, timers, datagrams, process
completion, signals, eventfd events, pipe/TTY operations, resolver operations,
and generic fd readiness are future extensions and are not part of the 0.002
API contract.

=head1 SEE ALSO

L<Linux::Event::Async::Stream>, L<Linux::Event::Async::Future>,
L<Linux::Event>, L<Future::AsyncAwait>

=cut
