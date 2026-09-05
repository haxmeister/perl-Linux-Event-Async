package Linux::Event::Async;
use v5.36;
use strict;
use warnings;

our $VERSION = '0.001';

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

  async sub consume ($stream) {
      while (defined(my $message = await $stream->recv)) {
          say $message;
      }
  }

  my $task = consume($stream);
  $task->AWAIT_WAIT;

=head1 DESCRIPTION

C<Linux::Event::Async> adds Future::AsyncAwait syntax and Awaitable objects to
the callback-first L<Linux::Event> reactor. It is a separate distribution so
Linux::Event itself remains independent of Future and Future::AsyncAwait.

Importing this module installs C<async> and C<await> through
L<Future::AsyncAwait> and selects L<Linux::Event::Async::Future> as the Future
class used for C<async sub> results.

The first stable release concentrates on framed stream receive. A
L<Linux::Event::Async::Stream> owns one persistent native receive Awaitable.
C<recv> arms that state and returns the same Awaitable view for each receive;
it does not allocate one Future or one Awaitable per message. The Future for an
entire C<async sub> remains a normal independently owned object.

=head1 ARCHITECTURE

Linux::Event continues to own epoll dispatch, socket lifecycle, TLS, framing,
buffering, backpressure, fairness, and the versioned native consumer ABI.
Linux::Event::Async owns coroutine-facing receive state, Future::AsyncAwait
integration, cancellation semantics, and async-sub result Futures.

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

Only one receive may be pending on a Stream. Clean EOF resolves to C<undef>.
Cancelling a pending receive does not close the Stream and does not consume the
next message. I/O, framing, and lifecycle failures are delivered as the typed
errors supplied by Linux::Event.

See L<Linux::Event::Async::Stream> for the complete receive contract and tuning
model.

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

Version 0.001 provides the async-sub Future implementation and framed
C<SOCK_STREAM> receive path. Additional awaitable operations such as accept,
drain, timers, process completion, and resolver work are future extensions and
are not part of the 0.001 API contract.

=head1 SEE ALSO

L<Linux::Event::Async::Stream>, L<Linux::Event::Async::Future>,
L<Linux::Event>, L<Future::AsyncAwait>

=cut
