package Linux::Event::Async::Future;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

our $VERSION = '0.002';

require XSLoader;
XSLoader::load(__PACKAGE__, $VERSION);

sub done ($self, @result) {
    return $self->AWAIT_DONE(@result);
}

sub fail ($self, $failure) {
    return $self->AWAIT_FAIL($failure);
}

sub get ($self) {
    return $self->AWAIT_GET;
}

sub is_ready ($self) {
    return $self->AWAIT_IS_READY;
}

sub is_cancelled ($self) {
    return $self->AWAIT_IS_CANCELLED;
}

sub on_ready ($self, $callback) {
    $self->AWAIT_ON_READY($callback);
    return $self;
}

sub on_cancel ($self, $callback) {
    $self->AWAIT_ON_CANCEL($callback);
    return $self;
}

sub AWAIT_WAIT ($self) {
    while (!$self->AWAIT_IS_READY) {
        my $loop = $self->loop
            // croak 'cannot wait without a Linux::Event loop';
        $loop->run_once(-1);
    }
    return $self->AWAIT_GET;
}

sub CLONE_SKIP ($class) { 1 }

1;

__END__

=head1 NAME

Linux::Event::Async::Future - native Future for Linux::Event async operations

=head1 SYNOPSIS

  use Linux::Event::Async;

  async sub operation ($stream) {
      await $stream->ready;
      return await $stream->recv;
  }

  my $future = operation($stream);
  my $message = $future->AWAIT_WAIT;

=head1 DESCRIPTION

C<Linux::Event::Async::Future> is the native Future class selected by
L<Linux::Event::Async> for results of C<async sub> declarations. It is also used
directly for cold one-shot asynchronous operations such as
C<< $stream->ready >>. It implements the Awaitable protocol expected by
L<Future::AsyncAwait> while keeping result, failure, readiness, cancellation,
and cancellation-chain state in XS.

Hot repeated Stream receives use a different persistent
L<Linux::Event::Async::Stream> Awaitable and therefore do not allocate one
Future for each incoming message. This distinction lets one-shot lifecycle
operations use a simple Future without imposing per-message allocation on the
receive hot path.

C<Linux::Event::Async::Future> is not a subclass of L<Future> and should not be
assumed to implement the complete API of that distribution. Use the methods
documented here and the Future::AsyncAwait Awaitable protocol.

=head1 CONSTRUCTION

=head2 new

  my $future = Linux::Event::Async::Future->new;
  my $future = Linux::Event::Async::Future->new(loop => $loop);

Creates a pending Future. The optional Linux::Event Loop is used by
C<AWAIT_WAIT>. Futures created internally for an C<async sub> follow the Loop
associated with the operation currently awaited by that computation. One-shot
operation Futures such as Stream readiness are created with the resource's
attached Loop.

=head1 COMPLETION

=head2 done

  $future->done(@result);

Completes a pending Future successfully. Multiple result values are preserved
for list-context C<AWAIT_GET>; scalar context returns the first result.

=head2 fail

  $future->fail($error);

Completes a pending Future with one failure value. Retrieving a failed Future
throws that value.

=head2 cancel

  $future->cancel;

Cancels a pending Future and propagates cancellation through the currently
chained Awaitable operation when applicable. Cancelling an async-sub Future that
is waiting for C<< $stream->recv >> cancels that receive without closing the
Stream.

Cancellation semantics for a directly returned operation Future belong to that
operation. For example, cancelling C<< $stream->ready >> cancels only that
readiness wait; it does not close or cancel the Stream connection.

=head1 OBSERVATION

=head2 get

  my $value = $future->get;

Returns the completed result or throws the stored failure. Calling C<get> on a
pending or cancelled Future throws.

=head2 is_ready

Returns true after successful completion, failure, or cancellation.

=head2 is_cancelled

Returns true only for a cancelled Future.

=head2 on_ready

  $future->on_ready(sub { ... });

Registers a callback for completion and returns the Future. A callback added
after completion runs immediately.

=head2 on_cancel

  $future->on_cancel(sub { ... });

Registers a callback for cancellation and returns the Future.

=head1 WAITING

=head2 AWAIT_WAIT

  my $result = $future->AWAIT_WAIT;

Drives one C<run_once(-1)> operation at a time on the Linux::Event Loop
associated with the operation currently awaited by the async sub. Effective
Loop lookup follows nested child Futures and is repeated between dispatches, so
sequential awaits may move between different Loops. A directly created
one-shot operation Future uses its configured Loop.

C<AWAIT_WAIT> returns or throws with the same semantics as C<AWAIT_GET>. It
throws if the Future is pending but no Linux::Event Loop can be associated with
the current operation.

Calling C<< $loop->run >> directly is different: the core Loop continues until
C<stop> is called and does not stop automatically when Futures become ready.

=head1 THREADS

Future objects are not cloned into Perl ithreads. C<CLONE_SKIP> is enabled for
this class.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Stream>,
L<Future::AsyncAwait>, L<Linux::Event::Loop>

=cut
