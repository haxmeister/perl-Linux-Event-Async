package Linux::Event::Async::Stream;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::IO::Sock::Stream';
use Carp qw(croak);
use Linux::Event::Async::Future ();
use Linux::Event::Framer ();

our $VERSION = '0.001';

require XSLoader;
XSLoader::load('Linux::Event::Async', $VERSION);

Linux::Event::Framer->declare_native_consumer(
    __PACKAGE__,
    {
        provider           => \&_consumer_operations_address,
        abi_version        => 1,
        operations_address => _consumer_operations_address(),
    },
);

sub cancel_recv ($self) {
    _recv_cancel($self);
    return $self;
}

package Linux::Event::Async::Stream::Awaitable;

sub AWAIT_CHAIN_CANCEL ($self, $other) {
    $self->AWAIT_ON_CANCEL(sub {
        $other->cancel if $other && $other->can('cancel');
    });
    return $self;
}

sub AWAIT_CLONE ($self) {
    return Linux::Event::Async::Future->new(loop => $self->_stream->loop);
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_stream->loop;
    Carp::croak 'cannot wait on a Stream that is not attached to a loop'
        if !$loop && !$self->AWAIT_IS_READY;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

package Linux::Event::Async::Stream;

1;

__END__

=head1 NAME

Linux::Event::Async::Stream - awaitable framed SOCK_STREAM connections

=head1 SYNOPSIS

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

  package main;
  use Linux::Event::Async;

  async sub read_one ($stream) {
      return await $stream->recv;
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Stream> is the async receive specialization of
L<Linux::Event::IO::Sock::Stream>. It preserves Linux::Event's native socket,
TLS, framing, buffering, backpressure, timeout, and tuning machinery while
replacing framed message callbacks with a reusable Awaitable receive path.

A concrete protocol subclass declares a built-in native framer with
L<Linux::Event::Framer>. Linux::Event resolves the framer, tuning, TLS policy,
socket policy, lifecycle callbacks, and native consumer into the cached class
descriptor. Steady-state receive does not perform method lookup or select
between callback styles.

Each Stream owns one native receive context and one persistent
C<Linux::Event::Async::Stream::Awaitable> view. C<recv> arms that context and
returns the same Awaitable view for each generation. No Future or Awaitable is
allocated per received message.

=head1 SUBCLASSING, FRAMING, TLS, AND TUNING

Subclassing is the intended protocol boundary because it lets one class declare
wire framing, TLS policy, socket policy, and stream tuning once and reuse that
cached configuration for every connection.

A plain framed protocol can be as small as:

  package PacketStream;
  use parent 'Linux::Event::Async::Stream';
  use Linux::Event::Framer 'U32BE';

The same subclass model composes with TLS:

  package SecureLineStream;
  use parent 'Linux::Event::Async::Stream';
  use Linux::Event::Framer 'Delimiter', "\n";
  use Linux::Event::TLS
      verify => 1,
      alpn   => ['my-protocol'];

Outbound C<connect>, accepted sockets, Unix-domain stream sockets, socket
options, and TLS lifecycle retain the behavior documented by
L<Linux::Event::IO::Sock::Stream> and L<Linux::Event::TLS>.

=head2 stream_options

Async Stream subclasses retain the ordered-byte tuning controls that apply to a
framed native consumer. The complete applicable set is:

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

These values are Linux::Event's defaults. C<read_size> controls the native read
size. C<read_budget_bytes> limits bytes drained during one readiness dispatch;
zero means no byte budget. C<high_watermark> and C<low_watermark> control output
backpressure. C<max_pending_bytes> sets the hard queued-output limit; zero means
no explicit hard limit. C<max_buffer> limits retained input. The three timeout
values are in seconds and zero disables the corresponding established-stream
timeout.

C<read_batch_bytes> is a raw-stream callback option and therefore does not apply
to a framed Async Stream. C<message_batch_size> is a framed callback batching
option and cannot be combined with the native Async consumer. Async receive has
its own bounded prefetch behavior described below.

=head1 RECEIVE API

=head2 recv

  my $message = await $stream->recv;

Arms one receive and returns the Stream's persistent Awaitable view. Only one
receive may be active. Starting another receive while one is pending throws.
After a receive becomes ready, its result must be consumed before the next
receive generation starts.

A completed framed message is returned as the framer payload. Clean EOF returns
C<undef>. I/O, framing, close, detach, and read-side lifecycle failures throw
the typed L<Linux::Event::Error> supplied by the core stream engine.

=head2 cancel_recv

  $stream->cancel_recv;

Cancels only the pending receive and returns the Stream. Cancellation does not
close the connection and does not consume the next message. Bytes arriving
while no receive is armed remain subject to the normal native buffering and
backpressure rules.

Cancelling the L<Linux::Event::Async::Future> for an async sub that is currently
awaiting C<recv> propagates cancellation to that receive.

=head1 BOUNDED PREFETCH

Within one native framed-input drain, the consumer may retain up to 64
additional messages and approximately 256 KiB of payload. The byte boundary
permits one complete-frame overshoot. When either boundary is reached without a
new receive being armed, native delivery pauses and Linux::Event's normal
backpressure takes over.

This bounded prefetch lets a coroutine issue consecutive receives against
already parsed messages without paying one Future allocation or one event-loop
suspension per frame, while preventing an unbounded decoded-message queue.

=head1 CALLBACK EXCLUSIVITY

The Async native consumer is the framed message delivery mechanism for this
class. A concrete Async Stream subclass must not define C<on_message> or
C<on_messages>, and C<message_batch_size> must remain disabled. Linux::Event
rejects those combinations when building the class descriptor.

Lifecycle behavior remains available through the underlying Stream API; this
restriction applies to framed message delivery, not to socket, TLS, tuning, or
output policy.

=head1 OUTPUT

Output remains the ordinary Linux::Event Stream API. C<write($bytes)> writes raw
ordered bytes, while C<send($payload)> applies the subclass's declared framer.
Output queue limits, watermarks, drain behavior, half-close, and TLS encryption
are handled by L<Linux::Event::IO::Sock::Stream>.

=head1 AWAITABLE LIFETIME

The object returned by C<recv> is an internal persistent view over native
receive state. Applications should await it or use the documented Awaitable
protocol; they should not construct C<Linux::Event::Async::Stream::Awaitable>
objects directly.

Provider-owned calls into Linux::Event use the consumer ABI v1 retain/release
contract so a callback or resumed coroutine may close the Stream reentrantly
without leaving the native receive frame with a dangling host or provider
context.

=head1 WAITING

The receive Awaitable implements C<AWAIT_WAIT>. It drives C<run_once(-1)> on the
Stream's attached Loop until the receive is ready, then returns or throws with
C<AWAIT_GET> semantics. Waiting on a pending receive whose Stream is not
attached to a Loop throws.

=head1 REQUIREMENTS

This implementation requires the public native consumer ABI shipped by
Linux::Event 0.110 or newer.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Future>,
L<Linux::Event::IO::Sock::Stream>, L<Linux::Event::Framer>,
L<Linux::Event::TLS>, L<Linux::Event::Error>

=cut
