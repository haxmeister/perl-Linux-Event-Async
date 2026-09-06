package Linux::Event::Async::Stream;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::IO::Sock::Stream';
use Carp qw(croak);
use Scalar::Util qw(refaddr weaken);
use Linux::Event::Async::Future ();
use Linux::Event::Error ();
use Linux::Event::Framer ();

our $VERSION = '0.002';

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

sub _owns_drain_callback ($class) {
    my $actual = $class->can('on_drain');
    my $owned = __PACKAGE__->can('on_drain');
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub new ($class, %option) {
    croak "$class must not override on_drain(); await drain() is the backpressure sink"
        if !_owns_drain_callback($class);
    croak 'new(): on_drain is reserved by Linux::Event::Async::Stream; use await drain()'
        if exists $option{on_drain};

    my $accepted = $option{_accepted} // 0;
    my $self = $class->SUPER::new(%option);

    # A directly adopted plain connected socket is application-ready at
    # construction time. Accepted sockets deliberately wait for Listener's
    # on_accept phase to finish before _fire_ready runs.
    $self->{_async_ready_established} = 1
        if !$accepted && !$self->transport;
    return $self;
}

sub _ready_waiter_failure ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'event',
        operation => 'ready',
        message   => 'Stream closed before becoming application-ready',
    );
}

sub _drain_waiter_failure ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'event',
        operation => 'drain',
        message   => 'Stream closed before output backpressure drained',
    );
}

sub _finish_waiter_list ($self, $waiters, $mode, $value) {
    my $failure;

    for my $future (@$waiters) {
        next if !$future || $future->is_ready;
        my $ok = eval {
            if ($mode eq 'done') {
                $future->done($value);
            } else {
                $future->fail($value);
            }
            1;
        };
        $failure //= $@ if !$ok;
    }
    return $failure;
}

sub _finish_ready_waiters ($self, $mode, $value) {
    my $waiters = delete $self->{_async_ready_waiters} // [];
    return $self->_finish_waiter_list($waiters, $mode, $value);
}

sub _finish_drain_waiters ($self, $mode, $value) {
    my $waiters = delete $self->{_async_drain_waiters} // [];
    return $self->_finish_waiter_list($waiters, $mode, $value);
}

sub ready ($self) {
    my $future = Linux::Event::Async::Future->new(loop => $self->loop);

    if ($self->{_async_ready_established}) {
        return $future->done($self);
    }
    if ($self->is_closed) {
        return $future->fail($self->_ready_waiter_failure);
    }
    croak 'ready(): pending Stream must be attached to a Linux::Event loop'
        if !$self->loop;

    push @{ $self->{_async_ready_waiters} //= [] }, $future;
    weaken($self->{_async_ready_waiters}[-1]);
    return $future;
}

sub drain ($self) {
    my $future = Linux::Event::Async::Future->new(loop => $self->loop);

    if ($self->is_closed) {
        return $future->fail($self->_drain_waiter_failure);
    }
    if (!$self->is_write_blocked) {
        return $future->done($self);
    }
    croak 'drain(): blocked Stream must be attached to a Linux::Event loop'
        if !$self->loop;

    push @{ $self->{_async_drain_waiters} //= [] }, $future;
    weaken($self->{_async_drain_waiters}[-1]);
    return $future;
}

sub _fire_ready ($self) {
    return $self->SUPER::_fire_ready if $self->is_closed;

    # Mark the one-shot readiness outcome before user on_ready runs. This makes
    # a reentrant close from on_ready historical success rather than a failed
    # connection attempt. Waiters themselves resume after core on_ready.
    $self->{_async_ready_established} = 1;

    my $core_failure;
    my $ok = eval { $self->SUPER::_fire_ready; 1 };
    $core_failure = $@ if !$ok;

    my $future_failure = $self->_finish_ready_waiters('done', $self);
    die $core_failure if defined($core_failure) && length($core_failure);
    die $future_failure if defined($future_failure) && length($future_failure);
    return;
}

sub on_drain ($self) {
    # Core owns the exact blocked->low-watermark transition and already defers
    # preconnect drain notification until application readiness. This reserved
    # callback is therefore the stable bridge for the Async drain operation on
    # both Linux::Event 0.110 and current core.
    my $waiters = delete $self->{_async_drain_waiters} // [];
    my $failure = $self->_finish_waiter_list($waiters, 'done', $self);
    die $failure if defined($failure) && length($failure);
    return;
}

sub _close_now ($self, $close_fh) {
    my $fail_ready = !$self->{_async_ready_established};

    my $core_failure;
    my $ok = eval { $self->SUPER::_close_now($close_fh); 1 };
    $core_failure = $@ if !$ok;

    my $future_failure;
    if ($fail_ready) {
        $future_failure = $self->_finish_ready_waiters(
            'fail', $self->_ready_waiter_failure,
        );
    }
    my $drain_failure = $self->_finish_drain_waiters(
        'fail', $self->_drain_waiter_failure,
    );
    $future_failure //= $drain_failure;

    die $core_failure if defined($core_failure) && length($core_failure);
    die $future_failure if defined($future_failure) && length($future_failure);
    return;
}

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
  use Linux::Event::Loop;

  my $loop = Linux::Event::Loop->new;
  my $stream = LineStream->connect(
      loop => $loop,
      host => 'example.com',
      port => 443,
  );

  async sub request ($stream) {
      await $stream->ready;
      $stream->send('hello');
      await $stream->drain if $stream->is_write_blocked;
      return await $stream->recv;
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Stream> is the async specialization of
L<Linux::Event::IO::Sock::Stream>. It preserves Linux::Event's native socket,
TLS, framing, buffering, backpressure, timeout, and tuning machinery while
adding Awaitable application readiness and output drain plus a reusable
Awaitable framed receive path.

A concrete protocol subclass declares a built-in native framer with
L<Linux::Event::Framer>. Linux::Event resolves the framer, tuning, TLS policy,
socket policy, lifecycle callbacks, and native consumer into the cached class
descriptor. Steady-state receive does not perform method lookup or select
between callback styles.

Each Stream owns one native receive context and one persistent
C<Linux::Event::Async::Stream::Awaitable> view. C<recv> arms that context and
returns the same Awaitable view for each generation. No Future or Awaitable is
allocated per received message.

Readiness and drain are different: they observe comparatively cold lifecycle or
backpressure transitions and return L<Linux::Event::Async::Future> objects. This
keeps the hot receive path allocation-free without requiring specialized native
state for every one-shot wait.

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

=head1 CONNECTION READINESS

=head2 ready

  my $stream = Client->connect(
      loop => $loop,
      host => 'example.com',
      port => 443,
  );

  await $stream->ready;

C<ready> returns a L<Linux::Event::Async::Future> that resolves with the Stream
when it becomes application-ready. For an outbound TCP connection this includes
hostname resolution and successful connect. For a TLS-declared Stream it also
includes the TLS handshake, certificate verification, and negotiated transport
state such as ALPN. Application protocol work may begin after the Future
resolves.

The existing core C<on_ready($stream)> lifecycle callback remains fully
supported. If both callback and Future styles are used on one Stream, the core
C<on_ready> callback runs first and readiness Future waiters resume after it.
A close performed reentrantly by C<on_ready> does not retroactively turn the
completed readiness transition into failure.

Connection, resolution, transport, TLS, timeout, or setup failure completes all
pending readiness Futures with the same L<Linux::Event::Error> recorded by the
Stream. Explicitly closing a Stream before application readiness completes the
waiters with an C<event>/C<ready> error.

Multiple callers may wait for readiness concurrently. Cancelling one returned
Future cancels only that wait; it does not close the Stream or cancel another
readiness waiter. A later C<ready> call may wait again while the connection is
still pending.

A directly adopted plain connected socket is already application-ready, so
C<ready> returns an immediately completed Future. An accepted plain Stream does
not complete C<ready> until the Listener's C<on_accept> phase has finished and
the normal Stream readiness transition occurs.

A pending Stream must be attached to a Linux::Event Loop before C<ready> is
called so the returned Future has a Loop for C<await> and C<AWAIT_WAIT>. A
detached outbound Stream can therefore be constructed first, added to a Loop,
and then awaited:

  my $stream = Client->connect(host => $host, port => $port);
  $loop->add($stream);
  await $stream->ready;

=head1 OUTPUT BACKPRESSURE

=head2 drain

  $stream->send($payload);
  await $stream->drain if $stream->is_write_blocked;

C<drain> returns a L<Linux::Event::Async::Future>. If the Stream is not currently
write-blocked, the Future is already complete. If output has crossed the
configured C<high_watermark>, the Future waits for that blocked period to end
when queued output falls through the C<low_watermark>. The Future resolves with
the Stream.

C<drain> observes Linux::Event's cooperative backpressure transition; it does
not promise that C<pending_bytes> is zero. Code that merely needs permission to
produce more output should wait for C<drain>, not busy-wait for an empty queue.

C<on_drain> is reserved by C<Linux::Event::Async::Stream> as the bridge from the
core blocked-to-low-watermark transition to pending drain Futures. Concrete
Async Stream subclasses must not override C<on_drain>, and the constructor
C<on_drain> callback form is not accepted. Application backpressure handling
belongs around C<< await $stream->drain >>.

Multiple Futures may wait on one blocked period. Cancelling one drain Future
cancels only that wait; it neither closes the Stream nor discards queued output.
A Stream error or explicit close before the blocked period drains fails pending
waiters with the Stream's L<Linux::Event::Error> or an C<event>/C<drain> error.

Preconnect backpressure preserves core ordering automatically: Linux::Event
does not invoke the reserved C<on_drain> bridge until the connection becomes
application-ready, even when queued output crossed the high watermark before
connect completed.

A blocked Stream must be attached to a Loop before C<drain> can create a pending
wait. An unblocked Stream may return an immediately completed drain Future even
when no Loop is needed.

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

C<on_drain> is also reserved by the Async drain operation. Other lifecycle
callbacks such as C<on_ready>, C<on_eof>, C<on_error>, and C<on_close> remain
available through the underlying Stream API.

=head1 OUTPUT

Output otherwise remains the ordinary Linux::Event Stream API. C<write($bytes)>
writes raw ordered bytes, while C<send($payload)> applies the subclass's
declared framer. Output queue limits, watermarks, half-close, and TLS encryption
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

C<ready> and C<drain> use ordinary L<Linux::Event::Async::Future> instances
because their transitions are outside the repeated message-delivery hot path.
Stream waiter lists hold weak Future references, so discarded or cancelled
operation Futures do not create Stream/Future ownership cycles.

=head1 WAITING

The receive Awaitable implements C<AWAIT_WAIT>. It drives C<run_once(-1)> on the
Stream's attached Loop until the receive is ready, then returns or throws with
C<AWAIT_GET> semantics. Waiting on a pending receive whose Stream is not
attached to a Loop throws.

Readiness and drain Futures use the same Loop through
L<Linux::Event::Async::Future/AWAIT_WAIT>.

=head1 REQUIREMENTS

This implementation requires the public native consumer ABI shipped by
Linux::Event 0.110 or newer.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Future>,
L<Linux::Event::IO::Sock::Stream>, L<Linux::Event::Framer>,
L<Linux::Event::TLS>, L<Linux::Event::Error>

=cut
