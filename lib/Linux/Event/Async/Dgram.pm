package Linux::Event::Async::Dgram;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::IO::Sock::Dgram';
use Carp qw(croak);
use Scalar::Util qw(refaddr weaken);

use Linux::Event::Async::Future ();
use Linux::Event::Error ();

our $VERSION = '0.002';

sub _owns_bridge ($class, $name) {
    my $actual = $class->can($name);
    my $owned = __PACKAGE__->can($name);
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub _validate_async_class ($class, $method, $option) {
    croak "$class must not override on_datagram(); await recv() is the datagram sink"
        if !_owns_bridge($class, 'on_datagram');
    croak "$class must not override on_drain(); await drain() is the backpressure sink"
        if !_owns_bridge($class, 'on_drain');

    for my $name (qw(on_datagram on_drain on_ready on_error on_close)) {
        croak "$method(): per-instance $name callbacks are not supported by Linux::Event::Async::Dgram; define lifecycle policy on the subclass"
            if exists $option->{$name};
    }

    if (exists $option->{max_datagrams_per_tick}) {
        my $maximum = $option->{max_datagrams_per_tick};
        croak "$method(): Linux::Event::Async::Dgram requires max_datagrams_per_tick => 1"
            if !defined($maximum) || ref($maximum) || "$maximum" ne '1';
    }
    if (exists $option->{edge_triggered}) {
        my $edge = $option->{edge_triggered};
        croak "$method(): Linux::Event::Async::Dgram does not support edge_triggered receive"
            if !defined($edge) || ref($edge) || "$edge" ne '0';
    }
    $option->{max_datagrams_per_tick} = 1;
    $option->{edge_triggered} = 0;
    return;
}

sub _async_initialize ($self) {
    my $awaitable = bless {
        socket    => $self,
        state     => 'idle',
        payload   => undef,
        peer      => undef,
        failure   => undef,
        on_ready  => [],
        on_cancel => [],
    }, 'Linux::Event::Async::Dgram::Awaitable';
    weaken($awaitable->{socket});

    $self->{_async_recv_awaitable} = $awaitable;
    $self->{_async_recv_armed} = 0;
    $self->{_async_write_blocked} = 0;
    $self->{_async_initialized} = 1;

    # Pull-style receive owns read interest. Keep datagrams in the kernel queue
    # until application code explicitly arms recv(). This flag also survives a
    # detached/resolving socket and is honored when core later registers it.
    $self->pause_read if !$self->is_terminal;
    return $self;
}

sub new ($class, %option) {
    croak 'new(): must be called as a class method' if ref $class;
    _validate_async_class($class, 'new', \%option);
    my $self = $class->SUPER::new(%option);
    return $self->_async_initialize;
}

sub connect ($class, %option) {
    croak 'connect(): must be called as a class method' if ref $class;
    _validate_async_class($class, 'connect', \%option);
    my $self = $class->SUPER::connect(%option);
    return $self->_async_initialize;
}

sub _ready_failure ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'event',
        operation => 'dgram_ready',
        message   => 'Datagram socket closed before becoming application-ready',
    );
}

sub _recv_failure ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'event',
        operation => 'dgram_receive',
        message   => 'Datagram socket closed before receive completed',
    );
}

sub _drain_failure ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'event',
        operation => 'dgram_drain',
        message   => 'Datagram socket closed before output backpressure drained',
    );
}

sub _finish_future_list ($self, $name, $mode, $value) {
    my $waiters = delete $self->{$name} // [];
    my $failure;
    for my $future (@$waiters) {
        next if !$future || $future->is_ready;
        my $ok = eval {
            $mode eq 'done' ? $future->done($value) : $future->fail($value);
            1;
        };
        $failure //= $@ if !$ok;
    }
    return $failure;
}

sub ready ($self) {
    my $future = Linux::Event::Async::Future->new(loop => $self->loop);

    return $future->done($self) if $self->{_async_ready_established};
    return $future->fail($self->_ready_failure) if $self->is_terminal;
    croak 'ready(): Datagram socket must be attached to a Linux::Event loop'
        if !$self->loop;

    push @{ $self->{_async_ready_waiters} //= [] }, $future;
    weaken($self->{_async_ready_waiters}[-1]);
    return $future;
}

sub recv ($self) {
    my $awaitable = $self->{_async_recv_awaitable}
        // croak 'recv(): Datagram construction is incomplete';
    my $state = $awaitable->{state};

    croak 'recv(): previous datagram or receive failure has not been consumed'
        if $state eq 'done' || $state eq 'failed';
    croak 'recv(): another receive is already pending'
        if $state eq 'pending';

    if (my $available = delete $self->{_async_available_datagram}) {
        $awaitable->_arm_done(@$available);
        return $awaitable;
    }
    if ($self->is_terminal) {
        $awaitable->_arm_failed($self->_recv_failure);
        return $awaitable;
    }
    croak 'recv(): Datagram socket must be active; await ready() first'
        if !$self->is_active;
    croak 'recv(): Datagram socket must be attached to a Linux::Event loop'
        if !$self->loop;

    $awaitable->_arm;
    $self->{_async_recv_armed} = 1;
    $self->resume_read if $self->is_read_paused;
    return $awaitable;
}

sub cancel_recv ($self) {
    my $awaitable = $self->{_async_recv_awaitable};
    $awaitable->cancel if $awaitable;
    return $self;
}

sub send ($self, $payload, %option) {
    my $result = $self->SUPER::send($payload, %option);
    $self->{_async_write_blocked} = 1 if defined($result) && !$result;
    return $result;
}

sub drain ($self) {
    my $future = Linux::Event::Async::Future->new(loop => $self->loop);

    return $future->fail($self->_drain_failure) if $self->is_terminal;
    return $future->done($self) if !$self->{_async_write_blocked};
    croak 'drain(): blocked Datagram socket must be attached to a Linux::Event loop'
        if !$self->loop;

    push @{ $self->{_async_drain_waiters} //= [] }, $future;
    weaken($self->{_async_drain_waiters}[-1]);
    return $future;
}

sub _fire_ready ($self) {
    return $self->SUPER::_fire_ready if !$self->{_async_initialized};
    return $self->SUPER::_fire_ready if $self->is_terminal;

    $self->{_async_ready_established} = 1;
    my $core_failure;
    my $ok = eval { $self->SUPER::_fire_ready; 1 };
    $core_failure = $@ if !$ok;

    my $future_failure = $self->_finish_future_list(
        '_async_ready_waiters', 'done', $self,
    );
    die $core_failure if defined($core_failure) && length($core_failure);
    die $future_failure if defined($future_failure) && length($future_failure);
    return;
}

sub on_datagram ($self, $payload, $peer) {
    my $awaitable = $self->{_async_recv_awaitable};

    if (!$awaitable || $awaitable->{state} ne 'pending'
        || !$self->{_async_recv_armed}) {
        # The one-packet-per-turn receive policy should make this unreachable.
        # Preserve the packet rather than silently discard it if a future core
        # change violates the pull-read invariant.
        $self->{_async_available_datagram} = [$payload, $peer];
        $self->pause_read if $self->is_active && !$self->is_read_paused;
        return;
    }

    $self->{_async_recv_armed} = 0;
    my $failure = $awaitable->_complete_done($payload, $peer);

    # Future::AsyncAwait resumes synchronously. A continuation that immediately
    # arms the next recv() sets the flag again before control returns here.
    $self->pause_read
        if $self->is_active && !$self->{_async_recv_armed}
        && !$self->is_read_paused;
    die $failure if defined($failure) && length($failure);
    return;
}

sub on_drain ($self) {
    $self->{_async_write_blocked} = 0;
    my $failure = $self->_finish_future_list(
        '_async_drain_waiters', 'done', $self,
    );
    die $failure if defined($failure) && length($failure);
    return;
}

sub _finish_recv_failure ($self, $error) {
    my $awaitable = $self->{_async_recv_awaitable};
    return if !$awaitable || $awaitable->{state} ne 'pending';

    $self->{_async_recv_armed} = 0;
    my $failure = $awaitable->_complete_failed($error);
    $self->pause_read
        if $self->is_active && !$self->{_async_recv_armed}
        && !$self->is_read_paused;
    return $failure;
}

sub _route_reported_error ($self, $error) {
    my $operation = eval { $error->operation } // '';
    my $failure;

    if ($self->is_terminal || $operation eq 'receive' || $operation eq 'socket') {
        my $recv_failure = $self->_finish_recv_failure($error);
        $failure //= $recv_failure;
    }
    if ($self->is_terminal || $operation eq 'send' || $operation eq 'socket') {
        my $drain_failure = $self->_finish_future_list(
            '_async_drain_waiters', 'fail', $error,
        );
        $failure //= $drain_failure;
    }
    if ($self->is_terminal && !$self->{_async_ready_established}) {
        my $ready_failure = $self->_finish_future_list(
            '_async_ready_waiters', 'fail', $error,
        );
        $failure //= $ready_failure;
    }
    return $failure;
}

sub _report ($self, $error) {
    return $self->SUPER::_report($error) if !$self->{_async_initialized};

    local $self->{_async_reporting_error} = 1;
    my $core_failure;
    my $ok = eval { $self->SUPER::_report($error); 1 };
    $core_failure = $@ if !$ok;

    my $async_failure = $self->_route_reported_error($error);
    die $core_failure if defined($core_failure) && length($core_failure);
    die $async_failure if defined($async_failure) && length($async_failure);
    return;
}

sub _shutdown ($self, $state, $fire_close = 1, $retain_loop = 0) {
    return $self->SUPER::_shutdown($state, $fire_close, $retain_loop)
        if !$self->{_async_initialized};

    # Core failure reporting calls _shutdown('failed') before _report(). Delay
    # Awaitable failure in that case so an application on_error callback keeps
    # its normal ordering before suspended coroutines resume. The same applies
    # to an explicit close performed reentrantly from on_error.
    my $defer = $state eq 'failed' || $self->{_async_reporting_error};

    my $core_failure;
    my $ok = eval {
        $self->SUPER::_shutdown($state, $fire_close, $retain_loop);
        1;
    };
    $core_failure = $@ if !$ok;

    my $async_failure;
    if (!$defer) {
        if (!$self->{_async_ready_established}) {
            $async_failure = $self->_finish_future_list(
                '_async_ready_waiters', 'fail', $self->_ready_failure,
            );
        }
        my $recv_failure = $self->_finish_recv_failure($self->_recv_failure);
        $async_failure //= $recv_failure;
        my $drain_failure = $self->_finish_future_list(
            '_async_drain_waiters', 'fail', $self->_drain_failure,
        );
        $async_failure //= $drain_failure;
    }

    die $core_failure if defined($core_failure) && length($core_failure);
    die $async_failure if defined($async_failure) && length($async_failure);
    return;
}

sub CLONE_SKIP ($class) { 1 }

package Linux::Event::Async::Dgram::Awaitable;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

sub _socket ($self) {
    return $self->{socket}
        // croak 'Linux::Event::Async Datagram socket is no longer available';
}

sub _clear_callbacks ($self) {
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return;
}

sub _arm ($self) {
    $self->{state} = 'pending';
    $self->{payload} = undef;
    $self->{peer} = undef;
    $self->{failure} = undef;
    $self->_clear_callbacks;
    return $self;
}

sub _arm_done ($self, $payload, $peer) {
    $self->{state} = 'done';
    $self->{payload} = $payload;
    $self->{peer} = $peer;
    $self->{failure} = undef;
    $self->_clear_callbacks;
    return $self;
}

sub _arm_failed ($self, $error) {
    $self->{state} = 'failed';
    $self->{payload} = undef;
    $self->{peer} = undef;
    $self->{failure} = $error;
    $self->_clear_callbacks;
    return $self;
}

sub _call_callbacks ($callbacks) {
    my $failure;
    for my $callback (@$callbacks) {
        my $ok = eval { $callback->(); 1 };
        $failure //= $@ if !$ok;
    }
    return $failure;
}

sub _complete_done ($self, $payload, $peer) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'done';
    $self->{payload} = $payload;
    $self->{peer} = $peer;
    $self->{failure} = undef;
    my $callbacks = $self->{on_ready};
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return _call_callbacks($callbacks);
}

sub _complete_failed ($self, $error) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'failed';
    $self->{payload} = undef;
    $self->{peer} = undef;
    $self->{failure} = $error;
    my $callbacks = $self->{on_ready};
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return _call_callbacks($callbacks);
}

sub AWAIT_IS_READY ($self) {
    return $self->{state} eq 'done' || $self->{state} eq 'failed'
        || $self->{state} eq 'cancelled';
}

sub AWAIT_IS_CANCELLED ($self) {
    return $self->{state} eq 'cancelled';
}

sub AWAIT_GET ($self) {
    my $state = $self->{state};
    croak 'cannot get a cancelled datagram receive' if $state eq 'cancelled';
    croak 'AWAIT_GET called while datagram receive is pending'
        if $state eq 'pending';
    croak 'AWAIT_GET called without an armed datagram receive'
        if $state eq 'idle';

    my $payload = $self->{payload};
    my $peer = $self->{peer};
    my $failure = $self->{failure};
    $self->{payload} = undef;
    $self->{peer} = undef;
    $self->{failure} = undef;
    $self->{state} = 'idle';

    die $failure if $state eq 'failed';
    return wantarray ? ($payload, $peer) : $payload;
}

sub AWAIT_ON_READY ($self, $callback) {
    croak 'AWAIT_ON_READY requires a coderef' if ref($callback) ne 'CODE';
    if ($self->AWAIT_IS_READY) {
        $callback->();
    } elsif ($self->{state} eq 'pending') {
        push @{ $self->{on_ready} }, $callback;
    } else {
        croak 'AWAIT_ON_READY called without an armed datagram receive';
    }
    return;
}

sub AWAIT_ON_CANCEL ($self, $callback) {
    croak 'AWAIT_ON_CANCEL requires a coderef' if ref($callback) ne 'CODE';
    if ($self->{state} eq 'cancelled') {
        $callback->();
    } elsif ($self->{state} eq 'pending') {
        push @{ $self->{on_cancel} }, $callback;
    }
    return;
}

sub AWAIT_CHAIN_CANCEL ($self, $other) {
    $self->AWAIT_ON_CANCEL(sub {
        $other->cancel if $other && $other->can('cancel');
    });
    return $self;
}

sub AWAIT_CLONE ($self) {
    return Linux::Event::Async::Future->new(loop => $self->_socket->loop);
}

sub AWAIT_LOOP ($self) {
    return $self->_socket->loop;
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_socket->loop;
    croak 'cannot wait on a Datagram socket that is not attached to a loop'
        if !$loop && !$self->AWAIT_IS_READY;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

sub cancel ($self) {
    return $self if $self->{state} ne 'pending';
    my $socket = $self->_socket;

    $socket->{_async_recv_armed} = 0;
    $socket->pause_read if $socket->is_active && !$socket->is_read_paused;
    $self->{state} = 'cancelled';
    $self->{payload} = undef;
    $self->{peer} = undef;
    $self->{failure} = undef;

    my $cancel_callbacks = $self->{on_cancel};
    my $ready_callbacks = $self->{on_ready};
    $self->{on_cancel} = [];
    $self->{on_ready} = [];

    my $failure = _call_callbacks($cancel_callbacks);
    my $ready_failure = _call_callbacks($ready_callbacks);
    $failure //= $ready_failure;
    die $failure if defined($failure) && length($failure);
    return $self;
}

package Linux::Event::Async::Dgram;

1;

__END__

=head1 NAME

Linux::Event::Async::Dgram - awaitable Linux SOCK_DGRAM I/O

=head1 SYNOPSIS

  package PacketSocket;
  use parent 'Linux::Event::Async::Dgram';

  sub datagram_options ($class) {
      return (
          max_datagram_size => 65_535,
          receive_buffer    => 1_048_576,
      );
  }

  package main;
  use Linux::Event::Async;
  use Linux::Event::Loop;

  my $loop = Linux::Event::Loop->new;
  my $socket = PacketSocket->new(
      loop => $loop,
      host => '127.0.0.1',
      port => 9999,
  );

  async sub receive_loop ($socket) {
      await $socket->ready;
      while (1) {
          my ($payload, $peer) = await $socket->recv;
          my $ok = $socket->send($payload, to => $peer);
          await $socket->drain if defined($ok) && !$ok;
      }
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Dgram> adapts
L<Linux::Event::IO::Sock::Dgram> for Future::AsyncAwait while preserving kernel
packet boundaries and peer addresses. Bound UDP, connected UDP, Unix-domain
datagram sockets, and adopted datagram handles retain the core socket and queue
semantics.

C<recv> is a hot repeated operation and uses one persistent Awaitable per
Datagram socket. C<ready> and output C<drain> are comparatively cold lifecycle
and backpressure transitions and return L<Linux::Event::Async::Future> objects.

=head1 SUBCLASSING AND DATAGRAM TUNING

Subclassing remains the policy boundary. C<datagram_options> is resolved by
Linux::Event and applies to every instance of the class. The complete option set
is:

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

The values shown are the effective Async defaults. Core Linux::Event normally
defaults C<max_datagrams_per_tick> to 256, but Async fixes it to 1 and fixes
C<edge_triggered> to 0 for pull-based receive correctness. Constructor attempts
to select another value are rejected; class policy for the remaining options is
unchanged.

C<max_datagram_size> bounds one received or sent packet. High/low watermarks
provide cooperative output backpressure. C<max_pending_bytes> and
C<max_pending_datagrams> are hard output queue limits when nonzero. Socket policy
options retain the semantics documented by L<Linux::Event::IO::Sock::Dgram>.

=head1 APPLICATION READINESS

  await $socket->ready;

C<ready> resolves with the same Datagram socket when core application readiness
fires. For a connected UDP socket using a hostname, this includes asynchronous
name resolution and socket activation. Bound/adopted sockets become ready on
the normal core ready turn after Loop attachment.

Subclass C<on_ready> remains available and runs before readiness Future waiters
resume. Cancelling one readiness Future cancels only that wait. Failure or close
before readiness fails pending waiters with a L<Linux::Event::Error>.

=head1 RECEIVE

  my ($payload, $peer) = await $socket->recv;

C<recv> arms one receive and returns the socket's persistent Awaitable. In list
context it returns payload and L<Linux::Event::Address> peer. In scalar context
it returns the payload, matching the Future::AsyncAwait rule that scalar
C<AWAIT_GET> yields the first result.

Only one receive may be pending, and a completed result or failure must be
consumed before another receive is armed. Cancelling a receive pauses read
interest but does not close the socket or consume the next packet.

The Async socket is pull-based. Read interest is paused when no C<recv> is
pending. Linux::Event core normally batches multiple C<recvmsg> operations per
readiness turn; a pull Awaitable cannot safely consume the first packet after
core has already removed a tail batch from the kernel queue. Therefore 0.002
uses one level-triggered datagram receive per armed wait. Additional packets
remain in the kernel receive queue.

Oversized packets and receive I/O failures fail the current receive with the
same typed L<Linux::Event::Error> reported by core. Existing subclass C<on_error>
callbacks remain supported and run before the suspended receive resumes.

=head1 SENDING AND DRAIN

Connected sockets use:

  my $ok = $socket->send($payload);

Unconnected sockets use:

  my $ok = $socket->send($payload, to => $peer);

One call remains one atomic datagram. The return value retains Linux::Event's
backpressure meaning. When a send crosses the high watermark it returns false;
Async records that blocked period so application code can write:

  my $ok = $socket->send($payload, to => $peer);
  await $socket->drain if defined($ok) && !$ok;

C<drain> resolves when the blocked output period crosses the configured low
watermark. It does not promise an empty packet queue. C<on_drain> is reserved by
Async Dgram as the bridge from the core transition to drain Futures.

Cancelling a drain Future affects only that waiter. Datagram send or socket
errors fail pending drain waiters after any subclass C<on_error> callback has
run. Core send return values and typed errors remain authoritative for the send
that encountered the error.

=head1 CALLBACK POLICY

C<on_datagram> and C<on_drain> are reserved by this class for Awaitable
operation delivery. Concrete subclasses must not override them.

Subclass C<on_ready>, C<on_error>, C<on_close>, C<configure_socket>, and
C<datagram_options> remain available. Per-instance callback constructor options
are intentionally rejected so the API behaves consistently on the minimum
supported Linux::Event 0.110 as well as current core.

=head1 CANCELLATION AND LIFETIME

Receive cancellation is wait-local. C<cancel_recv> or cancellation propagated
from an awaiting async sub pauses read interest but leaves the socket active.
A later C<recv> may rearm it.

Explicit close/detach fails pending receive, readiness, and drain waits. Core
terminal failures are routed after C<on_error>, preserving callback-before-await
ordering even when C<on_error> closes the socket reentrantly.

=head1 SEE ALSO

L<Linux::Event::IO::Sock::Dgram>, L<Linux::Event::Async>,
L<Linux::Event::Async::Future>, L<Linux::Event::Address>

=cut
