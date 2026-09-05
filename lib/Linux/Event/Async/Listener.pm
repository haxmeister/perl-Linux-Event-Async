package Linux::Event::Async::Listener;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::IO::Sock::Listener';
use Carp qw(croak);
use Scalar::Util qw(refaddr weaken);

use Linux::Event::Async::Future ();
use Linux::Event::Error ();

our $VERSION = '0.002';

sub _reserved_callback ($class, $name) {
    my $actual = $class->can($name);
    my $owned = __PACKAGE__->can($name);
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub new ($class, %option) {
    croak 'new(): must be called as a class method' if ref $class;
    croak "$class must not override on_accept(); await accept() is the acceptance sink"
        if !_reserved_callback($class, 'on_accept');
    croak "$class must not override on_error(); accept errors are delivered through await accept()"
        if !_reserved_callback($class, 'on_error');

    if (exists $option{max_accept_per_tick}) {
        my $maximum = $option{max_accept_per_tick};
        croak 'new(): Linux::Event::Async::Listener requires max_accept_per_tick => 1'
            if !defined($maximum) || ref($maximum) || "$maximum" ne '1';
    }
    if (exists $option{edge_triggered}) {
        my $edge = $option{edge_triggered};
        croak 'new(): Linux::Event::Async::Listener does not support edge_triggered acceptance'
            if !defined($edge) || ref($edge) || "$edge" ne '0';
    }
    $option{max_accept_per_tick} = 1;
    $option{edge_triggered} = 0;

    my $self = $class->SUPER::new(%option);
    my $awaitable = bless {
        listener  => $self,
        state     => 'idle',
        result    => undef,
        failure   => undef,
        on_ready  => [],
        on_cancel => [],
    }, 'Linux::Event::Async::Listener::Awaitable';
    weaken($awaitable->{listener});
    $self->{_async_accept_awaitable} = $awaitable;
    return $self;
}

sub _attach_to_loop ($self, $loop) {
    $self->SUPER::_attach_to_loop($loop);
    $self->pause if !$self->{_async_accept_armed};
    return $self;
}

sub _accept_closed_error ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'event',
        operation => 'accept',
        message   => 'Listener closed before accept completed',
    );
}

sub accept ($self) {
    my $awaitable = $self->{_async_accept_awaitable}
        // croak 'accept(): Listener construction is incomplete';
    my $state = $awaitable->{state};

    croak 'accept(): previous accepted Stream or failure has not been consumed'
        if $state eq 'done' || $state eq 'failed';
    croak 'accept(): another accept is already pending'
        if $state eq 'pending';

    if ($self->is_terminal) {
        $awaitable->_arm_failed($self->_accept_closed_error);
        return $awaitable;
    }
    croak 'accept(): Listener must be attached to a Linux::Event loop'
        if !$self->loop;

    $awaitable->_arm;
    $self->{_async_accept_armed} = 1;
    $self->resume if $self->is_paused;
    return $awaitable;
}

sub cancel_accept ($self) {
    my $awaitable = $self->{_async_accept_awaitable};
    $awaitable->cancel if $awaitable;
    return $self;
}

sub on_accept ($self, $stream) {
    my $awaitable = $self->{_async_accept_awaitable};
    if (!$awaitable || $awaitable->{state} ne 'pending'
        || !$self->{_async_accept_armed}) {
        # This should be unreachable because Async Listener is paused whenever
        # no accept is armed. Do not silently retain an unowned connection if a
        # core or application misuse violates that invariant.
        $stream->close;
        $self->pause if $self->is_running;
        return;
    }

    $self->{_async_accept_armed} = 0;
    my $failure = $awaitable->_complete_done($stream);

    # Future::AsyncAwait resumes synchronously from the ready callback. If the
    # coroutine immediately arms the next accept, it sets _async_accept_armed
    # again before control returns here and acceptance remains enabled.
    $self->pause if $self->is_running && !$self->{_async_accept_armed};
    die $failure if defined($failure) && length($failure);
    return;
}

sub on_error ($self, $error) {
    my $awaitable = $self->{_async_accept_awaitable};
    my $failure;
    if ($awaitable && $awaitable->{state} eq 'pending') {
        $self->{_async_accept_armed} = 0;
        $failure = $awaitable->_complete_failed($error);
    }
    $self->pause if $self->is_running && !$self->{_async_accept_armed};
    die $failure if defined($failure) && length($failure);
    return;
}

sub _shutdown ($self, $state, $retain_loop = 0) {
    my $awaitable = $self->{_async_accept_awaitable};
    my $pending = $awaitable && $awaitable->{state} eq 'pending';
    my $error = $pending ? $self->_accept_closed_error : undef;

    my $core_failure;
    my $ok = eval { $self->SUPER::_shutdown($state, $retain_loop); 1 };
    $core_failure = $@ if !$ok;

    my $future_failure;
    if ($pending && $awaitable->{state} eq 'pending') {
        $self->{_async_accept_armed} = 0;
        $future_failure = $awaitable->_complete_failed($error);
    }

    die $core_failure if defined($core_failure) && length($core_failure);
    die $future_failure if defined($future_failure) && length($future_failure);
    return;
}

sub CLONE_SKIP ($class) { 1 }

package Linux::Event::Async::Listener::Awaitable;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

sub _listener ($self) {
    return $self->{listener}
        // croak 'Linux::Event::Async Listener is no longer available';
}

sub _clear_callbacks ($self) {
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return;
}

sub _arm ($self) {
    $self->{state} = 'pending';
    $self->{result} = undef;
    $self->{failure} = undef;
    $self->_clear_callbacks;
    return $self;
}

sub _arm_failed ($self, $error) {
    $self->{state} = 'failed';
    $self->{result} = undef;
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

sub _complete_done ($self, $stream) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'done';
    $self->{result} = $stream;
    $self->{failure} = undef;
    my $callbacks = $self->{on_ready};
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return _call_callbacks($callbacks);
}

sub _complete_failed ($self, $error) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'failed';
    $self->{result} = undef;
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
    croak 'cannot get a cancelled accept' if $state eq 'cancelled';
    croak 'AWAIT_GET called while accept is pending' if $state eq 'pending';
    croak 'AWAIT_GET called without an armed accept' if $state eq 'idle';

    my $result = $self->{result};
    my $failure = $self->{failure};
    $self->{result} = undef;
    $self->{failure} = undef;
    $self->{state} = 'idle';

    die $failure if $state eq 'failed';
    return $result;
}

sub AWAIT_ON_READY ($self, $callback) {
    croak 'AWAIT_ON_READY requires a coderef' if ref($callback) ne 'CODE';
    if ($self->AWAIT_IS_READY) {
        $callback->();
    } elsif ($self->{state} eq 'pending') {
        push @{ $self->{on_ready} }, $callback;
    } else {
        croak 'AWAIT_ON_READY called without an armed accept';
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
    return Linux::Event::Async::Future->new(loop => $self->_listener->loop);
}

sub AWAIT_LOOP ($self) {
    return $self->_listener->loop;
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_listener->loop;
    croak 'cannot wait on a Listener that is not attached to a loop'
        if !$loop && !$self->AWAIT_IS_READY;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

sub cancel ($self) {
    return $self if $self->{state} ne 'pending';
    my $listener = $self->_listener;

    $listener->{_async_accept_armed} = 0;
    $listener->pause if $listener->is_running;
    $self->{state} = 'cancelled';
    $self->{result} = undef;
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

package Linux::Event::Async::Listener;

1;

__END__

=head1 NAME

Linux::Event::Async::Listener - awaitable Linux SOCK_STREAM listener

=head1 SYNOPSIS

  package LineStream;
  use parent 'Linux::Event::Async::Stream';
  use Linux::Event::Framer 'Delimiter', "\n";

  package main;
  use Linux::Event::Async;
  use Linux::Event::Async::Listener;
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
          # Start per-connection work here.
      }
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Listener> is the coroutine-facing specialization of
L<Linux::Event::IO::Sock::Listener>. C<accept> waits for one incoming connection
and returns the configured C<stream_class> object. The listener owns one
persistent Awaitable view, so a normal accept loop does not allocate one Future
or one Awaitable per accepted connection.

The core Listener still owns bind/listen/accept4, socket policy, accepted Stream
construction, address handling, and lifecycle. Async changes how accepted
Streams are consumed, not how listening sockets are implemented.

=head1 STREAM CLASS, SUBCLASSING, TLS, AND TUNING

The configured C<stream_class> remains the important protocol policy boundary.
Using a L<Linux::Event::Async::Stream> subclass lets every accepted connection
share cached native framing, TLS server identity and verification policy,
socket policy, Stream tuning, and lifecycle behavior:

  package SecureProtocol;
  use parent 'Linux::Event::Async::Stream';
  use Linux::Event::Framer 'U32BE';
  use Linux::Event::TLS
      cert_file => '/path/server.crt',
      key_file  => '/path/server.key';

  sub stream_options ($class) {
      return (
          read_size         => 65_536,
          read_budget_bytes => 262_144,
          max_buffer        => 8_388_608,
      );
  }

C<Linux::Event::Async::Listener> accepts the same bind, socket, ownership, and
backlog options as the core Listener except for the acceptance-dispatch controls
described below.

=head1 ACCEPT

=head2 accept

  my $stream = await $listener->accept;

Arms one accept and returns the Listener's persistent Awaitable. Only one accept
may be pending at a time. The accepted result must be consumed before another
accept is armed.

The returned value is the exact configured Stream subclass created by
Linux::Event. No generic socket wrapper is introduced.

For a plain accepted Stream, core Listener C<on_accept> processing completes
before the Stream's normal application-readiness transition. The Async Listener
uses that C<on_accept> slot to deliver the Stream to C<accept>. A coroutine that
needs application readiness can immediately write:

  my $stream = await $listener->accept;
  await $stream->ready;

For a TLS Stream, C<accept> similarly returns the constructed Stream before its
TLS handshake finishes; C<< await $stream->ready >> waits through TLS handshake
and verification.

=head1 PULL-BASED ACCEPTANCE AND BACKPRESSURE

The Async Listener is paused whenever no C<accept> is pending. Arming C<accept>
resumes the core Listener; successful delivery, failure, or cancellation pauses
it again unless the resumed coroutine synchronously arms the next accept.

This keeps connection backlog in the kernel rather than building an unbounded
queue of accepted Stream objects when application code is not awaiting new
connections.

Linux::Event core normally supports batched C<accept4>. A pull-style Awaitable
must not allow core to accept a whole batch and then discard already accepted
connections when the first Awaitable completes. Therefore Async Listener fixes
C<max_accept_per_tick> to 1 and uses level-triggered acceptance. Supplying any
other C<max_accept_per_tick> or enabling C<edge_triggered> is rejected.

This is a correctness-first release contract. A later native accept provider may
add bounded accept prefetch if measurement shows that one accept per readiness
turn is materially limiting.

=head1 CANCELLATION

=head2 cancel_accept

  $listener->cancel_accept;

Cancels a pending accept without closing the listening socket. The Listener is
paused until another C<accept> is armed.

Cancelling an async-sub Future while it is awaiting C<accept> propagates to the
persistent accept Awaitable with the same behavior. A new accept may be armed
after cancellation.

=head1 ERRORS AND CLOSE

Accept and Listener failures complete the pending Awaitable with the
L<Linux::Event::Error> reported by the core Listener. Explicit close or detach
while an accept is pending completes it with an C<event>/C<accept> error when no
more specific core error exists.

C<on_accept> and C<on_error> are reserved by this subclass as the bridge from
core Listener callbacks to the persistent Awaitable. Concrete Async Listener
subclasses must not override them. Application acceptance and error handling
belong around C<< await $listener->accept >> instead.

=head1 ATTACHMENT AND MANUAL PAUSE

Construct with C<loop =E<gt> $loop> or add the Listener to a Loop before calling
C<accept>. An attached Async Listener begins paused and is resumed automatically
by C<accept>.

Because pause/resume are used internally to implement pull-based acceptance,
application code should normally not call them directly on an Async Listener.
C<close> and C<detach> retain their core meanings.

=head1 AWAITABLE LIFETIME

The object returned by C<accept> is a persistent internal view. The Listener
owns that view and the view holds only a weak reference back to the Listener, so
the pair does not create an ownership cycle. Applications should await the
object or use the documented Future::AsyncAwait Awaitable protocol rather than
constructing it directly.

=head1 REQUIREMENTS

This implementation works with Linux::Event 0.110 or newer and does not require
a new core ABI.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Stream>,
L<Linux::Event::Async::Future>, L<Linux::Event::IO::Sock::Listener>,
L<Linux::Event::IO::Sock::Stream>, L<Linux::Event::Error>

=cut
