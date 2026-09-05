package Linux::Event::Async::Event;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::Kernel::Event';
use Carp qw(croak);
use Hash::Util::FieldHash qw(fieldhash);
use Scalar::Util qw(refaddr weaken);

use Linux::Event::Async::Future ();
use Linux::Event::Error ();

our $VERSION = '0.002';

fieldhash my %ASYNC_STATE;

sub _owns_event_callback ($class) {
    my $actual = $class->can('on_event');
    my $owned = __PACKAGE__->can('on_event');
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub new ($class, %option) {
    croak "$class must not override on_event(); await wait() is the event sink"
        if !_owns_event_callback($class);
    croak 'new(): on_event is reserved by Linux::Event::Async::Event; use await wait()'
        if exists $option{on_event};

    my $event = $class->SUPER::new(%option);
    my $awaitable = bless {
        event     => $event,
        state     => 'idle',
        result    => undef,
        failure   => undef,
        on_ready  => [],
        on_cancel => [],
    }, 'Linux::Event::Async::Event::Awaitable';
    weaken($awaitable->{event});
    $ASYNC_STATE{$event} = {
        awaitable => $awaitable,
        available => 0,
    };
    return $event;
}

sub _async_state ($self) {
    return $ASYNC_STATE{$self}
        // croak 'Linux::Event::Async::Event state is unavailable in this interpreter';
}

sub _wait_failure ($self, $message = undef) {
    return Linux::Event::Error->new(
        type      => 'event',
        operation => 'event_wait',
        message   => $message // 'Event became terminal before wait completed',
    );
}

sub wait ($self) {
    my $state = $self->_async_state;
    my $awaitable = $state->{awaitable};
    my $await_state = $awaitable->{state};

    croak 'wait(): another Event wait is already pending'
        if $await_state eq 'pending';
    croak 'wait(): previous Event result or failure has not been consumed'
        if $await_state eq 'done' || $await_state eq 'failed';

    if ($state->{available} > 0) {
        my $count = $state->{available};
        $state->{available} = 0;
        $awaitable->_arm_done($count);
        return $awaitable;
    }
    if ($self->is_terminal) {
        $awaitable->_arm_failed($self->_wait_failure);
        return $awaitable;
    }
    croak 'wait(): Event must be attached to a Linux::Event loop'
        if !$self->loop;

    $awaitable->_arm;
    return $awaitable;
}

sub cancel_wait ($self) {
    $self->_async_state->{awaitable}->cancel;
    return $self;
}

sub on_event ($self, $count) {
    my $state = $self->_async_state;
    $count = 1 if !defined($count) || $count < 1;
    my $awaitable = $state->{awaitable};

    if ($awaitable->{state} eq 'pending') {
        my $failure = $awaitable->_complete_done($count);
        die $failure if defined($failure) && length($failure);
    } else {
        $state->{available} += $count;
    }
    return;
}

sub cancel ($self) {
    my $state = $ASYNC_STATE{$self};
    my $pending = $state && $state->{awaitable}{state} eq 'pending';

    my $core_failure;
    my $ok = eval { $self->SUPER::cancel; 1 };
    $core_failure = $@ if !$ok;

    my $future_failure;
    if ($state) {
        $state->{available} = 0;
        if ($pending && $state->{awaitable}{state} eq 'pending') {
            $future_failure = $state->{awaitable}->_complete_failed(
                $self->_wait_failure('Event was cancelled before wait completed'),
            );
        }
    }

    die $core_failure if defined($core_failure) && length($core_failure);
    die $future_failure if defined($future_failure) && length($future_failure);
    return $self;
}

sub CLONE ($class) {
    %ASYNC_STATE = ();
    return;
}

package Linux::Event::Async::Event::Awaitable;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

sub _event ($self) {
    return $self->{event}
        // croak 'Linux::Event::Async Event is no longer available';
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

sub _arm_done ($self, $count) {
    $self->{state} = 'done';
    $self->{result} = $count;
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

sub _complete_done ($self, $count) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'done';
    $self->{result} = $count;
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
    croak 'cannot get a cancelled Event wait' if $state eq 'cancelled';
    croak 'AWAIT_GET called while Event wait is pending' if $state eq 'pending';
    croak 'AWAIT_GET called without an armed Event wait' if $state eq 'idle';

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
        croak 'AWAIT_ON_READY called without an armed Event wait';
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
    return Linux::Event::Async::Future->new(loop => $self->_event->loop);
}

sub AWAIT_LOOP ($self) {
    return $self->_event->loop;
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_event->loop;
    croak 'cannot wait on an Event that is not attached to a loop'
        if !$loop && !$self->AWAIT_IS_READY;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

sub cancel ($self) {
    return $self if $self->{state} ne 'pending';
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

package Linux::Event::Async::Event;

1;

__END__

=head1 NAME

Linux::Event::Async::Event - awaitable Linux eventfd notification

=head1 SYNOPSIS

  use Linux::Event::Async;
  use Linux::Event::Async::Event;
  use Linux::Event::Loop;

  my $loop = Linux::Event::Loop->new;
  my $event = Linux::Event::Async::Event->new(loop => $loop);

  # A producer publishes its real payload first, then wakes the Loop.
  $event->signal;

  async sub consume_notification ($event) {
      my $count = await $event->wait;
      return $count;
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Event> adapts the public eventfd-backed
L<Linux::Event::Kernel::Event> primitive to Future::AsyncAwait. Linux::Event
continues to own the eventfd descriptor, atomic signaling, Loop registration,
thread/fork handle rules, and terminal cancellation.

Event delivery is a repeated operation, so each Async Event owns one persistent
Awaitable. Repeated C<wait> calls therefore avoid allocating one Future or
Awaitable per eventfd drain.

The eventfd carries notification count only. Application payloads still belong
in a queue, shared native structure, pipe, socket, or other appropriate IPC
channel and must be published before C<signal>.

=head1 WAIT

  my $count = await $event->wait;

C<wait> returns the persistent Awaitable and resolves with the eventfd counter
value represented by the notification. Only one wait may be pending at a time,
and a completed result or failure must be consumed before another wait is
armed.

=head1 IDLE DELIVERY AND BOUNDS

Linux::Event drains the eventfd when its Loop dispatches it even if no Async
wait is armed. Async therefore retains the delivered count in one scalar until
the next C<wait>.

Additional idle callbacks add their eventfd counts to that scalar. This is
constant-shape state rather than an event queue: no per-notification object or
list entry is allocated. The next C<wait> consumes the accumulated count in one
result.

As in core, C<$count> is notification accounting rather than an application
payload count. Producers that need one-to-one work identity should use their
payload queue as the source of truth.

=head1 SIGNALING

C<signal> and C<signal($increment)> retain the core eventfd semantics and return
the Event object. Signaling never resumes a coroutine inline; the owning Loop
must drain the eventfd before C<wait> completes.

A detached Event may be signaled before attachment. The counter remains in the
kernel eventfd until the Event is attached and the Loop dispatches it.

=head1 CANCELLATION

C<cancel_wait> cancels only the currently pending suspension. The Event remains
active and may be signaled or waited again. A later eventfd delivery is still
available to a later C<wait>.

Cancelling an async-sub Future while it is awaiting C<wait> propagates to this
wait-local behavior.

Core C<$event-E<gt>cancel> remains terminal. It closes the eventfd and Loop
registration, discards an unconsumed Async count, and fails a currently pending
wait with a typed C<event>/C<event_wait> error.

=head1 CALLBACK EXCLUSIVITY

C<on_event> is reserved by C<Linux::Event::Async::Event> as the bridge from core
eventfd delivery to the persistent Awaitable. Concrete Async Event subclasses
must not override it, and constructor C<on_event> callbacks are not accepted.
Application behavior belongs after C<< await $event->wait >>.

=head1 THREAD AND FORK BOUNDARY

The core signaling boundary is retained. On an ithread-enabled Perl, a cloned
Event handle may still call C<signal> but cannot manage the owning Loop or Async
wait state. Async clears coroutine state in the cloned interpreter; C<wait>,
C<cancel_wait>, data access, and terminal resource management remain owner-side
operations.

A forked child may signal the inherited eventfd until exec, as documented by
core. Payload transfer still requires real IPC or shared storage.

=head1 AWAITABLE LIFETIME

Owner-side Async state is associated with the Event through a field hash rather
than its internal representation. The Event owns the persistent Awaitable and
the Awaitable holds only a weak reference back to the Event.

=head1 REQUIREMENTS

This implementation works with Linux::Event 0.110 or newer and requires no new
core ABI.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Future>,
L<Linux::Event::Kernel::Event>, L<Linux::Event::Loop>

=cut
