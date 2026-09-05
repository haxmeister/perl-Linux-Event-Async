package Linux::Event::Async::Signal;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::Kernel::Signal';
use Carp qw(croak);
use Hash::Util::FieldHash qw(fieldhash);
use Scalar::Util qw(refaddr weaken);

use Linux::Event::Async::Future ();
use Linux::Event::Error ();

our $VERSION = '0.002';

fieldhash my %ASYNC_STATE;

sub _owns_signal_callback ($class) {
    my $actual = $class->can('on_signal');
    my $owned = __PACKAGE__->can('on_signal');
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub new ($class, %option) {
    croak "$class must not override on_signal(); await wait() is the signal sink"
        if !_owns_signal_callback($class);
    croak 'new(): on_signal is reserved by Linux::Event::Async::Signal; use await wait()'
        if exists $option{on_signal};

    my $signal = $class->SUPER::new(%option);
    my $awaitable = bless {
        signal    => $signal,
        state     => 'idle',
        number    => undef,
        count     => undef,
        failure   => undef,
        on_ready  => [],
        on_cancel => [],
    }, 'Linux::Event::Async::Signal::Awaitable';
    weaken($awaitable->{signal});
    $ASYNC_STATE{$signal} = {
        awaitable      => $awaitable,
        pending_counts => {},
        pending_order  => [],
    };
    return $signal;
}

sub _async_state ($self) {
    return $ASYNC_STATE{$self}
        // croak 'Linux::Event::Async::Signal state is no longer available';
}

sub _wait_failure ($self, $message = undef) {
    return Linux::Event::Error->new(
        type      => 'event',
        operation => 'signal_wait',
        message   => $message // 'Signal subscription became terminal before wait completed',
    );
}

sub _take_available ($state) {
    my $number = shift @{ $state->{pending_order} };
    return if !defined $number;
    my $count = delete $state->{pending_counts}{$number};
    return ($number, $count);
}

sub wait ($self) {
    my $state = $self->_async_state;
    my $awaitable = $state->{awaitable};
    my $await_state = $awaitable->{state};

    croak 'wait(): another Signal wait is already pending'
        if $await_state eq 'pending';
    croak 'wait(): previous Signal result or failure has not been consumed'
        if $await_state eq 'done' || $await_state eq 'failed';

    if ($self->is_terminal) {
        $awaitable->_arm_failed($self->_wait_failure);
        return $awaitable;
    }
    if (@{ $state->{pending_order} }) {
        my ($number, $count) = _take_available($state);
        $awaitable->_arm_done($number, $count);
        return $awaitable;
    }
    croak 'wait(): Signal must be attached to a Linux::Event loop'
        if !$self->loop;

    $awaitable->_arm;
    return $awaitable;
}

sub cancel_wait ($self) {
    $self->_async_state->{awaitable}->cancel;
    return $self;
}

sub on_signal ($self, $number, $count) {
    my $state = $self->_async_state;
    $count = 1 if !defined($count) || $count < 1;
    my $awaitable = $state->{awaitable};

    if ($awaitable->{state} eq 'pending') {
        my $failure = $awaitable->_complete_done($number, $count);
        die $failure if defined($failure) && length($failure);
        return;
    }

    if (!exists $state->{pending_counts}{$number}) {
        push @{ $state->{pending_order} }, $number;
        $state->{pending_counts}{$number} = 0;
    }
    $state->{pending_counts}{$number} += $count;
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
        $state->{pending_counts} = {};
        $state->{pending_order} = [];
        if ($pending && $state->{awaitable}{state} eq 'pending') {
            $future_failure = $state->{awaitable}->_complete_failed(
                $self->_wait_failure('Signal subscription was cancelled before wait completed'),
            );
        }
    }

    die $core_failure if defined($core_failure) && length($core_failure);
    die $future_failure if defined($future_failure) && length($future_failure);
    return $self;
}

sub CLONE ($class) {
    %ASYNC_STATE = ();
    $class->SUPER::CLONE if $class->SUPER::can('CLONE');
    return;
}

sub CLONE_SKIP ($class) { 1 }

package Linux::Event::Async::Signal::Awaitable;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

sub _signal ($self) {
    return $self->{signal}
        // croak 'Linux::Event::Async Signal is no longer available';
}

sub _clear_callbacks ($self) {
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return;
}

sub _arm ($self) {
    $self->{state} = 'pending';
    $self->{number} = undef;
    $self->{count} = undef;
    $self->{failure} = undef;
    $self->_clear_callbacks;
    return $self;
}

sub _arm_done ($self, $number, $count) {
    $self->{state} = 'done';
    $self->{number} = $number;
    $self->{count} = $count;
    $self->{failure} = undef;
    $self->_clear_callbacks;
    return $self;
}

sub _arm_failed ($self, $error) {
    $self->{state} = 'failed';
    $self->{number} = undef;
    $self->{count} = undef;
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

sub _complete_done ($self, $number, $count) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'done';
    $self->{number} = $number;
    $self->{count} = $count;
    $self->{failure} = undef;
    my $callbacks = $self->{on_ready};
    $self->{on_ready} = [];
    $self->{on_cancel} = [];
    return _call_callbacks($callbacks);
}

sub _complete_failed ($self, $error) {
    return if $self->{state} ne 'pending';
    $self->{state} = 'failed';
    $self->{number} = undef;
    $self->{count} = undef;
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
    croak 'cannot get a cancelled Signal wait' if $state eq 'cancelled';
    croak 'AWAIT_GET called while Signal wait is pending' if $state eq 'pending';
    croak 'AWAIT_GET called without an armed Signal wait' if $state eq 'idle';

    my $number = $self->{number};
    my $count = $self->{count};
    my $failure = $self->{failure};
    $self->{number} = undef;
    $self->{count} = undef;
    $self->{failure} = undef;
    $self->{state} = 'idle';

    die $failure if $state eq 'failed';
    return wantarray ? ($number, $count) : $number;
}

sub AWAIT_ON_READY ($self, $callback) {
    croak 'AWAIT_ON_READY requires a coderef' if ref($callback) ne 'CODE';
    if ($self->AWAIT_IS_READY) {
        $callback->();
    } elsif ($self->{state} eq 'pending') {
        push @{ $self->{on_ready} }, $callback;
    } else {
        croak 'AWAIT_ON_READY called without an armed Signal wait';
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
    return Linux::Event::Async::Future->new(loop => $self->_signal->loop);
}

sub AWAIT_LOOP ($self) {
    return $self->_signal->loop;
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_signal->loop;
    croak 'cannot wait on a Signal that is not attached to a loop'
        if !$loop && !$self->AWAIT_IS_READY;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

sub cancel ($self) {
    return $self if $self->{state} ne 'pending';
    $self->{state} = 'cancelled';
    $self->{number} = undef;
    $self->{count} = undef;
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

package Linux::Event::Async::Signal;

1;

__END__

=head1 NAME

Linux::Event::Async::Signal - awaitable Linux signalfd delivery

=head1 SYNOPSIS

  use Linux::Event::Async;
  use Linux::Event::Async::Signal;
  use Linux::Event::Loop;
  use POSIX qw(SIGINT SIGTERM);

  my $loop = Linux::Event::Loop->new;
  my $signal = Linux::Event::Async::Signal->new(
      loop    => $loop,
      signals => [SIGINT, SIGTERM],
  );

  async sub shutdown_wait ($signal) {
      my ($number, $count) = await $signal->wait;
      return ($number, $count);
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Signal> adapts the public
L<Linux::Event::Kernel::Signal> signalfd subscription to Future::AsyncAwait.
Linux::Event continues to own signal masking, the per-Loop shared signalfd,
native fan-out, record aggregation, and subscription lifecycle.

Signal delivery is a repeated operation, so each Async Signal owns one
persistent Awaitable. C<wait> therefore avoids allocating one Future or
Awaitable per delivered signal event.

=head1 WAIT

  my ($number, $count) = await $signal->wait;

C<wait> returns the persistent Awaitable. List context yields the numeric signal
and the aggregated signalfd record count. Scalar context yields the signal
number, matching the first-result convention used by other Async multi-result
operations.

Only one wait may be pending at a time, and a completed result or failure must
be consumed before another wait is armed.

=head1 IDLE DELIVERY AND BOUNDS

A Signal subscription cannot pause only its own delivery while remaining in the
shared signalfd fan-out. Linux::Event may therefore invoke C<on_signal> while no
Async wait is armed.

Async does not drop those notifications and does not build an unbounded event
queue. Instead it retains at most one pending entry per subscribed signal number
and accumulates the delivered count into that entry. The first observed pending
signal-number order is retained until those entries are consumed.

This bound is fixed by the subscription's C<signals> set. Ordinary signals may
already coalesce in the kernel before signalfd observes them; real-time signals
remain queued records and their delivered counts are accumulated by Async while
idle.

=head1 CANCELLATION

C<cancel_wait> cancels only the currently pending suspension. The Signal
subscription remains active, so later notifications continue to be coalesced
and a later C<wait> may consume them.

Cancelling an async-sub Future while it is awaiting C<wait> propagates to this
wait-local behavior.

Core C<$signal-E<gt>cancel> remains terminal. It removes the subscription,
restores mask ownership as appropriate, discards any unconsumed Async pending
counts, and fails a currently pending wait with a typed
C<event>/C<signal_wait> error.

=head1 CALLBACK EXCLUSIVITY

C<on_signal> is reserved by C<Linux::Event::Async::Signal> as the bridge from
core signalfd delivery to the persistent Awaitable. Concrete Async Signal
subclasses must not override it, and constructor C<on_signal> callbacks are not
accepted.

Signal behavior belongs after C<< await $signal->wait >>. C<data>, C<signals>,
and the normal core lifecycle accessors remain available.

=head1 SIGNAL MASK OWNERSHIP

All Linux::Event signal-mask rules remain unchanged. Subscribed signals are
consumed through signalfd rather than Perl C<%SIG> handlers. A signal number may
belong to only one Linux::Event Loop in a process. Signal masks are per-thread;
attach subscriptions before creating application threads or explicitly arrange
equivalent blocking in those threads.

=head1 AWAITABLE LIFETIME

Async state is associated with the native Signal through a field hash, so this
module does not depend on the Signal object's internal representation. The
Signal owns the persistent Awaitable and the Awaitable holds only a weak
reference back to the Signal.

=head1 REQUIREMENTS

This implementation works with Linux::Event 0.110 or newer and requires no new
core ABI.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Future>,
L<Linux::Event::Kernel::Signal>, L<Linux::Event::Loop>

=cut
