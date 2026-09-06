package Linux::Event::Async::Timer;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::Kernel::Timer';
use Carp qw(croak);
use Hash::Util::FieldHash qw(fieldhash);
use Scalar::Util qw(refaddr weaken);

use Linux::Event::Async::Future ();
use Linux::Event::Error ();

our $VERSION = '0.002';

fieldhash my %ASYNC_STATE;

sub _owns_timer_callback ($class) {
    my $actual = $class->can('on_timer');
    my $owned = __PACKAGE__->can('on_timer');
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub new ($class, %option) {
    croak "$class must not override on_timer(); await wait() is the timer sink"
        if !_owns_timer_callback($class);
    croak 'new(): on_timer is reserved by Linux::Event::Async::Timer; use await wait()'
        if exists $option{on_timer};

    my $timer = $class->SUPER::new(%option);
    my $awaitable = bless {
        timer     => $timer,
        state     => 'idle',
        result    => undef,
        failure   => undef,
        on_ready  => [],
        on_cancel => [],
    }, 'Linux::Event::Async::Timer::Awaitable';
    weaken($awaitable->{timer});
    $ASYNC_STATE{$timer} = {
        awaitable      => $awaitable,
        available      => 0,
        one_shot_fired => 0,
    };
    return $timer;
}

sub _async_state ($self) {
    return $ASYNC_STATE{$self}
        // croak 'Linux::Event::Async::Timer state is no longer available';
}

sub _wait_failure ($self, $message = undef) {
    return Linux::Event::Error->new(
        type      => 'event',
        operation => 'timer_wait',
        message   => $message // 'Timer became terminal before wait completed',
    );
}

sub wait ($self) {
    my $state = $self->_async_state;
    my $awaitable = $state->{awaitable};
    my $await_state = $awaitable->{state};

    croak 'wait(): another Timer wait is already pending'
        if $await_state eq 'pending';
    croak 'wait(): previous Timer result or failure has not been consumed'
        if $await_state eq 'done' || $await_state eq 'failed';

    if ($state->{available} > 0) {
        my $count = $state->{available};
        $state->{available} = 0;
        $awaitable->_arm_done($count);
        return $awaitable;
    }
    if ($state->{one_shot_fired} || $self->is_terminal) {
        $awaitable->_arm_failed($self->_wait_failure);
        return $awaitable;
    }
    croak 'wait(): Timer must be attached to a Linux::Event loop'
        if !$self->loop;

    $awaitable->_arm;
    return $awaitable;
}

sub cancel_wait ($self) {
    $self->_async_state->{awaitable}->cancel;
    return $self;
}

sub on_timer ($self) {
    my $state = $self->_async_state;
    my $count = $self->expirations;
    $count = 1 if !defined($count) || $count < 1;

    $state->{one_shot_fired} = 1 if !$self->interval;
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
    if ($pending && $state->{awaitable}{state} eq 'pending') {
        $future_failure = $state->{awaitable}->_complete_failed(
            $self->_wait_failure('Timer was cancelled before wait completed'),
        );
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

package Linux::Event::Async::Timer::Awaitable;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

sub _timer ($self) {
    return $self->{timer}
        // croak 'Linux::Event::Async Timer is no longer available';
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
    croak 'cannot get a cancelled Timer wait' if $state eq 'cancelled';
    croak 'AWAIT_GET called while Timer wait is pending' if $state eq 'pending';
    croak 'AWAIT_GET called without an armed Timer wait' if $state eq 'idle';

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
        croak 'AWAIT_ON_READY called without an armed Timer wait';
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
    return Linux::Event::Async::Future->new(loop => $self->_timer->loop);
}

sub AWAIT_LOOP ($self) {
    return $self->_timer->loop;
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_timer->loop;
    croak 'cannot wait on a Timer that is not attached to a loop'
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

package Linux::Event::Async::Timer;

1;

__END__

=head1 NAME

Linux::Event::Async::Timer - awaitable monotonic Linux::Event timer

=head1 SYNOPSIS

  use Linux::Event::Async;
  use Linux::Event::Async::Timer;
  use Linux::Event::Loop;

  my $loop = Linux::Event::Loop->new;
  my $timer = Linux::Event::Async::Timer->new(
      loop  => $loop,
      every => 1,
  );

  async sub heartbeat ($timer) {
      while (1) {
          my $expirations = await $timer->wait;
          say "tick x$expirations";
      }
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Timer> is the coroutine-facing specialization of
L<Linux::Event::Kernel::Timer>. It retains the core monotonic scheduler,
fixed-rate recurrence, coalesced expiration accounting, rescheduling, and
lifecycle while replacing C<on_timer> delivery with a persistent Awaitable
C<wait> operation.

Each Timer owns one persistent Awaitable. Repeated timer waits therefore do not
allocate one Future or one Awaitable per tick.

=head1 SCHEDULES

Construction uses the same schedule grammar as the core Timer:

  after => $seconds
  at    => $monotonic_seconds
  every => $seconds

C<after> and C<at> are mutually exclusive. C<every> may stand alone or combine
with C<after> or C<at> to choose a different first deadline. Durations are
seconds and may be fractional. Zero-delay one-shot work is delivered on a later
Loop turn rather than inline from construction.

C<reschedule> retains the core Timer contract and may replace an active schedule
while a wait is pending.

=head1 WAIT

=head2 wait

  my $count = await $timer->wait;

Arms one timer wait and returns the Timer's persistent Awaitable. Only one wait
may be pending at a time. The completed result must be consumed before another
wait is armed.

The result is the number of expirations represented by the event. Recurring
Linux::Event timers are fixed-rate; if the Loop is late, missed intervals are
coalesced rather than delivered as a catch-up storm. C<wait> preserves that
information instead of flattening every callback to one tick.

If a recurring Timer fires while no wait is armed, expiration counts accumulate
in one scalar. The next C<wait> completes immediately with that coalesced count.
This avoids an unbounded tick queue while preserving elapsed recurring periods.

A one-shot Timer may be waited exactly through its final expiration. After that
result is consumed, another C<wait> fails because the underlying Timer is
terminal. This remains true even when a resumed coroutine attempts another wait
reentrantly before core final-expiration cleanup has returned.

A pending wait requires the Timer to be attached to a Linux::Event Loop.
Detached Timers may be constructed first, added to a Loop, and then waited.

=head1 CANCELLATION

=head2 cancel_wait

  $timer->cancel_wait;

Cancels only the currently pending wait. It does not cancel or reschedule the
underlying Timer. A recurring Timer continues to run, and expirations that occur
while no wait is armed are coalesced for the next wait.

Cancelling an async-sub Future while it is awaiting C<wait> propagates to this
wait-only cancellation behavior.

=head2 cancel

C<cancel> retains the core Timer meaning: it terminates the Timer itself. If a
wait is pending, that wait fails with a typed C<event>/C<timer_wait> error. A
cancelled Timer cannot be rescheduled or waited for a future expiration.

=head1 CALLBACK EXCLUSIVITY

C<on_timer> is reserved by C<Linux::Event::Async::Timer> as the bridge from the
core scheduler to the persistent Awaitable. Concrete Async Timer subclasses must
not override it, and constructor C<on_timer> callbacks are not accepted.
Application timer behavior belongs after C<< await $timer->wait >>.

=head1 AWAITABLE LIFETIME

Async state is associated with the native Timer through a field hash, so this
module does not depend on the Timer object's internal representation. The Timer
owns the persistent Awaitable and the Awaitable holds only a weak reference back
to the Timer.

=head1 REQUIREMENTS

This implementation works with Linux::Event 0.110 or newer and requires no new
core ABI.

=head1 SEE ALSO

L<Linux::Event::Async>, L<Linux::Event::Async::Future>,
L<Linux::Event::Kernel::Timer>, L<Linux::Event::Loop>

=cut
