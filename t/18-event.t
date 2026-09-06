use v5.36;
use strict;
use warnings;

use Test::More;
use Config ();
use Scalar::Util qw(refaddr);

use Linux::Event::Loop;
use Linux::Event::Async;
use Linux::Event::Async::Event;

{
    package T::BadAsyncEvent;
    use parent 'Linux::Event::Async::Event';
    sub on_event ($event, $count) { return }
}

sub thrown ($code) {
    my $error;
    my $ok = eval { $code->(); 1 };
    $error = $@ if !$ok;
    return ($ok, $error);
}

my $phase = 'startup';
local $SIG{ALRM} = sub { die "Event regression timed out during $phase\n" };
alarm 30;

{
    $phase = 'basic armed event wait';
    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new(loop => $loop);

    my $first = $event->wait;
    $event->signal(3);
    is($first->AWAIT_WAIT, 3,
        'Event wait returns drained eventfd counter');

    my $second = $event->wait;
    is(refaddr($second), refaddr($first),
        'Event wait reuses one persistent Awaitable');
    $event->signal;
    is($second->AWAIT_WAIT, 1,
        'default Event signal increment is preserved');

    $event->cancel;
    ok($event->is_terminal, 'Event cancel remains terminal');
}

{
    $phase = 'idle event coalescing';
    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new(loop => $loop);

    $event->signal(2);
    $loop->run_once(-1);
    $event->signal(5);
    $loop->run_once(-1);

    is($event->wait->AWAIT_WAIT, 7,
        'idle eventfd callbacks accumulate into one bounded scalar result');
    $event->cancel;
}

{
    $phase = 'wait-local Event cancellation';
    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new(loop => $loop);

    my $cancelled = $event->wait;
    $event->cancel_wait;
    ok($cancelled->AWAIT_IS_CANCELLED,
        'cancel_wait cancels only current Event wait');
    ok($event->is_active,
        'cancel_wait leaves Event active');

    $event->signal(4);
    is($event->wait->AWAIT_WAIT, 4,
        'later Event wait receives notification after wait-local cancellation');
    $event->cancel;
}

{
    $phase = 'detached Event signaling and attach';
    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new;

    $event->signal(6);
    my ($ok, $error) = thrown(sub { $event->wait });
    ok(!$ok, 'pending Event wait requires Loop attachment');
    like("$error", qr/must be attached/, 'detached wait error is explicit');

    $loop->add($event);
    is($event->wait->AWAIT_WAIT, 6,
        'eventfd count signaled before attachment survives in kernel');
    $event->cancel;
}

{
    $phase = 'terminal Event cancellation';
    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new(loop => $loop);

    my $wait = $event->wait;
    $event->cancel;
    ok($wait->AWAIT_IS_READY,
        'terminal Event cancel completes pending wait');
    my ($ok, $error) = thrown(sub { $wait->AWAIT_GET });
    ok(!$ok, 'terminal Event cancel fails pending wait');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'event_wait',
        'terminal Event cancellation identifies event_wait');

    my $later = $event->wait;
    ($ok, $error) = thrown(sub { $later->AWAIT_GET });
    ok(!$ok, 'wait after terminal Event cancellation fails immediately');
    isa_ok($error, 'Linux::Event::Error');
}

{
    $phase = 'reentrant Event cancellation from resumed coroutine';
    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new(loop => $loop);

    async sub wait_then_cancel ($notification) {
        my $count = await $notification->wait;
        $notification->cancel;
        return $count;
    }

    my $task = wait_then_cancel($event);
    $event->signal(8);
    is($task->AWAIT_WAIT, 8,
        'resumed coroutine receives Event count before cancelling resource');
    ok($event->is_terminal,
        'Event may be cancelled reentrantly from resumed coroutine');
}

SKIP: {
    skip 'Perl was built without ithreads', 4
        if !$Config::Config{useithreads};

    $phase = 'ithread Event signaling boundary';
    require threads;

    my $loop = Linux::Event::Loop->new;
    my $event = Linux::Event::Async::Event->new(loop => $loop);
    my $wait = $event->wait;

    my $worker = threads->create(sub {
        my ($ok, $error) = thrown(sub { $event->wait });
        $event->signal(9);
        return [$ok, "$error"];
    });
    my $worker_result = $worker->join;

    ok(!$worker_result->[0],
        'ithread Event clone cannot own Async wait state');
    like($worker_result->[1], qr/Async::Event state is unavailable/,
        'ithread wait rejection identifies unavailable Async owner state');
    is($wait->AWAIT_WAIT, 9,
        'ithread Event clone can signal owner-side pending wait');
    ok($event->is_active,
        'worker signaling leaves owner-side Event active');

    $event->cancel;
}

{
    $phase = 'Event callback exclusivity';
    my ($ok, $error) = thrown(sub {
        T::BadAsyncEvent->new;
    });
    ok(!$ok, 'Async Event subclass cannot replace reserved on_event');
    like("$error", qr/must not override on_event/,
        'reserved on_event subclass error is explicit');

    ($ok, $error) = thrown(sub {
        Linux::Event::Async::Event->new(on_event => sub { return });
    });
    ok(!$ok, 'Async Event rejects constructor on_event callback');
    like("$error", qr/on_event is reserved/,
        'constructor callback rejection explains await wait policy');
}

alarm 0;
done_testing;
