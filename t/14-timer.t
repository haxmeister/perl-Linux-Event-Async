use v5.36;
use strict;
use warnings;

use Test::More;
use Scalar::Util qw(refaddr);
use Time::HiRes qw(sleep);

use Linux::Event::Loop;
use Linux::Event::Async;
use Linux::Event::Async::Timer;

{
    package T::BadAsyncTimer;
    use parent 'Linux::Event::Async::Timer';
    sub on_timer ($timer) { return }
}

sub thrown ($code) {
    my $error;
    my $ok = eval { $code->(); 1 };
    $error = $@ if !$ok;
    return ($ok, $error);
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        after => 0,
    );

    my $awaitable = $timer->wait;
    ok(!$awaitable->AWAIT_IS_READY,
        'one-shot wait starts pending');
    is($awaitable->AWAIT_WAIT, 1,
        'one-shot wait resolves with one expiration');
    is($timer->state, 'expired',
        'one-shot core Timer becomes expired');

    my $terminal = $timer->wait;
    is(refaddr($terminal), refaddr($awaitable),
        'Timer reuses one persistent Awaitable');
    ok($terminal->AWAIT_IS_READY,
        'wait after consumed one-shot expiration fails immediately');
    my ($ok, $error) = thrown(sub { $terminal->AWAIT_GET });
    ok(!$ok, 'terminal one-shot wait throws');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'timer_wait',
        'terminal one-shot failure identifies timer_wait');
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(after => 0);

    my ($ok, $error) = thrown(sub { $timer->wait });
    ok(!$ok, 'detached pending Timer cannot be waited');
    like("$error", qr/must be attached to a Linux::Event loop/,
        'detached wait reports attachment requirement');

    $loop->add($timer);
    is($timer->wait->AWAIT_WAIT, 1,
        'detached Timer may be attached and then awaited');
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        every => 0.01,
    );

    my $awaitable = $timer->wait;
    is($awaitable->AWAIT_WAIT, 1,
        'first recurring wait represents one tick');

    sleep 0.035;
    my $next = $timer->wait;
    is(refaddr($next), refaddr($awaitable),
        'recurring waits reuse the same Awaitable');
    cmp_ok($next->AWAIT_WAIT, '>=', 3,
        'late recurring wait preserves coalesced expiration count');

    my $cancelled = $timer->wait;
    $timer->cancel_wait;
    ok($cancelled->AWAIT_IS_CANCELLED,
        'cancel_wait cancels only the pending wait');
    ok($timer->is_active,
        'cancel_wait leaves recurring Timer active');

    sleep 0.012;
    my $after_cancel = $timer->wait;
    is(refaddr($after_cancel), refaddr($awaitable),
        'persistent Awaitable is reusable after wait cancellation');
    cmp_ok($after_cancel->AWAIT_WAIT, '>=', 1,
        'recurring Timer still produces expirations after wait cancellation');

    $timer->cancel;
    is($timer->state, 'cancelled',
        'core cancel remains terminal for recurring Timer');
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        every => 0.005,
    );

    sleep 0.02;
    $loop->run_once(100);

    my $available = $timer->wait;
    ok($available->AWAIT_IS_READY,
        'expiration accumulated while no wait was armed');
    cmp_ok($available->AWAIT_GET, '>=', 1,
        'unarmed recurring expirations coalesce into one scalar result');

    $timer->cancel;
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        every => 60,
    );

    my $pending = $timer->wait;
    $timer->cancel;

    ok($pending->AWAIT_IS_READY,
        'Timer cancellation completes a pending wait');
    my ($ok, $error) = thrown(sub { $pending->AWAIT_GET });
    ok(!$ok, 'pending wait fails when underlying Timer is cancelled');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'timer_wait',
        'Timer cancellation failure identifies timer_wait');
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        after => 60,
    );

    my $pending = $timer->wait;
    $timer->reschedule(after => 0);
    is($pending->AWAIT_WAIT, 1,
        'reschedule preserves an already pending wait');
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        every => 60,
    );

    async sub wait_once ($timer) {
        return await $timer->wait;
    }

    my $task = wait_once($timer);
    ok(!$task->is_ready,
        'async task suspends on Timer wait');
    $task->cancel;
    ok($task->is_cancelled,
        'cancelling async task cancels the current Timer wait');
    ok($timer->is_active,
        'async task cancellation does not cancel underlying Timer');

    $timer->reschedule(after => 0, every => 60);
    is($timer->wait->AWAIT_WAIT, 1,
        'Timer can be awaited again after async task cancellation');
    $timer->cancel;
}

{
    my $loop = Linux::Event::Loop->new;
    my $timer = Linux::Event::Async::Timer->new(
        loop  => $loop,
        after => 0,
    );

    async sub wait_twice ($timer) {
        await $timer->wait;
        return await $timer->wait;
    }

    my $task = wait_twice($timer);
    my ($ok, $error) = thrown(sub { $task->AWAIT_WAIT });
    ok(!$ok,
        'one-shot second wait fails even when armed reentrantly from continuation');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'timer_wait',
        'reentrant terminal wait reports timer_wait');
}

{
    my ($ok, $error) = thrown(sub {
        T::BadAsyncTimer->new(after => 0);
    });
    ok(!$ok,
        'Async Timer subclass cannot replace reserved on_timer bridge');
    like("$error", qr/must not override on_timer/,
        'reserved callback error is explicit');
}

done_testing;
