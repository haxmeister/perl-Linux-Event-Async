use v5.36;
use strict;
use warnings;

use Test::More;
use Scalar::Util qw(refaddr);
use POSIX qw(SIGUSR1 SIGUSR2);

use Linux::Event::Loop;
use Linux::Event::Async;
use Linux::Event::Async::Signal;

{
    package T::BadAsyncSignal;
    use parent 'Linux::Event::Async::Signal';
    sub on_signal ($signal, $number, $count) { return }
}

sub thrown ($code) {
    my $error;
    my $ok = eval { $code->(); 1 };
    $error = $@ if !$ok;
    return ($ok, $error);
}

my $phase = 'startup';
local $SIG{ALRM} = sub { die "Signal regression timed out during $phase\n" };
alarm 30;

{
    $phase = 'basic armed signal wait';
    my $loop = Linux::Event::Loop->new;
    my $signal = Linux::Event::Async::Signal->new(
        loop    => $loop,
        signals => [SIGUSR1, SIGUSR2],
    );

    my $first = $signal->wait;
    kill SIGUSR1, $$ or die "kill SIGUSR1: $!";
    my ($number, $count) = $first->AWAIT_WAIT;
    is($number, SIGUSR1, 'Signal wait returns delivered signal number');
    is($count, 1, 'Signal wait returns delivered record count');

    my $second = $signal->wait;
    is(refaddr($second), refaddr($first),
        'Signal wait reuses one persistent Awaitable');
    kill SIGUSR2, $$ or die "kill SIGUSR2: $!";
    is(scalar $second->AWAIT_WAIT, SIGUSR2,
        'scalar Signal wait returns signal number as first result');

    $signal->cancel;
    ok($signal->is_terminal, 'Signal cancel remains terminal');
}

{
    $phase = 'idle signal coalescing';
    my $loop = Linux::Event::Loop->new;
    my $signal = Linux::Event::Async::Signal->new(
        loop    => $loop,
        signals => [SIGUSR1, SIGUSR2],
    );

    kill SIGUSR2, $$ or die "kill first SIGUSR2: $!";
    $loop->run_once(-1);
    kill SIGUSR2, $$ or die "kill second SIGUSR2: $!";
    $loop->run_once(-1);
    kill SIGUSR1, $$ or die "kill SIGUSR1: $!";
    $loop->run_once(-1);

    my ($number, $count) = $signal->wait->AWAIT_WAIT;
    is($number, SIGUSR2,
        'first idle signal number retains first-observed pending order');
    is($count, 2,
        'idle delivery coalesces counts for the same subscribed signal');

    ($number, $count) = $signal->wait->AWAIT_WAIT;
    is($number, SIGUSR1, 'second pending signal follows bounded pending order');
    is($count, 1, 'second pending signal retains its count');

    $signal->cancel;
}

{
    $phase = 'wait-local Signal cancellation';
    my $loop = Linux::Event::Loop->new;
    my $signal = Linux::Event::Async::Signal->new(
        loop    => $loop,
        signals => [SIGUSR1],
    );

    my $cancelled = $signal->wait;
    $signal->cancel_wait;
    ok($cancelled->AWAIT_IS_CANCELLED,
        'cancel_wait cancels only the current Signal wait');
    ok($signal->is_active,
        'cancel_wait leaves the Signal subscription active');

    kill SIGUSR1, $$ or die "kill SIGUSR1 after cancel_wait: $!";
    is(scalar $signal->wait->AWAIT_WAIT, SIGUSR1,
        'later wait receives a signal after wait-local cancellation');

    $signal->cancel;
}

{
    $phase = 'terminal Signal cancellation';
    my $loop = Linux::Event::Loop->new;
    my $signal = Linux::Event::Async::Signal->new(
        loop    => $loop,
        signals => [SIGUSR1],
    );

    my $wait = $signal->wait;
    $signal->cancel;
    ok($wait->AWAIT_IS_READY,
        'terminal Signal cancel completes pending wait');
    my ($ok, $error) = thrown(sub { $wait->AWAIT_GET });
    ok(!$ok, 'terminal Signal cancel fails pending wait');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'signal_wait',
        'terminal cancellation identifies signal_wait operation');

    my $later = $signal->wait;
    ($ok, $error) = thrown(sub { $later->AWAIT_GET });
    ok(!$ok, 'wait after terminal cancellation fails immediately');
    isa_ok($error, 'Linux::Event::Error');
}

{
    $phase = 'reentrant Signal cancellation from resumed coroutine';
    my $loop = Linux::Event::Loop->new;
    my $signal = Linux::Event::Async::Signal->new(
        loop    => $loop,
        signals => [SIGUSR1],
    );

    async sub wait_then_cancel ($subscription) {
        my ($number, $count) = await $subscription->wait;
        $subscription->cancel;
        return ($number, $count);
    }

    my $task = wait_then_cancel($signal);
    kill SIGUSR1, $$ or die "kill reentrant SIGUSR1: $!";
    my ($number, $count) = $task->AWAIT_WAIT;
    is($number, SIGUSR1,
        'resumed coroutine receives signal before cancelling subscription');
    is($count, 1, 'resumed coroutine preserves signal count');
    ok($signal->is_terminal,
        'subscription may be cancelled reentrantly from resumed coroutine');
}

{
    $phase = 'Signal callback exclusivity';
    my ($ok, $error) = thrown(sub {
        T::BadAsyncSignal->new(signals => [SIGUSR1]);
    });
    ok(!$ok, 'Async Signal subclass cannot replace reserved on_signal');
    like("$error", qr/must not override on_signal/,
        'reserved on_signal subclass error is explicit');

    ($ok, $error) = thrown(sub {
        Linux::Event::Async::Signal->new(
            signals   => [SIGUSR1],
            on_signal => sub { return },
        );
    });
    ok(!$ok, 'Async Signal rejects constructor on_signal callback');
    like("$error", qr/on_signal is reserved/,
        'constructor callback rejection explains await wait policy');
}

alarm 0;
done_testing;
