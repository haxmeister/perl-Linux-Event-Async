use v5.36;
use strict;
use warnings;

use Test::More;
use Scalar::Util qw(refaddr);

use Linux::Event::Loop;
use Linux::Event::Async;
use Linux::Event::Async::Process;

our $PHASE = 'initialization';
our $STDOUT = '';
our $STDERR = '';
our $STDOUT_EOF = 0;
our $STDERR_EOF = 0;
our @ERRORS;

$SIG{ALRM} = sub { die "Process regression timed out during $PHASE\n" };
alarm 30;

{
    package T::AsyncProcess;
    use parent 'Linux::Event::Async::Process';
}

{
    package T::OutputProcess;
    use parent 'Linux::Event::Async::Process';

    sub process_options ($class) {
        return (
            read_size            => 4,
            max_reads_per_tick   => 2,
            stdin_high_watermark => 4096,
            stdin_low_watermark  => 1024,
            max_pending_stdin    => 0,
        );
    }

    sub on_stdout ($process, $bytes) { $main::STDOUT .= $bytes }
    sub on_stderr ($process, $bytes) { $main::STDERR .= $bytes }
    sub on_stdout_eof ($process) { $main::STDOUT_EOF++ }
    sub on_stderr_eof ($process) { $main::STDERR_EOF++ }
    sub on_error ($process, $error) { push @main::ERRORS, $error }
}

{
    package T::BadExitProcess;
    use parent 'Linux::Event::Async::Process';
    sub on_exit ($process) { return }
}

sub thrown ($code) {
    my $error;
    my $ok = eval { $code->(); 1 };
    $error = $@ if !$ok;
    return ($ok, $error);
}

{
    $PHASE = 'normal exit';
    my $loop = Linux::Event::Loop->new;
    my $process = T::AsyncProcess->spawn(
        loop    => $loop,
        command => [$^X, '-e', 'exit 7'],
    );

    my $wait = $process->wait;
    ok(!$wait->is_ready, 'Process wait starts pending while child runs');
    my $result = $wait->AWAIT_WAIT;

    is(refaddr($result), refaddr($process),
        'Process wait resolves with the same Process object');
    is($process->state, 'exited', 'Process reaches exited state');
    is($process->exit_code, 7, 'Process exit code is stable after wait');
    ok(!defined($process->loop),
        'core releases Process loop after exit callback frame');

    my $again = $process->wait;
    ok($again->is_ready, 'wait is historical after Process exit');
    is(refaddr($again->get), refaddr($process),
        'historical Process wait returns same object');
}

{
    $PHASE = 'detached attach then wait';
    my $loop = Linux::Event::Loop->new;
    my $process = T::AsyncProcess->spawn(
        command => [$^X, '-e', 'exit 0'],
    );

    my ($ok, $error) = thrown(sub { $process->wait });
    ok(!$ok, 'detached Process cannot create a pending wait');
    like("$error", qr/must be attached to a Linux::Event loop/,
        'detached Process wait explains attachment requirement');

    $loop->add($process);
    is(refaddr($process->wait->AWAIT_WAIT), refaddr($process),
        'detached Process may be attached and then awaited');
    is($process->exit_code, 0, 'attached Process exits normally');
}

{
    $PHASE = 'multiple waiters and cancellation';
    my $loop = Linux::Event::Loop->new;
    my $process = T::AsyncProcess->spawn(
        loop    => $loop,
        command => [
            $^X, '-e',
            'select(undef, undef, undef, 0.15); exit 3',
        ],
    );

    my $cancelled = $process->wait;
    my $survivor = $process->wait;
    $cancelled->cancel;

    ok($cancelled->is_cancelled, 'one Process wait may be cancelled');
    ok($process->is_running,
        'cancelling Process wait does not signal or stop child');
    ok(!$survivor->is_ready,
        'another Process exit waiter remains pending');

    my $result = $survivor->AWAIT_WAIT;
    is(refaddr($result), refaddr($process),
        'surviving Process waiter completes normally');
    is($process->exit_code, 3,
        'wait cancellation does not change eventual child status');
}

{
    $PHASE = 'stdout stderr final drain';
    $STDOUT = '';
    $STDERR = '';
    $STDOUT_EOF = 0;
    $STDERR_EOF = 0;
    @ERRORS = ();

    my $loop = Linux::Event::Loop->new;
    my $process = T::OutputProcess->spawn(
        loop    => $loop,
        command => [
            $^X, '-e',
            'print STDOUT "stdout-data"; print STDERR "stderr-data"; exit 0',
        ],
        stdout => 'pipe',
        stderr => 'pipe',
    );

    $process->wait->AWAIT_WAIT;

    is($STDOUT, 'stdout-data',
        'stdout callbacks deliver all output before wait completes');
    is($STDERR, 'stderr-data',
        'stderr callbacks deliver all output before wait completes');
    is($STDOUT_EOF, 1,
        'stdout EOF callback fires before Process wait completes');
    is($STDERR_EOF, 1,
        'stderr EOF callback fires before Process wait completes');
    is_deeply(\@ERRORS, [], 'Process output path reports no errors');
}

{
    $PHASE = 'reentrant historical wait';
    my $loop = Linux::Event::Loop->new;
    my $process = T::AsyncProcess->spawn(
        loop    => $loop,
        command => [$^X, '-e', 'exit 0'],
    );

    my $reentrant;
    my $first = $process->wait;
    $first->on_ready(sub {
        $reentrant = $process->wait;
    });
    $first->AWAIT_WAIT;

    ok($reentrant && $reentrant->is_ready,
        'continuation may call wait again during core on_exit frame');
    is(refaddr($reentrant->get), refaddr($process),
        'reentrant historical wait resolves with Process');
}

{
    $PHASE = 'signal termination';
    my $loop = Linux::Event::Loop->new;
    my $process = T::AsyncProcess->spawn(
        loop    => $loop,
        command => [
            $^X, '-e',
            'kill 15, $$; select(undef, undef, undef, 1)',
        ],
    );

    $process->wait->AWAIT_WAIT;
    is($process->term_signal, 15,
        'Process wait exposes signal termination status');
    ok(!defined($process->exit_code),
        'signal-terminated Process has no exit code');
}

{
    $PHASE = 'callback policy';
    my ($ok, $error) = thrown(sub {
        T::BadExitProcess->spawn(
            command => [$^X, '-e', 'exit 0'],
        );
    });
    ok(!$ok, 'Async Process subclass cannot replace reserved on_exit bridge');
    like("$error", qr/must not override on_exit/,
        'reserved Process on_exit error is explicit');

    ($ok, $error) = thrown(sub {
        T::AsyncProcess->spawn(
            command => [$^X, '-e', 'exit 0'],
            on_exit => sub ($process) { return },
        );
    });
    ok(!$ok, 'Async Process rejects per-instance callback configuration');
    like("$error", qr/per-instance on_exit callbacks are not supported/,
        'per-instance callback rejection is explicit');
}

alarm 0;
done_testing;
