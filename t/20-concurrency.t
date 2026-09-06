use v5.36;
use strict;
use warnings;
use Test::More;
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);

use Linux::Event::Loop;
use Linux::Event::Async;

{
    package T::ConcurrentLine;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
}

sub pair ($loop) {
    socketpair(my $stream_fh, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $stream = T::ConcurrentLine->new(loop => $loop, fh => $stream_fh);
    return ($stream, $peer);
}

sub close_pair ($stream, $peer) {
    $stream->close if !$stream->is_closed;
    close $peer;
}

sub with_timeout ($code) {
    local $SIG{ALRM} = sub { die "concurrency test timed out\n" };
    alarm 5;
    my $wantarray = wantarray;
    my (@result, $result);
    my $ok = eval {
        $wantarray ? (@result = $code->()) : ($result = $code->());
        1;
    };
    my $error = $@;
    alarm 0;
    die $error if !$ok;
    return $wantarray ? @result : $result;
}

async sub read_one ($stream) {
    return await $stream->recv;
}

async sub read_two_streams ($first, $second) {
    my $one = await $first->recv;
    my $two = await $second->recv;
    return "$one:$two";
}

async sub read_then_fail ($stream) {
    await $stream->recv;
    die "intentional task failure\n";
}

async sub await_both ($first, $second) {
    my $one = await $first;
    my $two = await $second;
    return "$one:$two";
}

async sub await_nested ($child) {
    return await $child;
}

subtest 'two tasks share one loop and may complete out of order' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream_one, $peer_one) = pair($loop);
    my ($stream_two, $peer_two) = pair($loop);
    my $future_one = read_one($stream_one);
    my $future_two = read_one($stream_two);

    syswrite($peer_two, "two\n") == 4 or die "write two: $!";
    cmp_ok($loop->run_once(100), '>=', 1,
        'shared loop dispatches the second task first');
    ok($future_two->is_ready, 'second task is ready first');
    ok(!$future_one->is_ready, 'first task remains pending');

    syswrite($peer_one, "one\n") == 4 or die "write one: $!";
    is(with_timeout(sub { $future_one->AWAIT_WAIT }), 'one',
        'first task completes through AWAIT_WAIT');
    is($future_two->get, 'two', 'earlier second-task result remains available');

    close_pair($stream_one, $peer_one);
    close_pair($stream_two, $peer_two);
};

subtest 'one task failure does not prevent another task completing' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($bad_stream, $bad_peer) = pair($loop);
    my ($good_stream, $good_peer) = pair($loop);
    my $bad = read_then_fail($bad_stream);
    my $good = read_one($good_stream);

    syswrite($bad_peer, "fail\n") == 5 or die "write failure: $!";
    syswrite($good_peer, "good\n") == 5 or die "write good: $!";
    my $error = eval { with_timeout(sub { $bad->AWAIT_WAIT }); 1 }
        ? '' : $@;
    like($error, qr/intentional task failure/,
        'failed async sub reports its exception');
    is(with_timeout(sub { $good->AWAIT_WAIT }), 'good',
        'independent task still completes');

    close_pair($bad_stream, $bad_peer);
    close_pair($good_stream, $good_peer);
};

subtest 'cancelling one task does not cancel another' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream_one, $peer_one) = pair($loop);
    my ($stream_two, $peer_two) = pair($loop);
    my $future_one = read_one($stream_one);
    my $future_two = read_one($stream_two);

    $future_one->cancel;
    ok($future_one->is_cancelled, 'first task is cancelled');
    syswrite($peer_one, "preserved\n") == 10 or die "write preserved: $!";
    syswrite($peer_two, "second\n") == 7 or die "write second: $!";
    is(with_timeout(sub { $future_two->AWAIT_WAIT }), 'second',
        'second task completes after first task cancellation');
    is(with_timeout(sub { read_one($stream_one)->AWAIT_WAIT }), 'preserved',
        'cancelled task did not consume its Stream message');

    close_pair($stream_one, $peer_one);
    close_pair($stream_two, $peer_two);
};

subtest 'cancellation does not reach a reused historical Awaitable' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream_one, $peer_one) = pair($loop);
    my ($stream_two, $peer_two) = pair($loop);
    my $parent = read_two_streams($stream_one, $stream_two);

    syswrite($peer_one, "first\n") == 6 or die "write first: $!";
    cmp_ok($loop->run_once(100), '>=', 1,
        'parent consumes first Stream and advances to second');
    ok(!$parent->is_ready, 'parent is now waiting on second Stream');

    my $unrelated = read_one($stream_one);
    ok(!$unrelated->is_ready,
        'historical Stream Awaitable may be rearmed by unrelated work');

    $parent->cancel;
    ok($parent->is_cancelled, 'parent task is cancelled');
    ok(!$unrelated->is_cancelled,
        'parent cancellation does not cancel rearmed historical Stream wait');
    ok(!$unrelated->is_ready,
        'rearmed historical Stream wait remains pending after parent cancellation');

    syswrite($peer_one, "preserved\n") == 10 or die "write preserved: $!";
    is(with_timeout(sub { $unrelated->AWAIT_WAIT }), 'preserved',
        'rearmed historical Stream wait still receives its message');

    close_pair($stream_one, $peer_one);
    close_pair($stream_two, $peer_two);
};

subtest 'two readers on one Stream fail without damaging the first' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream, $peer) = pair($loop);
    my $first = read_one($stream);
    my $second = read_one($stream);

    ok($second->is_ready, 'overlapping-reader task fails immediately');
    my $error = eval { $second->get; 1 } ? '' : $@;
    like($error, qr/a receive is already pending/,
        'overlapping reader reports the one-reader contract');
    syswrite($peer, "first\n") == 6 or die "write first: $!";
    is(with_timeout(sub { $first->AWAIT_WAIT }), 'first',
        'original pending reader remains valid');

    close_pair($stream, $peer);
};

subtest 'composed task follows sequential work across different loops' => sub {
    my $loop_one = Linux::Event::Loop->new;
    my $loop_two = Linux::Event::Loop->new;
    my ($stream_one, $peer_one) = pair($loop_one);
    my ($stream_two, $peer_two) = pair($loop_two);
    my $combined = await_both(
        read_one($stream_one),
        read_one($stream_two),
    );

    syswrite($peer_one, "one\n") == 4 or die "write one: $!";
    syswrite($peer_two, "two\n") == 4 or die "write two: $!";
    is(with_timeout(sub { $combined->AWAIT_WAIT }), 'one:two',
        'AWAIT_WAIT follows the currently awaited Future to its loop');

    close_pair($stream_one, $peer_one);
    close_pair($stream_two, $peer_two);
};

subtest 'parent task follows a child task across different loops' => sub {
    my $loop_one = Linux::Event::Loop->new;
    my $loop_two = Linux::Event::Loop->new;
    my ($stream_one, $peer_one) = pair($loop_one);
    my ($stream_two, $peer_two) = pair($loop_two);
    my $child = await_both(
        read_one($stream_one),
        read_one($stream_two),
    );
    my $parent = await_nested($child);

    syswrite($peer_one, "one\n") == 4 or die "write one: $!";
    syswrite($peer_two, "two\n") == 4 or die "write two: $!";
    is(with_timeout(sub { $parent->AWAIT_WAIT }), 'one:two',
        'effective Loop lookup follows nested async-sub composition');

    close_pair($stream_one, $peer_one);
    close_pair($stream_two, $peer_two);
};

subtest 'run exits only after an explicit shared completion condition' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream_one, $peer_one) = pair($loop);
    my ($stream_two, $peer_two) = pair($loop);
    my $future_one = read_one($stream_one);
    my $future_two = read_one($stream_two);
    my $remaining = 2;
    my $finished = sub { $loop->stop if --$remaining == 0 };

    $future_one->on_ready($finished);
    $future_two->on_ready($finished);
    syswrite($peer_one, "one\n") == 4 or die "write one: $!";
    syswrite($peer_two, "two\n") == 4 or die "write two: $!";
    with_timeout(sub { $loop->run });
    is($remaining, 0, 'both completion callbacks ran before explicit stop');
    is($future_one->get, 'one', 'first result remains available');
    is($future_two->get, 'two', 'second result remains available');

    close_pair($stream_one, $peer_one);
    close_pair($stream_two, $peer_two);
};

done_testing;
