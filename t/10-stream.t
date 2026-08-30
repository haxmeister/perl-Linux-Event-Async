use v5.36;
use strict;
use warnings;
use Test::More;
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);
use Scalar::Util qw(refaddr);

use Linux::Event::Loop;
use Linux::Event::Async;

{
    package T::AsyncLine;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
}

sub pair () {
    socketpair(my $stream_fh, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $loop = Linux::Event::Loop->new;
    my $stream = T::AsyncLine->new(loop => $loop, fh => $stream_fh);
    return ($loop, $stream, $peer);
}

{
    my ($loop, $stream, $peer) = pair();
    my $recv = $stream->recv;
    isa_ok($recv, 'Linux::Event::Async::Stream::Awaitable');
    isnt(refaddr($recv), refaddr($stream),
        'recv returns a native Awaitable view, not the Stream hash');
    ok(!$recv->AWAIT_IS_READY, 'receive starts pending');
    my $pending_error = eval { $stream->recv; 1 } ? '' : $@;
    like($pending_error, qr/a receive is already pending/,
        'a second receive cannot overlap the current generation');

    my $first_ready = 0;
    $recv->AWAIT_ON_READY(sub { $first_ready++; $loop->stop });
    syswrite($peer, "one\ntwo\n") == 8 or die "short write: $!";
    $loop->run;
    ok($recv->AWAIT_IS_READY, 'first framed message makes receive ready');
    is($first_ready, 1, 'first-generation ready callback fires once');
    my $unconsumed_error = eval { $stream->recv; 1 } ? '' : $@;
    like($unconsumed_error, qr/previous receive result has not been consumed/,
        'next generation cannot start before the result is consumed');
    is($recv->AWAIT_GET, 'one', 'first framed message is returned');

    my $second = $stream->recv;
    is(refaddr($second), refaddr($recv),
        'recv reuses one persistent Awaitable view');
    ok($second->AWAIT_IS_READY,
        'buffered second frame completes synchronously when receive is rearmed');
    is($first_ready, 1,
        'callback from the prior generation is not reused');
    is($second->AWAIT_GET, 'two', 'second framed message preserves order');

    $stream->close;
    close $peer;
}

{
    my ($loop, $stream, $peer) = pair();

    async sub read_numbered ($s, $count) {
        my @messages;
        for my $index (1 .. $count) {
            push @messages, await $s->recv;
        }
        return \@messages;
    }

    my @expected = map { sprintf 'message-%03d', $_ } 1 .. 150;
    my $wire = join '', map { "$_\n" } @expected;
    syswrite($peer, $wire) == length($wire) or die "short bulk write: $!";
    Linux::Event::Async::Stream::_recv_profile_start($stream);
    my $messages = read_numbered($stream, scalar @expected)->AWAIT_WAIT;
    is_deeply($messages, \@expected,
        'bounded prefetch preserves order across more than one full ring');
    my $profile = Linux::Event::Async::Stream::_recv_profile_stats($stream);
    cmp_ok($profile->{recv_immediate}, '>', 100,
        'bulk buffered input completes most receives immediately');
    cmp_ok($profile->{await_suspended}, '<', 5,
        'bulk buffered input avoids per-message FAA suspension');

    $stream->close;
    close $peer;
}

{
    my ($loop, $stream, $peer) = pair();

    async sub read_slice ($s, $count) {
        my @messages;
        for my $index (1 .. $count) {
            push @messages, await $s->recv;
        }
        return \@messages;
    }

    my @expected = map { "item-$_" } 1 .. 100;
    my $wire = join '', map { "$_\n" } @expected;
    my $first = read_slice($stream, 10);
    syswrite($peer, $wire) == length($wire) or die "short queued write: $!";
    is_deeply($first->AWAIT_WAIT, [@expected[0 .. 9]],
        'consumer may stop with prefetched messages retained');
    is_deeply(read_slice($stream, 90)->AWAIT_WAIT, [@expected[10 .. 99]],
        'a later receive drains retained messages before native input');

    $stream->close;
    close $peer;
}

{
    my ($loop, $stream, $peer) = pair();

    async sub read_through_eof ($s, $count) {
        my @messages;
        for my $index (1 .. $count) {
            push @messages, await $s->recv;
        }
        my $eof = await $s->recv;
        return [\@messages, $eof];
    }

    my @expected = map { "eof-$_" } 1 .. 100;
    my $wire = join '', map { "$_\n" } @expected;
    my $future = read_through_eof($stream, scalar @expected);
    syswrite($peer, $wire) == length($wire) or die "short EOF write: $!";
    close $peer;
    my ($messages, $eof) = $future->AWAIT_WAIT->@*;
    is_deeply($messages, \@expected,
        'terminal EOF is ordered after every prefetched message');
    is($eof, undef, 'receive after the prefetched batch observes clean EOF');

    $stream->close;
}

{
    my ($loop, $stream, $peer) = pair();
    my $recv = $stream->recv;
    my ($cancelled, $ready) = (0, 0);
    $recv->AWAIT_ON_CANCEL(sub { $cancelled++ });
    $recv->AWAIT_ON_READY(sub { $ready++ });
    $stream->cancel_recv;

    ok($recv->AWAIT_IS_CANCELLED, 'cancelled receive reports cancelled');
    ok($recv->AWAIT_IS_READY, 'cancelled receive is a ready Awaitable state');
    is($cancelled, 1, 'cancel callback fires once');
    is($ready, 1, 'ready callback also fires on cancellation');

    syswrite($peer, "preserved\n") == 10 or die "short write: $!";
    $loop->run_for(0.01);
    my $next = $stream->recv;
    $next->AWAIT_ON_READY(sub { $loop->stop }) if !$next->AWAIT_IS_READY;
    $loop->run if !$next->AWAIT_IS_READY;
    is($next->AWAIT_GET, 'preserved',
        'cancellation does not consume the next message');

    $stream->close;
    close $peer;
}

{
    my ($loop, $stream, $peer) = pair();

    async sub read_two ($s) {
        my $first = await $s->recv;
        my $second = await $s->recv;
        return "$first:$second";
    }

    my $future = read_two($stream);
    isa_ok($future, 'Linux::Event::Async::Future');
    syswrite($peer, "alpha\nbeta\n") == 11 or die "short write: $!";
    is($future->AWAIT_WAIT, 'alpha:beta',
        'async sub resumes reentrantly across consecutive receives');

    $stream->close;
    close $peer;
}

{
    my ($loop, $stream, $peer) = pair();
    my $recv = $stream->recv;
    $recv->AWAIT_ON_READY(sub { $loop->stop });
    close $peer;
    $loop->run;
    ok($recv->AWAIT_IS_READY, 'EOF completes a pending receive');
    is($recv->AWAIT_GET, undef, 'clean EOF resolves as undef');
    $stream->close;
}

{
    my ($loop, $stream, $peer) = pair();

    async sub wait_one ($s) {
        return await $s->recv;
    }

    my $future = wait_one($stream);
    $future->cancel;
    ok($future->AWAIT_IS_CANCELLED, 'async-sub result can be cancelled');
    ok(!$stream->recv->AWAIT_IS_READY,
        'cancelling async-sub result releases the pending Stream receive');
    $stream->cancel_recv;

    $stream->close;
    close $peer;
}

done_testing;
