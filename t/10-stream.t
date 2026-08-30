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

    $recv->AWAIT_ON_READY(sub { $loop->stop });
    syswrite($peer, "one\ntwo\n") == 8 or die "short write: $!";
    $loop->run;
    ok($recv->AWAIT_IS_READY, 'first framed message makes receive ready');
    is($recv->AWAIT_GET, 'one', 'first framed message is returned');

    my $second = $stream->recv;
    is(refaddr($second), refaddr($recv),
        'recv reuses one persistent Awaitable view');
    ok($second->AWAIT_IS_READY,
        'buffered second frame completes synchronously when receive is rearmed');
    is($second->AWAIT_GET, 'two', 'second framed message preserves order');

    $stream->close;
    close $peer;
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
