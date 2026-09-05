use v5.36;
use strict;
use warnings;
use Test::More;
use Scalar::Util qw(refaddr);

use Linux::Event::Loop;
use Linux::Event::Async;
use Linux::Event::Async::Listener;

{
    package T::AcceptStream;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
}

{
    package T::BadAcceptListener;
    use parent 'Linux::Event::Async::Listener';
    sub on_accept ($listener, $stream) { return }
}

sub listener ($loop) {
    return Linux::Event::Async::Listener->new(
        loop         => $loop,
        stream_class => 'T::AcceptStream',
        host         => '127.0.0.1',
        port         => 0,
    );
}

sub client ($loop, $port) {
    return T::AcceptStream->connect(
        loop => $loop,
        host => '127.0.0.1',
        port => $port,
    );
}

sub close_stream ($stream) {
    $stream->close if $stream && !$stream->is_closed;
    return;
}

{
    my $loop = Linux::Event::Loop->new;
    my $listener = listener($loop);

    ok($listener->is_paused,
        'attached Async Listener begins paused until accept is armed');

    my $accept = $listener->accept;
    isa_ok($accept, 'Linux::Event::Async::Listener::Awaitable');
    ok(!$accept->AWAIT_IS_READY, 'accept Awaitable starts pending');
    ok($listener->is_running && !$listener->is_paused,
        'arming accept resumes Listener');

    my $overlap = eval { $listener->accept; 1 } ? '' : $@;
    like($overlap, qr/another accept is already pending/,
        'only one accept may be pending');

    my $client = client($loop, $listener->port);
    my $accepted = $accept->AWAIT_WAIT;
    isa_ok($accepted, 'T::AcceptStream');
    ok($listener->is_paused,
        'Listener pauses after delivering one accept without rearm');
    ok($accepted->ready->is_ready,
        'plain accepted Stream reaches application readiness in same dispatch');

    close_stream($accepted);
    close_stream($client);
    $listener->close;
}

{
    my $loop = Linux::Event::Loop->new;
    my $listener = listener($loop);

    my $first_awaitable = $listener->accept;
    my $client1 = client($loop, $listener->port);
    my $client2 = client($loop, $listener->port);
    my $accepted1 = $first_awaitable->AWAIT_WAIT;

    my $second_awaitable = $listener->accept;
    is(refaddr($second_awaitable), refaddr($first_awaitable),
        'accept reuses one persistent Awaitable view');
    my $accepted2 = $second_awaitable->AWAIT_WAIT;

    isnt(refaddr($accepted1), refaddr($accepted2),
        'two queued kernel connections produce distinct Streams');
    ok(!$client1->is_closed && !$client2->is_closed,
        'single-accept pull model does not discard the second connection');

    close_stream($_) for ($accepted1, $accepted2, $client1, $client2);
    $listener->close;
}

{
    my $loop = Linux::Event::Loop->new;
    my $listener = listener($loop);
    my $accept = $listener->accept;

    $accept->cancel;
    ok($accept->AWAIT_IS_CANCELLED, 'accept Awaitable records cancellation');
    ok($listener->is_paused,
        'cancelling accept pauses Listener without closing it');
    ok(!$listener->is_terminal, 'accept cancellation leaves Listener reusable');

    my $cancelled_get = eval { $accept->AWAIT_GET; 1 } ? '' : $@;
    like($cancelled_get, qr/cancelled accept/,
        'cancelled accept cannot be retrieved as a connection');

    my $retry = $listener->accept;
    is(refaddr($retry), refaddr($accept),
        'same Awaitable can be rearmed after cancellation');
    my $client = client($loop, $listener->port);
    my $accepted = $retry->AWAIT_WAIT;
    isa_ok($accepted, 'T::AcceptStream');

    close_stream($accepted);
    close_stream($client);
    $listener->close;
}

{
    my $loop = Linux::Event::Loop->new;
    my $listener = listener($loop);

    async sub accept_and_ready ($listener) {
        my $stream = await $listener->accept;
        await $stream->ready;
        return $stream;
    }

    my $task = accept_and_ready($listener);
    my $client = client($loop, $listener->port);
    my $accepted = $task->AWAIT_WAIT;

    isa_ok($accepted, 'T::AcceptStream');
    ok($accepted->ready->is_ready,
        'async sub can move directly from accept Awaitable to Stream readiness');

    close_stream($accepted);
    close_stream($client);
    $listener->close;
}

{
    my $loop = Linux::Event::Loop->new;
    my $listener = listener($loop);

    async sub accept_one ($listener) {
        return await $listener->accept;
    }

    my $task = accept_one($listener);
    ok(!$listener->is_paused, 'async sub arms Listener accept');
    $task->cancel;

    ok($task->is_cancelled,
        'cancelling async-sub Future cancels accept Awaitable');
    ok($listener->is_paused,
        'async-sub cancellation propagates pull backpressure to Listener');
    ok(!$listener->is_terminal,
        'async-sub cancellation does not close Listener');

    my $retry = $listener->accept;
    my $client = client($loop, $listener->port);
    my $accepted = $retry->AWAIT_WAIT;
    isa_ok($accepted, 'T::AcceptStream');

    close_stream($accepted);
    close_stream($client);
    $listener->close;
}

{
    my $loop = Linux::Event::Loop->new;
    my $listener = listener($loop);
    my $accept = $listener->accept;
    $listener->close;

    ok($accept->AWAIT_IS_READY,
        'closing Listener completes pending accept Awaitable');
    my $error;
    my $ok = eval { $accept->AWAIT_GET; 1 };
    $error = $@ if !$ok;
    ok(!$ok, 'closed Listener accept throws');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'accept',
        'explicit close failure identifies accept operation');
}

{
    my $loop = Linux::Event::Loop->new;
    my $error = eval {
        Linux::Event::Async::Listener->new(
            loop                => $loop,
            stream_class        => 'T::AcceptStream',
            host                => '127.0.0.1',
            port                => 0,
            max_accept_per_tick => 2,
        );
        1;
    } ? '' : $@;
    like($error, qr/requires max_accept_per_tick => 1/,
        'Async Listener rejects batched core acceptance');

    $error = eval {
        Linux::Event::Async::Listener->new(
            loop         => $loop,
            stream_class => 'T::AcceptStream',
            host         => '127.0.0.1',
            port         => 0,
            edge_triggered => 1,
        );
        1;
    } ? '' : $@;
    like($error, qr/does not support edge_triggered/,
        'Async Listener rejects edge-triggered pull acceptance');

    $error = eval {
        T::BadAcceptListener->new(
            loop         => $loop,
            stream_class => 'T::AcceptStream',
            host         => '127.0.0.1',
            port         => 0,
        );
        1;
    } ? '' : $@;
    like($error, qr/must not override on_accept/,
        'Async Listener reserves on_accept as Awaitable delivery sink');
}

{
    my $listener = Linux::Event::Async::Listener->new(
        stream_class => 'T::AcceptStream',
        host         => '127.0.0.1',
        port         => 0,
    );
    my $error = eval { $listener->accept; 1 } ? '' : $@;
    like($error, qr/must be attached to a Linux::Event loop/,
        'detached Listener must be attached before accept is armed');
    $listener->close;
}

done_testing;
