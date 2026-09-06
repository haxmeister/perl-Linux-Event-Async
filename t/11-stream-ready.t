use v5.36;
use strict;
use warnings;
use Test::More;
use Scalar::Util qw(refaddr);
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);

use Linux::Event::Loop;
use Linux::Event::IO::Sock::Listener;
use Linux::Event::Async;

{
    package T::AsyncReadyServer;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
}

{
    package T::AsyncReadyClient;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";

    sub on_ready ($stream) {
        push @{ $stream->data->{order} }, 'callback';
        return;
    }
}

{
    package T::AsyncCloseOnReady;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";

    sub on_ready ($stream) {
        push @{ $stream->data->{order} }, 'callback';
        $stream->close;
        return;
    }
}

{
    package T::AsyncReadyListener;
    use parent 'Linux::Event::IO::Sock::Listener';

    sub on_accept ($listener, $stream) {
        push @{ $listener->data->{accepted} }, $stream;
        return;
    }
}

sub tcp_listener ($loop, $state) {
    return T::AsyncReadyListener->new(
        loop         => $loop,
        stream_class => 'T::AsyncReadyServer',
        host         => '127.0.0.1',
        port         => 0,
        data         => $state,
    );
}

sub close_state ($listener, $client, $state) {
    $client->close if $client && !$client->is_closed;
    for my $stream (@{ $state->{accepted} // [] }) {
        $stream->close if $stream && !$stream->is_closed;
    }
    $listener->close if $listener && !$listener->is_terminal;
    return;
}

{
    socketpair(my $stream_fh, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $stream = T::AsyncReadyServer->new(fh => $stream_fh);
    my $ready = $stream->ready;

    isa_ok($ready, 'Linux::Event::Async::Future');
    ok($ready->is_ready,
        'directly adopted plain connected Stream is immediately ready');
    is(refaddr($ready->get), refaddr($stream),
        'immediate readiness Future resolves with the Stream');

    $stream->close;
    close $peer;
}

{
    my $loop = Linux::Event::Loop->new;
    my $state = { accepted => [], order => [] };
    my $listener = tcp_listener($loop, $state);
    my $client = T::AsyncReadyClient->connect(
        loop => $loop,
        host => '127.0.0.1',
        port => $listener->port,
        data => $state,
    );
    my $ready = $client->ready;

    ok(!$ready->is_ready, 'outbound readiness starts pending');
    $ready->on_ready(sub {
        push @{ $state->{order} }, 'future';
        $loop->stop;
    });
    $loop->run;

    ok($ready->is_ready, 'outbound connection completes readiness Future');
    is(refaddr($ready->get), refaddr($client),
        'outbound readiness resolves with the same Stream');
    is_deeply($state->{order}, [qw(callback future)],
        'core on_ready callback runs before readiness Future resumes');
    ok(@{ $state->{accepted} } >= 1, 'server accepted the connection');

    my $again = $client->ready;
    ok($again->is_ready, 'readiness is a persistent one-shot outcome');
    is(refaddr($again->get), refaddr($client),
        'later readiness observation still resolves with the Stream');

    close_state($listener, $client, $state);
}

{
    my $loop = Linux::Event::Loop->new;
    my $state = { accepted => [], order => [] };
    my $listener = tcp_listener($loop, $state);
    my $client = T::AsyncReadyServer->connect(
        loop => $loop,
        host => '127.0.0.1',
        port => $listener->port,
    );

    async sub wait_ready ($stream) {
        return await $stream->ready;
    }

    my $first = wait_ready($client);
    $first->cancel;
    ok($first->is_cancelled,
        'cancelling async sub cancels its readiness wait');
    ok(!$client->is_closed,
        'cancelling readiness wait does not close the Stream');

    my $retry = $client->ready;
    $retry->on_ready(sub { $loop->stop });
    $loop->run;
    is(refaddr($retry->get), refaddr($client),
        'a new readiness waiter succeeds after earlier cancellation');

    close_state($listener, $client, $state);
}

{
    my $loop = Linux::Event::Loop->new;
    my $state = { accepted => [], order => [] };
    my $listener = tcp_listener($loop, $state);
    my $client = T::AsyncCloseOnReady->connect(
        loop => $loop,
        host => '127.0.0.1',
        port => $listener->port,
        data => $state,
    );
    my $ready = $client->ready;
    $ready->on_ready(sub {
        push @{ $state->{order} }, 'future';
        $loop->stop;
    });
    $loop->run;

    ok($client->is_closed,
        'core on_ready callback may close Stream reentrantly');
    ok($ready->is_ready && !$ready->is_cancelled,
        'reentrant close does not retroactively fail readiness');
    is(refaddr($ready->get), refaddr($client),
        'reentrant-close readiness still resolves with original Stream');
    is_deeply($state->{order}, [qw(callback future)],
        'readiness Future resumes after reentrant on_ready callback');

    close_state($listener, $client, $state);
}

{
    my $loop = Linux::Event::Loop->new;
    my $state = { accepted => [], order => [] };
    my $listener = tcp_listener($loop, $state);
    my $client = T::AsyncReadyServer->connect(
        loop => $loop,
        host => '127.0.0.1',
        port => $listener->port,
    );
    my $ready = $client->ready;
    $client->close;

    ok($ready->is_ready, 'explicit close completes pending readiness waiter');
    my $error = eval { $ready->get; undef } // $@;
    isa_ok($error, 'Linux::Event::Error');
    is($error->type, 'event', 'explicit pre-ready close uses event error type');
    is($error->operation, 'ready',
        'explicit pre-ready close identifies readiness operation');

    close_state($listener, $client, $state);
}

{
    my $loop = Linux::Event::Loop->new;
    my $state = { accepted => [], order => [] };
    my $listener = tcp_listener($loop, $state);
    my $port = $listener->port;
    $listener->close;

    my $client = T::AsyncReadyServer->connect(
        loop    => $loop,
        host    => '127.0.0.1',
        port    => $port,
        timeout => 1,
    );
    my $ready = $client->ready;
    my $error;
    my $ok = eval { $ready->AWAIT_WAIT; 1 };
    $error = $@ if !$ok;

    ok(!$ok, 'connection failure throws through readiness Future');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'connect',
        'readiness failure preserves core connect error');
    ok($client->is_closed, 'failed outbound Stream is closed');
}

{
    my $stream = T::AsyncReadyServer->connect(
        host => '127.0.0.1',
        port => 9,
    );
    my $error = eval { $stream->ready; 1 } ? '' : $@;
    like($error, qr/pending Stream must be attached/,
        'pending detached Stream must be attached before readiness wait');
    $stream->close;
}

done_testing;
