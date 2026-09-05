use v5.36;
use strict;
use warnings;

use Test::More;
use Scalar::Util qw(refaddr);
use Socket qw(
    AF_INET AF_UNIX SOCK_DGRAM PF_UNSPEC SOL_SOCKET SO_SNDBUF
    inet_aton pack_sockaddr_in
);

use Linux::Event::Loop;
use Linux::Event::IO::Sock::Dgram;
use Linux::Event::Async;
use Linux::Event::Async::Dgram;

our @ORDER;
our @ERRORS;
our $PHASE = 'initialization';
$SIG{ALRM} = sub { die "Datagram regression timed out during $PHASE\n" };
alarm 30;

{
    package T::AsyncDgram;
    use parent 'Linux::Event::Async::Dgram';
    sub on_ready ($socket) { push @main::ORDER, 'ready-callback' }
    sub on_error ($socket, $error) {
        push @main::ERRORS, $error;
        push @main::ORDER, 'error-callback';
    }
}

{
    package T::TinyAsyncDgram;
    use parent 'Linux::Event::Async::Dgram';
    sub datagram_options ($class) { return max_datagram_size => 4 }
    sub on_error ($socket, $error) {
        push @main::ERRORS, $error;
        push @main::ORDER, 'error-callback';
    }
}

{
    package T::DrainAsyncDgram;
    use parent 'Linux::Event::Async::Dgram';
    sub datagram_options ($class) {
        return high_watermark => 4096, low_watermark => 1024;
    }
}

{
    package T::DrainReader;
    use parent 'Linux::Event::IO::Sock::Dgram';
    sub on_datagram ($socket, $payload, $peer) {
        $socket->data->{received}++;
        return;
    }
}

{
    package T::BadAsyncDgram;
    use parent 'Linux::Event::Async::Dgram';
    sub on_datagram ($socket, $payload, $peer) { return }
}

sub thrown ($code) {
    my $error;
    my $ok = eval { $code->(); 1 };
    $error = $@ if !$ok;
    return ($ok, $error);
}

sub close_dgram ($socket) {
    $socket->close if $socket && !$socket->is_terminal;
    return;
}

{
    $PHASE = 'bound readiness';
    @ORDER = ();
    @ERRORS = ();
    my $loop = Linux::Event::Loop->new;
    my $server = T::AsyncDgram->new(
        loop => $loop,
        host => '127.0.0.1',
        port => 0,
    );

    my $ready = $server->ready;
    ok(!$ready->is_ready, 'bound Datagram readiness starts pending');
    $ready->on_ready(sub { push @ORDER, 'ready-future' });
    my $result = $ready->AWAIT_WAIT;

    is(refaddr($result), refaddr($server),
        'Datagram ready resolves with the same socket');
    is_deeply(\@ORDER, [qw(ready-callback ready-future)],
        'subclass on_ready runs before readiness Future resumes');
    is_deeply(\@ERRORS, [], 'bound readiness reports no errors');
    ok($server->is_read_paused,
        'Async Datagram starts with pull receive paused');

    my $again = $server->ready;
    ok($again->is_ready, 'ready is historical after readiness fires');
    is(refaddr($again->get), refaddr($server),
        'historical ready still returns the socket');

    close_dgram($server);
}

{
    $PHASE = 'ordered UDP pull receive';
    @ERRORS = ();
    my $loop = Linux::Event::Loop->new;
    my $server = T::AsyncDgram->new(
        loop => $loop,
        host => '127.0.0.1',
        port => 0,
    );
    $server->ready->AWAIT_WAIT;

    my $client = T::AsyncDgram->connect(
        loop => $loop,
        host => 'localhost',
        port => $server->local->port,
    );
    $client->ready->AWAIT_WAIT;
    ok($client->is_connected,
        'connected Async Datagram reaches application readiness after resolver');

    ok($client->send('one'), 'first UDP packet sends');
    ok($client->send('two'), 'second UDP packet sends');
    ok($client->send('three'), 'third UDP packet sends');

    my $first_awaitable = $server->recv;
    my ($one, $peer) = $first_awaitable->AWAIT_WAIT;
    is($one, 'one', 'first pull receive preserves first packet');
    isa_ok($peer, 'Linux::Event::Address');
    cmp_ok($peer->port, '>', 0, 'pull receive returns peer address');

    my $second_awaitable = $server->recv;
    is(refaddr($second_awaitable), refaddr($first_awaitable),
        'Datagram receive reuses one persistent Awaitable');
    is(scalar $second_awaitable->AWAIT_WAIT, 'two',
        'scalar Datagram receive returns payload as first result');
    is(scalar $server->recv->AWAIT_WAIT, 'three',
        'packets queued while no wait was armed remain in kernel order');
    ok($server->is_read_paused,
        'read interest pauses again after pull receive is consumed');
    is_deeply(\@ERRORS, [], 'pull UDP exchange reports no errors');

    close_dgram($client);
    close_dgram($server);
}

{
    $PHASE = 'receive cancellation';
    my $loop = Linux::Event::Loop->new;
    my $server = T::AsyncDgram->new(
        loop => $loop,
        host => '127.0.0.1',
        port => 0,
    );
    my $client = T::AsyncDgram->connect(
        loop => $loop,
        host => '127.0.0.1',
        port => $server->local->port,
    );
    $server->ready->AWAIT_WAIT;
    $client->ready->AWAIT_WAIT;

    my $cancelled = $server->recv;
    $server->cancel_recv;
    ok($cancelled->AWAIT_IS_CANCELLED,
        'cancel_recv cancels the current Datagram wait');
    ok(!$server->is_terminal,
        'cancel_recv leaves Datagram socket active');
    ok($server->is_read_paused,
        'cancel_recv restores pull-read pause');

    $client->send('after-cancel');
    is(scalar $server->recv->AWAIT_WAIT, 'after-cancel',
        'cancelled receive does not consume the next packet');

    close_dgram($client);
    close_dgram($server);
}

{
    $PHASE = 'oversized receive error';
    @ORDER = ();
    @ERRORS = ();
    my $loop = Linux::Event::Loop->new;
    my $server = T::TinyAsyncDgram->new(
        loop => $loop,
        host => '127.0.0.1',
        port => 0,
    );
    $server->ready->AWAIT_WAIT;

    socket(my $raw, AF_INET, SOCK_DGRAM, 0) or die "socket: $!";
    my $destination = pack_sockaddr_in(
        $server->local->port, inet_aton('127.0.0.1'),
    );

    my $recv = $server->recv;
    $recv->AWAIT_ON_READY(sub { push @ORDER, 'recv-future' });
    CORE::send($raw, 'oversized', 0, $destination)
        or die "send oversized datagram: $!";

    my ($ok, $error) = thrown(sub { $recv->AWAIT_WAIT });
    ok(!$ok, 'oversized Datagram fails current receive');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'receive',
        'oversized receive preserves core receive operation');
    is_deeply(\@ORDER, [qw(error-callback recv-future)],
        'on_error runs before failed receive resumes');
    is(scalar @ERRORS, 1, 'receive error is reported exactly once');

    CORE::close($raw);
    close_dgram($server);
}

{
    $PHASE = 'pre-ready close';
    my $loop = Linux::Event::Loop->new;
    my $server = T::AsyncDgram->new(
        loop => $loop,
        host => '127.0.0.1',
        port => 0,
    );
    my $ready = $server->ready;
    $server->close;

    ok($ready->is_ready,
        'close before ready completes pending readiness Future');
    my ($ok, $error) = thrown(sub { $ready->get });
    ok(!$ok, 'close before ready fails readiness Future');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'dgram_ready',
        'explicit pre-ready close identifies dgram_ready');
}

{
    $PHASE = 'Datagram output drain';
    socketpair(my $a, my $b, AF_UNIX, SOCK_DGRAM, PF_UNSPEC)
        or die "socketpair: $!";
    setsockopt($a, SOL_SOCKET, SO_SNDBUF, pack('i', 4096))
        or die "setsockopt SO_SNDBUF: $!";

    my $loop = Linux::Event::Loop->new;
    my $state = { received => 0 };
    my $reader = T::DrainReader->new(
        loop => $loop,
        fh   => $b,
        data => $state,
    );
    my $writer = T::DrainAsyncDgram->new(
        loop => $loop,
        fh   => $a,
    );

    $writer->ready->AWAIT_WAIT;

    my $blocked = 0;
    for (1 .. 10_000) {
        my $accepted = $writer->send('x' x 1024);
        last if !defined $accepted;
        if (!$accepted) {
            $blocked = 1;
            last;
        }
    }
    ok($blocked, 'Datagram writer crosses high-watermark under backpressure');

    my $cancelled = $writer->drain;
    my $survivor = $writer->drain;
    $cancelled->cancel;
    ok($cancelled->is_cancelled,
        'one Datagram drain waiter can be cancelled');
    ok(!$writer->is_terminal,
        'drain waiter cancellation leaves Datagram socket active');

    my $result = $survivor->AWAIT_WAIT;
    is(refaddr($result), refaddr($writer),
        'Datagram drain resolves with same socket');
    cmp_ok($state->{received}, '>', 0,
        'peer consumption makes queued Datagram output progress');

    close_dgram($writer);
    close_dgram($reader);
}

{
    $PHASE = 'construction policy';
    my ($ok, $error) = thrown(sub {
        T::BadAsyncDgram->new(
            host => '127.0.0.1',
            port => 0,
        );
    });
    ok(!$ok, 'Async Datagram subclass cannot replace reserved on_datagram');
    like("$error", qr/must not override on_datagram/,
        'reserved on_datagram error is explicit');

    ($ok, $error) = thrown(sub {
        T::AsyncDgram->new(
            host                   => '127.0.0.1',
            port                   => 0,
            max_datagrams_per_tick => 2,
        );
    });
    ok(!$ok, 'Async Datagram rejects batched pull receive');
    like("$error", qr/requires max_datagrams_per_tick => 1/,
        'batched receive rejection explains required bound');

    ($ok, $error) = thrown(sub {
        T::AsyncDgram->new(
            host           => '127.0.0.1',
            port           => 0,
            edge_triggered => 1,
        );
    });
    ok(!$ok, 'Async Datagram rejects edge-triggered pull receive');
    like("$error", qr/does not support edge_triggered/,
        'edge-trigger rejection is explicit');
}

alarm 0;
done_testing;
