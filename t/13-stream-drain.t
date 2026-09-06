use v5.36;
use strict;
use warnings;
use Test::More;
use Scalar::Util qw(refaddr);
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC SOL_SOCKET SO_SNDBUF);

use Linux::Event::Loop;
use Linux::Event::IO::Sock::Stream;
use Linux::Event::Async;

{
    package T::DrainReader;
    use parent 'Linux::Event::IO::Sock::Stream';
    sub on_data ($stream, $bytes) {
        $stream->data->{received} += length($bytes);
        return;
    }
}

{
    package T::DrainWriter;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
    sub stream_options ($class) {
        return high_watermark => 4096, low_watermark => 1024;
    }
}

{
    package T::BadDrainWriter;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
    sub on_drain ($stream) { return }
}

sub pair ($with_loop = 1) {
    socketpair(my $a, my $b, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    setsockopt($a, SOL_SOCKET, SO_SNDBUF, pack('i', 4096))
        or die "setsockopt SO_SNDBUF: $!";

    my $loop = $with_loop ? Linux::Event::Loop->new : undef;
    my $state = { received => 0 };
    my $reader = T::DrainReader->new(
        ($loop ? (loop => $loop) : ()),
        fh   => $b,
        data => $state,
    );
    my $writer = T::DrainWriter->new(
        ($loop ? (loop => $loop) : ()),
        fh   => $a,
        data => $state,
    );
    return ($loop, $reader, $writer, $state);
}

sub close_stream ($stream) {
    $stream->close if $stream && !$stream->is_closed;
    return;
}

my $payload = 'x' x (2 * 1024 * 1024);

{
    my ($loop, $reader, $writer, $state) = pair();

    my $immediate = $writer->drain;
    ok($immediate->is_ready,
        'drain is immediately ready when Stream is not write-blocked');
    is(refaddr($immediate->get), refaddr($writer),
        'immediate drain resolves with the Stream');

    ok(!$writer->write($payload), 'large write crosses high watermark');
    ok($writer->is_write_blocked, 'write-blocked state is visible');

    my $drain = $writer->drain;
    ok(!$drain->is_ready, 'drain waits while output is write-blocked');
    my $result = $drain->AWAIT_WAIT;

    is(refaddr($result), refaddr($writer),
        'drain resolves with the same Stream');
    ok(!$writer->is_write_blocked,
        'drain completes after blocked period clears');

    close_stream($writer);
    close_stream($reader);
}

{
    my ($loop, $reader, $writer, $state) = pair();
    ok(!$writer->write($payload), 'writer blocks for multiple-waiter test');

    my $first = $writer->drain;
    my $second = $writer->drain;
    $first->AWAIT_WAIT;

    ok($second->is_ready,
        'all Futures waiting on one blocked period complete together');
    is(refaddr($second->get), refaddr($writer),
        'second drain waiter resolves with Stream');

    close_stream($writer);
    close_stream($reader);
}

{
    my ($loop, $reader, $writer, $state) = pair();
    ok(!$writer->write($payload), 'writer blocks for cancellation test');

    my $cancelled = $writer->drain;
    my $survivor = $writer->drain;
    $cancelled->cancel;

    ok($cancelled->is_cancelled, 'one drain waiter can be cancelled');
    ok(!$writer->is_closed,
        'cancelling drain waiter does not close Stream');
    ok($writer->pending_bytes > 0,
        'cancelling drain waiter does not discard queued output');

    $survivor->AWAIT_WAIT;
    ok(!$writer->is_write_blocked,
        'another waiter still observes the drain transition');

    close_stream($writer);
    close_stream($reader);
}

{
    my ($loop, $reader, $writer, $state) = pair();
    ok(!$writer->write($payload), 'writer blocks for reentrant-close test');
    my $drain = $writer->drain;
    $drain->on_ready(sub { $writer->close });
    my $result = $drain->AWAIT_WAIT;

    ok($writer->is_closed,
        'drain continuation may close Stream reentrantly');
    is(refaddr($result), refaddr($writer),
        'reentrant close does not retroactively fail completed drain');

    close_stream($reader);
}

{
    my ($loop, $reader, $writer, $state) = pair();
    ok(!$writer->write($payload), 'writer blocks before explicit close');
    my $drain = $writer->drain;
    $writer->close;

    ok($drain->is_ready,
        'closing blocked Stream completes pending drain Future');
    my $error;
    my $ok = eval { $drain->get; 1 };
    $error = $@ if !$ok;
    ok(!$ok, 'closed Stream drain throws');
    isa_ok($error, 'Linux::Event::Error');
    is($error->operation, 'drain',
        'explicit close failure identifies drain operation');

    close_stream($reader);
}

{
    my ($loop, $reader, $writer, $state) = pair(0);
    ok(!$writer->write($payload), 'detached writer becomes write-blocked');
    my $error = eval { $writer->drain; 1 } ? '' : $@;
    like($error, qr/blocked Stream must be attached/,
        'pending drain requires attached Linux::Event Loop');

    close_stream($writer);
    close_stream($reader);
}

{
    socketpair(my $a, my $b, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $error = eval { T::BadDrainWriter->new(fh => $a); 1 } ? '' : $@;
    like($error, qr/must not override on_drain/,
        'Async Stream reserves on_drain as the drain bridge');
    close $a if defined fileno($a);
    close $b if defined fileno($b);
}

{
    socketpair(my $a, my $b, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $error = eval {
        T::DrainWriter->new(fh => $a, on_drain => sub { return });
        1;
    } ? '' : $@;
    like($error, qr/on_drain is reserved/,
        'constructor on_drain callback cannot bypass async drain bridge');
    close $a if defined fileno($a);
    close $b if defined fileno($b);
}

done_testing;
