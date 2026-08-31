use v5.36;
use strict;
use warnings;
use Test::More;
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);

use Linux::Event::Loop;
use Linux::Event::Async;

{
    package T::GlobalDestructionLine;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
}

sub pair ($loop) {
    socketpair(my $stream_fh, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $stream = T::GlobalDestructionLine->new(
        loop => $loop,
        fh   => $stream_fh,
    );
    return ($stream, $peer);
}

async sub read_one ($stream) {
    return await $stream->recv;
}

async sub await_both ($first, $second) {
    my $one = await $first;
    my $two = await $second;
    return "$one:$two";
}

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
is($combined->AWAIT_WAIT, 'one:two',
    'cross-loop task completes before global destruction');

done_testing;

# Deliberately leave both loops, Streams, peers, and Futures alive. This file
# must exit cleanly when Perl destroys those objects in global-destruction
# order rather than through explicit Stream close calls.
