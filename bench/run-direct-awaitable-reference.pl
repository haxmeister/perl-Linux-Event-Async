#!/usr/bin/env perl
use v5.36;
use strict;
use warnings;

use Getopt::Long qw(GetOptions);
use POSIX qw(_exit);
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
use XSLoader;

use Linux::Event;
use Linux::Event::Loop;
use Linux::Event::Stream;

XSLoader::load('Linux::Event::DirectAwaitable', $Linux::Event::VERSION);

our $READ_SIZE = 262_144;

{
    package LEA::Reference::Callback;
    use parent 'Linux::Event::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";

    sub stream_options ($class) {
        return read_size => $main::READ_SIZE,
            read_budget_bytes => $main::READ_SIZE;
    }

    sub on_message ($stream, $message) {
        my $state = $stream->data;
        $state->{count}++;
        $state->{loop}->stop if $state->{count} == $state->{target};
        return;
    }
}

{
    package LEA::Reference::Direct;
    use parent 'Linux::Event::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";

    sub stream_options ($class) {
        return read_size => $main::READ_SIZE,
            read_budget_bytes => $main::READ_SIZE;
    }
}

async sub consume_direct ($stream, $target) {
    my $count = 0;
    while ($count < $target) {
        my $awaitable = Linux::Event::DirectAwaitable->_recv_stream_state(
            $stream->{xs_state});
        my $message = await $awaitable;
        die "unexpected EOF after $count messages" if !defined $message;
        $count++;
    }
    return $count;
}

my $sizes = '2500,35000,200000';
my $repeat = 7;
my $warmup = 2;

GetOptions(
    'sizes=s'  => \$sizes,
    'repeat=i' => \$repeat,
    'warmup=i' => \$warmup,
) or die "invalid options\n";

die "repeat must be positive\n" if $repeat < 1;
die "warmup must be non-negative\n" if $warmup < 0;

my @sizes = map { 0 + $_ } split /,/, $sizes;
die "sizes must contain positive integers\n"
    if !@sizes || grep { $_ < 1 } @sizes;

sub messages_for_size ($size) {
    return 100_000 if $size <= 2_500;
    return 20_000 if $size <= 35_000;
    return 5_000;
}

sub median (@values) {
    @values = sort { $a <=> $b } @values;
    return $values[int(@values / 2)];
}

sub write_all ($fh, $bytes) {
    my $offset = 0;
    my $length = length $bytes;
    while ($offset < $length) {
        my $written = syswrite($fh, $bytes, $length - $offset, $offset);
        die "producer write: $!" if !defined $written;
        $offset += $written;
    }
}

sub run_once ($kind, $payload_size, $messages) {
    socketpair(my $receiver_fh, my $producer_fh,
        AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "socketpair: $!";
    pipe(my $barrier_r, my $barrier_w) or die "pipe: $!";

    my $pid = fork();
    die "fork: $!" if !defined $pid;
    if ($pid == 0) {
        close $receiver_fh;
        close $barrier_w;
        my $go = '';
        sysread($barrier_r, $go, 1) == 1 or _exit(2);
        close $barrier_r;

        my $frame = ('x' x $payload_size) . "\n";
        my $batch_frames = $payload_size <= 2_500 ? 64
            : $payload_size <= 35_000 ? 8 : 1;
        my $batch = $frame x $batch_frames;
        my $remaining = $messages;
        eval {
            while ($remaining >= $batch_frames) {
                write_all($producer_fh, $batch);
                $remaining -= $batch_frames;
            }
            write_all($producer_fh, $frame x $remaining) if $remaining;
            1;
        } or _exit(3);
        close $producer_fh;
        _exit(0);
    }

    close $producer_fh;
    close $barrier_r;

    my $loop = Linux::Event::Loop->new;
    my $state = {
        count  => 0,
        loop   => $loop,
        target => $messages,
    };
    my $class = $kind eq 'callback'
        ? 'LEA::Reference::Callback' : 'LEA::Reference::Direct';
    my $receiver = $class->new(
        loop => $loop,
        fh   => $receiver_fh,
        data => $state,
    );
    my $task = $kind eq 'direct'
        ? consume_direct($receiver, $messages) : undef;

    my $started = clock_gettime(CLOCK_MONOTONIC);
    syswrite($barrier_w, 'G') == 1 or die "barrier release: $!";
    close $barrier_w;

    my $count;
    if ($task) {
        $count = $loop->run($task);
    }
    else {
        $loop->run;
        $count = $state->{count};
    }
    my $elapsed = clock_gettime(CLOCK_MONOTONIC) - $started;

    waitpid($pid, 0);
    die "producer failed with status $?" if $? != 0;
    die "$kind delivered $count of $messages messages at size $payload_size\n"
        if $count != $messages;

    $receiver->close if !$receiver->is_closed;
    return $elapsed;
}

say "Historical Direct Awaitable matched reference";
say "transport=AF_UNIX producer=forked barrier=yes read_size=$READ_SIZE read_budget_bytes=$READ_SIZE framing=native delimiter";
printf "%-8s %10s %13s %13s %10s\n",
    qw(payload messages callback direct direct_pct);

for my $payload_size (@sizes) {
    my $messages = messages_for_size($payload_size);
    my @kinds = qw(callback direct);

    for (1 .. $warmup) {
        run_once($_, $payload_size, $messages) for @kinds;
    }

    my %samples = map { $_ => [] } @kinds;
    for my $sample (1 .. $repeat) {
        my @order = $sample % 2 ? @kinds : reverse @kinds;
        for my $kind (@order) {
            push $samples{$kind}->@*, run_once($kind, $payload_size, $messages);
        }
    }

    my %rate = map {
        $_ => $messages / median($samples{$_}->@*)
    } @kinds;

    printf "%-8d %10d %13.0f %13.0f %9.1f%%\n",
        $payload_size, $messages, $rate{callback}, $rate{direct},
        100 * $rate{direct} / $rate{callback};

    say "samples payload=$payload_size";
    for my $kind (@kinds) {
        say "  $kind " . join(' ', map { sprintf '%.6f', $_ }
            $samples{$kind}->@*);
    }
}
