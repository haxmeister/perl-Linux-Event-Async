#!/usr/bin/env perl
use v5.36;
use strict;
use warnings;

use Getopt::Long qw(GetOptions);
use POSIX qw(_exit);
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

use Linux::Event::Loop;
use Linux::Event::Stream;
use Linux::Event::Async;

our $READ_SIZE = 262_144;

{
    package LEA::Bench::Callback;
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
    package LEA::Bench::Async;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";

    sub stream_options ($class) {
        return read_size => $main::READ_SIZE,
            read_budget_bytes => $main::READ_SIZE;
    }
}

async sub consume_async ($stream, $target) {
    my $count = 0;
    while ($count < $target) {
        my $message = await $stream->recv;
        die "unexpected EOF after $count messages" if !defined $message;
        $count++;
    }
    return $count;
}

sub consume_manual ($stream, $state) {
    my $ready;
    my $awaitable;
    $ready = sub {
        my $message = $awaitable->AWAIT_GET;
        die "unexpected EOF after $state->{count} messages" if !defined $message;
        $state->{count}++;
        if ($state->{count} == $state->{target}) {
            $state->{loop}->stop;
            return;
        }
        $awaitable = $stream->recv;
        $awaitable->AWAIT_ON_READY($ready);
        return;
    };
    $awaitable = $stream->recv;
    $awaitable->AWAIT_ON_READY($ready);
    return;
}

sub consume_manual_xs ($stream, $state) {
    my $ready;
    $ready = sub {
        my $message = Linux::Event::Async::Stream::_recv_get($stream);
        die "unexpected EOF after $state->{count} messages" if !defined $message;
        $state->{count}++;
        if ($state->{count} == $state->{target}) {
            $state->{loop}->stop;
            return;
        }
        Linux::Event::Async::Stream::_recv_arm($stream);
        Linux::Event::Async::Stream::_recv_on_ready($stream, $ready);
        return;
    };
    Linux::Event::Async::Stream::_recv_arm($stream);
    Linux::Event::Async::Stream::_recv_on_ready($stream, $ready);
    return;
}

my $sizes = '2500,35000,200000';
my $repeat = 7;
my $warmup = 2;
my $profile = 0;
my ($receiver_cpu, $producer_cpu);

GetOptions(
    'sizes=s'  => \$sizes,
    'repeat=i' => \$repeat,
    'warmup=i' => \$warmup,
    'profile!' => \$profile,
    'receiver-cpu=i' => \$receiver_cpu,
    'producer-cpu=i' => \$producer_cpu,
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

sub pin_cpu ($cpu) {
    return if !defined $cpu;
    my $target = $$;
    my $helper = fork();
    die "fork affinity helper: $!" if !defined $helper;
    if ($helper == 0) {
        open STDOUT, '>', '/dev/null' or _exit(126);
        open STDERR, '>&', \*STDOUT or _exit(126);
        {
            no warnings 'exec';
            exec 'taskset', '-pc', $cpu, $target;
            _exit(127);
        }
    }
    waitpid($helper, 0);
    die "cannot pin process $target to CPU $cpu\n" if $? != 0;
}

sub run_once ($kind, $payload_size, $messages) {
    pin_cpu($receiver_cpu);
    socketpair(my $receiver_fh, my $producer_fh,
        AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "socketpair: $!";
    pipe(my $barrier_r, my $barrier_w) or die "pipe: $!";

    my $pid = fork();
    die "fork: $!" if !defined $pid;
    if ($pid == 0) {
        pin_cpu($producer_cpu);
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
        ? 'LEA::Bench::Callback' : 'LEA::Bench::Async';
    my $receiver = $class->new(
        loop => $loop,
        fh   => $receiver_fh,
        data => $state,
    );
    Linux::Event::Async::Stream::_recv_profile_start($receiver)
        if $profile && $kind eq 'async';

    my $task;
    if ($kind eq 'async') {
        $task = consume_async($receiver, $messages);
    }
    elsif ($kind eq 'manual') {
        consume_manual($receiver, $state);
    }
    elsif ($kind eq 'manual_xs') {
        consume_manual_xs($receiver, $state);
    }

    my $started = clock_gettime(CLOCK_MONOTONIC);
    syswrite($barrier_w, 'G') == 1 or die "barrier release: $!";
    close $barrier_w;

    my $count;
    if ($task) {
        $count = $task->AWAIT_WAIT;
    } else {
        $loop->run;
        $count = $state->{count};
    }
    my $elapsed = clock_gettime(CLOCK_MONOTONIC) - $started;

    waitpid($pid, 0);
    die "producer failed with status $?" if $? != 0;
    die "$kind delivered $count of $messages messages at size $payload_size\n"
        if $count != $messages;

    my $recv_profile = $profile && $kind eq 'async'
        ? Linux::Event::Async::Stream::_recv_profile_stats($receiver)
        : undef;
    $receiver->close if !$receiver->is_closed;
    return wantarray ? ($elapsed, $recv_profile) : $elapsed;
}

my %historical = (
    2500 => {
        callback => 924_262,
        stream_awaitable => 170_677,
        direct_awaitable => 533_964,
    },
    35000 => {
        callback => 119_449,
        stream_awaitable => 86_258,
        direct_awaitable => 120_209,
    },
    200000 => {
        callback => 20_948,
        stream_awaitable => 22_780,
        direct_awaitable => 24_061,
    },
);

say "Linux::Event::Async::Stream receive-path isolation benchmark";
say "transport=AF_UNIX producer=forked barrier=yes read_size=$READ_SIZE read_budget_bytes=$READ_SIZE framing=native delimiter";
say 'affinity receiver=' . (defined $receiver_cpu ? $receiver_cpu : 'inherited')
    . ' producer=' . (defined $producer_cpu ? $producer_cpu : 'inherited');
printf "%-8s %10s %12s %12s %12s %12s %13s %10s\n",
    qw(payload messages callback manual manual_xs async direct_target retained);

for my $payload_size (@sizes) {
    my $messages = messages_for_size($payload_size);
    my @kinds = qw(callback manual manual_xs async);

    for (1 .. $warmup) {
        run_once($_, $payload_size, $messages) for @kinds;
    }

    my %samples = map { $_ => [] } @kinds;
    for my $sample (1 .. $repeat) {
        my $rotation = ($sample - 1) % @kinds;
        my @order = (@kinds[$rotation .. $#kinds], @kinds[0 .. $rotation - 1]);
        for my $kind (@order) {
            my ($elapsed, $recv_profile) = run_once(
                $kind, $payload_size, $messages);
            push $samples{$kind}->@*, $elapsed;
            if ($recv_profile) {
                say "profile payload=$payload_size sample=$sample "
                    . join(' ', map { "$_=$recv_profile->{$_}" }
                        sort keys $recv_profile->%*);
            }
        }
    }

    my %rate = map {
        $_ => $messages / median($samples{$_}->@*)
    } @kinds;
    my $old = $historical{$payload_size}
        or die "no historical comparison for payload $payload_size\n";
    my $retained = 100 * $rate{async} / $old->{direct_awaitable};

    printf "%-8d %10d %12.0f %12.0f %12.0f %12.0f %13d %9.1f%%\n",
        $payload_size, $messages,
        @rate{qw(callback manual manual_xs async)},
        $old->{direct_awaitable}, $retained;

    say "samples payload=$payload_size";
    for my $kind (@kinds) {
        say "  $kind " . join(' ', map { sprintf '%.6f', $_ }
            $samples{$kind}->@*);
    }
}

__END__

=head1 NAME

run-async-stream-native-bench.pl - isolate native Stream receive overhead

=head1 OPTIONS

=over

=item C<--profile>

Enable receive-protocol counters for the measured async samples. The report
includes FAA readiness checks, suspended awaits, continuation callbacks,
immediate receives, bounded-prefetch transfers, and native-drain flushes.

=item C<--receiver-cpu=N>, C<--producer-cpu=N>

Pin the benchmark receiver and forked producer to separate Linux CPUs with
C<taskset>. Affinity is established before the timing barrier.

=back
