use v5.36;
use strict;
use warnings;
use Test::More;
use Socket qw(AF_UNIX SOCK_STREAM PF_UNSPEC);

use Linux::Event::Loop;
use Linux::Event::Async;

{
    package T::StressLine;
    use parent 'Linux::Event::Async::Stream';
    use Linux::Event::Framer 'Delimiter', "\n";
}

sub pair ($loop) {
    socketpair(my $stream_fh, my $peer, AF_UNIX, SOCK_STREAM, PF_UNSPEC)
        or die "socketpair: $!";
    my $stream = T::StressLine->new(loop => $loop, fh => $stream_fh);
    return ($stream, $peer);
}

sub close_pair ($stream, $peer) {
    $stream->close if $stream && !$stream->is_closed;
    close $peer if $peer;
}

sub with_timeout ($code) {
    local $SIG{ALRM} = sub { die "stress test timed out\n" };
    alarm 10;
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

async sub collect_messages ($stream, $count) {
    my @messages;
    for my $index (1 .. $count) {
        push @messages, await $stream->recv;
    }
    return \@messages;
}

async sub read_one ($stream) {
    return await $stream->recv;
}

async sub read_then_fail ($stream, $number) {
    await $stream->recv;
    die "stress failure $number\n";
}

async sub pass_through ($future) {
    return await $future;
}

subtest 'many tasks share one loop under shuffled readiness' => sub {
    my $loop = Linux::Event::Loop->new;
    my $stream_count = 32;
    my $messages_each = 20;
    my (@streams, @peers, @futures, @expected);

    for my $index (0 .. $stream_count - 1) {
        my ($stream, $peer) = pair($loop);
        push @streams, $stream;
        push @peers, $peer;
        my @messages = map { "stream-$index-message-$_" }
            1 .. $messages_each;
        push @expected, \@messages;
        push @futures, collect_messages($stream, $messages_each);
    }

    my $remaining = $stream_count;
    $_->on_ready(sub { $loop->stop if --$remaining == 0 }) for @futures;

    for my $position (0 .. $stream_count - 1) {
        my $index = ($position * 17) % $stream_count;
        my $wire = join '', map { "$_\n" } $expected[$index]->@*;
        syswrite($peers[$index], $wire) == length($wire)
            or die "short stress write for stream $index: $!";
    }

    with_timeout(sub { $loop->run });
    is($remaining, 0, 'every task reached its completion callback');
    for my $index (0 .. $stream_count - 1) {
        is_deeply($futures[$index]->get, $expected[$index],
            "stream $index retained ordered results");
        close_pair($streams[$index], $peers[$index]);
    }
};

subtest 'mixed success failure and cancellation remain isolated' => sub {
    my $loop = Linux::Event::Loop->new;
    my $task_count = 24;
    my (@streams, @peers, @futures, @kind);

    for my $index (0 .. $task_count - 1) {
        my ($stream, $peer) = pair($loop);
        push @streams, $stream;
        push @peers, $peer;
        if ($index % 5 == 0) {
            push @kind, 'cancel';
            push @futures, read_one($stream);
        }
        elsif ($index % 5 == 1) {
            push @kind, 'fail';
            push @futures, read_then_fail($stream, $index);
        }
        else {
            push @kind, 'done';
            push @futures, read_one($stream);
        }
    }

    my $remaining = $task_count;
    $_->on_ready(sub { $loop->stop if --$remaining == 0 }) for @futures;
    $futures[$_]->cancel for grep { $kind[$_] eq 'cancel' }
        0 .. $task_count - 1;

    for my $index (0 .. $task_count - 1) {
        my $wire = "value-$index\n";
        syswrite($peers[$index], $wire) == length($wire)
            or die "short mixed write for stream $index: $!";
    }

    with_timeout(sub { $loop->run }) if $remaining;
    is($remaining, 0, 'all mixed outcomes became ready');

    for my $index (0 .. $task_count - 1) {
        if ($kind[$index] eq 'cancel') {
            ok($futures[$index]->is_cancelled,
                "task $index reports cancellation");
            is(with_timeout(sub { read_one($streams[$index])->AWAIT_WAIT }),
                "value-$index", "task $index cancellation preserved input");
        }
        elsif ($kind[$index] eq 'fail') {
            my $error = eval { $futures[$index]->get; 1 } ? '' : $@;
            like($error, qr/stress failure $index/,
                "task $index reports its own failure");
        }
        else {
            is($futures[$index]->get, "value-$index",
                "task $index reports its own result");
        }
        close_pair($streams[$index], $peers[$index]);
    }
};

subtest 'completed generations cannot cancel later receives' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream, $peer) = pair($loop);
    my $prior;

    for my $generation (1 .. 100) {
        my $current = read_one($stream);
        $prior->cancel if $prior;
        my $wire = "generation-$generation\n";
        syswrite($peer, $wire) == length($wire)
            or die "short generation write: $!";
        is(with_timeout(sub { $current->AWAIT_WAIT }),
            "generation-$generation",
            "generation $generation survives stale Future cancellation");
        $prior = $current;
    }

    close_pair($stream, $peer);
};

subtest 'nested cancellation releases the currently awaited Stream' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream, $peer) = pair($loop);
    my $child = read_one($stream);
    my $middle = pass_through($child);
    my $parent = pass_through($middle);

    $parent->cancel;
    ok($parent->is_cancelled, 'outer task is cancelled');
    ok($middle->is_cancelled, 'cancellation reached the middle task');
    ok($child->is_cancelled, 'cancellation reached the child task');

    syswrite($peer, "preserved\n") == 10 or die "short nested write: $!";
    is(with_timeout(sub { read_one($stream)->AWAIT_WAIT }), 'preserved',
        'nested cancellation released the receive without consuming input');

    close_pair($stream, $peer);
};

subtest 'closing a Stream completes pending work without hanging' => sub {
    my $loop = Linux::Event::Loop->new;
    my ($stream, $peer) = pair($loop);
    my $future = read_one($stream);

    $stream->close;
    ok($future->is_ready, 'pending task becomes ready when Stream closes');
    my $error = eval { $future->get; 1 } ? '' : $@;
    like($error, qr/closed/i, 'pending task reports Stream closure');

    undef $stream;
    close $peer;
    pass('destroying the closed Stream with a retained Future is safe');
};

done_testing;
