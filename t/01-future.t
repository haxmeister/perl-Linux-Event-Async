use v5.36;
use strict;
use warnings;
use Test::More;
use Linux::Event::Async::Future;

my $future = Linux::Event::Async::Future->new;
ok(!$future->AWAIT_IS_READY, 'new future is pending');

my $ready = 0;
$future->AWAIT_ON_READY(sub { $ready++ });
$future->AWAIT_DONE('ok');
ok($future->AWAIT_IS_READY, 'done future is ready');
is($ready, 1, 'ready callback fired once');
is($future->AWAIT_GET, 'ok', 'result returned');

my $failed = Linux::Event::Async::Future->AWAIT_NEW_FAIL('boom');
my $error = eval { $failed->AWAIT_GET; 1 } ? '' : $@;
like($error, qr/boom/, 'failed future throws');

my $cancelled = Linux::Event::Async::Future->new;
my $cancel_count = 0;
$cancelled->AWAIT_ON_CANCEL(sub { $cancel_count++ });
$cancelled->cancel;
ok($cancelled->AWAIT_IS_CANCELLED, 'cancelled future reports cancelled');
is($cancel_count, 1, 'cancel callback fired once');

{
    package T::CancelTarget;
    sub new ($class) { bless { cancelled => 0 }, $class }
    sub cancel ($self) { $self->{cancelled}++; return $self }
}

my $chained = Linux::Event::Async::Future->new;
my $same_target = T::CancelTarget->new;
my $other_target = T::CancelTarget->new;
$chained->AWAIT_CHAIN_CANCEL($same_target) for 1 .. 3;
$chained->AWAIT_CHAIN_CANCEL($other_target);
$chained->cancel;
is($same_target->{cancelled}, 1,
    'repeated cancellation chaining to one object is deduplicated');
is($other_target->{cancelled}, 1,
    'distinct cancellation targets remain chained');

done_testing;
