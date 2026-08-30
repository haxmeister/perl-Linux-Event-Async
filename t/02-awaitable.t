use v5.36;
use strict;
use warnings;
use Test::More;
use Test::Future::AsyncAwait::Awaitable ();

use Linux::Event::Async::Future;

Test::Future::AsyncAwait::Awaitable::test_awaitable(
    'Linux::Event::Async::Future',
    class => 'Linux::Event::Async::Future',
);

done_testing;
