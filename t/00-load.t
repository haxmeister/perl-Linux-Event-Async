use v5.36;
use strict;
use warnings;
use Test::More;

use_ok('Linux::Event::Async');
use_ok('Linux::Event::Async::Future');
use_ok('Linux::Event::Async::Stream');
use_ok('Linux::Event::Async::Listener');
use_ok('Linux::Event::Async::Dgram');
use_ok('Linux::Event::Async::Timer');
use_ok('Linux::Event::Async::Process');
use_ok('Linux::Event::Async::Signal');
use_ok('Linux::Event::Async::Event');

done_testing;
