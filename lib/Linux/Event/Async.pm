package Linux::Event::Async;
use v5.36;
use strict;
use warnings;

our $VERSION = '0.001_001';

use Future::AsyncAwait 0.71 ();
use Linux::Event::Async::Future ();
use Linux::Event::Async::Stream ();

sub import {
    @_ = ('Future::AsyncAwait', future_class => 'Linux::Event::Async::Future');
    goto &Future::AsyncAwait::import;
}

1;

__END__

=head1 NAME

Linux::Event::Async - async/await integration for Linux::Event

=head1 SYNOPSIS

  use Linux::Event::Async;

  async sub consume ($stream) {
      while (defined(my $message = await $stream->recv)) {
          ...
      }
  }

=head1 DESCRIPTION

Linux::Event::Async provides Future::AsyncAwait syntax and awaitable objects
above the callback-first Linux::Event reactor. It is a separate distribution
from Linux::Event and depends on Linux::Event's versioned native Stream
consumer ABI.

The first implementation focuses on C<Linux::Event::Async::Stream>. A Stream
uses one reusable receive awaitable rather than allocating one Future per
message. A separate C<Linux::Event::Async::Future> represents the result of an
entire C<async sub>.

=cut
