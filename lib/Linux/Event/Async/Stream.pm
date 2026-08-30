package Linux::Event::Async::Stream;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::Stream';
use Carp qw(croak);
use Linux::Event::Async::Future ();

our $VERSION = '0.001_001';

require XSLoader;
XSLoader::load('Linux::Event::Async', $VERSION);

Linux::Event::Stream->_declare_consumer(
    __PACKAGE__,
    {
        provider           => \&_consumer_operations_address,
        abi_version        => 1,
        operations_address => _consumer_operations_address(),
    },
);

sub recv ($self) {
    return _recv_arm($self);
}

sub cancel_recv ($self) {
    _recv_cancel($self);
    return $self;
}

package Linux::Event::Async::Stream::Awaitable;

sub AWAIT_CHAIN_CANCEL ($self, $other) {
    $self->AWAIT_ON_CANCEL(sub {
        $other->cancel if $other && $other->can('cancel');
    });
    return $self;
}

sub AWAIT_CLONE ($self) {
    return Linux::Event::Async::Future->new(loop => $self->_stream->loop);
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->_stream->loop;
    Carp::croak 'cannot wait on a Stream that is not attached to a loop'
        if !$loop && !$self->AWAIT_IS_READY;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

package Linux::Event::Async::Stream;

1;

__END__

=head1 NAME

Linux::Event::Async::Stream - reusable receive awaitable for Linux::Event

=head1 SYNOPSIS

  package LineStream;
  use parent 'Linux::Event::Async::Stream';
  use Linux::Event::Framer 'Delimiter', "\n";

  package main;
  use Linux::Event::Async;

  async sub read_one ($stream) {
      return await $stream->recv;
  }

=head1 DESCRIPTION

Each Stream owns one native receive context and one persistent native Awaitable
view supplied through Linux::Event's versioned framed-message consumer ABI.
C<recv> arms that context and returns the same Awaitable view each time. No
Future or Awaitable is allocated per received message.

Only one receive may be active. Clean EOF resolves to C<undef>. Cancelling a
pending receive pauses consumer delivery without closing the Stream or consuming
the next message.

A concrete subclass must declare a built-in native framer. Callback delivery
through C<on_message> or C<on_messages> cannot be mixed with this consumer.

=cut
