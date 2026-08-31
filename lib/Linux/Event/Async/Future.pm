package Linux::Event::Async::Future;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

our $VERSION = '0.001_001';

require XSLoader;
XSLoader::load(__PACKAGE__, $VERSION);

sub done ($self, @result) {
    return $self->AWAIT_DONE(@result);
}

sub fail ($self, $failure) {
    return $self->AWAIT_FAIL($failure);
}

sub get ($self) {
    return $self->AWAIT_GET;
}

sub is_ready ($self) {
    return $self->AWAIT_IS_READY;
}

sub is_cancelled ($self) {
    return $self->AWAIT_IS_CANCELLED;
}

sub on_ready ($self, $callback) {
    $self->AWAIT_ON_READY($callback);
    return $self;
}

sub on_cancel ($self, $callback) {
    $self->AWAIT_ON_CANCEL($callback);
    return $self;
}

sub AWAIT_WAIT ($self) {
    while (!$self->AWAIT_IS_READY) {
        my $loop = $self->loop
            // croak 'cannot wait without a Linux::Event loop';
        $loop->run_once(-1);
    }
    return $self->AWAIT_GET;
}

sub CLONE_SKIP ($class) { 1 }

1;

__END__

=head1 NAME

Linux::Event::Async::Future - async-sub result Future for Linux::Event

=head1 DESCRIPTION

This native Future implements the Awaitable protocol used by
Future::AsyncAwait. It represents the result of an entire C<async sub>; Stream
receives use a separate persistent Awaitable and do not allocate one Future per
message.

=head2 AWAIT_WAIT

Drives one C<run_once(-1)> operation at a time on the Linux::Event Loop
associated with the operation currently awaited by the async sub. Effective
Loop lookup follows nested child Futures and is repeated between dispatches, so
sequential awaits may safely move between different Loops.

C<AWAIT_WAIT> returns or throws with the same semantics as C<AWAIT_GET>.

Calling C<< $loop->run >> directly is different: the core Loop continues until
C<stop> is called and does not stop automatically when Futures become ready.

=cut
