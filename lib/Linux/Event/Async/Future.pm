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
    return $self->AWAIT_GET if $self->AWAIT_IS_READY;
    my $loop = $self->loop
        // croak 'cannot wait without a Linux::Event loop';
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

sub CLONE_SKIP ($class) { 1 }

1;
