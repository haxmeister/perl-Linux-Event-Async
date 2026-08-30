package Linux::Event::Async::Future;
use v5.36;
use strict;
use warnings;

use Carp qw(croak);

our $VERSION = '0.001_001';

sub new ($class, %opt) {
    return bless {
        loop       => delete $opt{loop},
        state      => 'pending',
        result     => undef,
        error      => undef,
        on_ready   => [],
        on_cancel  => [],
        cancel     => [],
    }, $class;
}

sub AWAIT_NEW_DONE ($class, @result) {
    my $self = $class->new;
    $self->AWAIT_DONE(@result);
    return $self;
}

sub AWAIT_NEW_FAIL ($class, $error) {
    my $self = $class->new;
    $self->{state} = 'failed';
    $self->{error} = $error;
    $self->_fire_ready;
    return $self;
}

sub AWAIT_CLONE ($self) {
    return ref($self)->new(loop => $self->{loop});
}

sub AWAIT_DONE ($self, @result) {
    croak 'future is not pending' if $self->{state} ne 'pending';
    $self->{state} = 'done';
    $self->{result} = \@result;
    $self->_fire_ready;
    return $self;
}

sub AWAIT_FAIL ($self, $error) {
    croak 'future is not pending' if $self->{state} ne 'pending';
    $self->{state} = 'failed';
    $self->{error} = $error;
    $self->_fire_ready;
    return $self;
}

sub AWAIT_IS_READY ($self) { $self->{state} ne 'pending' }
sub AWAIT_IS_CANCELLED ($self) { $self->{state} eq 'cancelled' }

sub AWAIT_GET ($self) {
    if ($self->{state} eq 'failed') {
        die $self->{error} if ref $self->{error};
        croak defined($self->{error}) ? $self->{error} : '';
    }
    croak 'cannot get a pending future' if $self->{state} eq 'pending';
    croak 'cannot get a cancelled future' if $self->{state} eq 'cancelled';
    return wantarray ? @{$self->{result}} : $self->{result}[0];
}

sub AWAIT_ON_READY ($self, $code) {
    croak 'AWAIT_ON_READY requires a coderef' if ref($code) ne 'CODE';
    if ($self->AWAIT_IS_READY) {
        $code->($self);
    }
    else {
        push @{$self->{on_ready}}, $code;
    }
    return $self;
}

sub AWAIT_CHAIN_CANCEL ($self, $other) {
    push @{$self->{cancel}}, $other if $self->{state} eq 'pending';
    return $self;
}

sub AWAIT_ON_CANCEL ($self, $code) {
    croak 'AWAIT_ON_CANCEL requires a coderef' if ref($code) ne 'CODE';
    if ($self->{state} eq 'cancelled') {
        $code->($self);
    }
    elsif ($self->{state} eq 'pending') {
        push @{$self->{on_cancel}}, $code;
    }
    return $self;
}

sub cancel ($self) {
    return $self if $self->{state} ne 'pending';
    $self->{state} = 'cancelled';
    for my $other (@{delete($self->{cancel}) // []}) {
        next if !$other;
        if ($other->can('cancel')) {
            $other->cancel;
        }
        elsif ($other->can('cancel_recv')) {
            $other->cancel_recv;
        }
    }
    $_->($self) for @{delete($self->{on_cancel}) // []};
    $self->_fire_ready;
    return $self;
}

sub AWAIT_WAIT ($self) {
    my $loop = $self->{loop};
    croak 'cannot wait without a Linux::Event loop'
        if !$self->AWAIT_IS_READY && !$loop;
    $loop->run_once(-1) while !$self->AWAIT_IS_READY;
    return $self->AWAIT_GET;
}

sub _fire_ready ($self) {
    my $callbacks = delete($self->{on_ready}) // [];
    $_->($self) for @$callbacks;
    return;
}

1;
