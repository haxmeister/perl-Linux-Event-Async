package Linux::Event::Async::Process;
use v5.36;
use strict;
use warnings;

use parent 'Linux::Event::Kernel::Process';
use Carp qw(croak);
use Scalar::Util qw(refaddr weaken);

use Linux::Event::Async::Future ();
use Linux::Event::Error ();

our $VERSION = '0.002';

my @CALLBACK = qw(
    on_exit on_stdout on_stderr on_stdout_eof on_stderr_eof
    on_stdin_drain on_error
);

sub _owns_exit_bridge ($class) {
    my $actual = $class->can('on_exit');
    my $owned = __PACKAGE__->can('on_exit');
    return $actual && $owned && refaddr($actual) == refaddr($owned);
}

sub _validate_async_class ($class, $method, $option) {
    croak "$class must not override on_exit(); await wait() is the Process completion sink"
        if !_owns_exit_bridge($class);

    for my $name (@CALLBACK) {
        croak "$method(): per-instance $name callbacks are not supported by Linux::Event::Async::Process; define reusable callbacks on the subclass"
            if exists $option->{$name};
    }
    return;
}

sub _async_initialize ($self) {
    $self->{_async_process_initialized} = 1;
    return $self;
}

sub new ($class, %option) {
    croak 'new(): must be called as a class method' if ref $class;
    _validate_async_class($class, 'new', \%option);
    my $self = $class->SUPER::new(%option);
    return $self->_async_initialize;
}

sub spawn ($class, %option) {
    croak 'spawn(): must be called as a class method' if ref $class;
    _validate_async_class($class, 'spawn', \%option);
    my $self = $class->SUPER::spawn(%option);
    return $self->_async_initialize;
}

sub _wait_failure ($self) {
    return $self->last_error // Linux::Event::Error->new(
        type      => 'process',
        operation => 'process_wait',
        message   => 'Process failed before normal exit completion',
    );
}

sub _finish_waiters ($self, $mode, $value) {
    my $waiters = delete $self->{_async_process_waiters} // [];
    my $failure;

    for my $future (@$waiters) {
        next if !$future || $future->is_ready;
        my $ok = eval {
            $mode eq 'done' ? $future->done($value) : $future->fail($value);
            1;
        };
        $failure //= $@ if !$ok;
    }
    return $failure;
}

sub wait ($self) {
    my $future = Linux::Event::Async::Future->new(loop => $self->loop);
    my $state = $self->state;

    return $future->done($self) if $state eq 'exited';
    return $future->fail($self->_wait_failure) if $state eq 'failed';
    croak 'wait(): Process must be attached to a Linux::Event loop'
        if !$self->loop;
    croak "wait(): Process is not running (state $state)"
        if $state ne 'running';

    push @{ $self->{_async_process_waiters} //= [] }, $future;
    weaken($self->{_async_process_waiters}[-1]);
    return $future;
}

sub on_exit ($self) {
    # Core sets state=exited and drains any remaining stdout/stderr before this
    # callback runs. Completing here therefore means process status and final
    # pipe output are both stable. Core still exposes loop() during this frame.
    my $failure = $self->_finish_waiters('done', $self);
    die $failure if defined($failure) && length($failure);
    return;
}

sub _report ($self, $error) {
    return $self->SUPER::_report($error)
        if !$self->{_async_process_initialized};

    my $core_failure;
    my $ok = eval { $self->SUPER::_report($error); 1 };
    $core_failure = $@ if !$ok;

    my $async_failure;
    if ($self->state eq 'failed') {
        $async_failure = $self->_finish_waiters('fail', $error);
    }

    die $core_failure if defined($core_failure) && length($core_failure);
    die $async_failure if defined($async_failure) && length($async_failure);
    return;
}

sub CLONE_SKIP ($class) { 1 }

1;

__END__

=head1 NAME

Linux::Event::Async::Process - awaitable Linux pidfd process completion

=head1 SYNOPSIS

  package WorkerProcess;
  use parent 'Linux::Event::Async::Process';

  sub process_options ($class) {
      return (
          read_size            => 65_536,
          max_reads_per_tick   => 64,
          stdin_high_watermark => 1_048_576,
          stdin_low_watermark  => 262_144,
          max_pending_stdin    => 0,
      );
  }

  sub on_stdout ($process, $bytes) {
      print $bytes;
  }

  sub on_error ($process, $error) {
      warn "$error\n";
  }

  package main;
  use Linux::Event::Async;
  use Linux::Event::Loop;

  my $loop = Linux::Event::Loop->new;
  my $process = WorkerProcess->spawn(
      loop    => $loop,
      command => [$^X, '-e', 'print "done\\n"'],
      stdout  => 'pipe',
  );

  async sub run ($process) {
      await $process->wait;
      return $process->exit_code;
  }

=head1 DESCRIPTION

C<Linux::Event::Async::Process> adds Awaitable process completion to
L<Linux::Event::Kernel::Process>. Process creation, pidfd ownership,
C<posix_spawnp>, signaling, standard I/O, exit decoding, and pipe fairness remain
Linux::Event core behavior.

C<wait> is a cold one-shot lifecycle operation and returns a
L<Linux::Event::Async::Future>. There is no specialized persistent Awaitable for
one Process exit because a Process has exactly one terminal completion.

Core invokes the reserved C<on_exit> bridge only after pidfd reaping and after
remaining stdout/stderr pipe data has been drained. Consequently a completed
C<wait> means exit status and final output callbacks are already settled.

=head1 SUBCLASSING AND PROCESS TUNING

Subclassing remains useful even though C<on_exit> is replaced by C<await
$process-E<gt>wait>. It provides reusable stdout/stderr/error behavior and one
cached place for Process I/O tuning.

The complete C<process_options> set is:

  sub process_options ($class) {
      return (
          read_size            =>    65_536,
          max_reads_per_tick   =>         64,
          stdin_high_watermark =>  1_048_576,
          stdin_low_watermark  =>    262_144,
          max_pending_stdin    =>          0,
      );
  }

These are Linux::Event defaults. C<read_size> is the maximum bytes delivered by
one stdout/stderr callback. C<max_reads_per_tick> bounds successful pipe reads
per readiness dispatch for fairness. The stdin high/low watermarks control
cooperative C<write_stdin> backpressure, while C<max_pending_stdin> is a hard
queued-byte limit when nonzero.

C<spawn> may override the same option names for one Process, retaining the core
validation and semantics.

=head1 WAITING FOR EXIT

=head2 wait

  my $process = WorkerProcess->spawn(
      loop    => $loop,
      command => ['/usr/bin/worker', '--once'],
  );

  await $process->wait;

C<wait> resolves with the same Process object after normal Process exit
completion. For a reaped child, C<exit_code>, C<term_signal>, C<core_dumped>, and
C<raw_status> are then stable. For C<reap =E<gt> 0> observation, completion still
reports termination but decoded status remains unavailable as in core.

Multiple Futures may wait for the same Process exit. Cancelling one Future is
wait-local: it does not signal, kill, detach, or otherwise alter the Process and
does not cancel other exit waiters.

Calling C<wait> after normal exit returns an immediately completed Future. A
Process that entered the core C<failed> state completes pending and later waits
with its C<last_error>, or a C<process>/C<process_wait> error if no more specific
error is available.

A Process must be attached and running before a pending wait can be created.
Detached construction remains useful:

  my $process = WorkerProcess->spawn(command => [$program]);
  $loop->add($process);
  await $process->wait;

=head1 OUTPUT CALLBACKS

Version 0.002 deliberately leaves stdout/stderr delivery on the mature callback
path:

  sub on_stdout ($process, $bytes) { ... }
  sub on_stdout_eof ($process) { ... }
  sub on_stderr ($process, $bytes) { ... }
  sub on_stderr_eof ($process) { ... }
  sub on_error ($process, $error) { ... }

Those callbacks may be defined on the Async Process subclass and coexist with
C<wait>. Final output is drained before the Process exit Future resumes.

Linux::Event can drain multiple pipe reads during one readiness turn. A future
pull-style stdout/stderr Awaitable therefore needs an explicit bounded buffering
or one-read fairness contract, analogous to the care taken for Listener accept
and Datagram receive. Version 0.002 does not silently change pipe batching just
to expose an C<await> spelling.

=head1 STANDARD INPUT

C<write_stdin>, C<close_stdin>, and C<pending_stdin_bytes> retain their core
behavior. C<write_stdin> may return false after accepted queued input crosses
the configured high watermark.

C<on_stdin_drain> is intentionally not exposed as an Async bridge in 0.002.
Core permits that callback only when C<stdin =E<gt> 'pipe'>. Reserving it on the
base Async Process would therefore force every Process to configure piped stdin,
including processes that only need exit notification. A future stdin-drain
Awaitable should use an optional public bridge rather than distort Process
construction semantics.

=head1 CALLBACK POLICY

C<on_exit> is reserved by C<Linux::Event::Async::Process> as the bridge to
C<wait> Futures. Concrete subclasses must not override it.

For compatibility with the minimum supported Linux::Event 0.110, per-instance
callback constructor options are not accepted by Async Process. Define reusable
C<on_stdout>, C<on_stderr>, EOF, C<on_stdin_drain>, and C<on_error> behavior on
the subclass when those core callbacks are needed. C<on_stdin_drain> remains a
normal core callback only on a concrete subclass that always uses piped stdin;
it is not connected to an Async wait in 0.002.

=head1 PROCESS CONTROL

C<signal($number)> remains the core pidfd-based signaling operation. There is no
Async cancellation-to-signal mapping: cancelling a C<wait> Future never sends a
signal. Process lifetime policy remains explicit application code.

=head1 SEE ALSO

L<Linux::Event::Kernel::Process>, L<Linux::Event::Async>,
L<Linux::Event::Async::Future>

=cut
