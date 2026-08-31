# Linux::Event::Async

`Linux::Event::Async` is a separate distribution providing
Future::AsyncAwait-compatible async/await support above Linux::Event.

The initial implementation concentrates on `Linux::Event::Async::Stream`.
A framed Stream owns one persistent native receive Awaitable view. Calling
`recv` arms the receive context in XS and returns that same view; receiving a
message does not allocate a Future or Awaitable. The Future allocated by
Future::AsyncAwait represents the result of the entire `async sub` and is
implemented by `Linux::Event::Async::Future`.

```perl
package LineStream;
use parent 'Linux::Event::Async::Stream';
use Linux::Event::Framer 'Delimiter', "\n";

package main;
use Linux::Event::Async;

async sub consume ($stream) {
    while (defined(my $line = await $stream->recv)) {
        ...
    }
}
```

## Driving concurrent work

Calling an `async sub` starts it immediately and returns its result Future. Two
tasks created before either is awaited therefore remain concurrent while
`AWAIT_WAIT` drives their associated Loop:

```perl
my $left  = consume($left_stream);
my $right = consume($right_stream);

my $left_result  = $left->AWAIT_WAIT;
my $right_result = $right->AWAIT_WAIT;
```

`AWAIT_WAIT` follows the operation currently awaited by the async sub, including
when sequential operations belong to different Linux::Event Loops and when
that transition occurs inside a nested child async sub.

`$loop->run` retains the Linux::Event core contract: it runs until `stop` is
called. Completion of one or more async-sub Futures does not implicitly stop a
shared Loop because unrelated watchers or tasks may still be active. Code that
drives `run` directly must provide an explicit completion condition:

```perl
my $remaining = 2;
my $finished = sub { $loop->stop if --$remaining == 0 };

$left->on_ready($finished);
$right->on_ready($finished);
$loop->run;
```

Only one receive may be pending on a Stream. Starting a second reader on the
same Stream produces a failed async-sub Future without invalidating the first
pending reader.

## Development dependency

This development branch targets the Linux::Event native Stream consumer ABI v1
currently developed on Linux::Event branch `feature/native-consumer-abi`.
The Async distribution must not be released until a compatible Linux::Event
release contains that ABI and the associated core regression gates have passed.

See `ASYNC-ROADMAP.md` for architecture, benchmark evidence, ownership rules,
and staged implementation work.
