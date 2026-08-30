# Linux::Event::Async

`Linux::Event::Async` is a separate distribution providing
Future::AsyncAwait-compatible async/await support above Linux::Event.

The initial implementation concentrates on `Linux::Event::Async::Stream`.
A framed Stream owns one reusable native receive Awaitable. Calling `recv`
arms that state and returns the Stream itself; receiving a message does not
allocate a Future. The Future allocated by Future::AsyncAwait represents the
result of the entire `async sub` and is implemented by
`Linux::Event::Async::Future`.

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

## Development dependency

This development branch targets the Linux::Event native Stream consumer ABI v1
currently developed on Linux::Event branch `feature/native-consumer-abi`.
The Async distribution must not be released until a compatible Linux::Event
release contains that ABI and the associated core regression gates have passed.

See `ASYNC-ROADMAP.md` for architecture, benchmark evidence, ownership rules,
and staged implementation work.
