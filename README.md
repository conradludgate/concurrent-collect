# concurrent-collect

An alternative impl for [`ConcurrentStream::collect`](https://docs.rs/futures-concurrency/latest/futures_concurrency/concurrent_stream/trait.ConcurrentStream.html#method.collect).

## Performance

```
Timer precision: 41 ns
collect       fastest       │ slowest       │ median        │ mean          │ samples │ iters
├─ baseline   219.4 µs      │ 230.4 µs      │ 222 µs        │ 222.7 µs      │ 100     │ 10000
╰─ this impl  73.47 µs      │ 78.84 µs      │ 74.44 µs      │ 74.66 µs      │ 100     │ 10000
```
