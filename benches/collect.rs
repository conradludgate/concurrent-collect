use std::{
    future::{ready, Ready},
    pin::pin,
};

use concurrent_collect::collect_vec;
use divan::Bencher;
use futures_concurrency::concurrent_stream::{ConcurrentStream, Consumer};
use tokio::runtime::Builder;

fn main() {
    divan::main()
}

async fn work(x: u64) -> u64 {
    tokio::task::yield_now().await;
    x
}

#[divan::bench(sample_count = 100, sample_size = 100)]
fn baseline(b: Bencher) {
    let rt = Builder::new_current_thread().enable_time().build().unwrap();
    b.bench_local(|| rt.block_on(async { Iter(0..1024).map(work).collect::<Vec<u64>>().await }));
}

#[divan::bench(sample_count = 100, sample_size = 100)]
fn mine(b: Bencher) {
    let rt = Builder::new_current_thread().enable_time().build().unwrap();
    b.bench_local(|| rt.block_on(async { collect_vec(Iter(0..1024).map(work)).await }));
}

struct Iter<I>(I);

impl<I: IntoIterator> ConcurrentStream for Iter<I> {
    type Item = I::Item;

    type Future = Ready<I::Item>;

    async fn drive<C>(self, consumer: C) -> C::Output
    where
        C: Consumer<Self::Item, Self::Future>,
    {
        let mut consumer = pin!(consumer);
        for item in self.0 {
            consumer.as_mut().send(ready(item)).await;
        }
        consumer.flush().await
    }

    fn concurrency_limit(&self) -> Option<core::num::NonZeroUsize> {
        None
    }
}
