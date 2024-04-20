use std::{
    collections::VecDeque,
    future::{poll_fn, Future},
    mem::MaybeUninit,
    pin::Pin,
    task::Poll,
};

use futures_buffered::FuturesUnorderedBounded;
use futures_concurrency::concurrent_stream::{
    ConcurrentStream, Consumer, ConsumerState, IntoConcurrentStream,
};
use futures_core::Stream;
use pin_project::pin_project;

pub async fn collect_vec<S>(iter: S) -> Vec<S::Item>
where
    S: IntoConcurrentStream,
{
    let stream = iter.into_co_stream();
    let (lower, _) = stream.size_hint();
    stream.drive(VecConsumer::with_capacity(lower)).await
}

#[pin_project]
struct IndexFut<F> {
    #[pin]
    inner: F,
    index: usize,
}

impl<F: Future> Future for IndexFut<F> {
    type Output = (F::Output, usize);

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let index = self.index;
        self.project().inner.poll(cx).map(|x| (x, index))
    }
}

// TODO: replace this with a generalized `fold` operation
pub(crate) struct VecConsumer<Fut: Future> {
    len: usize,
    rem: usize,
    group: VecDeque<FuturesUnorderedBounded<IndexFut<Fut>>>,
    output: Vec<MaybeUninit<Fut::Output>>,
}

impl<F: Future> Unpin for VecConsumer<F> {}

impl<Fut: Future> VecConsumer<Fut> {
    pub(crate) fn with_capacity(mut n: usize) -> Self {
        if n < 32 {
            n = 32;
        }
        Self {
            len: 0,
            rem: 0,
            group: VecDeque::from_iter([FuturesUnorderedBounded::new(n)]),
            output: Vec::with_capacity(n),
        }
    }
}

impl<Item, Fut> Consumer<Item, Fut> for VecConsumer<Fut>
where
    Fut: Future<Output = Item>,
{
    type Output = Vec<Item>;

    async fn send(mut self: Pin<&mut Self>, future: Fut) -> ConsumerState {
        let this = &mut *self;
        let len = this.len;
        this.rem += 1;
        this.len += 1;

        let last = this.group.back_mut().unwrap();
        let fut = IndexFut {
            inner: future,
            index: len,
        };
        match last.try_push(fut) {
            Ok(()) => {}
            Err(future) => {
                let mut next = FuturesUnorderedBounded::new(last.capacity() * 2);
                next.push(future);
                this.group.push_back(next);
            }
        }
        ConsumerState::Continue
    }

    async fn progress(mut self: Pin<&mut Self>) -> ConsumerState {
        let this = &mut *self;
        poll_fn(|cx| {
            loop {
                let mut done = true;
                let mut i = 0;
                while i < this.group.len() {
                    let poll = Pin::new(&mut this.group[i]).poll_next(cx);
                    match poll {
                        Poll::Ready(Some((x, index))) => {
                            this.rem -= 1;
                            done = false;
                            i += 1;

                            if index >= this.output.len() {
                                this.output.reserve(this.len - this.output.len());
                                this.output.resize_with(this.len, || MaybeUninit::uninit());
                            }
                            this.output[index].write(x);
                        }
                        Poll::Ready(None) => {
                            debug_assert!(this.group[i].is_empty());
                            if i + 1 < this.group.len() {
                                this.group.remove(i);
                            } else if this.group.len() == 1 {
                                debug_assert_eq!(this.rem, 0);
                                return Poll::Ready(());
                            } else {
                                break;
                            }
                        }
                        Poll::Pending => {
                            i += 1;
                        }
                    }
                }
                if done {
                    break;
                }
            }
            Poll::Pending
        })
        .await;
        ConsumerState::Empty
    }
    async fn flush(mut self: Pin<&mut Self>) -> Self::Output {
        self.as_mut().progress().await;
        debug_assert_eq!(self.output.len(), self.len);

        // we have initiatlised all fields
        let mut output = std::mem::take(&mut self.output);
        output.truncate(self.len);
        unsafe {
            let mut output = std::mem::ManuallyDrop::new(output);

            Vec::from_raw_parts(output.as_mut_ptr().cast(), self.len, output.capacity())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::{ready, Ready},
        pin::pin,
    };

    use futures_concurrency::concurrent_stream::{ConcurrentStream, Consumer};
    use tokio::runtime::Builder;

    use crate::collect_vec;

    #[cfg(miri)]
    async fn work(x: u64) -> u64 {
        tokio::task::yield_now().await;
        x
    }
    #[cfg(not(miri))]
    async fn work(x: u64) -> u64 {
        let ms = x.reverse_bits() >> (64 - 10);
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        x
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

    #[test]
    fn ordered() {
        let rt = Builder::new_current_thread().enable_time().build().unwrap();

        #[cfg(miri)]
        const N: u64 = 256;
        #[cfg(not(miri))]
        const N: u64 = 1024;

        let vec = rt.block_on(collect_vec(Iter(0..N).map(work)));
        assert_eq!(vec, (0..N).collect::<Vec<_>>());
    }
}
