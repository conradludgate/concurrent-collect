use std::{
    collections::{BinaryHeap, VecDeque},
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

pub async fn fold<Acc, Fold, Stream>(iter: Stream, init: Acc, mut fold: Fold) -> Acc
where
    Stream: IntoConcurrentStream,
    Fold: FnMut(Acc, Stream::Item) -> Acc,
{
    let stream = iter.into_co_stream();
    let (mut lower, _) = stream.size_hint();
    if lower < 32 {
        lower = 32;
    }

    let mut next = 0;
    let mut hold = BinaryHeap::<IndexFut<Stream::Item>>::new();

    let fold_mut = &mut fold;
    let mut acc = stream
        .drive(FoldConsumer {
            len: 0,
            rem: 0,
            group: VecDeque::from_iter([FuturesUnorderedBounded::new(lower)]),
            acc: Some(init),
            fold: |mut acc, indexed: IndexFut<Stream::Item>| {
                if indexed.index == next {
                    let mut x = indexed.inner;
                    loop {
                        acc = fold_mut(acc, x);
                        next += 1;
                        let Some(head) = hold.peek() else { break };
                        if head.index != next {
                            break;
                        }
                        x = hold.pop().unwrap().inner;
                    }
                } else {
                    hold.push(indexed);
                }
                acc
            },
        })
        .await;

    loop {
        let Some(head) = hold.pop() else { break };
        acc = fold(acc, head.inner);
    }
    acc
}

#[pin_project]
struct IndexFut<F> {
    #[pin]
    inner: F,
    index: usize,
}

impl<F> PartialOrd for IndexFut<F> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<F> Ord for IndexFut<F> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.index.cmp(&other.index).reverse()
    }
}
impl<F> PartialEq for IndexFut<F> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}
impl<F> Eq for IndexFut<F> {}

impl<F: Future> Future for IndexFut<F> {
    type Output = IndexFut<F::Output>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let index = self.index;
        self.project()
            .inner
            .poll(cx)
            .map(|inner| IndexFut { inner, index })
    }
}

pub(crate) struct FoldConsumer<Acc, Fut: Future, Fold> {
    len: usize,
    rem: usize,
    group: VecDeque<FuturesUnorderedBounded<IndexFut<Fut>>>,
    acc: Option<Acc>,
    fold: Fold,
}
impl<Acc, F: Future, Fold> Unpin for FoldConsumer<Acc, F, Fold> {}

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
                        Poll::Ready(Some(IndexFut { inner, index })) => {
                            this.rem -= 1;
                            done = false;
                            i += 1;

                            if index >= this.output.len() {
                                this.output.reserve(this.len - this.output.len());
                                this.output.resize_with(this.len, || MaybeUninit::uninit());
                            }
                            this.output[index].write(inner);
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

impl<Acc, Fut, Fold> Consumer<Fut::Output, Fut> for FoldConsumer<Acc, Fut, Fold>
where
    Fut: Future,
    Fold: FnMut(Acc, IndexFut<Fut::Output>) -> Acc,
{
    type Output = Acc;

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
                        Poll::Ready(Some(x)) => {
                            this.rem -= 1;
                            done = false;
                            i += 1;

                            this.acc = Some((this.fold)(this.acc.take().unwrap(), x));
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
        std::mem::take(&mut self.acc).unwrap()
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

    use crate::{collect_vec, fold};

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

    #[test]
    fn fold_test() {
        let rt = Builder::new_current_thread().enable_time().build().unwrap();

        #[cfg(miri)]
        const N: u64 = 256;
        #[cfg(not(miri))]
        const N: u64 = 1024;

        let vec = rt.block_on(fold(Iter(0..N).map(work), Vec::<u64>::new(), |mut v, i| {
            v.push(i);
            v
        }));
        assert_eq!(vec, (0..N).collect::<Vec<_>>());
    }
}
