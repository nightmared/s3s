use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use futures::future::{TryFuture, TryFutureExt};

struct WatchedObjects<Fut> {
    inner: Mutex<Vec<Fut>>,
    nb_operations: usize,
}

impl<Fut: Unpin> Unpin for WatchedObjects<Fut> {}

pub struct Selector<Fut> {
    inner: Arc<WatchedObjects<Fut>>,
}

pub fn selector<F>(v: Vec<F>, nb_operations: usize) -> Selector<F>
where
    F: TryFuture + Unpin,
{
    Selector {
        inner: Arc::new(WatchedObjects {
            inner: Mutex::new(v),
            nb_operations,
        }),
    }
}

impl<Fut> Selector<Fut> {
    pub fn add(&self, val: Fut) {
        let mut inner = self.inner.inner.lock().unwrap();
        inner.push(val);
    }
}

impl<Fut: TryFuture + Unpin> Future for Selector<Fut> {
    type Output = Result<(), Fut::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut to_delete = Vec::new();

        let mut inner = self.inner.inner.lock().unwrap();
        for (i, f) in inner.iter_mut().enumerate().take(self.inner.nb_operations) {
            if let Poll::Ready(r) = f.try_poll_unpin(cx) {
                to_delete.push(i);
                if let Err(e) = r {
                    return Poll::Ready(Err(e));
                }
            }
        }

        // delete in reverse order, so that indexes are not invalidated
        for idx in to_delete.iter().rev() {
            inner.swap_remove(*idx);
        }

        if inner.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

// do not preserve ordering
pub struct SelectVec<'a, R, Fut: Future<Output = R>> {
    inner: &'a mut Vec<Fut>,
    output: &'a mut Vec<R>,
}

impl<'a, R, Fut: Future<Output = R> + Unpin> Unpin for SelectVec<'a, R, Fut> {}

pub fn select_vec<'a, R, F: Future<Output = R>>(
    v: &'a mut Vec<F>,
    res: &'a mut Vec<R>,
) -> SelectVec<'a, R, F>
where
    F: Unpin,
{
    SelectVec {
        inner: v,
        output: res,
    }
}

impl<'a, R, Fut: Future<Output = R> + Unpin> Future for SelectVec<'a, R, Fut> {
    type Output = Option<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.inner.len() == 0 {
            return Poll::Ready(None);
        }
        let item =
            self.inner
                .iter_mut()
                .enumerate()
                .find_map(|(i, f)| match Pin::new(f).poll(cx) {
                    Poll::Pending => None,
                    Poll::Ready(e) => Some((i, e)),
                });
        if let Some((idx, res)) = item {
            self.inner.swap_remove(idx);
            self.output.push(res);

            if self.inner.is_empty() {
                return Poll::Ready(Some(()));
            }
        }
        Poll::Pending
    }
}
