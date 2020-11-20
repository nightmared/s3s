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
