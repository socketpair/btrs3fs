//! A semaphore whose limit changes at run time (SPEC.md §6.1): growing lets
//! waiters in at once; shrinking takes free permits away and makes busy ones
//! vanish when they come back, so nothing in flight is cancelled.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Semaphore, SemaphorePermit};

pub struct Limiter {
    sem: Semaphore,
    /// (limit, debt): busy permits to forget when they come back.
    state: Mutex<(usize, usize)>,
    in_use: AtomicUsize,
    waiting: AtomicUsize,
}

/// One slot; given back (or forgotten, after a shrink) when dropped.
pub struct LimiterPermit<'a> {
    limiter: &'a Limiter,
    permit: Option<SemaphorePermit<'a>>,
}

impl Limiter {
    pub fn new(limit: usize) -> Self {
        Self {
            sem: Semaphore::new(limit),
            state: Mutex::new((limit, 0)),
            in_use: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
        }
    }

    pub async fn acquire(&self) -> LimiterPermit<'_> {
        /// Counts a waiter until it gets its slot or gives up (dropped future).
        struct Waiting<'a>(&'a AtomicUsize);
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        let waiting = Waiting(&self.waiting);
        // Never closed, so acquiring cannot fail.
        let permit = self.sem.acquire().await.unwrap();
        drop(waiting);
        self.in_use.fetch_add(1, Ordering::SeqCst);
        LimiterPermit {
            limiter: self,
            permit: Some(permit),
        }
    }

    pub fn set_limit(&self, n: usize) {
        let mut st = self.state.lock().unwrap();
        let (limit, debt) = &mut *st;
        if n > *limit {
            let grow = n - *limit;
            let cancelled = grow.min(*debt);
            *debt -= cancelled;
            self.sem.add_permits(grow - cancelled);
        } else {
            let shrink = *limit - n;
            let taken = self.sem.forget_permits(shrink);
            *debt += shrink - taken;
        }
        *limit = n;
    }

    pub fn limit(&self) -> usize {
        self.state.lock().unwrap().0
    }

    /// Slots taken now (may exceed the limit right after a shrink).
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::SeqCst)
    }

    /// Requests waiting for a slot.
    #[cfg(test)]
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }
}

impl Drop for LimiterPermit<'_> {
    fn drop(&mut self) {
        let l = self.limiter;
        let permit = self.permit.take().expect("dropped once");
        let mut st = l.state.lock().unwrap();
        l.in_use.fetch_sub(1, Ordering::SeqCst);
        if st.1 > 0 {
            st.1 -= 1;
            permit.forget();
        }
        // Otherwise the permit goes back when dropped here.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn pending<F: std::future::Future>(f: F) -> bool {
        tokio::time::timeout(Duration::from_millis(30), f)
            .await
            .is_err()
    }

    #[tokio::test]
    async fn grow_lets_waiters_in() {
        let l = std::sync::Arc::new(Limiter::new(1));
        let held = l.acquire().await;
        let waiter = tokio::spawn({
            let l = l.clone();
            async move { drop(l.acquire().await) }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(l.waiting(), 1);
        l.set_limit(2);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter got the new permit")
            .unwrap();
        assert_eq!(l.limit(), 2);
        drop(held);
    }

    #[tokio::test]
    async fn shrink_while_busy() {
        let l = Limiter::new(4);
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(l.acquire().await);
        }
        l.set_limit(1);
        assert_eq!(l.limit(), 1);
        assert_eq!(l.in_use(), 4); // nothing in flight is cancelled
        held.truncate(1);
        assert_eq!(l.in_use(), 1);
        assert!(pending(l.acquire()).await, "over the new limit");
        held.clear();
        let p = l.acquire().await;
        assert_eq!(l.in_use(), 1);
        assert!(pending(l.acquire()).await);
        drop(p);
        // Grow again after paying the debt off.
        l.set_limit(3);
        let a = (l.acquire().await, l.acquire().await, l.acquire().await);
        assert_eq!(l.in_use(), 3);
        assert!(pending(l.acquire()).await);
        drop(a);
    }

    #[tokio::test]
    async fn shrink_then_grow_before_debt_is_paid() {
        let l = Limiter::new(4);
        let held: Vec<_> = futures_util::future::join_all((0..4).map(|_| l.acquire())).await;
        l.set_limit(1); // debt 3
        l.set_limit(3); // cancels 2 of the debt
        drop(held);
        let a = (l.acquire().await, l.acquire().await, l.acquire().await);
        assert!(pending(l.acquire()).await);
        drop(a);
    }
}
