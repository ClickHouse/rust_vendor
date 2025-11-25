//! mintex is a *min*imal Mutex.
//!
//! Most of the implementation is lifted from [`std::sync::Mutex`].
//! The reason for this mutex existing is that I'd like a mutex which is
//! quite lightweight and does not perform allocations.

use std::cell::UnsafeCell;
use std::fmt;
use std::hint;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;

// Empirically a good number on an M1
const LOOP_LIMIT: usize = 250;

/// Mutex implementation.
pub struct Mutex<T: ?Sized> {
    lock: AtomicBool,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}
unsafe impl<T: ?Sized + Send> Sync for Mutex<T> {}

impl<T> From<T> for Mutex<T> {
    /// Creates a new mutex in an unlocked state ready for use.
    /// This is equivalent to [`Mutex::new`].
    fn from(t: T) -> Self {
        Mutex::new(t)
    }
}

impl<T: ?Sized + Default> Default for Mutex<T> {
    /// Creates a `Mutex<T>`, with the `Default` value for T.
    fn default() -> Mutex<T> {
        Mutex::new(Default::default())
    }
}

impl<T> Mutex<T> {
    #[inline]
    /// Create a new Mutex which wraps the provided data.
    pub const fn new(data: T) -> Self {
        Self {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }
}

impl<T: ?Sized> Mutex<T> {
    /// Acquire a lock which returns a RAII MutexGuard over the locked data.
    pub fn lock(&self) -> MutexGuard<'_, T> {
        let mut loop_count = 0;
        loop {
            match self
                .lock
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            {
                Ok(v) => {
                    debug_assert!(!v);
                    unsafe {
                        return MutexGuard::new(self);
                    }
                }
                Err(_e) => {
                    if loop_count > LOOP_LIMIT {
                        loop_count = 0;
                        thread::yield_now();
                    } else {
                        loop_count += 1;
                        hint::spin_loop();
                    }
                }
            }
        }
    }
    /// Unlock a mutex by dropping the MutexGuard.
    pub fn unlock(guard: MutexGuard<'_, T>) {
        drop(guard);
    }
}

/// RAII Guard over locked data.
pub struct MutexGuard<'a, T: ?Sized + 'a> {
    mutex: &'a Mutex<T>,
}

// It would be nice to mark the MutexGuard as !Sync, but not stable yet.
// impl<T: ?Sized> !Send for MutexGuard<'_, T> {}
unsafe impl<T: ?Sized + Sync> Sync for MutexGuard<'_, T> {}

impl<'mutex, T: ?Sized> MutexGuard<'mutex, T> {
    unsafe fn new(mutex: &'mutex Mutex<T>) -> MutexGuard<'mutex, T> {
        MutexGuard { mutex }
    }
}

impl<T: ?Sized> Deref for MutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.mutex.data.get() }
    }
}

impl<T: ?Sized> DerefMut for MutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.mutex.data.get() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.mutex.lock.store(false, Ordering::Release);
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn exercise_mutex_lock() {
        const N: usize = 100;

        // Spawn a few threads to increment a shared variable (non-atomically), and
        // let the main thread know once all increments are done.

        let (tx, rx) = channel();

        let data: usize = 0;

        let my_lock = Arc::new(Mutex::new(data));

        for _ in 0..N {
            let tx = tx.clone();
            let my_lock = my_lock.clone();
            thread::spawn(move || {
                let mut data = my_lock.lock();
                *data += 1;
                println!("after data: {}", data);
                if *data == N {
                    tx.send(()).unwrap();
                }
            });
        }

        rx.recv().unwrap();
    }
}
