use alloc::{boxed::Box, collections::VecDeque, sync::Arc, task::Wake};
use core::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};

use kspin::SpinNoIrq;
use lazyinit::LazyInit;

use crate::TaskId;

/// Global ready queue for async tasks.
///
/// This queue stores `AsyncTask`s that are ready to be polled.
/// The executor loop will pop tasks from this queue and run them.
static READY_QUEUE: LazyInit<SpinNoIrq<VecDeque<Arc<AsyncTask>>>> = LazyInit::new();
static READY_QUEUE_INITED: AtomicBool = AtomicBool::new(false);
/// Wake counter: incremented on spawn/wake, used by runners to detect new tasks.
static WAKE_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Initialize the executor module.
pub(crate) fn init() {
    if READY_QUEUE_INITED.swap(true, Ordering::AcqRel) {
        return;
    }
    READY_QUEUE.init_once(SpinNoIrq::new(VecDeque::new()));
}

/// An asynchronous task that wraps a future.
pub struct AsyncTask {
    id: TaskId,
    /// The future to be executed.
    ///
    /// It is wrapped in `SpinNoIrq` to provide interior mutability, which is required
    /// because `poll` takes `&self` (via `Arc<Self>`) but the future's `poll` method
    /// requires `Pin<&mut F>`.
    future: SpinNoIrq<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

impl AsyncTask {
    /// Creates a new `AsyncTask` with the given future.
    pub fn new(future: impl Future<Output = ()> + Send + 'static) -> Arc<Self> {
        Arc::new(Self {
            id: TaskId::new(),
            future: SpinNoIrq::new(Box::pin(future)),
        })
    }

    /// Returns the unique identifier of the task.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Polls the inner future.
    ///
    /// This creates a `Waker` from the `Arc<AsyncTask>` and passes it to the future's context.
    pub(crate) fn poll(self: &Arc<Self>) -> Poll<()> {
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = self.future.lock();
        future.as_mut().poll(&mut cx)
    }
}

impl Wake for AsyncTask {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        READY_QUEUE.lock().push_back(self.clone());
        WAKE_COUNT.fetch_add(1, Ordering::Release);
    }
}

/// Spawns a future as an asynchronous task.
///
/// The task is immediately added to the ready queue.
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    let task = AsyncTask::new(future);
    READY_QUEUE.lock().push_back(task);
    WAKE_COUNT.fetch_add(1, Ordering::Release);
}

/// Poll one ready task if present.
pub fn run_once() -> bool {
    if let Some(task) = READY_QUEUE.lock().pop_front() {
        let _ = task.poll();
        true
    } else {
        false
    }
}

/// Run up to `max_steps` tasks; returns true if any task ran.
pub fn run_for(max_steps: usize) -> bool {
    let mut ran = false;
    for _ in 0..max_steps {
        if !run_once() {
            break;
        }
        ran = true;
    }
    ran
}

/// Runs the executor loop until no ready tasks remain.
///
/// This function drains all ready tasks. It uses an atomic counter to detect
/// if new tasks arrive while draining; if so, it continues. It returns once
/// the queue is empty and no new wakes have occurred.
pub fn run_until_idle() {
    loop {
        // Snapshot the wake counter before draining.
        let seen = WAKE_COUNT.load(Ordering::Acquire);

        // Drain all currently ready tasks.
        while run_once() {}

        // If queue is empty, check whether any new wakes happened.
        if READY_QUEUE.lock().is_empty() {
            // Re-check counter: if unchanged, no new tasks arrived, we're done.
            if WAKE_COUNT.load(Ordering::Acquire) == seen {
                break;
            }
            // Otherwise, new wake happened; continue to drain.
        }
        // Yield CPU briefly to avoid tight spin.
        core::hint::spin_loop();
    }
}