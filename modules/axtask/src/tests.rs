use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, Once};

use crate::{api as axtask, current, executor, WaitQueue};
use core::future::poll_fn;

static INIT: Once = Once::new();
static SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn test_sched_fifo() {
    let _lock = SERIAL.lock();
    INIT.call_once(axtask::init_scheduler);

    const NUM_TASKS: usize = 10;
    static FINISHED_TASKS: AtomicUsize = AtomicUsize::new(0);

    for i in 0..NUM_TASKS {
        axtask::spawn_raw(
            move || {
                println!("sched-fifo: Hello, task {}! ({})", i, current().id_name());
                axtask::yield_now();
                let order = FINISHED_TASKS.fetch_add(1, Ordering::Release);
                assert_eq!(order, i); // FIFO scheduler
            },
            format!("T{}", i),
            0x1000,
        );
    }

    while FINISHED_TASKS.load(Ordering::Acquire) < NUM_TASKS {
        axtask::yield_now();
    }
}

#[test]
fn test_fp_state_switch() {
    let _lock = SERIAL.lock();
    INIT.call_once(axtask::init_scheduler);

    const NUM_TASKS: usize = 5;
    const FLOATS: [f64; NUM_TASKS] = [
        3.141592653589793,
        2.718281828459045,
        -1.4142135623730951,
        0.0,
        0.618033988749895,
    ];
    static FINISHED_TASKS: AtomicUsize = AtomicUsize::new(0);

    for (i, float) in FLOATS.iter().enumerate() {
        axtask::spawn(move || {
            let mut value = float + i as f64;
            axtask::yield_now();
            value -= i as f64;

            println!("fp_state_switch: Float {} = {}", i, value);
            assert!((value - float).abs() < 1e-9);
            FINISHED_TASKS.fetch_add(1, Ordering::Release);
        });
    }
    while FINISHED_TASKS.load(Ordering::Acquire) < NUM_TASKS {
        axtask::yield_now();
    }
}

#[test]
fn test_wait_queue() {
    let _lock = SERIAL.lock();
    INIT.call_once(axtask::init_scheduler);

    const NUM_TASKS: usize = 10;

    static WQ1: WaitQueue = WaitQueue::new();
    static WQ2: WaitQueue = WaitQueue::new();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    for _ in 0..NUM_TASKS {
        axtask::spawn(move || {
            COUNTER.fetch_add(1, Ordering::Release);
            println!("wait_queue: task {:?} started", current().id());
            WQ1.notify_one(true); // WQ1.wait_until()
            WQ2.wait();

            COUNTER.fetch_sub(1, Ordering::Release);
            println!("wait_queue: task {:?} finished", current().id());
            WQ1.notify_one(true); // WQ1.wait_until()
        });
    }

    println!("task {:?} is waiting for tasks to start...", current().id());
    WQ1.wait_until(|| COUNTER.load(Ordering::Acquire) == NUM_TASKS);
    axtask::yield_now();
    assert_eq!(COUNTER.load(Ordering::Acquire), NUM_TASKS);
    WQ2.notify_all(true); // WQ2.wait()

    println!(
        "task {:?} is waiting for tasks to finish...",
        current().id()
    );
    WQ1.wait_until(|| COUNTER.load(Ordering::Acquire) == 0);
    assert_eq!(COUNTER.load(Ordering::Acquire), 0);
}

#[test]
fn test_task_join() {
    let _lock = SERIAL.lock();
    INIT.call_once(axtask::init_scheduler);

    const NUM_TASKS: usize = 10;
    let mut tasks = Vec::with_capacity(NUM_TASKS);

    for i in 0..NUM_TASKS {
        tasks.push(axtask::spawn_raw(
            move || {
                println!("task_join: task {}! ({})", i, current().id_name());
                axtask::yield_now();
                axtask::exit(i as _);
            },
            format!("T{}", i),
            0x1000,
        ));
    }

    for i in 0..NUM_TASKS {
        assert_eq!(tasks[i].join(), Some(i as _));
    }
}

fn init_env() {
    INIT.call_once(axtask::init_scheduler);
}

/// Test for the executor running tasks manually with run_until_idle.
#[test]
fn test_async_executor_basic_completion() {
    let _lock = SERIAL.lock();
    init_env();

    static DONE: AtomicBool = AtomicBool::new(false);

    executor::spawn(async {
        DONE.store(true, Ordering::Release);
    });

    executor::run_until_idle();
    assert!(DONE.load(Ordering::Acquire));
}

#[test]
fn test_async_executor_self_wake() {
    let _lock = SERIAL.lock();
    init_env();

    static POLL_COUNT: AtomicUsize = AtomicUsize::new(0);

    executor::spawn(async {
        poll_fn(|cx| {
            let prev = POLL_COUNT.fetch_add(1, Ordering::AcqRel);
            if prev == 0 {
                cx.waker().wake_by_ref();
                return core::task::Poll::Pending;
            }
            core::task::Poll::Ready(())
        })
        .await
    });

    executor::run_until_idle();
    assert_eq!(POLL_COUNT.load(Ordering::Acquire), 2);
}

#[test]
fn test_async_executor_two_task_fairness() {
    let _lock = SERIAL.lock();
    init_env();

    static ORDER: Mutex<Vec<String>> = Mutex::new(Vec::new());

    fn two_step_task(name: &'static str) -> impl core::future::Future<Output = ()> + Send {
        let state = std::sync::Arc::new(AtomicUsize::new(0));
        async move {
            poll_fn(move |cx| {
                let seq = state.fetch_add(1, Ordering::AcqRel);
                let mut order = ORDER.lock().unwrap();
                order.push(format!("{}-{}", name, seq + 1));
                if seq == 0 {
                    cx.waker().wake_by_ref();
                    core::task::Poll::Pending
                } else {
                    core::task::Poll::Ready(())
                }
            })
            .await
        }
    }

    executor::spawn(two_step_task("A"));
    executor::spawn(two_step_task("B"));

    executor::run_until_idle();

    let order = ORDER.lock().unwrap().clone();
    assert_eq!(order, ["A-1", "B-1", "A-2", "B-2"]);
}

/// Test for the executor running tasks manually with run_for.
#[test]
fn test_async_executor_run_for_progress() {
    let _lock = SERIAL.lock();
    init_env();

    static CNT: AtomicUsize = AtomicUsize::new(0);

    executor::spawn(async {
        CNT.fetch_add(1, Ordering::Release);
    });
    executor::spawn(async {
        CNT.fetch_add(1, Ordering::Release);
    });

    // Drive executor manually, one task per step.
    executor::run_for(1);
    assert_eq!(CNT.load(Ordering::Acquire), 1);

    executor::run_for(1);
    assert_eq!(CNT.load(Ordering::Acquire), 2);
}

