//! Shutdown: the watch is torn down with the run, and teardown fits the budget.
//!
//! The shutdown budget is arithmetic, not hope — grace plus teardown plus a
//! bounded final flush must fit inside the orchestrator's kill window. An
//! observer whose task outlived the run, or whose teardown took an unbounded
//! amount of time, would spend somebody else's budget.

//! The whole file is `#![cfg(feature = "k8s")]`: the observer task lives behind
//! the default-off `k8s` feature, so a bare `cargo test --workspace` compiles this
//! to nothing and CI's dedicated `--features k8s` step is what runs it.
#![cfg(feature = "k8s")]

mod k8s_support;

use std::time::Duration;

use dagr_cli::pod_observer::PodObserver;
use dagr_k8s::api::{PodPhase, WatchDelivery};
use dagr_k8s::fake::fake_api;
use dagr_k8s::identity::AttemptKey;
use dagr_k8s::observer::ObserverLimits;
use k8s_support::{RUN_ID, pod, selector};

fn limits() -> ObserverLimits {
    ObserverLimits::default()
}

/// **Test-plan scenario: given the run ending, the observer's watch is torn down
/// and its task does not outlive the run.**
#[tokio::test(start_paused = true)]
async fn the_watch_is_torn_down_and_the_task_does_not_outlive_the_run() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Running, "100"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let _waiter = observer
        .watch_attempt(AttemptKey::new(RUN_ID, "extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;
    assert!(
        control.watch_is_open(),
        "a watch is open while the run is live"
    );

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");

    assert!(
        !control.watch_is_open(),
        "the watch must be torn down with the run, not left to the process exit"
    );
    assert_eq!(
        control.open_watches(),
        0,
        "no watch outlives the observer that opened it"
    );
}

/// Teardown completes inside the budget — measured, not assumed. The observer is
/// mid-stall (its longest natural wait) when the signal arrives, which is the
/// worst case for a loop that only checked for shutdown between events.
#[tokio::test(start_paused = true)]
async fn teardown_completes_inside_the_shutdown_budget() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Running, "100"));

    let observer = PodObserver::spawn(api, selector(), limits());
    let _waiter = observer
        .watch_attempt(AttemptKey::new(RUN_ID, "extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;

    // Nothing will ever arrive on this watch: the observer is parked on its stall
    // bound, tens of seconds away.
    let budget = Duration::from_secs(2);
    let started = tokio::time::Instant::now();
    let report = observer
        .shutdown(budget)
        .await
        .expect("shutdown completes inside the budget");
    let elapsed = started.elapsed();

    assert!(
        elapsed < budget,
        "teardown took {elapsed:?}, which does not fit a {budget:?} budget"
    );
    assert!(report.failure.is_none());
    assert!(!control.watch_is_open());
}

/// A waiter registered before shutdown is closed by it, so a caller awaiting a
/// pod cannot be left hanging on an observer that has gone.
#[tokio::test(start_paused = true)]
async fn shutdown_closes_every_outstanding_waiter() {
    let (api, control) = fake_api();
    let observer = PodObserver::spawn(api, selector(), limits());
    let mut waiter = observer
        .watch_attempt(AttemptKey::new(RUN_ID, "extract", 1))
        .await
        .expect("the observer accepts a waiter");
    control.await_watch().await;

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");

    assert!(
        tokio::time::timeout(Duration::from_secs(1), waiter.next())
            .await
            .expect("a closed waiter resolves rather than blocking")
            .is_none()
    );
}

/// Registering after shutdown is refused rather than silently accepted — an
/// attempt whose observer has gone would otherwise wait forever for a transition
/// nobody is watching for.
#[tokio::test(start_paused = true)]
async fn registering_after_shutdown_is_refused() {
    let (api, control) = fake_api();
    let observer = PodObserver::spawn(api, selector(), limits());
    control.await_watch().await;
    let handle = observer.handle();

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");

    assert!(
        handle
            .watch_attempt(AttemptKey::new(RUN_ID, "extract", 1))
            .await
            .is_err(),
        "a stopped observer refuses new waiters instead of accepting one it cannot serve"
    );
}

/// An observation delivered before its waiter registers is not lost: the
/// executor writes its submission record and registers, then submits, but the
/// two are not one atomic act and a pod that is already terminal on the very
/// first LIST is a real ordering.
#[tokio::test(start_paused = true)]
async fn an_observation_that_precedes_its_waiter_is_still_delivered() {
    let (api, control) = fake_api();
    control.upsert(pod("extract", 1, PodPhase::Succeeded, "140"));

    let observer = PodObserver::spawn(api, selector(), limits());
    control.await_watch().await;
    control
        .deliver(WatchDelivery::Bookmark {
            resource_version: "141".to_string(),
        })
        .await;

    let mut waiter = observer
        .watch_attempt(AttemptKey::new(RUN_ID, "extract", 1))
        .await
        .expect("the observer accepts a late waiter");
    let observation = waiter
        .terminal()
        .await
        .expect("the buffered terminal arrives");
    assert_eq!(observation.phase, PodPhase::Succeeded);

    observer
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown completes");
}
