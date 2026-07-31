//! T101 spike probe — throwaway. Exercises the three ADR 115 bets against a
//! real cluster through the client the ADR names (kube-rs), because the bets
//! are about what *a client* observes, not only what the API server sends.
//!
//! Subcommands:
//!   watch   --mode expired|future|live|runtime --seconds N
//!   latency --n N --image IMG --tag TAG
//!   deps    (prints nothing; see `cargo tree`)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, PostParams, WatchEvent, WatchParams};
use kube::runtime::watcher;
use kube::Client;
use serde_json::json;
use tokio::sync::Mutex;

const NS: &str = "t101";

fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or(0.0)
}

fn log(s: &str) {
    println!("[{:.0}] {s}", now_ms());
}

fn arg(name: &str, default: &str) -> String {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    // kube-rs `rustls-tls` alone leaves rustls 0.23 with no process-level
    // CryptoProvider and it panics on the first TLS handshake. A T107 note.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cmd = std::env::args().nth(1).unwrap_or_default();
    match cmd.as_str() {
        "watch" => watch_cmd().await,
        "latency" => latency_cmd().await,
        other => anyhow::bail!("unknown subcommand {other:?}"),
    }
}

// === Bet 1 — watch reliability ============================================

async fn watch_cmd() -> Result<()> {
    let mode = arg("--mode", "live");
    let seconds: u64 = arg("--seconds", "20").parse()?;
    let client = Client::try_default().await.context("kubeconfig")?;
    let api: Api<Pod> = Api::namespaced(client, NS);

    log(&format!("MODE={mode} seconds={seconds}"));

    if mode == "runtime" {
        return runtime_watcher(api, seconds).await;
    }

    let lp = ListParams::default();
    let list = api.list(&lp).await.context("initial LIST")?;
    let current_rv = list.metadata.resource_version.clone().unwrap_or_default();
    log(&format!(
        "LIST ok items={} resourceVersion={current_rv}",
        list.items.len()
    ));

    let rv = match mode.as_str() {
        // A resourceVersion the server has certainly compacted past.
        "expired" => "1".to_string(),
        // A resourceVersion the server has not reached: the silent-stall case.
        "future" => "999999999999".to_string(),
        _ => current_rv.clone(),
    };

    let wp = WatchParams {
        bookmarks: true,
        timeout: Some(seconds as u32 + 30),
        ..Default::default()
    };
    log(&format!("WATCH from resourceVersion={rv} (bookmarks=on)"));

    let stream = match api.watch(&wp, &rv).await {
        Ok(s) => s,
        Err(e) => {
            // The other place a 410 can land: as a transport-level error on the
            // request itself rather than as an in-stream event.
            log(&format!("WATCH-REQUEST-ERROR class=kube::Error detail={e}"));
            log("VERDICT=surfaced-as-request-error");
            return Ok(());
        }
    };
    let mut stream = Box::pin(stream);

    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut events = 0usize;
    let mut bookmarks = 0usize;
    let mut errors = 0usize;
    let mut silence_since = Instant::now();
    // Stall bound: the probe treats "neither an event nor an error within this
    // window" as broken, which is the discipline ADR 115 §3 needs.
    let stall = Duration::from_secs(10);

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining.min(Duration::from_secs(2)), stream.next()).await {
            Err(_elapsed) => {
                if silence_since.elapsed() >= stall {
                    log(&format!(
                        "STALL-DETECTED silence={:.1}s events={events} bookmarks={bookmarks}",
                        silence_since.elapsed().as_secs_f64()
                    ));
                    log("VERDICT=silent-stall (no data, no error)");
                    return Ok(());
                }
            }
            Ok(None) => {
                log(&format!(
                    "STREAM-ENDED cleanly after events={events} errors={errors}"
                ));
                log("VERDICT=stream-ended");
                return Ok(());
            }
            Ok(Some(Err(e))) => {
                errors += 1;
                silence_since = Instant::now();
                log(&format!("STREAM-ITEM-ERROR detail={e}"));
            }
            Ok(Some(Ok(ev))) => {
                events += 1;
                silence_since = Instant::now();
                match ev {
                    WatchEvent::Added(p) => log(&format!("ADDED {}", name_of(&p))),
                    WatchEvent::Modified(p) => log(&format!(
                        "MODIFIED {} phase={}",
                        name_of(&p),
                        phase_of(&p)
                    )),
                    WatchEvent::Deleted(p) => log(&format!("DELETED {}", name_of(&p))),
                    WatchEvent::Bookmark(b) => {
                        bookmarks += 1;
                        log(&format!(
                            "BOOKMARK resourceVersion={}",
                            b.metadata.resource_version
                        ));
                    }
                    WatchEvent::Error(err) => {
                        errors += 1;
                        log(&format!(
                            "IN-STREAM-ERROR code={:?} reason={:?} message={:?}",
                            err.code, err.reason, err.message
                        ));
                        log("VERDICT=in-stream-ERROR-event");
                        return Ok(());
                    }
                }
            }
        }
    }
    log(&format!(
        "WINDOW-CLOSED events={events} bookmarks={bookmarks} errors={errors}"
    ));
    if events == 0 && errors == 0 {
        log("VERDICT=inconclusive (no event, no error, no interruption observed)");
    } else {
        log("VERDICT=delivered");
    }
    Ok(())
}

async fn runtime_watcher(api: Api<Pod>, seconds: u64) -> Result<()> {
    let cfg = watcher::Config::default();
    let stream = watcher(api, cfg);
    let mut stream = Box::pin(stream);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let (mut applies, mut errs, mut inits) = (0usize, 0usize, 0usize);
    let mut silence_since = Instant::now();
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining.min(Duration::from_secs(2)), stream.next()).await {
            Err(_) => {
                if silence_since.elapsed() >= Duration::from_secs(15) {
                    log(&format!(
                        "RUNTIME-STALL silence={:.1}s",
                        silence_since.elapsed().as_secs_f64()
                    ));
                    silence_since = Instant::now();
                }
            }
            Ok(None) => {
                log("RUNTIME-STREAM-ENDED");
                return Ok(());
            }
            Ok(Some(Err(e))) => {
                errs += 1;
                silence_since = Instant::now();
                log(&format!("RUNTIME-ERROR n={errs} detail={e}"));
            }
            Ok(Some(Ok(ev))) => {
                silence_since = Instant::now();
                match ev {
                    watcher::Event::Init => {
                        inits += 1;
                        log(&format!("RUNTIME-INIT n={inits} (re-list starting)"));
                    }
                    watcher::Event::InitApply(p) => {
                        log(&format!("RUNTIME-INIT-APPLY {}", name_of(&p)))
                    }
                    watcher::Event::InitDone => log("RUNTIME-INIT-DONE"),
                    watcher::Event::Apply(p) => {
                        applies += 1;
                        log(&format!("RUNTIME-APPLY {} phase={}", name_of(&p), phase_of(&p)));
                    }
                    watcher::Event::Delete(p) => log(&format!("RUNTIME-DELETE {}", name_of(&p))),
                }
            }
        }
    }
    log(&format!(
        "RUNTIME-WINDOW-CLOSED applies={applies} errors={errs} relists={inits}"
    ));
    Ok(())
}

fn name_of(p: &Pod) -> String {
    p.metadata.name.clone().unwrap_or_default()
}

fn phase_of(p: &Pod) -> String {
    p.status
        .as_ref()
        .and_then(|s| s.phase.clone())
        .unwrap_or_else(|| "-".into())
}

fn container_started(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .map(|cs| {
            cs.iter().any(|c| {
                c.state
                    .as_ref()
                    .map(|st| st.running.is_some() || st.terminated.is_some())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

// === Bet 2 — submission-to-start latency ==================================

async fn latency_cmd() -> Result<()> {
    let n: usize = arg("--n", "1").parse()?;
    let image = arg("--image", "busybox:1.36");
    let tag = arg("--tag", "run");
    let client = Client::try_default().await?;
    let api: Api<Pod> = Api::namespaced(client, NS);

    let submits: Arc<Mutex<HashMap<String, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let running: Arc<Mutex<HashMap<String, f64>>> = Arc::new(Mutex::new(HashMap::new()));
    let started: Arc<Mutex<HashMap<String, f64>>> = Arc::new(Mutex::new(HashMap::new()));

    // ONE shared watch for the whole fan-out — the ADR 115 §2 shape — started
    // BEFORE the first create, so observation is continuous rather than
    // beginning after the submission loop (the methodology error the k3s run
    // recorded).
    let wp = WatchParams {
        label_selector: Some(format!("spike=t101,batch={tag}")),
        bookmarks: true,
        timeout: Some(290),
        ..Default::default()
    };
    let list = api
        .list(&ListParams::default().labels(&format!("spike=t101,batch={tag}")))
        .await?;
    let rv = list.metadata.resource_version.clone().unwrap_or_default();
    let stream = api.watch(&wp, &rv).await?;

    let (subs_w, run_w, start_w) = (submits.clone(), running.clone(), started.clone());
    let observer = tokio::spawn(async move {
        let mut stream = Box::pin(stream);
        loop {
            match tokio::time::timeout(Duration::from_secs(180), stream.try_next()).await {
                Ok(Ok(Some(ev))) => {
                    let p = match ev {
                        WatchEvent::Added(p) | WatchEvent::Modified(p) => p,
                        WatchEvent::Error(e) => {
                            log(&format!("OBSERVER-IN-STREAM-ERROR {e:?}"));
                            break;
                        }
                        _ => continue,
                    };
                    let name = name_of(&p);
                    let t0 = { subs_w.lock().await.get(&name).copied() };
                    let Some(t0) = t0 else { continue };
                    let el = t0.elapsed().as_secs_f64();
                    let phase = phase_of(&p);
                    if phase != "Pending" {
                        run_w.lock().await.entry(name.clone()).or_insert(el);
                    }
                    if container_started(&p) {
                        start_w.lock().await.entry(name.clone()).or_insert(el);
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    log(&format!("OBSERVER-ERROR {e}"));
                    break;
                }
                Err(_) => break,
            }
        }
    });

    // Concurrent submission: every create is issued at once, so no pod's
    // measured latency absorbs another's submission cost.
    let mut creates = Vec::new();
    for i in 0..n {
        let api = api.clone();
        let image = image.clone();
        let tag = tag.clone();
        let submits = submits.clone();
        let name = format!("lat-{tag}-{i}");
        creates.push(tokio::spawn(async move {
            let spec: Pod = serde_json::from_value(json!({
                "apiVersion": "v1", "kind": "Pod",
                "metadata": { "name": name, "labels": { "spike": "t101", "batch": tag } },
                "spec": {
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 0,
                    "containers": [{
                        "name": "w", "image": image,
                        "command": ["sh", "-c", "sleep 6"],
                        "resources": { "requests": { "cpu": "5m", "memory": "12Mi" },
                                       "limits": { "memory": "48Mi" } }
                    }]
                }
            }))
            .expect("pod json");
            submits.lock().await.insert(name.clone(), Instant::now());
            match api.create(&PostParams::default(), &spec).await {
                Ok(_) => {}
                Err(e) => log(&format!("CREATE-ERROR {name} {e}")),
            }
        }));
    }
    for c in creates {
        let _ = c.await;
    }
    log(&format!("SUBMITTED n={n} tag={tag} image={image}"));

    // Wait until every pod has both marks, or 180s.
    let hard = Instant::now() + Duration::from_secs(180);
    loop {
        let (r, s) = (running.lock().await.len(), started.lock().await.len());
        if (r >= n && s >= n) || Instant::now() > hard {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    observer.abort();

    let to_stats = |m: &HashMap<String, f64>| -> serde_json::Value {
        let mut v: Vec<f64> = m.values().copied().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if v.is_empty() {
            return json!({ "n": 0 });
        }
        let pick = |q: f64| -> f64 {
            let idx = ((q * v.len() as f64).ceil() as usize).max(1) - 1;
            v[idx.min(v.len() - 1)]
        };
        let mean = v.iter().sum::<f64>() / v.len() as f64;
        let sd = (v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / v.len() as f64).sqrt();
        json!({
            "n": v.len(), "min": v[0], "p50": pick(0.5), "p99": pick(0.99),
            "max": v[v.len() - 1], "mean": mean, "stdev": sd
        })
    };

    let out = json!({
        "tag": tag, "n": n, "image": image,
        "submission_to_phase_running": to_stats(&*running.lock().await),
        "submission_to_container_started": to_stats(&*started.lock().await),
    });
    println!("RESULT {}", serde_json::to_string(&out)?);

    let _ = api
        .delete_collection(
            &DeleteParams::default().grace_period(0),
            &ListParams::default().labels(&format!("spike=t101,batch={tag}")),
        )
        .await;
    Ok(())
}
