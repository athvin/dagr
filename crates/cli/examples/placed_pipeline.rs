//! **One binary, two executors** — a pipeline with a node placed on remote compute.
//!
//! This is the M10 capability demo (T112, ADR 115). The pipeline is deliberately
//! small: a `sample` node that declares a **size** (500m CPU, 512Mi memory) and a
//! `summarise` node that does not. Run it two ways from the same binary:
//!
//! ```text
//! # Local — every node in this process. No cluster, no kubeconfig, no warning.
//! cargo run --example placed_pipeline -- run placed_pipeline --store ./runs
//! cargo run --example placed_pipeline -- run placed_pipeline --store ./runs --dagr.executor=local
//!
//! # Placed — `sample` becomes one pod with those requests; `summarise` stays here.
//! # Needs a build that compiled the default-off `k8s` feature, and the three
//! # deployment facts below.
//! DAGR_DEMO_NAMESPACE=dagr \
//! DAGR_DEMO_IMAGE=localhost/dagr-placed-demo:latest \
//! DAGR_DEMO_IMAGE_DIGEST=sha256:… \
//! DAGR_DEMO_BLOBS=/dagr-blobs \
//!   cargo run --features k8s --example placed_pipeline -- \
//!     run placed_pipeline --store ./runs --dagr.executor=k8s --run-id my-run-1
//! ```
//!
//! ## What is worth copying, and what is not
//!
//! The **pipeline** is worth copying verbatim: a node declares where it wants to run
//! by attaching a [`Placement`] at registration, through the `Payload`-bounded
//! `register_source_placed` registrar. That bound is the compile-time half of remote
//! eligibility — a placed node whose value cannot cross a process boundary is a
//! compile error naming the missing bound, not a run that discovers it at submission
//! time.
//!
//! The **deployment facts** — namespace, image, digest, shared blob container — are
//! read from this example's own environment variables, and that is a property of the
//! example rather than of dagr. They are not `--dagr.*` knobs: a namespace and an
//! image digest are facts about how *this binary was deployed*, and the binary that
//! was built into that image is the thing that knows them. Your `main` will get them
//! from wherever your deployment puts them.
//!
//! ## What placement changes, and what it does not
//!
//! Placement is node **policy**, not an execution class. It feeds the *policy hash*
//! and never the structural fingerprint, so moving a node between local and placed is
//! a printed policy diff a resume proceeds through — never a refusal. Under
//! `--dagr.executor=local` a placement is **recorded and ignored**: it is in the graph
//! artifact, and the node runs in-process with its ordinary declared cost. That is
//! what makes one binary genuinely both.
//!
//! Read `docs/cookbook.md`'s *Placing a node on remote compute* before the first real
//! run: it has the RBAC an operator must apply, the shared-volume versus object-store
//! choice, and the latency caveat that decides whether placement is worth it at all.

use std::process::ExitCode;

use dagr_cli::registry::{FlowRegistry, run_registry};
use dagr_cli::run_flow::RunnableFlow;
use dagr_core::assembly::{NodePolicy, Placement};
use dagr_core::context::RunContext;
use dagr_core::task::Task;
// `Payload` and `StableName` each name a trait *and* a derive; the one import
// brings both, which is what lets a payload type be declared in three lines.
use dagr_core::{Payload, StableName, TaskError};

/// The flow's name — what `run <flow>` selects and what the run store is keyed on.
const FLOW: &str = "placed_pipeline";

// ===========================================================================
// The payload
// ===========================================================================

/// The value the placed node produces.
///
/// `#[derive(Payload)]` is what makes a node carrying it **remote-eligible**: a
/// placed attempt runs in another process, so its output has to be encodable. The
/// registrar's `T::Output: Payload` bound turns "this node cannot be placed" into a
/// compile error rather than a submission-time surprise.
#[derive(Clone, Copy, StableName, Payload)]
struct Sample {
    /// How many rows the sample covered.
    rows: u64,
}

/// The terminal value, produced locally.
#[derive(Clone, Copy, StableName, Payload)]
struct Summary {
    /// The sampled row count, doubled — the demo's only arithmetic.
    total: u64,
}

// ===========================================================================
// The tasks
// ===========================================================================

/// The **placed** node: the one whose work wants infrastructure this machine may not
/// have. In a real pipeline this is the 64 GiB join or the GPU inference step; here
/// it is a constant, so the demo is deterministic and finishes instantly.
#[derive(StableName)]
struct TakeSample {
    rows: u64,
}

impl Task for TakeSample {
    type Input = ();
    type Output = Sample;
    async fn run(&mut self, _ctx: &RunContext, _input: ()) -> Result<Sample, TaskError> {
        Ok(Sample { rows: self.rows })
    }
}

/// The **unplaced** node: it consumes the sample and runs wherever the invocation
/// runs. Mixing the two in one graph is the ordinary case, not a special one — the
/// run loop, the readiness cascade and the admission ledger do not know the
/// difference.
#[derive(StableName)]
struct Summarise;

impl Task for Summarise {
    type Input = Sample;
    type Output = Summary;
    async fn run(&mut self, _ctx: &RunContext, input: Sample) -> Result<Summary, TaskError> {
        Ok(Summary {
            total: input.rows * 2,
        })
    }
}

// ===========================================================================
// The flow
// ===========================================================================

/// Build the demo flow. A factory rather than a stored value, because a
/// `RunnableFlow` is consumed by the verb that drives it.
fn build() -> RunnableFlow {
    let mut flow = RunnableFlow::new();

    // The declared size. Every string is opaque: dagr never parses `"500m"` or
    // `"512Mi"`, it carries them to the platform verbatim, which is why a cluster
    // that grows a new unit does not need a dagr release.
    let sample = flow.register_source_placed(
        "sample",
        TakeSample { rows: 21 },
        NodePolicy::new(),
        Placement::new().cpu("500m").memory("512Mi"),
    );

    // No placement: this node runs in the orchestrator's own process under both
    // executors.
    let _summary = flow.register_payload("summarise", Summarise, sample);
    flow
}

// ===========================================================================
// main
// ===========================================================================

fn main() -> ExitCode {
    let registry = FlowRegistry::new().add(FLOW, build);
    let argv: Vec<std::ffi::OsString> = std::env::args_os().collect();

    // Under `--dagr.executor=k8s`, the run needs a cluster to place `sample` on, and
    // supplying it is the *binary's* job. `run_registry` owns every other verb.
    #[cfg(feature = "k8s")]
    if remote::selected(&argv) {
        return remote::run(&argv);
    }

    ExitCode::from(run_registry(&registry, argv).as_u8())
}

// ===========================================================================
// The remote branch
// ===========================================================================

/// Everything the placed run needs beyond the flow itself.
///
/// It is a separate module so the local example above reads as the whole story for
/// somebody who never places a node — which is most readers, most of the time.
#[cfg(feature = "k8s")]
mod remote {
    use std::ffi::OsString;
    use std::process::ExitCode;

    use dagr_cli::contract::ExitCode as DagrExit;
    use dagr_cli::driver::RunConfig;
    use dagr_cli::executor::ExecutorKind;
    use dagr_cli::remote::{RemoteCluster, RemoteTarget};
    use dagr_cli::run_store::{DEFAULT_STORE_BASE, FileSink, SystemClock};
    use dagr_k8s::access::AccessProbe;
    use dagr_k8s::client::KubePodApi;
    use dagr_k8s::observer::ObserverLimits;

    /// The deployment facts, from this example's own environment. Not `--dagr.*`
    /// knobs: see the module docs.
    const NAMESPACE: &str = "DAGR_DEMO_NAMESPACE";
    const IMAGE: &str = "DAGR_DEMO_IMAGE";
    const DIGEST: &str = "DAGR_DEMO_IMAGE_DIGEST";
    const BLOBS: &str = "DAGR_DEMO_BLOBS";

    /// Whether this invocation asked for the remote executor.
    pub fn selected(argv: &[OsString]) -> bool {
        matches!(
            dagr_cli::config::parse_executor_flag(argv),
            Ok(Some(ExecutorKind::Kubernetes))
        ) || std::env::var("DAGR_EXECUTOR").as_deref() == Ok("k8s")
    }

    /// Read one required environment variable, or say which one is missing.
    fn need(key: &str) -> Result<String, String> {
        std::env::var(key).map_err(|_| format!("the remote demo needs `{key}` in the environment"))
    }

    /// A flag's value from `--flag=value` or `--flag value`.
    fn flag(argv: &[OsString], name: &str) -> Option<String> {
        let mut it = argv.iter().map(|a| a.to_string_lossy().into_owned());
        while let Some(arg) = it.next() {
            if let Some(rest) = arg.strip_prefix(&format!("{name}=")) {
                return Some(rest.to_string());
            }
            if arg == name {
                return it.next();
            }
        }
        None
    }

    /// Drive the flow with its placed node on the cluster this environment names.
    pub fn run(argv: &[OsString]) -> ExitCode {
        // Building the client is `async`, so it needs a runtime. This one exists
        // only for the connect; every *call* on the resulting client is forwarded
        // by `run_placed` onto the runtime the pod observer owns, so the client is
        // never polled on two reactors.
        let boot = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(err) => {
                eprintln!("dagr run {}: cannot build a runtime: {err}", super::FLOW);
                return ExitCode::from(DagrExit::BootstrapFailure.as_u8());
            }
        };

        let build = || -> Result<_, String> {
            let namespace = need(NAMESPACE)?;
            let image = need(IMAGE)?;
            let digest = need(DIGEST)?;
            let blobs = need(BLOBS)?;
            let access = dagr_k8s::access::resolve(&AccessProbe::from_env())
                .map_err(|e| format!("no cluster access: {e}"))?;
            let limits = ObserverLimits::default();
            let api = boot
                .block_on(KubePodApi::connect(&access, &namespace, &limits))
                .map_err(|e| format!("cannot reach the cluster: {e}"))?;
            let target = RemoteTarget::new(namespace, image, digest, &blobs);
            Ok((api, target))
        };

        let (api, target) = match build() {
            Ok(pair) => pair,
            Err(why) => {
                eprintln!("dagr run {}: {why}", super::FLOW);
                return ExitCode::from(DagrExit::BootstrapFailure.as_u8());
            }
        };

        // Adoption after a restart is scoped to a run id, so a placed run takes an
        // explicit one. Restarting with the SAME id is what makes "every node
        // executed exactly once" hold across a kill.
        let run_id = flag(argv, "--run-id").unwrap_or_else(|| {
            eprintln!(
                "dagr run {}: no --run-id given; minting one. A restart can only adopt \
                 this run's pods if you pass the SAME id again.",
                super::FLOW
            );
            format!("placed-demo-{}", std::process::id())
        });
        let store = flag(argv, "--store").unwrap_or_else(|| DEFAULT_STORE_BASE.to_string());

        let sink = match FileSink::create_in_store(&store, super::FLOW, &run_id) {
            Ok(sink) => sink,
            Err(err) => {
                eprintln!("dagr run {}: cannot open the run store: {err}", super::FLOW);
                return ExitCode::from(DagrExit::SinkFailure.as_u8());
            }
        };

        let config = RunConfig::new(&store)
            .run_id(&run_id)
            .executor(ExecutorKind::Kubernetes);
        // The same factory the registry holds. A `RunnableFlow` is consumed by the
        // verb that drives it, so the flow is built here rather than shared.
        let flow = super::build();

        // The same client is both ports: `PodApi` is the read half the observer
        // holds, `PodLifecycle` the write half the executor and the adoption pass
        // hold. They are separate types because the RBAC grants are separable —
        // `pod-observer-rbac.yaml` grants only the read half — not because two
        // clients are needed.
        match flow.run_placed(
            super::FLOW,
            &config,
            sink,
            SystemClock::new(),
            RemoteCluster::new(api.clone(), api, target),
        ) {
            Ok(placed) => {
                for line in &placed.diagnostics {
                    eprintln!("dagr run {}: {line}", super::FLOW);
                }
                if !placed.discovery.adopted.is_empty() {
                    eprintln!(
                        "dagr run {}: adopted {} pod(s) a previous orchestrator left \
                         running: {}",
                        super::FLOW,
                        placed.discovery.adopted.len(),
                        placed.discovery.adopted.join(", ")
                    );
                }
                ExitCode::from(
                    dagr_cli::contract::exit_code_for_run(placed.report.driver_report()).as_u8(),
                )
            }
            Err(err) => {
                eprintln!("dagr run {}: {err}", super::FLOW);
                ExitCode::from(DagrExit::BootstrapFailure.as_u8())
            }
        }
    }
}
