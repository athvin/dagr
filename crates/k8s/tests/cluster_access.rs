//! Cluster access: kubeconfig on a developer's machine, service account inside a
//! pod, and an actionable error when there is neither.
//!
//! The laptop path is the point of the feature — "iterate locally, execute
//! remotely" is why the state path takes no callback — so out-of-cluster is not a
//! convenience here, it is the primary configuration. In-cluster matters for the
//! day the orchestrator is itself a pod. Resolution is a *pure* decision over
//! what exists on disk, so it is testable on both CI platforms with no cluster
//! and no client.

use std::fs;
use std::path::{Path, PathBuf};

use dagr_k8s::access::{AccessProbe, ClusterAccess, KubeconfigSource, resolve};

/// A scratch directory that removes itself, so the suite leaves nothing behind.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "dagr-k8s-access-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after the epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&dir).expect("scratch directory");
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn write(&self, relative: &str, contents: &str) -> PathBuf {
        let path = self.0.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("scratch parent");
        }
        fs::write(&path, contents).expect("scratch file");
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A probe that finds nothing: an empty home, an empty service-account directory,
/// and no Kubernetes service environment.
fn empty_probe(scratch: &Scratch) -> AccessProbe {
    AccessProbe {
        kubeconfig_env: None,
        home: Some(scratch.path().join("home")),
        service_account_dir: scratch.path().join("serviceaccount"),
        service_host: None,
        service_port: None,
    }
}

/// **Test-plan scenario: given a kubeconfig, the client configures out-of-cluster.**
#[test]
fn a_kubeconfig_configures_out_of_cluster() {
    let scratch = Scratch::new("kubeconfig");
    let path = scratch.write("home/.kube/config", "apiVersion: v1\nkind: Config\n");

    let access = resolve(&empty_probe(&scratch)).expect("a home kubeconfig is enough");
    match access {
        ClusterAccess::OutOfCluster {
            kubeconfig,
            source: KubeconfigSource::Home,
        } => assert_eq!(kubeconfig, path),
        other => panic!("expected an out-of-cluster resolution, got {other:?}"),
    }
}

/// An explicit `KUBECONFIG` beats an ambient home file: explicit operator intent
/// outranks a file that merely happens to be there.
#[test]
fn an_explicit_kubeconfig_outranks_the_home_file() {
    let scratch = Scratch::new("explicit");
    scratch.write("home/.kube/config", "apiVersion: v1\nkind: Config\n");
    let explicit = scratch.write("elsewhere/admin.yaml", "apiVersion: v1\nkind: Config\n");

    let mut probe = empty_probe(&scratch);
    probe.kubeconfig_env = Some(explicit.clone().into_os_string());

    match resolve(&probe).expect("the explicit path resolves") {
        ClusterAccess::OutOfCluster {
            kubeconfig,
            source: KubeconfigSource::Environment,
        } => assert_eq!(kubeconfig, explicit),
        other => panic!("expected the explicit kubeconfig, got {other:?}"),
    }
}

/// **Test-plan scenario: given in-cluster service-account files, it configures
/// in-cluster.** The three files *and* the two environment variables — a token
/// with no service address is not an in-cluster environment.
#[test]
fn service_account_files_configure_in_cluster() {
    let scratch = Scratch::new("incluster");
    let token = scratch.write("serviceaccount/token", "a.b.c");
    let ca = scratch.write("serviceaccount/ca.crt", "-----BEGIN CERTIFICATE-----\n");
    let namespace = scratch.write("serviceaccount/namespace", "dagr");

    let mut probe = empty_probe(&scratch);
    probe.service_host = Some("10.96.0.1".to_string());
    probe.service_port = Some("443".to_string());

    match resolve(&probe).expect("a mounted service account is enough") {
        ClusterAccess::InCluster {
            token: t,
            ca: c,
            namespace: n,
            host,
            port,
        } => {
            assert_eq!(t, token);
            assert_eq!(c, ca);
            assert_eq!(n, namespace);
            assert_eq!(host, "10.96.0.1");
            assert_eq!(port, "443");
        }
        other @ ClusterAccess::OutOfCluster { .. } => {
            panic!("expected an in-cluster resolution, got {other:?}")
        }
    }
}

/// Being a pod is a stronger signal than a stray home file, so in-cluster
/// outranks `$HOME/.kube/config` — but not an explicit `KUBECONFIG`.
#[test]
fn in_cluster_outranks_the_home_file_but_not_an_explicit_kubeconfig() {
    let scratch = Scratch::new("precedence");
    scratch.write("home/.kube/config", "apiVersion: v1\nkind: Config\n");
    scratch.write("serviceaccount/token", "a.b.c");
    scratch.write("serviceaccount/ca.crt", "-----BEGIN CERTIFICATE-----\n");
    scratch.write("serviceaccount/namespace", "dagr");
    let explicit = scratch.write("elsewhere/admin.yaml", "apiVersion: v1\nkind: Config\n");

    let mut probe = empty_probe(&scratch);
    probe.service_host = Some("10.96.0.1".to_string());
    probe.service_port = Some("443".to_string());

    assert!(matches!(
        resolve(&probe).expect("in-cluster wins over the home file"),
        ClusterAccess::InCluster { .. }
    ));

    probe.kubeconfig_env = Some(explicit.into_os_string());
    assert!(matches!(
        resolve(&probe).expect("an explicit kubeconfig wins over in-cluster"),
        ClusterAccess::OutOfCluster { .. }
    ));
}

/// **Test-plan scenario: given neither, it fails with an actionable error naming
/// what it looked for.**
#[test]
fn neither_present_is_an_actionable_error_naming_what_it_looked_for() {
    let scratch = Scratch::new("neither");
    let probe = empty_probe(&scratch);

    let err = resolve(&probe).expect_err("nothing is configured");
    let message = err.to_string();

    for expected in [
        "KUBECONFIG",
        ".kube/config",
        "token",
        "ca.crt",
        "namespace",
        "KUBERNETES_SERVICE_HOST",
        "KUBERNETES_SERVICE_PORT",
    ] {
        assert!(
            message.contains(expected),
            "the error must name {expected}; it said: {message}"
        );
    }
    assert!(
        message.contains(
            &scratch
                .path()
                .join("home")
                .join(".kube")
                .display()
                .to_string()
        ) || message.contains(&scratch.path().display().to_string()),
        "the error must name the paths it actually probed: {message}"
    );
}

/// An explicit `KUBECONFIG` that does not exist is a misconfiguration, not a
/// reason to quietly use something else. Silent substitution is the failure mode
/// this project refuses everywhere else it can: an operator who named a file
/// deserves to be told the file is not there.
#[test]
fn an_explicit_kubeconfig_that_does_not_exist_is_refused_by_name() {
    let scratch = Scratch::new("missing");
    scratch.write("home/.kube/config", "apiVersion: v1\nkind: Config\n");

    let mut probe = empty_probe(&scratch);
    let missing = scratch.path().join("nope.yaml");
    probe.kubeconfig_env = Some(missing.clone().into_os_string());

    let err = resolve(&probe).expect_err("a named file that is absent is an error");
    let message = err.to_string();
    assert!(
        message.contains(&missing.display().to_string()),
        "the refusal must name the missing file: {message}"
    );
    assert!(message.contains("KUBECONFIG"));
}

/// `KUBECONFIG` may carry several paths. The first that exists wins, and all of
/// them are named when none does.
#[test]
fn a_multi_path_kubeconfig_takes_the_first_that_exists() {
    let scratch = Scratch::new("multipath");
    let second = scratch.write("b/config", "apiVersion: v1\nkind: Config\n");
    let first = scratch.path().join("a").join("config");

    let mut probe = empty_probe(&scratch);
    let joined = std::env::join_paths([first.clone(), second.clone()]).expect("joinable paths");
    probe.kubeconfig_env = Some(joined);

    match resolve(&probe).expect("the second entry exists") {
        ClusterAccess::OutOfCluster { kubeconfig, .. } => assert_eq!(kubeconfig, second),
        other @ ClusterAccess::InCluster { .. } => {
            panic!("expected an out-of-cluster resolution, got {other:?}")
        }
    }
}

/// The default probe reads the real environment, so an orchestrator gets the
/// behaviour above with no wiring. It is asserted structurally rather than by
/// mutating process-global state, which a parallel test suite must never do.
#[test]
fn the_default_probe_reads_the_ambient_environment() {
    let probe = AccessProbe::from_env();
    assert_eq!(
        probe.service_account_dir,
        PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount"),
        "the in-cluster mount point is the platform's, not ours to choose"
    );
    assert_eq!(
        probe.kubeconfig_env,
        std::env::var_os("KUBECONFIG"),
        "the probe reads KUBECONFIG rather than inventing one"
    );
    assert_eq!(
        probe.service_host,
        std::env::var("KUBERNETES_SERVICE_HOST").ok()
    );
    assert_eq!(
        probe.service_port,
        std::env::var("KUBERNETES_SERVICE_PORT").ok()
    );
}
