//! **Cluster access** — kubeconfig on a developer's machine, service account
//! inside a pod, and an actionable error when there is neither.
//!
//! The laptop path is the point of the feature: the state path takes no callback
//! precisely so an orchestrator on a developer's machine — which a pod in a
//! cluster cannot dial — can place work remotely. So out-of-cluster is not a
//! convenience here, it is the primary configuration; in-cluster matters for the
//! day the orchestrator is itself a pod.
//!
//! Resolution is a **pure decision** over what exists on disk and what is in the
//! environment, deliberately separated from the client that consumes it. That is
//! what makes the precedence, and the error, testable on both platforms with no
//! cluster and no client compiled in.
//!
//! # Precedence, and why
//!
//! 1. **An explicit `KUBECONFIG`.** An operator who named a file meant it.
//! 2. **A mounted service account** *and* the service address in the
//!    environment. Being a pod is a strong ambient signal; a token with no
//!    service address is not an in-cluster environment, it is a leftover mount.
//! 3. **`$HOME/.kube/config`.** The weakest signal: a file that merely happens to
//!    be there.
//!
//! An explicit `KUBECONFIG` naming a file that does not exist is an **error**,
//! not a reason to fall through. Silently using something else is the substitution
//! failure this project refuses everywhere it can: an operator who asked for one
//! cluster and got another would have no way to notice.

use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};

/// Where an in-cluster service account is mounted. The platform's path, not ours.
pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// The environment variable naming a kubeconfig explicitly.
pub const KUBECONFIG_VAR: &str = "KUBECONFIG";

/// The environment variable carrying the in-cluster API service host.
pub const SERVICE_HOST_VAR: &str = "KUBERNETES_SERVICE_HOST";

/// The environment variable carrying the in-cluster API service port.
pub const SERVICE_PORT_VAR: &str = "KUBERNETES_SERVICE_PORT";

/// The kubeconfig path relative to a home directory.
const HOME_KUBECONFIG: &str = ".kube/config";

/// What resolution is allowed to look at.
///
/// Injected rather than read inline so the decision is a function of its inputs.
/// A test that had to mutate process-global environment state to exercise a
/// branch would be a test that cannot run in parallel with any other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessProbe {
    /// The `KUBECONFIG` value, if set. May name several paths, separated the way
    /// the platform separates a path list.
    pub kubeconfig_env: Option<OsString>,
    /// The home directory to look for `.kube/config` under.
    pub home: Option<PathBuf>,
    /// Where the service account would be mounted.
    pub service_account_dir: PathBuf,
    /// The in-cluster API service host, if set.
    pub service_host: Option<String>,
    /// The in-cluster API service port, if set.
    pub service_port: Option<String>,
}

impl AccessProbe {
    /// Read the ambient environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self {
            kubeconfig_env: std::env::var_os(KUBECONFIG_VAR),
            home: home_dir(),
            service_account_dir: PathBuf::from(SERVICE_ACCOUNT_DIR),
            service_host: std::env::var(SERVICE_HOST_VAR).ok(),
            service_port: std::env::var(SERVICE_PORT_VAR).ok(),
        }
    }
}

/// The user's home directory, without reaching for a deprecated helper or a
/// dependency: `HOME` on Unix, `USERPROFILE` on Windows.
fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Which kubeconfig was chosen, and on whose say-so.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KubeconfigSource {
    /// Named explicitly by [`KUBECONFIG_VAR`].
    Environment,
    /// Found under the home directory.
    Home,
}

/// How this process reaches the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAccess {
    /// A kubeconfig, on a machine that is not in the cluster.
    OutOfCluster {
        /// The file to read.
        kubeconfig: PathBuf,
        /// Who chose it.
        source: KubeconfigSource,
    },
    /// A mounted service account, in a pod.
    InCluster {
        /// The projected token.
        token: PathBuf,
        /// The API server's certificate authority bundle.
        ca: PathBuf,
        /// The namespace this pod runs in.
        namespace: PathBuf,
        /// The API service host.
        host: String,
        /// The API service port.
        port: String,
    },
}

/// Nothing configured the cluster, and here is everything that was tried.
///
/// The list is the point. "Could not find a cluster" is not actionable; naming
/// the two environment variables, the explicit path, the home path and the three
/// service-account files is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoClusterAccess {
    /// Each place that was looked at, in the order it was tried.
    pub looked_for: Vec<String>,
}

impl fmt::Display for NoClusterAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "no Kubernetes cluster is configured for this process — looked for: {}",
            self.looked_for.join("; ")
        )
    }
}

impl std::error::Error for NoClusterAccess {}

/// Decide how this process reaches the cluster.
///
/// # Errors
///
/// Returns [`NoClusterAccess`], naming every path and variable it consulted,
/// when nothing is configured — and also when `KUBECONFIG` names a file that does
/// not exist, because an explicit setting that is wrong is a misconfiguration
/// rather than an invitation to substitute something else.
pub fn resolve(probe: &AccessProbe) -> Result<ClusterAccess, NoClusterAccess> {
    let mut looked_for = Vec::new();

    // 1. Explicit intent.
    if let Some(raw) = probe.kubeconfig_env.as_ref() {
        let candidates: Vec<PathBuf> = std::env::split_paths(raw)
            .filter(|path| !path.as_os_str().is_empty())
            .collect();
        for candidate in &candidates {
            if candidate.is_file() {
                return Ok(ClusterAccess::OutOfCluster {
                    kubeconfig: candidate.clone(),
                    source: KubeconfigSource::Environment,
                });
            }
        }
        if candidates.is_empty() {
            // A shell that exports an empty variable has not named a file, so
            // this is an absent setting rather than a wrong one.
            looked_for.push(format!("{KUBECONFIG_VAR} (set but empty)"));
        } else {
            // Named, and not there. Refuse by name rather than fall through.
            let named = candidates
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(NoClusterAccess {
                looked_for: vec![format!(
                    "{KUBECONFIG_VAR} names {named}, and no such file exists — \
                     refusing to substitute another cluster for the one you asked for"
                )],
            });
        }
    } else {
        looked_for.push(format!("{KUBECONFIG_VAR} (unset)"));
    }

    // 2. Being a pod.
    let token = probe.service_account_dir.join("token");
    let ca = probe.service_account_dir.join("ca.crt");
    let namespace = probe.service_account_dir.join("namespace");
    let files_present = token.is_file() && ca.is_file() && namespace.is_file();
    match (
        files_present,
        probe.service_host.clone(),
        probe.service_port.clone(),
    ) {
        (true, Some(host), Some(port)) => {
            return Ok(ClusterAccess::InCluster {
                token,
                ca,
                namespace,
                host,
                port,
            });
        }
        _ => {
            looked_for.push(format!(
                "an in-cluster service account: {}, {}, {} plus {SERVICE_HOST_VAR} and \
                 {SERVICE_PORT_VAR}",
                token.display(),
                ca.display(),
                namespace.display()
            ));
        }
    }

    // 3. A file that happens to be there.
    if let Some(home) = probe.home.as_ref() {
        let candidate = home_kubeconfig(home);
        if candidate.is_file() {
            return Ok(ClusterAccess::OutOfCluster {
                kubeconfig: candidate,
                source: KubeconfigSource::Home,
            });
        }
        looked_for.push(candidate.display().to_string());
    } else {
        looked_for.push(format!("a home directory to find {HOME_KUBECONFIG} under"));
    }

    Err(NoClusterAccess { looked_for })
}

/// `<home>/.kube/config`, built one component at a time so the separator is the
/// platform's.
fn home_kubeconfig(home: &Path) -> PathBuf {
    let mut path = home.to_path_buf();
    for component in HOME_KUBECONFIG.split('/') {
        path.push(component);
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_home_kubeconfig_path_is_built_from_components() {
        let path = home_kubeconfig(Path::new("/home/dev"));
        assert!(path.ends_with("config"));
        assert!(path.parent().is_some_and(|p| p.ends_with(".kube")));
    }

    #[test]
    fn an_empty_kubeconfig_value_is_treated_as_unset_intent() {
        // A shell that exports an empty variable has not named a file, so the
        // refusal names the variable rather than an empty path.
        let probe = AccessProbe {
            kubeconfig_env: Some(OsString::new()),
            home: None,
            service_account_dir: PathBuf::from("/nonexistent-dagr-probe"),
            service_host: None,
            service_port: None,
        };
        let err = resolve(&probe).expect_err("nothing is configured");
        assert!(err.to_string().contains(KUBECONFIG_VAR));
    }
}
