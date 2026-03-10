use std::collections::HashMap;
use std::marker::PhantomData;

use crate::api::{IngressConfig, JobSubmitRequestItem, ServiceConfig};
use crate::k8s::io::api::core::v1::PodSpec;

/// Typestate marker: builder has no pod spec yet — `.build()` is unavailable.
#[doc(hidden)]
pub struct NoPodSpec;
/// Typestate marker: builder has pod spec set — `.build()` is available.
#[doc(hidden)]
pub struct HasPodSpec;

/// Typestate builder for [`JobSubmitRequestItem`].
///
/// Start with [`JobRequestItemBuilder::new`], set fields, then call
/// `.pod_spec(spec)` or `.pod_specs(specs)` to transition to the `HasPodSpec`
/// state, which unlocks `.build()`. Attempting to call `.build()` before
/// providing a pod spec is a **compile-time error**.
///
/// # Example
///
/// ```ignore
/// let item = JobRequestItemBuilder::new()
///     .namespace("default")
///     .priority(1.0)
///     .label("app", "my-app")
///     .pod_spec(pod_spec)
///     .build();
/// ```
pub struct JobRequestItemBuilder<S> {
    priority: f64,
    namespace: String,
    client_id: String,
    labels: HashMap<String, String>,
    annotations: HashMap<String, String>,
    scheduler: String,
    ingress: Vec<IngressConfig>,
    services: Vec<ServiceConfig>,
    pod_specs: Vec<PodSpec>,
    _state: PhantomData<S>,
}

impl JobRequestItemBuilder<NoPodSpec> {
    /// Create a new builder with all fields set to their zero/empty defaults.
    ///
    /// Call setters to configure the job, then supply a pod spec with
    /// [`.pod_spec()`](JobRequestItemBuilder::pod_spec) or
    /// [`.pod_specs()`](JobRequestItemBuilder::pod_specs) to unlock
    /// [`.build()`](JobRequestItemBuilder::build).
    pub fn new() -> Self {
        Self {
            priority: 0.0,
            namespace: String::new(),
            client_id: String::new(),
            labels: HashMap::new(),
            annotations: HashMap::new(),
            scheduler: String::new(),
            ingress: Vec::new(),
            services: Vec::new(),
            pod_specs: Vec::new(),
            _state: PhantomData,
        }
    }
}

impl Default for JobRequestItemBuilder<NoPodSpec> {
    fn default() -> Self {
        Self::new()
    }
}

// Setters available in all states — preserve the state type via generics.
impl<S> JobRequestItemBuilder<S> {
    /// Job priority relative to others in the queue. Higher values are
    /// scheduled first. Must be non-negative; the server will reject negative
    /// values. Defaults to `0.0`.
    #[must_use]
    pub fn priority(mut self, p: f64) -> Self {
        self.priority = p;
        self
    }

    /// Set the Kubernetes namespace in which the job's pod will run.
    ///
    /// Typically `"default"` unless your cluster uses a dedicated namespace
    /// for batch workloads.
    #[must_use]
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = ns.into();
        self
    }

    /// Set an opaque client-supplied identifier for idempotency tracking.
    ///
    /// If the same `client_id` is submitted twice, Armada will deduplicate
    /// the request and return the existing job rather than creating a new one.
    /// Leave empty (the default) to disable deduplication.
    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    /// Replace the entire labels map.
    #[must_use]
    pub fn labels(mut self, l: HashMap<String, String>) -> Self {
        self.labels = l;
        self
    }

    /// Insert a single label. Can be chained multiple times.
    #[must_use]
    pub fn label(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.labels.insert(k.into(), v.into());
        self
    }

    /// Replace the entire annotations map.
    #[must_use]
    pub fn annotations(mut self, a: HashMap<String, String>) -> Self {
        self.annotations = a;
        self
    }

    /// Insert a single annotation. Can be chained multiple times.
    #[must_use]
    pub fn annotation(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.annotations.insert(k.into(), v.into());
        self
    }

    /// Override the scheduler for this job. Leave empty to use the cluster default.
    #[must_use]
    pub fn scheduler(mut self, s: impl Into<String>) -> Self {
        self.scheduler = s.into();
        self
    }

    /// Replace the entire ingress config list.
    #[must_use]
    pub fn ingress(mut self, i: Vec<IngressConfig>) -> Self {
        self.ingress = i;
        self
    }

    /// Append a single ingress config. Can be chained multiple times.
    #[must_use]
    pub fn add_ingress(mut self, i: IngressConfig) -> Self {
        self.ingress.push(i);
        self
    }

    /// Replace the entire service config list.
    #[must_use]
    pub fn services(mut self, s: Vec<ServiceConfig>) -> Self {
        self.services = s;
        self
    }

    /// Append a single service config. Can be chained multiple times.
    #[must_use]
    pub fn add_service(mut self, s: ServiceConfig) -> Self {
        self.services.push(s);
        self
    }

    /// Set a single pod spec and transition the builder to [`HasPodSpec`] state,
    /// which unlocks `.build()`.
    ///
    /// Shorthand for `.pod_specs(vec![spec])`. If called again on a
    /// `JobRequestItemBuilder<HasPodSpec>`, the previous spec is replaced.
    #[must_use]
    pub fn pod_spec(self, spec: PodSpec) -> JobRequestItemBuilder<HasPodSpec> {
        self.pod_specs(vec![spec])
    }

    /// Set multiple pod specs and transition the builder to [`HasPodSpec`] state,
    /// which unlocks `.build()`.
    ///
    /// If called again on a `JobRequestItemBuilder<HasPodSpec>`, the previous
    /// specs are replaced entirely.
    #[must_use]
    pub fn pod_specs(self, specs: Vec<PodSpec>) -> JobRequestItemBuilder<HasPodSpec> {
        JobRequestItemBuilder {
            priority: self.priority,
            namespace: self.namespace,
            client_id: self.client_id,
            labels: self.labels,
            annotations: self.annotations,
            scheduler: self.scheduler,
            ingress: self.ingress,
            services: self.services,
            pod_specs: specs,
            _state: PhantomData,
        }
    }
}

// `.build()` is only available when the builder is in `HasPodSpec` state.
impl JobRequestItemBuilder<HasPodSpec> {
    /// Consume the builder and return a [`JobSubmitRequestItem`] ready to be
    /// included in a [`crate::JobSubmitRequest`].
    ///
    /// This method is only available after calling
    /// [`.pod_spec()`](JobRequestItemBuilder::pod_spec) or
    /// [`.pod_specs()`](JobRequestItemBuilder::pod_specs). Calling `.build()`
    /// on a `JobRequestItemBuilder<NoPodSpec>` is a **compile-time error**.
    pub fn build(self) -> JobSubmitRequestItem {
        #[allow(deprecated)]
        JobSubmitRequestItem {
            priority: self.priority,
            namespace: self.namespace,
            client_id: self.client_id,
            labels: self.labels,
            annotations: self.annotations,
            pod_specs: self.pod_specs,
            ingress: self.ingress,
            services: self.services,
            scheduler: self.scheduler,
            // Deprecated singular fields — zeroed, not used by this builder
            pod_spec: None,
            required_node_labels: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_pod_spec() -> PodSpec {
        PodSpec {
            containers: vec![],
            ..Default::default()
        }
    }

    #[test]
    fn builder_with_pod_specs_builds_correctly() {
        let spec = minimal_pod_spec();
        let item = JobRequestItemBuilder::new()
            .namespace("default")
            .priority(1.0)
            .pod_specs(vec![spec])
            .build();

        assert_eq!(item.namespace, "default");
        assert_eq!(item.priority, 1.0);
        assert_eq!(item.pod_specs.len(), 1);
    }

    #[test]
    fn pod_spec_singular_shorthand() {
        let item = JobRequestItemBuilder::new()
            .pod_spec(minimal_pod_spec())
            .build();
        assert_eq!(item.pod_specs.len(), 1);
    }

    #[test]
    fn pod_spec_called_twice_replaces_previous() {
        let spec_a = minimal_pod_spec();
        let mut spec_b = minimal_pod_spec();
        spec_b.restart_policy = Some("Never".to_string());

        let item = JobRequestItemBuilder::new()
            .pod_spec(spec_a)
            .pod_spec(spec_b)
            .build();

        assert_eq!(item.pod_specs.len(), 1);
        assert_eq!(item.pod_specs[0].restart_policy.as_deref(), Some("Never"));
    }

    #[test]
    fn label_and_annotation_helpers() {
        let item = JobRequestItemBuilder::new()
            .label("app", "my-app")
            .label("env", "prod")
            .annotation("owner", "team-a")
            .pod_spec(minimal_pod_spec())
            .build();

        assert_eq!(item.labels.get("app").map(String::as_str), Some("my-app"));
        assert_eq!(item.labels.get("env").map(String::as_str), Some("prod"));
        assert_eq!(
            item.annotations.get("owner").map(String::as_str),
            Some("team-a")
        );
    }

    #[test]
    fn optional_fields_default_to_empty() {
        let item = JobRequestItemBuilder::new()
            .pod_specs(vec![minimal_pod_spec()])
            .build();

        assert_eq!(item.priority, 0.0);
        assert!(item.namespace.is_empty());
        assert!(item.client_id.is_empty());
        assert!(item.labels.is_empty());
        assert!(item.annotations.is_empty());
        assert!(item.scheduler.is_empty());
        assert!(item.ingress.is_empty());
        assert!(item.services.is_empty());
    }
}
