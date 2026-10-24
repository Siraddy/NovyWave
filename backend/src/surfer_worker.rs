//! Typed adapter for the waveform worker v1 HTTP/JSON contract.
//!
//! Wire structs intentionally live only in this module. NovyWave's shared
//! messages and renderer continue to use their existing semantic types.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use shared::{
    FileFormat, ScopeData, Signal, SignalTransition, SignalValue, SourceLocation, SourceMatch,
    SourceOperation, SourceResolution, SourceResolutionStatus, SourceSignalIdentity, SourceWarning,
    SurferFileSummary, SurferReloadOutcome, SurferWorkerError, UnifiedSignalData,
    UnifiedSignalRequest, WaveformFile,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

const CONTRACT_VERSION: u32 = 1;
const WORKER_URL_ENV: &str = "NOVYWAVE_SURFER_WORKER_URL";
const NANOSECOND_IN_ZEPTOSECONDS: u128 = 1_000_000_000_000;

pub fn source_id(index: usize) -> String {
    format!("surfer-worker://file/{index}")
}

pub fn parse_source_id(value: &str) -> Option<usize> {
    let index = value.strip_prefix("surfer-worker://file/")?.parse().ok()?;
    (source_id(index) == value).then_some(index)
}

#[derive(Clone, Debug)]
pub struct RemoteSourceRoute {
    pub file_index: usize,
    pub ns_per_tick: u64,
    pub scope_paths: HashMap<String, Vec<String>>,
}

#[derive(Debug)]
pub struct LoadedRemoteFile {
    pub waveform_file: WaveformFile,
    pub route: RemoteSourceRoute,
}

#[derive(Debug)]
pub struct RemoteSignalQueryResult {
    pub signal_data: Vec<UnifiedSignalData>,
    pub cursor_values: BTreeMap<String, SignalValue>,
}

#[derive(Clone)]
pub struct SurferWorkerClient {
    base_url: reqwest::Url,
    http: reqwest::Client,
}

impl SurferWorkerClient {
    pub fn from_env() -> Result<Self, SurferWorkerError> {
        let configured = std::env::var(WORKER_URL_ENV).map_err(|_| {
            local_error(
                "worker_not_configured",
                format!("{WORKER_URL_ENV} is not set"),
                false,
            )
        })?;
        Self::new(&configured)
    }

    fn new(base_url: &str) -> Result<Self, SurferWorkerError> {
        let normalized = format!("{}/", base_url.trim_end_matches('/'));
        let base_url = reqwest::Url::parse(&normalized).map_err(|error| {
            local_error(
                "invalid_worker_url",
                format!("Invalid {WORKER_URL_ENV}: {error}"),
                false,
            )
        })?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| {
                local_error(
                    "worker_client_initialization_failed",
                    error.to_string(),
                    false,
                )
            })?;
        Ok(Self { base_url, http })
    }

    pub async fn list_files(&self) -> Result<Vec<SurferFileSummary>, SurferWorkerError> {
        let response: WorkerFilesResponse = self.get("v1/files").await?;
        response.into_shared()
    }

    pub async fn load_file(&self, index: usize) -> Result<LoadedRemoteFile, SurferWorkerError> {
        let response: WorkerFileDetail = self.get(&format!("v1/files/{index}")).await?;
        response.into_loaded_file(index)
    }

    pub async fn query_signals(
        &self,
        route: &RemoteSourceRoute,
        requests: &[UnifiedSignalRequest],
        cursor_time_ns: Option<u64>,
    ) -> Result<RemoteSignalQueryResult, SurferWorkerError> {
        let mut wire_signals = Vec::with_capacity(requests.len());
        let mut seen = HashSet::with_capacity(requests.len());
        for request in requests {
            let scope_path = route.scope_paths.get(&request.scope_path).ok_or_else(|| {
                local_error(
                    "unknown_remote_scope",
                    format!(
                        "Worker source {} has no scope named '{}'",
                        route.file_index, request.scope_path
                    ),
                    false,
                )
            })?;
            if seen.insert((scope_path.clone(), request.variable_name.clone())) {
                wire_signals.push(WorkerSignalSelector {
                    scope_path: scope_path.clone(),
                    name: request.variable_name.clone(),
                });
            }
        }

        let cursor_tick = cursor_time_ns.map(|time_ns| time_ns / route.ns_per_tick);
        let (common_range, common_max_transitions) =
            worker_query_controls(requests, route.ns_per_tick);
        let worker_range_requested = common_range.is_some();
        let worker_downsample_requested = common_max_transitions.is_some();
        let body = WorkerSignalQueryRequest {
            signals: wire_signals,
            cursor_tick,
            range: common_range,
            max_transitions_per_signal: common_max_transitions,
            downsample: common_max_transitions.map(|_| WorkerDownsampleRequest {
                strategy: WorkerDownsampleStrategy::PreserveEdges,
            }),
        };
        let response: WorkerSignalQueryResponse = self
            .post(
                &format!("v1/files/{}/signals/query", route.file_index),
                &body,
            )
            .await?;
        response.into_shared(
            route,
            requests,
            cursor_time_ns.is_some(),
            worker_range_requested,
            worker_downsample_requested,
        )
    }

    pub async fn resolve_source(
        &self,
        index: usize,
        source_id: &str,
        operation: SourceOperation,
        signal: &SourceSignalIdentity,
        workspace_id: Option<&str>,
    ) -> Result<SourceResolution, SurferWorkerError> {
        let body = WorkerCrossprobeRequest {
            operation,
            signal: WorkerSourceSignal {
                scope_path: signal.scope_path.clone(),
                name: signal.name.clone(),
                waveform_file_index: Some(index),
                waveform_source_id: Some(source_id.to_string()),
                index_range: signal.index_range.clone().map(Into::into),
                language: signal.language.clone(),
                top_module: signal.top_module.clone(),
                elaboration_context: signal.elaboration_context.clone(),
            },
            workspace_id: workspace_id.map(str::to_string),
        };
        let response: WorkerCrossprobeResponse = self
            .post(&format!("v1/files/{index}/crossprobe"), &body)
            .await?;
        response.into_shared(index, operation)
    }

    pub async fn reload_file(
        &self,
        index: usize,
    ) -> Result<SurferReloadOutcome, SurferWorkerError> {
        let response: WorkerReloadResponse = self
            .post(&format!("v1/files/{index}/reload"), &EmptyRequest {})
            .await?;
        validate_contract_version(response.contract_version)?;
        if response.file_index != index || response.file.index != index {
            return Err(local_error(
                "invalid_worker_response",
                format!(
                    "Reload response identified file {} / metadata {}, expected {index}",
                    response.file_index, response.file.index
                ),
                false,
            ));
        }
        Ok(match response.outcome {
            WorkerReloadOutcome::Accepted => SurferReloadOutcome::Accepted,
            WorkerReloadOutcome::Unchanged => SurferReloadOutcome::Unchanged,
        })
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, SurferWorkerError> {
        let url = self.endpoint(path)?;
        let response = self.http.get(url).send().await.map_err(transport_error)?;
        decode_response(response).await
    }

    async fn post<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, SurferWorkerError> {
        let url = self.endpoint(path)?;
        let response = self
            .http
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(transport_error)?;
        decode_response(response).await
    }

    fn endpoint(&self, path: &str) -> Result<reqwest::Url, SurferWorkerError> {
        self.base_url.join(path).map_err(|error| {
            local_error(
                "invalid_worker_url",
                format!("Failed to construct worker endpoint: {error}"),
                false,
            )
        })
    }
}

async fn decode_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, SurferWorkerError> {
    let status = response.status();
    let bytes = response.bytes().await.map_err(transport_error)?;
    if !status.is_success() {
        if let Ok(envelope) = serde_json::from_slice::<WorkerErrorEnvelope>(&bytes) {
            if envelope.contract_version != CONTRACT_VERSION {
                return Err(version_error(envelope.contract_version));
            }
            return Err(envelope.error.into_shared());
        }
        return Err(local_error(
            "worker_http_error",
            format!("Worker returned HTTP {status}"),
            status.is_server_error(),
        ));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        local_error(
            "invalid_worker_response",
            format!("Worker returned invalid JSON: {error}"),
            false,
        )
    })
}

fn transport_error(error: reqwest::Error) -> SurferWorkerError {
    local_error("worker_unavailable", error.to_string(), true)
}

fn local_error(
    code: impl Into<String>,
    message: impl Into<String>,
    retryable: bool,
) -> SurferWorkerError {
    SurferWorkerError {
        code: code.into(),
        message: message.into(),
        retryable,
    }
}

fn validate_contract_version(version: u32) -> Result<(), SurferWorkerError> {
    if version == CONTRACT_VERSION {
        Ok(())
    } else {
        Err(version_error(version))
    }
}

fn version_error(actual: u32) -> SurferWorkerError {
    local_error(
        "unsupported_contract_version",
        format!("Worker contract version {actual} is not supported; expected {CONTRACT_VERSION}"),
        false,
    )
}

#[derive(Debug, Deserialize)]
struct WorkerFilesResponse {
    contract_version: u32,
    files: Vec<WorkerFileMetadata>,
}

impl WorkerFilesResponse {
    fn into_shared(self) -> Result<Vec<SurferFileSummary>, SurferWorkerError> {
        validate_contract_version(self.contract_version)?;
        let mut seen_indices = HashSet::with_capacity(self.files.len());
        let mut files = Vec::with_capacity(self.files.len());
        for file in self.files {
            if !seen_indices.insert(file.index) {
                return Err(local_error(
                    "duplicate_worker_file_index",
                    format!("Worker listed file index {} more than once", file.index),
                    false,
                ));
            }
            files.push(file.into_shared());
        }
        Ok(files)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct WorkerFileMetadata {
    index: usize,
    #[serde(default, rename = "generation_id")]
    _generation_id: Option<u64>,
    filename: String,
    format: String,
    bytes: u64,
    bytes_loaded: u64,
    reloading: bool,
    last_load_ok: bool,
    #[serde(rename = "last_modified")]
    _last_modified: Option<WorkerTimestamp>,
}

impl WorkerFileMetadata {
    fn into_shared(self) -> SurferFileSummary {
        SurferFileSummary {
            index: self.index,
            source_id: source_id(self.index),
            filename: self.filename,
            format: self.format,
            bytes: self.bytes,
            bytes_loaded: self.bytes_loaded,
            reloading: self.reloading,
            last_load_ok: self.last_load_ok,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct WorkerTimestamp {
    #[serde(rename = "secs_since_epoch")]
    _secs_since_epoch: u64,
    #[serde(rename = "nanos_since_epoch")]
    _nanos_since_epoch: u32,
}

#[derive(Debug, Deserialize)]
struct WorkerFileDetail {
    contract_version: u32,
    #[serde(default, rename = "generation_id")]
    _generation_id: Option<u64>,
    file: WorkerFileMetadata,
    timescale: Option<WorkerTimescale>,
    time_range: Option<WorkerTickRange>,
    scopes: Vec<WorkerScope>,
    signals: Vec<WorkerSignal>,
}

impl WorkerFileDetail {
    fn into_loaded_file(
        self,
        expected_index: usize,
    ) -> Result<LoadedRemoteFile, SurferWorkerError> {
        validate_contract_version(self.contract_version)?;
        if self.file.index != expected_index {
            return Err(local_error(
                "invalid_worker_response",
                format!(
                    "File detail identified index {}, expected {expected_index}",
                    self.file.index
                ),
                false,
            ));
        }
        let timescale = self.timescale.ok_or_else(|| {
            local_error(
                "missing_timescale",
                format!("Worker file {expected_index} does not declare a timescale"),
                false,
            )
        })?;
        let ns_per_tick = timescale.ns_per_tick()?;
        let time_range = self.time_range.ok_or_else(|| {
            local_error(
                "empty_time_range",
                format!("Worker file {expected_index} has no waveform time range"),
                false,
            )
        })?;
        let source_id = source_id(expected_index);
        let format = parse_file_format(&self.file.format)?;
        let scope_paths = collect_scope_paths(&self.scopes, &self.signals);
        let scopes = build_scope_tree(&source_id, &self.scopes, &self.signals);
        let filename = Path::new(&self.file.filename)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(&self.file.filename)
            .to_string();
        let min_time_ns = Some(tick_to_ns(time_range.start_tick, ns_per_tick)?);
        let max_time_ns = Some(tick_to_ns(time_range.end_tick, ns_per_tick)?);

        Ok(LoadedRemoteFile {
            waveform_file: WaveformFile {
                id: source_id.clone(),
                filename,
                format,
                scopes,
                min_time_ns,
                max_time_ns,
            },
            route: RemoteSourceRoute {
                file_index: expected_index,
                ns_per_tick,
                scope_paths,
            },
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
struct WorkerTimescale {
    multiplier: u64,
    unit: WorkerTimescaleUnit,
}

impl WorkerTimescale {
    fn ns_per_tick(self) -> Result<u64, SurferWorkerError> {
        let unit_zs = match self.unit {
            WorkerTimescaleUnit::Zs => 1u128,
            WorkerTimescaleUnit::As => 1_000u128,
            WorkerTimescaleUnit::Fs => 1_000_000u128,
            WorkerTimescaleUnit::Ps => 1_000_000_000u128,
            WorkerTimescaleUnit::Ns => NANOSECOND_IN_ZEPTOSECONDS,
            WorkerTimescaleUnit::Us => 1_000 * NANOSECOND_IN_ZEPTOSECONDS,
            WorkerTimescaleUnit::Ms => 1_000_000 * NANOSECOND_IN_ZEPTOSECONDS,
            WorkerTimescaleUnit::S => 1_000_000_000 * NANOSECOND_IN_ZEPTOSECONDS,
            WorkerTimescaleUnit::Unknown => {
                return Err(local_error(
                    "unsupported_time_precision",
                    "Worker reported an unknown timescale unit",
                    false,
                ));
            }
        };
        let tick_zs = u128::from(self.multiplier) * unit_zs;
        if tick_zs == 0 || tick_zs % NANOSECOND_IN_ZEPTOSECONDS != 0 {
            return Err(local_error(
                "unsupported_time_precision",
                format!(
                    "Timescale {} {:?} cannot be represented exactly in nanoseconds",
                    self.multiplier, self.unit
                ),
                false,
            ));
        }
        u64::try_from(tick_zs / NANOSECOND_IN_ZEPTOSECONDS).map_err(|_| {
            local_error(
                "time_conversion_overflow",
                "Worker timescale exceeds NovyWave's nanosecond time domain",
                false,
            )
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum WorkerTimescaleUnit {
    Zs,
    As,
    Fs,
    Ps,
    Ns,
    Us,
    Ms,
    S,
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct WorkerTickRange {
    start_tick: u64,
    end_tick: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkerScope {
    path: Vec<String>,
    name: String,
    kind: String,
    parent_path: Vec<String>,
    #[serde(default)]
    component: Option<String>,
    #[serde(default)]
    source_loc: Option<WorkerSourceLocation>,
    #[serde(default)]
    instantiation_source_loc: Option<WorkerSourceLocation>,
}

#[derive(Clone, Debug, Deserialize)]
struct WorkerSourceLocation {
    path: String,
    line: u64,
    #[serde(default)]
    column: Option<u32>,
    #[serde(default = "waveform_metadata_provenance")]
    provenance: String,
}

fn waveform_metadata_provenance() -> String {
    "waveform_metadata".to_string()
}

impl WorkerSourceLocation {
    fn into_shared(self) -> Option<SourceLocation> {
        (self.line > 0 && !self.path.trim().is_empty() && !self.path.chars().any(char::is_control))
            .then_some(SourceLocation {
                path: self.path,
                line: self.line,
                column: self.column,
                provenance: self.provenance,
            })
    }
}

#[derive(Clone, Debug, Deserialize)]
struct WorkerSignal {
    scope_path: Vec<String>,
    name: String,
    width: u32,
    kind: String,
    direction: String,
}

fn parse_file_format(format: &str) -> Result<FileFormat, SurferWorkerError> {
    match format {
        "vcd" => Ok(FileFormat::VCD),
        "fst" => Ok(FileFormat::FST),
        "ghw" => Ok(FileFormat::GHW),
        other => Err(local_error(
            "unsupported_file_format",
            format!("Worker file format '{other}' is not supported by NovyWave"),
            false,
        )),
    }
}

fn collect_scope_paths(
    scopes: &[WorkerScope],
    signals: &[WorkerSignal],
) -> HashMap<String, Vec<String>> {
    scopes
        .iter()
        .map(|scope| (scope.path.join("."), scope.path.clone()))
        .chain(
            signals
                .iter()
                .map(|signal| (signal.scope_path.join("."), signal.scope_path.clone())),
        )
        .collect()
}

fn build_scope_tree(
    source_id: &str,
    scopes: &[WorkerScope],
    signals: &[WorkerSignal],
) -> Vec<ScopeData> {
    let scopes_by_path: HashMap<Vec<String>, &WorkerScope> = scopes
        .iter()
        .map(|scope| (scope.path.clone(), scope))
        .collect();
    let mut signals_by_path: HashMap<Vec<String>, Vec<&WorkerSignal>> = HashMap::new();
    for signal in signals {
        signals_by_path
            .entry(signal.scope_path.clone())
            .or_default()
            .push(signal);
    }

    let mut root_paths: Vec<Vec<String>> = scopes
        .iter()
        .filter(|scope| {
            scope.parent_path.is_empty() || !scopes_by_path.contains_key(&scope.parent_path)
        })
        .map(|scope| scope.path.clone())
        .collect();
    root_paths.sort_by_key(|path| path.join(".").to_lowercase());

    root_paths
        .into_iter()
        .filter_map(|path| build_scope_node(source_id, &path, &scopes_by_path, &signals_by_path))
        .collect()
}

fn build_scope_node(
    source_id: &str,
    path: &[String],
    scopes_by_path: &HashMap<Vec<String>, &WorkerScope>,
    signals_by_path: &HashMap<Vec<String>, Vec<&WorkerSignal>>,
) -> Option<ScopeData> {
    let scope = scopes_by_path.get(path)?;
    let mut child_paths: Vec<Vec<String>> = scopes_by_path
        .values()
        .filter(|candidate| candidate.parent_path == path)
        .map(|candidate| candidate.path.clone())
        .collect();
    child_paths.sort_by_key(|candidate| candidate.join(".").to_lowercase());
    let children = child_paths
        .into_iter()
        .filter_map(|child_path| {
            build_scope_node(source_id, &child_path, scopes_by_path, signals_by_path)
        })
        .collect();

    let mut variables: Vec<Signal> = signals_by_path
        .get(path)
        .into_iter()
        .flat_map(|signals| signals.iter())
        .map(|signal| Signal {
            id: signal.name.clone(),
            name: signal.name.clone(),
            signal_type: if signal.direction == "unknown" {
                signal.kind.clone()
            } else {
                format!("{} ({})", signal.kind, signal.direction)
            },
            width: signal.width,
        })
        .collect();
    variables.sort_by_key(|signal| signal.name.to_lowercase());

    let full_name = path.join(".");
    Some(ScopeData {
        id: format!("{source_id}|{full_name}"),
        name: scope.name.clone(),
        full_name,
        path_components: Some(path.to_vec()),
        children,
        variables,
        scope_type: Some(scope.kind.clone()),
        component: scope.component.clone().filter(|component| {
            !component.trim().is_empty() && !component.chars().any(char::is_control)
        }),
        declaration_source: scope
            .source_loc
            .clone()
            .and_then(WorkerSourceLocation::into_shared),
        instantiation_source: scope
            .instantiation_source_loc
            .clone()
            .and_then(WorkerSourceLocation::into_shared),
    })
}

#[derive(Debug, Serialize)]
struct WorkerSignalQueryRequest {
    signals: Vec<WorkerSignalSelector>,
    #[serde(skip_serializing_if = "Option::is_none")]
    range: Option<WorkerTickRange>,
    cursor_tick: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_transitions_per_signal: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    downsample: Option<WorkerDownsampleRequest>,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct WorkerDownsampleRequest {
    strategy: WorkerDownsampleStrategy,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkerDownsampleStrategy {
    PreserveEdges,
}

#[derive(Debug, Serialize, PartialEq, Eq, Hash)]
struct WorkerSignalSelector {
    scope_path: Vec<String>,
    name: String,
}

fn common_requested_range(requests: &[UnifiedSignalRequest]) -> Option<(u64, u64)> {
    let first = requests.first()?.time_range_ns?;
    (first.0 < first.1
        && requests
            .iter()
            .all(|request| request.time_range_ns == Some(first)))
    .then_some(first)
}

fn common_max_transitions(requests: &[UnifiedSignalRequest]) -> Option<usize> {
    let first = requests.first()?.max_transitions?;
    (first > 0
        && requests
            .iter()
            .all(|request| request.max_transitions == Some(first)))
    .then_some(first)
}

fn worker_query_controls(
    requests: &[UnifiedSignalRequest],
    ns_per_tick: u64,
) -> (Option<WorkerTickRange>, Option<usize>) {
    let range =
        common_requested_range(requests).and_then(|range| range_to_ticks(range, ns_per_tick));
    let all_ranges_absent = requests
        .iter()
        .all(|request| request.time_range_ns.is_none());
    let max_transitions = (all_ranges_absent || range.is_some())
        .then(|| common_max_transitions(requests))
        .flatten();
    (range, max_transitions)
}

fn range_to_ticks(range: (u64, u64), ns_per_tick: u64) -> Option<WorkerTickRange> {
    let start_tick = range.0 / ns_per_tick;
    let end_tick = range.1.div_ceil(ns_per_tick);
    (start_tick < end_tick).then_some(WorkerTickRange {
        start_tick,
        end_tick,
    })
}

#[derive(Debug, Deserialize)]
struct WorkerSignalQueryResponse {
    contract_version: u32,
    file_index: usize,
    #[serde(default, rename = "generation_id")]
    _generation_id: Option<u64>,
    timescale: Option<WorkerTimescale>,
    #[serde(rename = "cursor_tick")]
    _cursor_tick: Option<u64>,
    #[serde(default, rename = "requested_range")]
    _requested_range: Option<WorkerTickRange>,
    #[serde(default, rename = "returned_range")]
    _returned_range: Option<WorkerTickRange>,
    #[serde(default, rename = "total_available_transitions")]
    _total_available_transitions: Option<usize>,
    #[serde(default, rename = "returned_transitions")]
    _returned_transitions: Option<usize>,
    #[serde(default, rename = "truncated")]
    _truncated: Option<bool>,
    #[serde(default, rename = "downsampled")]
    _downsampled: Option<bool>,
    #[serde(default, rename = "downsample_strategy")]
    _downsample_strategy: Option<String>,
    signals: Vec<WorkerSignalData>,
}

impl WorkerSignalQueryResponse {
    fn into_shared(
        self,
        route: &RemoteSourceRoute,
        requests: &[UnifiedSignalRequest],
        cursor_requested: bool,
        worker_range_requested: bool,
        worker_downsample_requested: bool,
    ) -> Result<RemoteSignalQueryResult, SurferWorkerError> {
        validate_contract_version(self.contract_version)?;
        if self.file_index != route.file_index {
            return Err(local_error(
                "invalid_worker_response",
                format!(
                    "Signal response identified file {}, expected {}",
                    self.file_index, route.file_index
                ),
                false,
            ));
        }
        let response_ns_per_tick = self
            .timescale
            .ok_or_else(|| {
                local_error(
                    "missing_timescale",
                    "Worker signal response omitted its timescale",
                    false,
                )
            })?
            .ns_per_tick()?;
        if response_ns_per_tick != route.ns_per_tick {
            return Err(local_error(
                "worker_timescale_changed",
                format!(
                    "Worker timescale changed from {}ns to {}ns per tick; reload the file",
                    route.ns_per_tick, response_ns_per_tick
                ),
                true,
            ));
        }

        let signals_by_key: HashMap<(Vec<String>, String), WorkerSignalData> = self
            .signals
            .into_iter()
            .map(|signal| ((signal.scope_path.clone(), signal.name.clone()), signal))
            .collect();
        let source_id = source_id(route.file_index);
        let mut signal_data = Vec::with_capacity(requests.len());
        let mut cursor_values = BTreeMap::new();

        for request in requests {
            let wire_scope = route.scope_paths.get(&request.scope_path).ok_or_else(|| {
                local_error(
                    "unknown_remote_scope",
                    format!("Unknown worker scope '{}'", request.scope_path),
                    false,
                )
            })?;
            let key = (wire_scope.clone(), request.variable_name.clone());
            let signal = signals_by_key.get(&key).ok_or_else(|| {
                local_error(
                    "invalid_worker_response",
                    format!(
                        "Worker response omitted '{}.{}'",
                        request.scope_path, request.variable_name
                    ),
                    false,
                )
            })?;
            let mut all_transitions = Vec::with_capacity(signal.transitions.len());
            for transition in &signal.transitions {
                all_transitions.push(SignalTransition {
                    time_ns: tick_to_ns(transition.tick, route.ns_per_tick)?,
                    value: transition.value.text.clone(),
                });
            }
            all_transitions.sort_by_key(|transition| transition.time_ns);
            let worker_reported_controls = signal.total_available_transitions.is_some()
                || signal.returned_transitions.is_some()
                || signal.returned_range.is_some()
                || signal.truncated.is_some()
                || signal.downsampled.is_some();
            let _worker_applied_range = worker_range_requested && worker_reported_controls;
            let worker_applied_downsample = worker_downsample_requested && worker_reported_controls;
            let total_transitions = signal
                .total_available_transitions
                .unwrap_or(all_transitions.len());
            // Worker ranges are whole ticks while the viewport is expressed in
            // integer nanoseconds. Clamp again after tick conversion so widened
            // tick bounds cannot leak data outside the exact requested range.
            let range_transitions = collect_range(&all_transitions, request.time_range_ns);
            let transitions = if worker_applied_downsample {
                range_transitions
            } else {
                downsample_transitions(range_transitions, request.max_transitions)
            };
            let actual_time_range_ns = transitions
                .first()
                .zip(transitions.last())
                .map(|(first, last)| (first.time_ns, last.time_ns));
            let unique_id = format!(
                "{source_id}|{}|{}",
                request.scope_path, request.variable_name
            );
            if cursor_requested {
                cursor_values.insert(
                    unique_id.clone(),
                    signal
                        .cursor_value
                        .as_ref()
                        .map(|value| SignalValue::Present(value.text.clone()))
                        .unwrap_or(SignalValue::Missing),
                );
            }
            signal_data.push(UnifiedSignalData {
                file_path: source_id.clone(),
                scope_path: request.scope_path.clone(),
                variable_name: request.variable_name.clone(),
                unique_id,
                transitions,
                total_transitions,
                actual_time_range_ns,
            });
        }

        Ok(RemoteSignalQueryResult {
            signal_data,
            cursor_values,
        })
    }
}

#[derive(Debug, Deserialize)]
struct WorkerSignalData {
    scope_path: Vec<String>,
    name: String,
    #[serde(rename = "width")]
    _width: u32,
    transitions: Vec<WorkerTransition>,
    cursor_value: Option<WorkerValue>,
    #[serde(default)]
    total_available_transitions: Option<usize>,
    #[serde(default)]
    returned_transitions: Option<usize>,
    #[serde(default)]
    returned_range: Option<WorkerTickRange>,
    #[serde(default)]
    truncated: Option<bool>,
    #[serde(default)]
    downsampled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct WorkerTransition {
    tick: u64,
    value: WorkerValue,
}

#[derive(Debug, Deserialize)]
struct WorkerValue {
    #[serde(rename = "kind")]
    _kind: WorkerValueKind,
    text: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum WorkerValueKind {
    Bits,
    Real,
    String,
    Event,
}

#[derive(Debug, Serialize)]
struct WorkerCrossprobeRequest {
    operation: SourceOperation,
    signal: WorkerSourceSignal,
    #[serde(skip_serializing_if = "Option::is_none")]
    workspace_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct WorkerSourceSignal {
    scope_path: Vec<String>,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    waveform_file_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    waveform_source_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_range: Option<WorkerSourceIndexRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elaboration_context: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Serialize)]
struct WorkerSourceIndexRange {
    left: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    right: Option<i64>,
}

impl From<shared::SourceIndexRange> for WorkerSourceIndexRange {
    fn from(value: shared::SourceIndexRange) -> Self {
        Self {
            left: value.left,
            right: value.right,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WorkerCrossprobeResponse {
    contract_version: u32,
    file_index: usize,
    operation: SourceOperation,
    #[serde(default)]
    status: Option<WorkerSourceResolutionStatus>,
    #[serde(default)]
    generation: Option<u64>,
    #[serde(default)]
    matches: Vec<WorkerSourceMatch>,
    #[serde(default)]
    warnings: Vec<WorkerSourceWarning>,
}

impl WorkerCrossprobeResponse {
    fn into_shared(
        self,
        expected_index: usize,
        expected_operation: SourceOperation,
    ) -> Result<SourceResolution, SurferWorkerError> {
        validate_contract_version(self.contract_version)?;
        if self.file_index != expected_index || self.operation != expected_operation {
            return Err(local_error(
                "invalid_worker_response",
                format!(
                    "Crossprobe response identified file {} / {:?}, expected {} / {:?}",
                    self.file_index, self.operation, expected_index, expected_operation
                ),
                false,
            ));
        }
        let status = self.status.map(Into::into).unwrap_or_else(|| {
            if self.matches.len() > 1 {
                SourceResolutionStatus::Ambiguous
            } else if self.matches.is_empty() {
                SourceResolutionStatus::NotFound
            } else {
                SourceResolutionStatus::Matched
            }
        });
        Ok(SourceResolution {
            status,
            generation: self.generation,
            matches: self
                .matches
                .into_iter()
                .map(WorkerSourceMatch::into_shared)
                .collect(),
            warnings: self
                .warnings
                .into_iter()
                .map(|warning| SourceWarning {
                    code: warning.code,
                    message: warning.message,
                })
                .collect(),
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WorkerSourceResolutionStatus {
    Matched,
    Ambiguous,
    NotFound,
    Unsupported,
    Unavailable,
}

impl From<WorkerSourceResolutionStatus> for SourceResolutionStatus {
    fn from(value: WorkerSourceResolutionStatus) -> Self {
        match value {
            WorkerSourceResolutionStatus::Matched => Self::Matched,
            WorkerSourceResolutionStatus::Ambiguous => Self::Ambiguous,
            WorkerSourceResolutionStatus::NotFound => Self::NotFound,
            WorkerSourceResolutionStatus::Unsupported => Self::Unsupported,
            WorkerSourceResolutionStatus::Unavailable => Self::Unavailable,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WorkerSourceMatch {
    file_id: String,
    line: u64,
    #[serde(default)]
    column: Option<u32>,
    #[serde(default)]
    end_line: Option<u64>,
    #[serde(default)]
    end_column: Option<u32>,
    symbol: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    hierarchy: Vec<WorkerHierarchyComponent>,
    confidence: String,
    provenance: String,
}

impl WorkerSourceMatch {
    fn into_shared(self) -> SourceMatch {
        SourceMatch {
            file_id: self.file_id,
            line: self.line,
            column: self.column,
            end_line: self.end_line,
            end_column: self.end_column,
            symbol: self.symbol,
            kind: self.kind,
            hierarchy: self
                .hierarchy
                .into_iter()
                .map(WorkerHierarchyComponent::into_identifier)
                .collect(),
            confidence: self.confidence,
            provenance: self.provenance,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WorkerHierarchyComponent {
    Identifier(String),
    Structured { identifier: String },
}

impl WorkerHierarchyComponent {
    fn into_identifier(self) -> String {
        match self {
            Self::Identifier(identifier) | Self::Structured { identifier } => identifier,
        }
    }
}

#[derive(Debug, Deserialize)]
struct WorkerSourceWarning {
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct EmptyRequest {}

#[derive(Debug, Deserialize)]
struct WorkerReloadResponse {
    contract_version: u32,
    file_index: usize,
    #[serde(default, rename = "generation_id")]
    _generation_id: Option<u64>,
    outcome: WorkerReloadOutcome,
    file: WorkerFileMetadata,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum WorkerReloadOutcome {
    Accepted,
    Unchanged,
}

#[derive(Debug, Deserialize)]
struct WorkerErrorEnvelope {
    contract_version: u32,
    error: WorkerError,
}

#[derive(Debug, Deserialize)]
struct WorkerError {
    code: String,
    message: String,
    retryable: bool,
}

impl WorkerError {
    fn into_shared(self) -> SurferWorkerError {
        SurferWorkerError {
            code: self.code,
            message: self.message,
            retryable: self.retryable,
        }
    }
}

fn tick_to_ns(tick: u64, ns_per_tick: u64) -> Result<u64, SurferWorkerError> {
    tick.checked_mul(ns_per_tick).ok_or_else(|| {
        local_error(
            "time_conversion_overflow",
            format!("Tick {tick} exceeds NovyWave's nanosecond time domain"),
            false,
        )
    })
}

fn collect_range(
    transitions: &[SignalTransition],
    range: Option<(u64, u64)>,
) -> Vec<SignalTransition> {
    let Some((start, end)) = range else {
        return transitions.to_vec();
    };
    if transitions.is_empty() || start >= end {
        return Vec::new();
    }

    let start_idx = transitions
        .binary_search_by(|transition| transition.time_ns.cmp(&start))
        .unwrap_or_else(|index| index);
    let end_idx = transitions
        .binary_search_by(|transition| transition.time_ns.cmp(&end))
        .map(|index| index + 1)
        .unwrap_or_else(|index| index)
        .min(transitions.len());
    let mut result = Vec::new();

    if start_idx > 0 {
        let previous = &transitions[start_idx - 1];
        let needs_synthetic = transitions
            .get(start_idx)
            .map(|transition| transition.time_ns != start)
            .unwrap_or(true);
        if needs_synthetic {
            result.push(SignalTransition {
                time_ns: start,
                value: previous.value.clone(),
            });
        }
    }
    if start_idx < end_idx {
        result.extend(transitions[start_idx..end_idx].iter().cloned());
    }
    if let Some(first) = result.first()
        && first.time_ns > start
    {
        result.insert(
            0,
            SignalTransition {
                time_ns: start,
                value: first.value.clone(),
            },
        );
    }
    result
}

fn downsample_transitions(
    transitions: Vec<SignalTransition>,
    max_count: Option<usize>,
) -> Vec<SignalTransition> {
    let Some(max_count) = max_count else {
        return transitions;
    };
    if transitions.len() <= max_count || max_count == 0 {
        return transitions;
    }

    let mut result = Vec::with_capacity(max_count + 1);
    let mut last_value = transitions[0].value.clone();
    result.push(transitions[0].clone());
    let stride = transitions.len().div_ceil(max_count).max(1);
    let mut steps = 0usize;
    for transition in transitions
        .iter()
        .skip(1)
        .take(transitions.len().saturating_sub(1))
    {
        steps += 1;
        if transition.value != last_value || steps >= stride {
            if result
                .last()
                .map(|previous| previous.time_ns != transition.time_ns)
                .unwrap_or(true)
            {
                result.push(transition.clone());
            }
            last_value = transition.value.clone();
            steps = 0;
            if result.len() >= max_count {
                break;
            }
        }
    }
    if let Some(last) = transitions.last()
        && result
            .last()
            .map(|previous| previous.time_ns != last.time_ns)
            .unwrap_or(true)
    {
        result.push(last.clone());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared::VarFormat;

    #[test]
    fn files_fixture_matches_worker_contract() {
        let response: WorkerFilesResponse =
            serde_json::from_str(include_str!("../tests/fixtures/worker_files.json"))
                .expect("deserialize files fixture");
        let files = response.into_shared().expect("map listed files");
        assert_eq!(files[0].source_id, "surfer-worker://file/0");
        assert_eq!(files[0].format, "vcd");
    }

    #[test]
    fn duplicate_worker_indices_are_rejected_before_creating_source_ids() {
        let response: WorkerFilesResponse = serde_json::from_str(
            r#"{
                "contract_version": 1,
                "files": [
                    {"index": 2, "filename": "a.vcd", "format": "vcd", "bytes": 1, "bytes_loaded": 1, "reloading": false, "last_load_ok": true, "last_modified": null},
                    {"index": 2, "filename": "b.vcd", "format": "vcd", "bytes": 1, "bytes_loaded": 1, "reloading": false, "last_load_ok": true, "last_modified": null}
                ]
            }"#,
        )
        .expect("deserialize duplicate files");
        let error = response
            .into_shared()
            .expect_err("duplicate file indices must be rejected");
        assert_eq!(error.code, "duplicate_worker_file_index");
    }

    #[test]
    fn detail_fixture_maps_to_existing_waveform_tree() {
        let detail: WorkerFileDetail =
            serde_json::from_str(include_str!("../tests/fixtures/worker_file_detail.json"))
                .expect("deserialize detail fixture");
        let loaded = detail.into_loaded_file(0).expect("map detail");
        assert_eq!(loaded.waveform_file.min_time_ns, Some(0));
        assert_eq!(loaded.waveform_file.max_time_ns, Some(100));
        assert_eq!(loaded.waveform_file.scopes[0].full_name, "top");
        assert_eq!(
            loaded.waveform_file.scopes[0].component.as_deref(),
            Some("counter")
        );
        assert_eq!(
            loaded.waveform_file.scopes[0]
                .declaration_source
                .as_ref()
                .map(|location| (location.path.as_str(), location.line, location.column)),
            Some(("rtl/counter.sv", 7, Some(1)))
        );
        assert_eq!(
            loaded.waveform_file.scopes[0]
                .instantiation_source
                .as_ref()
                .map(|location| (location.path.as_str(), location.line)),
            Some(("tb/counter_tb.sv", 21))
        );
        assert_eq!(loaded.waveform_file.scopes[0].variables[0].name, "clk");
        assert_eq!(loaded.route.scope_paths["top"], vec![String::from("top")]);
    }

    #[test]
    fn query_fixture_maps_to_unified_signal_shapes() {
        let expected_request: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_signal_query_request.json"
        ))
        .expect("deserialize query request fixture");
        let request_json = serde_json::to_value(WorkerSignalQueryRequest {
            signals: vec![WorkerSignalSelector {
                scope_path: vec!["top".into()],
                name: "clk".into(),
            }],
            cursor_tick: Some(15),
            range: None,
            max_transitions_per_signal: None,
            downsample: None,
        })
        .expect("serialize query request");
        assert_eq!(request_json, expected_request);

        let response: WorkerSignalQueryResponse =
            serde_json::from_str(include_str!("../tests/fixtures/worker_signal_query.json"))
                .expect("deserialize query fixture");
        let route = RemoteSourceRoute {
            file_index: 0,
            ns_per_tick: 1,
            scope_paths: HashMap::from([("top".into(), vec!["top".into()])]),
        };
        let request = UnifiedSignalRequest {
            file_path: source_id(0),
            scope_path: "top".into(),
            variable_name: "clk".into(),
            time_range_ns: Some((5, 20)),
            max_transitions: Some(16),
            format: VarFormat::Binary,
        };
        let mapped = response
            .into_shared(&route, &[request], true, false, false)
            .expect("map query response");
        assert_eq!(mapped.signal_data[0].transitions[0].time_ns, 5);
        assert_eq!(mapped.signal_data[0].transitions[0].value, "0");
        assert_eq!(
            mapped.cursor_values["surfer-worker://file/0|top|clk"],
            SignalValue::Present("1".into())
        );
    }

    #[test]
    fn bounded_query_metadata_prevents_duplicate_adapter_downsampling() {
        let fixture = include_str!("../tests/fixtures/worker_signal_query_bounded.json");
        let response: WorkerSignalQueryResponse =
            serde_json::from_str(fixture).expect("deserialize bounded query fixture");
        let route = RemoteSourceRoute {
            file_index: 0,
            ns_per_tick: 1,
            scope_paths: HashMap::from([("top".into(), vec!["top".into()])]),
        };
        let request = UnifiedSignalRequest {
            file_path: source_id(0),
            scope_path: "top".into(),
            variable_name: "clk".into(),
            time_range_ns: Some((5, 20)),
            max_transitions: Some(2),
            format: VarFormat::Binary,
        };
        let mapped = response
            .into_shared(&route, &[request], true, true, true)
            .expect("map bounded response");
        assert_eq!(mapped.signal_data[0].total_transitions, 50);
        assert_eq!(mapped.signal_data[0].transitions.len(), 3);
        assert_eq!(mapped.signal_data[0].actual_time_range_ns, Some((5, 20)));

        let response: WorkerSignalQueryResponse =
            serde_json::from_str(fixture).expect("deserialize bounded query fixture again");
        let local_request = UnifiedSignalRequest {
            file_path: source_id(0),
            scope_path: "top".into(),
            variable_name: "clk".into(),
            time_range_ns: Some((6, 19)),
            max_transitions: Some(2),
            format: VarFormat::Binary,
        };
        let locally_controlled = response
            .into_shared(&route, &[local_request], true, false, false)
            .expect("apply per-request controls when worker controls were not sent");
        assert_eq!(locally_controlled.signal_data[0].transitions.len(), 2);
        assert_eq!(
            locally_controlled.signal_data[0].actual_time_range_ns,
            Some((6, 10))
        );

        assert_eq!(
            range_to_ticks((5, 20), 2),
            Some(WorkerTickRange {
                start_tick: 2,
                end_tick: 10
            })
        );
        let clamped = collect_range(
            &[
                SignalTransition::new(4, "0".into()),
                SignalTransition::new(8, "1".into()),
                SignalTransition::new(20, "0".into()),
            ],
            Some((5, 19)),
        );
        assert_eq!(
            clamped
                .iter()
                .map(|transition| transition.time_ns)
                .collect::<Vec<_>>(),
            vec![5, 8]
        );
    }

    #[test]
    fn ambiguous_crossprobe_fixture_preserves_all_matches() {
        let request = WorkerCrossprobeRequest {
            operation: SourceOperation::Declaration,
            signal: WorkerSourceSignal {
                scope_path: vec!["top".into(), "cpu".into()],
                name: "valid".into(),
                waveform_file_index: Some(0),
                waveform_source_id: Some(source_id(0)),
                index_range: Some(WorkerSourceIndexRange {
                    left: 7,
                    right: Some(0),
                }),
                language: Some("system_verilog".into()),
                top_module: Some("top".into()),
                elaboration_context: Some(BTreeMap::from([("parameter.WIDTH".into(), "8".into())])),
            },
            workspace_id: Some("default".into()),
        };
        let expected_request: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_crossprobe_request.json"
        ))
        .expect("deserialize expected crossprobe request");
        assert_eq!(
            serde_json::to_value(request).expect("serialize crossprobe request"),
            expected_request
        );

        let response: WorkerCrossprobeResponse = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_crossprobe_ambiguous.json"
        ))
        .expect("deserialize crossprobe fixture");
        let resolution = response
            .into_shared(0, SourceOperation::Declaration)
            .expect("map crossprobe response");
        assert_eq!(resolution.status, SourceResolutionStatus::Ambiguous);
        assert_eq!(resolution.matches.len(), 2);
        assert_eq!(resolution.matches[0].line, 42);
        assert_eq!(resolution.matches[1].hierarchy, vec!["top", "cpu"]);
        assert_eq!(resolution.warnings[0].code, "ambiguous_match");
    }

    #[test]
    fn worker_query_controls_are_only_shared_for_uniform_batches() {
        let request = |range, maximum| UnifiedSignalRequest {
            file_path: source_id(0),
            scope_path: "top".into(),
            variable_name: "clk".into(),
            time_range_ns: range,
            max_transitions: maximum,
            format: VarFormat::Binary,
        };
        let heterogeneous_ranges = [
            request(Some((0, 10)), Some(4)),
            request(Some((10, 20)), Some(4)),
        ];
        assert_eq!(
            worker_query_controls(&heterogeneous_ranges, 1),
            (None, None)
        );

        let heterogeneous_limits = [
            request(Some((0, 10)), Some(4)),
            request(Some((0, 10)), Some(8)),
        ];
        assert_eq!(
            worker_query_controls(&heterogeneous_limits, 1),
            (
                Some(WorkerTickRange {
                    start_tick: 0,
                    end_tick: 10,
                }),
                None,
            )
        );

        let full_trace = [request(None, Some(4)), request(None, Some(4))];
        assert_eq!(worker_query_controls(&full_trace, 1), (None, Some(4)));
    }

    #[test]
    fn malformed_source_path_is_not_forwarded() {
        let declaration_only: WorkerFileDetail = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_file_detail_declaration_only.json"
        ))
        .expect("deserialize declaration-only detail");
        let declaration_only = declaration_only
            .into_loaded_file(0)
            .expect("map declaration-only detail");
        assert!(
            declaration_only.waveform_file.scopes[0]
                .declaration_source
                .is_some()
        );
        assert!(
            declaration_only.waveform_file.scopes[0]
                .instantiation_source
                .is_none()
        );

        let no_metadata: WorkerFileDetail = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_file_detail_no_source_metadata.json"
        ))
        .expect("deserialize detail without metadata");
        let no_metadata = no_metadata
            .into_loaded_file(0)
            .expect("map detail without metadata");
        assert!(
            no_metadata.waveform_file.scopes[0]
                .declaration_source
                .is_none()
        );

        let malformed: WorkerFileDetail = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_file_detail_malformed_source.json"
        ))
        .expect("deserialize malformed source detail");
        let malformed = malformed
            .into_loaded_file(0)
            .expect("map malformed source detail");
        assert!(
            malformed.waveform_file.scopes[0]
                .declaration_source
                .is_none()
        );
    }

    #[test]
    fn reload_and_error_fixtures_match_worker_contract() {
        let reload: WorkerReloadResponse =
            serde_json::from_str(include_str!("../tests/fixtures/worker_reload.json"))
                .expect("deserialize reload fixture");
        assert!(matches!(reload.outcome, WorkerReloadOutcome::Accepted));

        let error: WorkerErrorEnvelope =
            serde_json::from_str(include_str!("../tests/fixtures/worker_error.json"))
                .expect("deserialize error fixture");
        assert_eq!(error.error.code, "surver_unavailable");
        assert!(error.error.retryable);
    }

    #[test]
    fn sub_nanosecond_timescale_is_rejected() {
        let error = WorkerTimescale {
            multiplier: 1,
            unit: WorkerTimescaleUnit::Ps,
        }
        .ns_per_tick()
        .expect_err("picosecond precision should be rejected");
        assert_eq!(error.code, "unsupported_time_precision");

        assert_eq!(
            WorkerTimescale {
                multiplier: 1_000,
                unit: WorkerTimescaleUnit::Ps,
            }
            .ns_per_tick()
            .expect("whole-nanosecond picosecond timescale"),
            1
        );
    }

    #[test]
    fn empty_time_range_and_malformed_source_ids_are_rejected() {
        let detail: WorkerFileDetail = serde_json::from_str(include_str!(
            "../tests/fixtures/worker_empty_file_detail.json"
        ))
        .expect("deserialize empty detail fixture");
        let error = detail
            .into_loaded_file(0)
            .expect_err("empty waveform must not enter nanosecond renderer");
        assert_eq!(error.code, "empty_time_range");

        assert_eq!(parse_source_id("surfer-worker://file/12"), Some(12));
        assert_eq!(parse_source_id("surfer-worker://file/012"), None);
        assert_eq!(parse_source_id("surfer-worker://file/12/more"), None);
        assert_eq!(parse_source_id("/tmp/surfer-worker://file/12"), None);
    }
}
