use futures::StreamExt;
use moonzoon_novyui::tokens::color::{neutral_1, neutral_2, neutral_4, neutral_8, neutral_11};
use moonzoon_novyui::*;
use shared::{
    SourceMatch, SourceOperation, SourceResolution, SourceResolutionStatus, SourceSignalIdentity,
    SurferFileSummary, SurferSourceOperation, SurferWorkerError, UpMsg,
};
use std::sync::Arc;
use zoon::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SurferFileListState {
    Idle,
    Loading,
    Empty,
    Ready(Vec<SurferFileSummary>),
    Error(SurferWorkerError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceNavigationState {
    Idle,
    Loading {
        request_id: String,
        source_id: String,
        operation: SourceOperation,
        symbol: String,
    },
    Resolved {
        source_id: String,
        operation: SourceOperation,
        symbol: String,
        resolution: SourceResolution,
    },
    Failed {
        source_id: String,
        operation: SourceOperation,
        symbol: String,
        error: SurferWorkerError,
    },
}

enum IntegrationEvent {
    PickerOpened,
    PickerClosed,
    FilesRefreshRequested,
    FilesListed(Vec<SurferFileSummary>),
    WorkerFailed {
        operation: SurferSourceOperation,
        error: SurferWorkerError,
    },
    FileSelected(SurferFileSummary),
    DeclarationRequested {
        source_id: String,
        scope_path: Vec<String>,
        symbol: String,
    },
    SourceResolved {
        request_id: String,
        source_id: String,
        operation: SourceOperation,
        resolution: SourceResolution,
    },
    SourceFailed {
        request_id: String,
        source_id: String,
        operation: SourceOperation,
        error: SurferWorkerError,
    },
    SourceDismissed,
}

/// Read-only reactive state owned by an actor loop.
///
/// Keeping the mutable cell behind this type prevents UI code from mutating
/// domain state directly while preserving Zoon signal interoperability.
#[derive(Clone)]
struct StateActor<T> {
    state: Mutable<T>,
}

impl<T> StateActor<T>
where
    T: Clone + 'static,
{
    fn new(initial: T) -> Self {
        Self {
            state: Mutable::new(initial),
        }
    }

    fn set(&self, next: T) {
        self.state.set(next);
    }

    fn signal_cloned(&self) -> impl Signal<Item = T> + use<T> {
        self.state.signal_cloned()
    }

    fn signal_ref<U, F>(&self, map: F) -> impl Signal<Item = U> + use<T, U, F>
    where
        U: 'static,
        F: FnMut(&T) -> U + 'static,
    {
        self.state.signal_ref(map)
    }
}

struct Relay<T> {
    sender: futures::channel::mpsc::UnboundedSender<T>,
}

impl<T> Clone for Relay<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<T> Relay<T> {
    fn send(&self, event: T) {
        let _ = self.sender.unbounded_send(event);
    }
}

fn relay<T>() -> (Relay<T>, futures::channel::mpsc::UnboundedReceiver<T>) {
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    (Relay { sender }, receiver)
}

/// Relay-driven domain for worker selection and source navigation.
///
/// Current values are cached only in the actor loop; UI code observes the two
/// explicit state signals and emits typed events through this domain.
#[derive(Clone)]
pub struct SurferIntegration {
    file_list_actor: StateActor<SurferFileListState>,
    picker_actor: StateActor<bool>,
    source_navigation_actor: StateActor<SourceNavigationState>,
    integration_event_relay: Relay<IntegrationEvent>,
    _actor_task: Arc<TaskHandle>,
}

impl SurferIntegration {
    pub fn new(tracked_files: crate::tracked_files::TrackedFiles) -> Self {
        let file_list_actor = StateActor::new(SurferFileListState::Idle);
        let picker_actor = StateActor::new(false);
        let source_navigation_actor = StateActor::new(SourceNavigationState::Idle);
        let (integration_event_relay, mut integration_event_stream) = relay();

        let file_list_for_actor = file_list_actor.clone();
        let picker_for_actor = picker_actor.clone();
        let source_for_actor = source_navigation_actor.clone();
        let actor_task = Arc::new(Task::start_droppable(async move {
            let mut next_request_id = 1u64;
            let mut active_request_id: Option<String> = None;
            let mut active_symbol = String::new();
            while let Some(event) = integration_event_stream.next().await {
                match event {
                    IntegrationEvent::PickerOpened => {
                        picker_for_actor.set(true);
                        request_file_list(&file_list_for_actor).await;
                    }
                    IntegrationEvent::PickerClosed => picker_for_actor.set(false),
                    IntegrationEvent::FilesRefreshRequested => {
                        request_file_list(&file_list_for_actor).await;
                    }
                    IntegrationEvent::FilesListed(files) => {
                        file_list_for_actor.set(if files.is_empty() {
                            SurferFileListState::Empty
                        } else {
                            SurferFileListState::Ready(files)
                        });
                    }
                    IntegrationEvent::WorkerFailed { operation, error } => {
                        if operation == SurferSourceOperation::List {
                            file_list_for_actor.set(SurferFileListState::Error(error));
                        }
                    }
                    IntegrationEvent::FileSelected(file) => {
                        let already_tracked =
                            tracked_files.get_current_files().iter().any(|item| {
                                item.id == file.source_id || item.canonical_path == file.source_id
                            });
                        tracked_files.notify_loading_started(
                            file.source_id.clone(),
                            display_filename(&file.filename),
                        );
                        picker_for_actor.set(false);
                        let request = if already_tracked {
                            UpMsg::ReloadSurferFile { index: file.index }
                        } else {
                            UpMsg::LoadSurferFile { index: file.index }
                        };
                        if let Err(message) = send_message(request).await {
                            tracked_files.update_file_state(
                                file.source_id.clone(),
                                shared::FileState::Failed(shared::FileError::IoError {
                                    path: file.source_id,
                                    error: message,
                                }),
                            );
                        }
                    }
                    IntegrationEvent::DeclarationRequested {
                        source_id,
                        scope_path,
                        symbol,
                    } => {
                        let operation = SourceOperation::Declaration;
                        if !source_id.starts_with("surfer-worker://file/") {
                            source_for_actor.set(SourceNavigationState::Failed {
                                source_id,
                                operation,
                                symbol,
                                error: SurferWorkerError {
                                    code: "source_resolution_unsupported".to_string(),
                                    message: "Local waveform source navigation is not configured"
                                        .to_string(),
                                    retryable: false,
                                },
                            });
                            continue;
                        }
                        let request_id = format!("source-resolution-{next_request_id}");
                        next_request_id = next_request_id.saturating_add(1);
                        active_request_id = Some(request_id.clone());
                        active_symbol = symbol.clone();
                        source_for_actor.set(SourceNavigationState::Loading {
                            request_id: request_id.clone(),
                            source_id: source_id.clone(),
                            operation,
                            symbol: symbol.clone(),
                        });
                        let message = UpMsg::ResolveSource {
                            request_id: request_id.clone(),
                            source_id: source_id.clone(),
                            operation,
                            signal: SourceSignalIdentity {
                                scope_path,
                                name: symbol.clone(),
                                index_range: None,
                                language: None,
                                top_module: None,
                                elaboration_context: None,
                            },
                            workspace_id: Some("default".to_string()),
                        };
                        if let Err(error) = send_message(message).await {
                            active_request_id = None;
                            source_for_actor.set(SourceNavigationState::Failed {
                                source_id,
                                operation,
                                symbol,
                                error: SurferWorkerError {
                                    code: "backend_unavailable".to_string(),
                                    message: error,
                                    retryable: true,
                                },
                            });
                        }
                    }
                    IntegrationEvent::SourceResolved {
                        request_id,
                        source_id,
                        operation,
                        resolution,
                    } => {
                        if active_request_id.as_deref() == Some(&request_id) {
                            source_for_actor.set(SourceNavigationState::Resolved {
                                source_id,
                                operation,
                                symbol: active_symbol.clone(),
                                resolution,
                            });
                            active_request_id = None;
                        }
                    }
                    IntegrationEvent::SourceFailed {
                        request_id,
                        source_id,
                        operation,
                        error,
                    } => {
                        if active_request_id.as_deref() == Some(&request_id) {
                            source_for_actor.set(SourceNavigationState::Failed {
                                source_id,
                                operation,
                                symbol: active_symbol.clone(),
                                error,
                            });
                            active_request_id = None;
                        }
                    }
                    IntegrationEvent::SourceDismissed => {
                        active_request_id = None;
                        source_for_actor.set(SourceNavigationState::Idle);
                    }
                }
            }
        }));

        Self {
            file_list_actor,
            picker_actor,
            source_navigation_actor,
            integration_event_relay,
            _actor_task: actor_task,
        }
    }
    fn send(&self, event: IntegrationEvent) {
        self.integration_event_relay.send(event);
    }

    pub fn open_picker(&self) {
        self.send(IntegrationEvent::PickerOpened);
    }

    pub fn close_picker(&self) {
        self.send(IntegrationEvent::PickerClosed);
    }

    pub fn refresh_files(&self) {
        self.send(IntegrationEvent::FilesRefreshRequested);
    }

    pub fn select_file(&self, file: SurferFileSummary) {
        self.send(IntegrationEvent::FileSelected(file));
    }

    pub fn files_listed(&self, files: Vec<SurferFileSummary>) {
        self.send(IntegrationEvent::FilesListed(files));
    }

    pub fn worker_failed(&self, operation: SurferSourceOperation, error: SurferWorkerError) {
        self.send(IntegrationEvent::WorkerFailed { operation, error });
    }

    pub fn resolve_declaration(&self, source_id: String, scope_path: Vec<String>, symbol: String) {
        self.send(IntegrationEvent::DeclarationRequested {
            source_id,
            scope_path,
            symbol,
        });
    }

    pub fn source_resolved(
        &self,
        request_id: String,
        source_id: String,
        operation: SourceOperation,
        resolution: SourceResolution,
    ) {
        self.send(IntegrationEvent::SourceResolved {
            request_id,
            source_id,
            operation,
            resolution,
        });
    }

    pub fn source_failed(
        &self,
        request_id: String,
        source_id: String,
        operation: SourceOperation,
        error: SurferWorkerError,
    ) {
        self.send(IntegrationEvent::SourceFailed {
            request_id,
            source_id,
            operation,
            error,
        });
    }

    pub fn dismiss_source_result(&self) {
        self.send(IntegrationEvent::SourceDismissed);
    }

    pub fn picker_visible_signal(&self) -> impl Signal<Item = bool> + use<> {
        self.picker_actor.signal_cloned()
    }

    pub fn source_result_visible_signal(&self) -> impl Signal<Item = bool> + use<> {
        self.source_navigation_actor
            .signal_ref(|state| !matches!(state, SourceNavigationState::Idle))
    }
}

async fn request_file_list(actor: &StateActor<SurferFileListState>) {
    actor.set(SurferFileListState::Loading);
    if let Err(message) = send_message(UpMsg::ListSurferFiles).await {
        actor.set(SurferFileListState::Error(SurferWorkerError {
            code: "backend_unavailable".to_string(),
            message,
            retryable: true,
        }));
    }
}

async fn send_message(message: UpMsg) -> Result<(), String> {
    use crate::platform::{CurrentPlatform, Platform};
    CurrentPlatform::send_message(message).await
}

fn display_filename(path: &str) -> String {
    std::path::Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path)
        .to_string()
}

pub fn worker_file_picker(domain: SurferIntegration) -> impl Element {
    modal_shell(
        "Surfer worker files",
        "Choose a worker waveform explicitly. Selecting an already loaded source safely reloads that synthetic ID.",
        domain.clone(),
        El::new()
            .s(Width::fill())
            .child_signal(domain.file_list_actor.signal_cloned().map({
                let domain = domain.clone();
                move |state| match state {
                    SurferFileListState::Idle => {
                        state_message("Worker files have not been requested.")
                    }
                    SurferFileListState::Loading => state_message("Loading worker files…"),
                    SurferFileListState::Empty => {
                        state_message("The worker has no waveform files configured.")
                    }
                    SurferFileListState::Error(error) => Column::new()
                        .s(Gap::new().y(SPACING_8))
                        .item(state_message(&format!("{}: {}", error.code, error.message)))
                        .item(
                            button()
                                .label("Retry")
                                .variant(ButtonVariant::Outline)
                                .size(ButtonSize::Small)
                                .on_press({
                                    let domain = domain.clone();
                                    move || domain.refresh_files()
                                })
                                .build(),
                        )
                        .unify(),
                    SurferFileListState::Ready(files) => Column::new()
                        .s(Width::fill())
                        .s(Gap::new().y(SPACING_6))
                        .items(files.into_iter().map({
                            let domain = domain.clone();
                            move |file| {
                                let details = format!(
                                    "{} · {} bytes loaded of {}{}{}",
                                    file.format.to_uppercase(),
                                    file.bytes_loaded,
                                    file.bytes,
                                    if file.reloading { " · reloading" } else { "" },
                                    if file.last_load_ok {
                                        ""
                                    } else {
                                        " · last load failed"
                                    },
                                );
                                Row::new()
                                    .s(Width::fill())
                                    .s(Align::new().center_y())
                                    .s(Gap::new().x(SPACING_8))
                                    .s(Padding::all(SPACING_8))
                                    .s(Background::new().color_signal(neutral_2()))
                                    .s(RoundedCorners::all(6))
                                    .item(
                                        Column::new()
                                            .s(Width::fill())
                                            .item(
                                                El::new()
                                                    .s(Font::new()
                                                        .weight(FontWeight::Medium)
                                                        .color_signal(neutral_11()))
                                                    .child(display_filename(&file.filename)),
                                            )
                                            .item(
                                                El::new()
                                                    .s(Font::new()
                                                        .size(12)
                                                        .color_signal(neutral_8()))
                                                    .child(details),
                                            ),
                                    )
                                    .item(
                                        button()
                                            .label("Load")
                                            .variant(ButtonVariant::Primary)
                                            .size(ButtonSize::Small)
                                            .on_press({
                                                let domain = domain.clone();
                                                let file = file.clone();
                                                move || domain.select_file(file.clone())
                                            })
                                            .build(),
                                    )
                            }
                        }))
                        .item(
                            button()
                                .label("Reload list")
                                .variant(ButtonVariant::Ghost)
                                .size(ButtonSize::Small)
                                .on_press({
                                    let domain = domain.clone();
                                    move || domain.refresh_files()
                                })
                                .build(),
                        )
                        .unify(),
                }
            })),
    )
}

pub fn source_resolution_dialog(domain: SurferIntegration) -> impl Element {
    let content = El::new().s(Width::fill()).child_signal(
        domain
            .source_navigation_actor
            .signal_cloned()
            .map(move |state| match state {
                SourceNavigationState::Idle => state_message("No source request."),
                SourceNavigationState::Loading { symbol, .. } => {
                    state_message(&format!("Resolving declaration for {symbol}…"))
                }
                SourceNavigationState::Failed { symbol, error, .. } => Column::new()
                    .s(Gap::new().y(SPACING_6))
                    .item(status_heading(failure_label(&error.code)))
                    .item(state_message(&format!(
                        "{symbol}: {} ({})",
                        error.message, error.code
                    )))
                    .item(state_message(platform_result_only_hint()))
                    .unify(),
                SourceNavigationState::Resolved {
                    symbol, resolution, ..
                } => resolution_content(&symbol, resolution),
            }),
    );
    modal_shell(
        "Go to declaration",
        platform_result_only_hint(),
        domain,
        content,
    )
}

fn resolution_content(symbol: &str, resolution: SourceResolution) -> RawElOrText {
    let heading = match resolution.status {
        SourceResolutionStatus::Matched => "Declaration resolved",
        SourceResolutionStatus::Ambiguous => "Ambiguous declaration",
        SourceResolutionStatus::NotFound => "Declaration not found",
        SourceResolutionStatus::Unsupported => "Declaration lookup unsupported",
        SourceResolutionStatus::Unavailable => "Source index unavailable",
    };
    Column::new()
        .s(Width::fill())
        .s(Gap::new().y(SPACING_8))
        .item(status_heading(heading))
        .item(state_message(&format!("Symbol: {symbol}")))
        .items(resolution.matches.into_iter().map(source_match_row))
        .items(resolution.warnings.into_iter().map(|warning| {
            state_message(&format!("Warning {}: {}", warning.code, warning.message))
        }))
        .unify()
}

fn source_match_row(source_match: SourceMatch) -> impl Element {
    let location = match source_match.column {
        Some(column) => format!("{}:{}:{}", source_match.file_id, source_match.line, column),
        None => format!("{}:{}", source_match.file_id, source_match.line),
    };
    Column::new()
        .s(Width::fill())
        .s(Gap::new().y(SPACING_2))
        .s(Padding::all(SPACING_8))
        .s(Background::new().color_signal(neutral_2()))
        .s(RoundedCorners::all(6))
        .item(
            El::new()
                .s(Font::new()
                    .weight(FontWeight::Medium)
                    .color_signal(neutral_11()))
                .child(location),
        )
        .item(state_message(&format!(
            "symbol={} · kind={} · confidence={} · provenance={}",
            source_match.symbol,
            source_match.kind,
            source_match.confidence,
            source_match.provenance
        )))
}

fn modal_shell(
    title: &'static str,
    subtitle: &'static str,
    domain: SurferIntegration,
    content: impl Element,
) -> impl Element {
    El::new()
        .s(Background::new().color("rgba(0, 0, 0, 0.72)"))
        .s(Width::fill())
        .s(Height::fill())
        .s(Align::center())
        .s(Padding::all(40))
        .update_raw_el(|raw_el| {
            raw_el
                .style("position", "fixed")
                .style("inset", "0")
                .style("z-index", "22000")
        })
        .child(
            Column::new()
                .s(Background::new().color_signal(neutral_1()))
                .s(RoundedCorners::all(8))
                .s(Borders::all_signal(
                    neutral_4().map(|color| Border::new().width(1).color(color)),
                ))
                .s(Width::fill().max(720))
                .s(Height::fill().max(560))
                .s(Padding::all(SPACING_12))
                .s(Gap::new().y(SPACING_10))
                .item(
                    Row::new()
                        .s(Width::fill())
                        .s(Align::new().center_y())
                        .item(
                            El::new()
                                .s(Font::new()
                                    .size(18)
                                    .weight(FontWeight::Bold)
                                    .color_signal(neutral_11()))
                                .child(title),
                        )
                        .item(El::new().s(Width::fill()))
                        .item(
                            button()
                                .label("Close")
                                .variant(ButtonVariant::Ghost)
                                .size(ButtonSize::Small)
                                .on_press(move || {
                                    domain.close_picker();
                                    domain.dismiss_source_result();
                                })
                                .build(),
                        ),
                )
                .item(
                    El::new()
                        .s(Font::new().size(13).color_signal(neutral_8()))
                        .child(subtitle),
                )
                .item(
                    El::new()
                        .s(Width::fill())
                        .s(Height::fill())
                        .s(Scrollbars::both())
                        .update_raw_el(|raw_el| raw_el.style("min-height", "0"))
                        .child(content),
                ),
        )
}

fn state_message(message: &str) -> RawElOrText {
    El::new()
        .s(Padding::all(SPACING_8))
        .s(Font::new().size(13).color_signal(neutral_8()))
        .child(message)
        .unify()
}

fn status_heading(message: &str) -> RawElOrText {
    El::new()
        .s(Font::new()
            .weight(FontWeight::Medium)
            .color_signal(neutral_11()))
        .child(message)
        .unify()
}

fn failure_label(code: &str) -> &'static str {
    if code.contains("unsupported") {
        "Declaration lookup unsupported"
    } else if code.contains("unavailable") || code.contains("not_configured") {
        "Source index unavailable"
    } else {
        "Declaration lookup failed"
    }
}

#[cfg(NOVYWAVE_PLATFORM = "WEB")]
fn platform_result_only_hint() -> &'static str {
    "Web mode cannot open arbitrary local files; the resolved logical location is shown here."
}

#[cfg(not(NOVYWAVE_PLATFORM = "WEB"))]
fn platform_result_only_hint() -> &'static str {
    "No approved desktop file-opening bridge is configured; the resolved logical location is shown here."
}
