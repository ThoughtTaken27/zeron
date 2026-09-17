//! Aside Browser CLI harness.
//!
//! This adapter uses only the public `aside` CLI surface:
//! - `aside mcp` over newline-delimited MCP JSON-RPC for execution/resume;
//! - `aside session steer` for an in-flight replacement prompt; and
//! - `aside session stop` for interruption.
//!
//! The MCP `exec` tool is completion-oriented rather than a token stream, so a
//! completed response is normalized into one text delta (when present) and a
//! terminal `Done` event. Session ids are carried through both the start and
//! terminal events and are passed back to `exec` for follow-up turns.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::{Map, Value, json};
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;

use zeron_proto::{
    AgentEvent, DoneStatus, HarnessId, Model, ModelOption, ModelOptionChoice, ReasoningLevel,
    RunRequest, SteeringMode,
};

use crate::jsonrpc::RpcClient;
use crate::process::{Child, Command, Stdio};
use crate::{Harness, HarnessError, RunControls, shutdown_child};

const DEFAULT_PROTOCOL_VERSION: &str = "2025-03-26";
const DEFAULT_MODEL: &str = "default";
const FAST_MODEL: &str = "fast";
const INTERRUPT_GRACE: Duration = Duration::from_secs(2);
const KILL_GRACE: Duration = Duration::from_secs(3);

/// Resolve the official Aside CLI without spawning it.
///
/// The explicit override wins, followed by the process PATH, the login-shell
/// PATH snapshot, the documented user install path, and the app bundles used
/// by Aside's macOS distribution. `find_on_paths` also covers the usual user
/// Node-manager bins, which matters when the CLI is a shim.
pub fn resolve_aside_executable() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("ASIDE_EXECUTABLE").filter(|p| !p.is_empty()) {
        return crate::executable::validate_native_override(&PathBuf::from(path)).ok();
    }

    let mut extra = Vec::new();
    if let Some(home) = crate::executable::home_dir() {
        extra.push(home.join(".local").join("bin").join("aside"));
        extra.push(
            home.join(".aside")
                .join("cli")
                .join("Aside CLI.app")
                .join("Contents")
                .join("MacOS")
                .join("aside"),
        );
    }
    extra.push(PathBuf::from(
        "/Applications/Aside.app/Contents/MacOS/Aside",
    ));
    crate::executable::find_on_paths("aside", extra)
}

/// The two routing models exposed by the public CLI: normal settings routing
/// and the documented fast route. Provider and host are intentionally not
/// model entries because their values are user/account-specific strings and
/// cannot be faithfully represented by [`ModelOption`].
pub fn static_models() -> Vec<Model> {
    [
        (DEFAULT_MODEL, "Default", "Aside's default model routing"),
        (FAST_MODEL, "Fast", "Aside's fast model routing"),
    ]
    .into_iter()
    .map(|(id, label, description)| Model {
        id: id.into(),
        label: label.into(),
        description: Some(description.into()),
        reasoning_levels: vec![
            ReasoningLevel::Off,
            ReasoningLevel::Minimal,
            ReasoningLevel::Low,
            ReasoningLevel::Medium,
            ReasoningLevel::High,
            ReasoningLevel::XHigh,
            ReasoningLevel::Max,
        ],
        options: vec![effort_option(), permission_option()],
    })
    .collect()
}

fn choices(values: &[(&str, &str)]) -> Vec<ModelOptionChoice> {
    values
        .iter()
        .map(|(id, label)| ModelOptionChoice {
            id: (*id).into(),
            label: (*label).into(),
        })
        .collect()
}

fn effort_option() -> ModelOption {
    ModelOption {
        id: "effort".into(),
        label: "Thinking Effort".into(),
        choices: choices(&[
            ("default", "Default"),
            ("off", "Off"),
            ("minimal", "Minimal"),
            ("low", "Low"),
            ("medium", "Medium"),
            ("high", "High"),
            ("xhigh", "XHigh"),
            ("max", "Max"),
            ("ultrabrowse", "UltraBrowse"),
        ]),
        default_choice: "default".into(),
    }
}

fn permission_option() -> ModelOption {
    ModelOption {
        id: "permission".into(),
        label: "Permission".into(),
        choices: choices(&[
            ("ask", "Ask"),
            ("guard", "Guard"),
            ("full-access", "Full Access"),
        ]),
        default_choice: "guard".into(),
    }
}

/// A native Aside CLI harness.
pub struct AsideHarness {
    executable: Option<PathBuf>,
    interrupt_grace: Duration,
    kill_grace: Duration,
}

impl Default for AsideHarness {
    fn default() -> Self {
        Self {
            executable: None,
            interrupt_grace: INTERRUPT_GRACE,
            kill_grace: KILL_GRACE,
        }
    }
}

impl AsideHarness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Use a fixed CLI binary, primarily for deterministic fake-CLI tests.
    pub fn with_executable(mut self, path: impl Into<PathBuf>) -> Self {
        self.executable = Some(path.into());
        self
    }

    /// Tune the documented stop-command wait and child cleanup grace periods.
    pub fn with_graces(mut self, interrupt_grace: Duration, kill_grace: Duration) -> Self {
        self.interrupt_grace = interrupt_grace;
        self.kill_grace = kill_grace;
        self
    }

    fn resolve_executable(&self) -> Result<PathBuf, HarnessError> {
        if let Some(path) = &self.executable {
            return crate::executable::validate_native_override(path);
        }
        resolve_aside_executable().ok_or_else(|| {
            HarnessError::NotInstalled(
                "aside (searched ASIDE_EXECUTABLE, PATH, the login shell's PATH, ~/.local/bin/aside, ~/.aside/cli/Aside CLI.app, and /Applications/Aside.app; set ASIDE_EXECUTABLE to override)".into(),
            )
        })
    }

    fn global_args(request: &RunRequest) -> Vec<String> {
        let mut args = Vec::new();
        let selected_model = request.model.as_deref().unwrap_or(DEFAULT_MODEL);
        let configured_speed = request.model_options.get("speed").and_then(option_string);
        if selected_model == FAST_MODEL || configured_speed == Some(FAST_MODEL) {
            args.extend(["--speed".into(), FAST_MODEL.into()]);
        } else if selected_model != DEFAULT_MODEL && !selected_model.is_empty() {
            // Keep the adapter useful for callers that already store an
            // explicit provider/model, while the advertised catalog remains
            // limited to Default and Fast routing.
            args.extend(["--model".into(), selected_model.into()]);
        }

        let option = |id: &str| {
            request
                .model_options
                .get(id)
                .and_then(option_string)
                .filter(|value| !value.is_empty())
        };
        if let Some(effort) = option("effort").filter(|value| *value != "default") {
            args.extend(["--effort".into(), effort.into()]);
        } else if let Some(reasoning) = request.reasoning.and_then(reasoning_flag) {
            args.extend(["--effort".into(), reasoning.into()]);
        }
        if let Some(permission) = option("permission") {
            args.extend(["--permission".into(), permission.into()]);
        }
        // These are documented CLI flags and are accepted from callers that
        // have a representable, explicit value, even though static_models does
        // not advertise arbitrary provider/host strings as picker choices.
        for id in ["provider", "host"] {
            if let Some(value) = option(id) {
                args.extend([format!("--{id}"), value.into()]);
            }
        }
        args
    }

    async fn spawn_mcp(
        &self,
        request: &RunRequest,
    ) -> Result<(PathBuf, Child, RpcClient), HarnessError> {
        let executable = self.resolve_executable()?;
        let mut command = Command::new(&executable);
        command.args(Self::global_args(request)).arg("mcp");
        crate::compose_child_path(&mut command, &executable);
        if !request.cwd.is_empty() {
            command.current_dir(&request.cwd);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                HarnessError::NotInstalled(executable.display().to_string())
            } else {
                HarnessError::Io(error)
            }
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| HarnessError::Protocol("aside mcp child has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| HarnessError::Protocol("aside mcp child has no stdout".into()))?;
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "zeron_harness::aside", "aside stderr: {line}");
                }
            });
        }
        let (client, mut incoming) = RpcClient::new(stdin, stdout);
        // MCP servers may emit startup notifications/events. Keep the reader
        // alive and discard those non-request messages; JSON-RPC responses are
        // still resolved by RpcClient's pending map.
        tokio::spawn(async move { while incoming.recv().await.is_some() {} });
        Ok((executable, child, client))
    }

    async fn run_session_command(
        &self,
        request: &RunRequest,
        command_name: &str,
        session_id: &str,
        prompt: Option<&str>,
    ) -> Result<std::process::ExitStatus, HarnessError> {
        let executable = self.resolve_executable()?;
        let mut command = Command::new(&executable);
        command.args(Self::global_args(request));
        command.args(["session", command_name, session_id]);
        if let Some(prompt) = prompt {
            command.arg(prompt);
        }
        crate::compose_child_path(&mut command, &executable);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                HarnessError::NotInstalled(executable.display().to_string())
            } else {
                HarnessError::Io(error)
            }
        })?;
        child.wait().await.map_err(HarnessError::Io)
    }
}

#[async_trait]
impl Harness for AsideHarness {
    fn id(&self) -> HarnessId {
        HarnessId::Aside
    }

    fn display_name(&self) -> &str {
        "Aside"
    }

    fn supports_steering(&self) -> bool {
        true
    }

    fn steering_mode(&self) -> SteeringMode {
        // Aside's documented `session steer` command interrupts the current
        // step and replaces it, so it is a true in-turn steering capability.
        SteeringMode::StepBoundary
    }

    fn reasoning_levels(&self) -> &[ReasoningLevel] {
        const LEVELS: &[ReasoningLevel] = &[
            ReasoningLevel::Off,
            ReasoningLevel::Minimal,
            ReasoningLevel::Low,
            ReasoningLevel::Medium,
            ReasoningLevel::High,
            ReasoningLevel::XHigh,
            ReasoningLevel::Max,
        ];
        LEVELS
    }

    fn installed(&self) -> bool {
        self.resolve_executable().is_ok()
    }

    // MCP exec returns only after the CLI has completed the task.
    fn deterministic_turn_end(&self) -> bool {
        true
    }

    async fn models(&self) -> Result<Vec<Model>, HarnessError> {
        self.resolve_executable()?;
        Ok(static_models())
    }

    async fn run(
        &self,
        request: RunRequest,
        controls: RunControls,
    ) -> Result<BoxStream<'static, Result<AgentEvent, HarnessError>>, HarnessError> {
        let (executable, child, client) = self.spawn_mcp(&request).await?;
        let (event_tx, event_rx) = mpsc::channel(32);
        tokio::spawn(run_session(AsideSession {
            executable,
            child,
            client,
            event_tx,
            request,
            controls,
            interrupt_grace: self.interrupt_grace,
            kill_grace: self.kill_grace,
        }));
        Ok(futures::stream::unfold(
            event_rx,
            |mut receiver| async move { receiver.recv().await.map(|event| (event, receiver)) },
        )
        .boxed())
    }
}

struct AsideSession {
    executable: PathBuf,
    child: Child,
    client: RpcClient,
    event_tx: mpsc::Sender<Result<AgentEvent, HarnessError>>,
    request: RunRequest,
    controls: RunControls,
    interrupt_grace: Duration,
    kill_grace: Duration,
}

async fn run_session(session: AsideSession) {
    let AsideSession {
        executable,
        mut child,
        client,
        event_tx,
        request,
        controls,
        interrupt_grace,
        kill_grace,
    } = session;
    let RunControls {
        request_input: _,
        mut steering,
        interrupt,
    } = controls;

    let initialize = client.request(
        "initialize",
        json!({
            "protocolVersion": DEFAULT_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {
                "name": "zeron-aside",
                "title": "Zeron",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }),
    );
    if let Err(error) = tokio::select! {
        result = initialize => result,
        _ = interrupt.cancelled() => {
            emit_done(&event_tx, DoneStatus::Interrupted, None, None, request.resume.clone()).await;
            shutdown_child(&mut child, kill_grace).await;
            return;
        }
    } {
        emit_done(
            &event_tx,
            DoneStatus::Errored,
            None,
            Some(error.to_string()),
            None,
        )
        .await;
        shutdown_child(&mut child, kill_grace).await;
        return;
    }
    client.notify("notifications/initialized", None);

    let mut arguments = Map::new();
    arguments.insert("prompt".into(), Value::String(request.prompt.clone()));
    if let Some(session_id) = request.resume.as_deref() {
        arguments.insert("session_id".into(), Value::String(session_id.into()));
    }
    let mut call = Box::pin(client.request(
        "tools/call",
        Value::Object(Map::from_iter([
            ("name".into(), Value::String("exec".into())),
            ("arguments".into(), Value::Object(arguments)),
        ])),
    ));
    let mut interrupted = false;
    let mut interrupted_session_id = request.resume.clone();
    // Aside MCP does not provide a message id in its public result contract;
    // mint one per Zeron turn so separate chats and follow-ups cannot collide
    // in the engine's transcript deduplication.
    let assistant_message_id = format!("aside-{}", uuid::Uuid::new_v4());
    let mut control_tasks = Vec::new();

    let response = loop {
        tokio::select! {
            result = &mut call => break Some(result),
            message = steering.recv(), if !interrupted => {
                let Some(message) = message else { continue };
                let Some(session_id) = interrupted_session_id.clone() else {
                    let _ = event_tx.send(Ok(AgentEvent::Error {
                        message: "Aside cannot steer a new MCP session until its session_id is known".into(),
                    })).await;
                    continue;
                };
                let harness = AsideHarness::new()
                    .with_executable(executable.clone())
                    .with_graces(interrupt_grace, kill_grace);
                let request_clone = request.clone();
                let event_tx_clone = event_tx.clone();
                control_tasks.push(tokio::spawn(async move {
                    let outcome = harness.run_session_command(&request_clone, "steer", &session_id, Some(&message.prompt)).await;
                    if matches!(outcome, Ok(status) if status.success()) {
                        let _ = event_tx_clone.send(Ok(AgentEvent::Steered {
                            assistant_message_id: None,
                            next_assistant_message_id: None,
                        })).await;
                    } else if let Err(error) = outcome {
                        let _ = event_tx_clone.send(Ok(AgentEvent::Error { message: error.to_string() })).await;
                    }
                }));
            },
            _ = interrupt.cancelled(), if !interrupted => {
                interrupted = true;
                if let Some(session_id) = interrupted_session_id.clone() {
                    let harness = AsideHarness::new()
                        .with_executable(executable.clone())
                        .with_graces(interrupt_grace, kill_grace);
                    let request_clone = request.clone();
                    control_tasks.push(tokio::spawn(async move {
                        let _ = harness.run_session_command(&request_clone, "stop", &session_id, None).await;
                    }));
                }
                match tokio::time::timeout(interrupt_grace, &mut call).await {
                    Ok(result) => break Some(result),
                    Err(_) => break None,
                }
            },
        }
    };

    for task in control_tasks {
        let _ = tokio::time::timeout(interrupt_grace, task).await;
    }

    if interrupted {
        emit_done(
            &event_tx,
            DoneStatus::Interrupted,
            None,
            None,
            interrupted_session_id,
        )
        .await;
        shutdown_child(&mut child, kill_grace).await;
        return;
    }

    match response {
        Some(Ok(result)) => {
            let mapped =
                map_completed_result(&result, request.resume.as_deref(), &assistant_message_id);
            interrupted_session_id = mapped.session_id.clone();
            if !mapped
                .events
                .iter()
                .any(|event| matches!(event, AgentEvent::SessionStarted { .. }))
            {
                let _ = event_tx
                    .send(Ok(AgentEvent::SessionStarted {
                        harness: HarnessId::Aside,
                        model: request
                            .model
                            .clone()
                            .unwrap_or_else(|| DEFAULT_MODEL.into()),
                        tools: Vec::new(),
                        cwd: request.cwd.clone(),
                        session_id: mapped.session_id.clone().unwrap_or_default(),
                        assistant_message_id: assistant_message_id.clone(),
                    }))
                    .await;
            }
            for event in mapped.events {
                let _ = event_tx.send(Ok(event)).await;
            }
            let _ = event_tx
                .send(Ok(AgentEvent::Done {
                    status: mapped.status,
                    result: mapped.result,
                    error: mapped.error,
                    session_id: mapped.session_id,
                }))
                .await;
        }
        Some(Err(error)) => {
            emit_done(
                &event_tx,
                DoneStatus::Errored,
                None,
                Some(error.to_string()),
                interrupted_session_id,
            )
            .await;
        }
        None => {
            emit_done(
                &event_tx,
                DoneStatus::Errored,
                None,
                Some("Aside MCP session ended without a result".into()),
                interrupted_session_id,
            )
            .await;
        }
    }
    shutdown_child(&mut child, kill_grace).await;
}

struct MappedResult {
    events: Vec<AgentEvent>,
    status: DoneStatus,
    result: Option<String>,
    error: Option<String>,
    session_id: Option<String>,
}

fn map_completed_result(
    response: &Value,
    fallback_session_id: Option<&str>,
    assistant_message_id: &str,
) -> MappedResult {
    let structured = response
        .get("structuredContent")
        .filter(|value| value.is_object())
        .cloned()
        .or_else(|| content_json(response))
        .unwrap_or_else(|| response.clone());
    let session_id = find_string(&[&structured, response], &["session_id", "sessionId"])
        .or_else(|| fallback_session_id.map(str::to_owned));
    let status = find_string(&[&structured, response], &["status"]);
    let tool_error = response.get("isError").and_then(Value::as_bool) == Some(true)
        || structured.get("isError").and_then(Value::as_bool) == Some(true)
        || has_nonempty_key(&structured, "error")
        || has_nonempty_key(response, "error")
        || matches!(
            status.as_deref(),
            Some("error" | "errored" | "failed" | "failure")
        );
    let error = find_string(&[&structured, response], &["error", "message"]);
    let result_text = find_string(
        &[&structured, response],
        &["result", "output", "answer", "text"],
    )
    .or_else(|| content_text(response));

    if tool_error || error.is_some() && result_text.is_none() {
        return MappedResult {
            events: Vec::new(),
            status: DoneStatus::Errored,
            result: None,
            error: error
                .or(result_text)
                .or_else(|| Some("Aside MCP execution failed".into())),
            session_id,
        };
    }

    let mut events = Vec::new();
    if let Some(event_values) = structured.get("events").and_then(Value::as_array) {
        for value in event_values {
            if let Ok(event) = serde_json::from_value::<AgentEvent>(value.clone()) {
                events.push(event);
            }
        }
    }
    // The outer MCP response is authoritative for completion, so avoid
    // emitting a second terminal event if a future CLI includes one in events.
    events.retain(|event| !matches!(event, AgentEvent::Done { .. }));
    if events.is_empty() {
        if let Some(text) = result_text.clone().filter(|text| !text.is_empty()) {
            events.push(AgentEvent::TextDelta { text });
            events.push(AgentEvent::AssistantMessageCompleted {
                assistant_message_id: assistant_message_id.into(),
            });
        }
    }
    MappedResult {
        events,
        status: DoneStatus::Completed,
        result: result_text,
        error: None,
        session_id,
    }
}

fn find_string(values: &[&Value], keys: &[&str]) -> Option<String> {
    values
        .iter()
        .find_map(|value| find_string_recursive(value, keys))
}

fn has_nonempty_key(value: &Value, key: &str) -> bool {
    if let Some(object) = value.as_object() {
        if object
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
        {
            return true;
        }
        return object.values().any(|child| has_nonempty_key(child, key));
    }
    value
        .as_array()
        .is_some_and(|array| array.iter().any(|child| has_nonempty_key(child, key)))
}

fn find_string_recursive(value: &Value, keys: &[&str]) -> Option<String> {
    if let Some(object) = value.as_object() {
        for key in keys {
            if let Some(text) = object
                .get(*key)
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                return Some(text.to_owned());
            }
        }
        for child in object.values() {
            if let Some(found) = find_string_recursive(child, keys) {
                return Some(found);
            }
        }
    } else if let Some(array) = value.as_array() {
        for child in array {
            if let Some(found) = find_string_recursive(child, keys) {
                return Some(found);
            }
        }
    }
    None
}

fn content_json(response: &Value) -> Option<Value> {
    response
        .get("content")
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find_map(|item| {
                item.get("text")
                    .and_then(Value::as_str)
                    .and_then(|text| serde_json::from_str::<Value>(text).ok())
                    .filter(Value::is_object)
            })
        })
}

fn content_text(response: &Value) -> Option<String> {
    let content = response.get("content")?.as_array()?;
    let mut text = String::new();
    for item in content {
        if let Some(value) = item.get("text").and_then(Value::as_str) {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(value);
        }
    }
    (!text.is_empty()).then_some(text)
}

fn option_string(value: &Value) -> Option<&str> {
    value.as_str().or_else(|| {
        value
            .as_bool()
            .map(|value| if value { "true" } else { "false" })
    })
}

fn reasoning_flag(level: ReasoningLevel) -> Option<&'static str> {
    Some(match level {
        ReasoningLevel::Off => return None,
        ReasoningLevel::Minimal => "minimal",
        ReasoningLevel::Low => "low",
        ReasoningLevel::Medium => "medium",
        ReasoningLevel::High => "high",
        ReasoningLevel::XHigh | ReasoningLevel::Ultracode => "xhigh",
        ReasoningLevel::Max | ReasoningLevel::Ultra => "max",
        ReasoningLevel::Ultrathink => return None,
    })
}

async fn emit_done(
    event_tx: &mpsc::Sender<Result<AgentEvent, HarnessError>>,
    status: DoneStatus,
    result: Option<String>,
    error: Option<String>,
    session_id: Option<String>,
) {
    let _ = event_tx
        .send(Ok(AgentEvent::Done {
            status,
            result,
            error,
            session_id,
        }))
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::ffi::OsString;

    #[test]
    fn detection_prefers_path_login_shell_then_user_and_app_locations() {
        let temp = tempfile::tempdir().unwrap();
        let path_dir = temp.path().join("path-bin");
        let shell_dir = temp.path().join("shell-bin");
        let home = temp.path().join("home");
        let local_dir = home.join(".local").join("bin");
        let app = temp.path().join("Aside.app");
        for dir in [&path_dir, &shell_dir, &local_dir, &app] {
            std::fs::create_dir_all(dir).unwrap();
        }
        let path_exe = path_dir.join("aside");
        let shell_exe = shell_dir.join("aside");
        let local_exe = local_dir.join("aside");
        let app_exe = app.join("aside");
        for exe in [&path_exe, &shell_exe, &local_exe, &app_exe] {
            std::fs::write(exe, b"fake").unwrap();
        }
        let joined = |dirs: &[&std::path::Path]| std::env::join_paths(dirs).unwrap();
        let mut values = HashMap::new();
        values.insert("HOME".into(), OsString::from(home));
        values.insert("PATH".into(), joined(&[path_dir.clone().as_path()]));
        let env = |key: &str| values.get(key).cloned();
        let extras = vec![local_exe.clone(), app_exe.clone()];

        assert_eq!(
            crate::executable::find_on_paths_with(
                "aside",
                extras.clone(),
                &env,
                Some(joined(&[shell_dir.as_path()])),
                crate::executable::Platform::Unix,
            ),
            Some(path_exe.clone())
        );
        std::fs::remove_file(path_exe).unwrap();
        assert_eq!(
            crate::executable::find_on_paths_with(
                "aside",
                extras.clone(),
                &env,
                Some(joined(&[shell_dir.as_path()])),
                crate::executable::Platform::Unix,
            ),
            Some(shell_exe.clone())
        );
        std::fs::remove_file(shell_exe).unwrap();
        assert_eq!(
            crate::executable::find_on_paths_with(
                "aside",
                extras.clone(),
                &env,
                None,
                crate::executable::Platform::Unix,
            ),
            Some(local_exe.clone())
        );
        std::fs::remove_file(local_exe).unwrap();
        assert_eq!(
            crate::executable::find_on_paths_with(
                "aside",
                extras,
                &env,
                None,
                crate::executable::Platform::Unix,
            ),
            Some(app_exe)
        );
    }

    #[test]
    fn maps_structured_completion_and_preserves_session_id() {
        let mapped = map_completed_result(
            &json!({
                "structuredContent": {"session_id": "ses-1", "result": "finished"},
                "content": [{"type": "text", "text": "finished"}]
            }),
            None,
            "aside-test-message",
        );
        assert_eq!(mapped.session_id.as_deref(), Some("ses-1"));
        assert_eq!(mapped.status, DoneStatus::Completed);
        assert_eq!(mapped.result.as_deref(), Some("finished"));
        assert!(
            matches!(mapped.events.first(), Some(AgentEvent::TextDelta { text }) if text == "finished")
        );
    }

    #[test]
    fn maps_json_encoded_completion_from_mcp_text_content() {
        let mapped = map_completed_result(
            &json!({
                "content": [{
                    "type": "text",
                    "text": "{\"sessionId\":\"ses-json\",\"result\":\"json result\"}"
                }]
            }),
            None,
            "aside-test-message",
        );
        assert_eq!(mapped.session_id.as_deref(), Some("ses-json"));
        assert_eq!(mapped.result.as_deref(), Some("json result"));
    }

    #[test]
    fn maps_mcp_tool_error() {
        let mapped = map_completed_result(
            &json!({"isError": true, "content": [{"type": "text", "text": "not running"}]}),
            Some("ses-2"),
            "aside-test-message",
        );
        assert_eq!(mapped.status, DoneStatus::Errored);
        assert_eq!(mapped.session_id.as_deref(), Some("ses-2"));
        assert_eq!(mapped.error.as_deref(), Some("not running"));
    }

    #[test]
    fn models_expose_only_supported_routing_and_picker_options() {
        let models = static_models();
        assert_eq!(
            models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            vec!["default", "fast"]
        );
        assert!(
            models
                .iter()
                .all(|model| model.options.iter().any(|option| option.id == "effort"))
        );
        assert!(
            models
                .iter()
                .all(|model| model.options.iter().any(|option| option.id == "permission"))
        );
        assert!(models.iter().all(|model| {
            model
                .options
                .iter()
                .all(|option| option.id != "provider" && option.id != "host")
        }));
    }

    #[test]
    fn reasoning_and_explicit_options_map_to_documented_flags() {
        let mut request = RunRequest {
            prompt: "p".into(),
            harness: None,
            model: Some("fast".into()),
            reasoning: Some(ReasoningLevel::High),
            model_options: Map::new(),
            cwd: String::new(),
            sandbox: zeron_proto::SandboxLevel::ReadOnly,
            auto_approve: false,
            resume: None,
            attachments: Vec::new(),
            worktree: None,
        };
        request
            .model_options
            .insert("permission".into(), "full-access".into());
        request
            .model_options
            .insert("provider".into(), "openai".into());
        request.model_options.insert("host".into(), "local".into());
        assert_eq!(
            AsideHarness::global_args(&request),
            vec![
                "--speed",
                "fast",
                "--effort",
                "high",
                "--permission",
                "full-access",
                "--provider",
                "openai",
                "--host",
                "local"
            ]
        );
    }
}
