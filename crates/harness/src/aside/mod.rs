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

use std::path::{Path, PathBuf};
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

/// Parse `aside account list` output into `(account id, signed-in)` rows.
///
/// Only the account id (`u` + digits) and the signed-in boolean are extracted;
/// emails and every other column are ignored/redacted. Lines without a
/// `signed in` / `signed out` marker or without a `u<digits>` token are
/// skipped. There is no `--json` flag, so this parses the human table shape
/// (`* u0 <email> signed in profiles: ...`).
pub fn parse_account_list(text: &str) -> Vec<(String, bool)> {
    let mut accounts = Vec::new();
    for line in text.lines() {
        let lower = line.to_lowercase();
        let signed_in = if lower.contains("signed in") {
            true
        } else if lower.contains("signed out") {
            false
        } else {
            continue;
        };
        let mut found: Option<String> = None;
        for raw in line.split_whitespace() {
            let token = raw.trim_matches(|c| c == '*' || c == ',' || c == ':');
            if token.len() > 1
                && token.starts_with('u')
                && token[1..].chars().all(|c| c.is_ascii_digit())
            {
                found = Some(token.to_owned());
                break;
            }
        }
        if let Some(id) = found {
            accounts.push((id, signed_in));
        }
    }
    accounts
}

fn provider_id_from(entry: &serde_json::Map<String, Value>, fallback: Option<&str>) -> Option<String> {
    for key in ["id", "name"] {
        if let Some(id) = entry
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            return Some(id.to_owned());
        }
    }
    fallback
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn model_id_from(entry: &serde_json::Map<String, Value>) -> Option<String> {
    for key in ["id", "modelId", "model", "name"] {
        if let Some(id) = entry
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        {
            return Some(id.to_owned());
        }
    }
    None
}

fn display_name_from_entry(entry: &serde_json::Map<String, Value>, fallback: &str) -> String {
    // Display-only: the `name` field never contributes to row ids.
    entry
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| fallback.to_owned())
}

fn push_model_ids_from_value(value: &Value, ids: &mut Vec<String>) {
    if let Some(text) = value.as_str().map(str::trim).filter(|t| !t.is_empty()) {
        ids.push(text.to_owned());
    } else if let Some(entry) = value.as_object() {
        if let Some(id) = model_id_from(entry) {
            ids.push(id);
        }
    }
}

fn dedup_ids(ids: &mut Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    ids.retain(|id| seen.insert(id.clone()));
}

/// Collect model ids from `models[]` plus `accountModelCatalog`.
///
/// `accountModelCatalog` may hold `modelIds[]` (plain strings) or a `models[]`
/// list variant (strings or objects). Both shapes are merged; callers deduplicate.
/// Secret-adjacent fields are never read.
fn collect_model_ids(entry: &serde_json::Map<String, Value>) -> Vec<String> {
    let mut ids = Vec::new();
    if let Some(models) = entry.get("models").and_then(Value::as_array) {
        for model in models {
            push_model_ids_from_value(model, &mut ids);
        }
    }
    if let Some(catalog) = entry.get("accountModelCatalog") {
        if let Some(obj) = catalog.as_object() {
            for key in ["modelIds", "model_ids", "models"] {
                if let Some(arr) = obj.get(key).and_then(Value::as_array) {
                    for model in arr {
                        push_model_ids_from_value(model, &mut ids);
                    }
                }
            }
        } else if let Some(arr) = catalog.as_array() {
            for model in arr {
                push_model_ids_from_value(model, &mut ids);
            }
        }
    }
    dedup_ids(&mut ids);
    ids
}

/// Inner catalog with display names: `(true provider id, display name, model ids)`.
///
/// For the dict shape the TRUE id is always the dict key; the entry `name`
/// field is captured only as display text for descriptions. For the array
/// shape the id still comes from the entry (`id` then `name`).
fn parse_catalog_with_display(value: &Value) -> Vec<(String, String, Vec<String>)> {
    let mut catalog = Vec::new();
    match value.get("providers") {
        Some(Value::Array(providers)) => {
            for provider in providers {
                let Some(entry) = provider.as_object() else {
                    continue;
                };
                let Some(provider_id) = provider_id_from(entry, None) else {
                    continue;
                };
                let display = display_name_from_entry(entry, &provider_id);
                let ids = collect_model_ids(entry);
                if !ids.is_empty() {
                    catalog.push((provider_id, display, ids));
                }
            }
        }
        Some(Value::Object(map)) => {
            for (key, provider) in map {
                let trimmed = key.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Some(entry) = provider.as_object() {
                    // TRUE id is the dict key; ignore entry `id`/`name` for ids.
                    let provider_id = trimmed.to_owned();
                    let display = display_name_from_entry(entry, &provider_id);
                    let ids = collect_model_ids(entry);
                    if !ids.is_empty() {
                        catalog.push((provider_id, display, ids));
                    }
                } else if let Some(models) = provider.as_array() {
                    let provider_id = trimmed.to_owned();
                    let display = provider_id.clone();
                    let mut ids = Vec::new();
                    for model in models {
                        push_model_ids_from_value(model, &mut ids);
                    }
                    dedup_ids(&mut ids);
                    if !ids.is_empty() {
                        catalog.push((provider_id, display, ids));
                    }
                }
            }
        }
        _ => {}
    }
    catalog
}

/// Parse a `models.json` value into `(provider, model ids)` rows.
///
/// The TRUE provider id for the dict shape is the dict key (e.g.
/// `bedrock-claude`); the entry `name` field (e.g. `Bedrock Claude`) is
/// display-only and never becomes part of a row id. Reads ONLY provider
/// keys plus each model's id-ish keys (`id`/`modelId`/`model`/`name`) and
/// `accountModelCatalog` (`modelIds[]` strings or a `models[]` list variant).
/// Secret-adjacent fields (`apiKey`, `authHeader`, `baseUrl`, …) are never
/// read and never logged. `providers` may be an array or a map; `models[]`
/// entries may be objects or bare strings. Providers without models are
/// skipped. Duplicate model strings within a provider (e.g. via both shapes)
/// appear once.
pub fn parse_models_catalog(value: &Value) -> Vec<(String, Vec<String>)> {
    parse_catalog_with_display(value)
        .into_iter()
        .map(|(id, _, models)| (id, models))
        .collect()
}

/// Parse `settings.json` `defaultModel { provider, modelId }` (non-secret).
///
/// Used ONLY to order the discovered catalog (that row moves right after
/// `default`/`fast`). No other semantics are inferred; `thinkingLevel` and
/// `fastMode` are deliberately ignored.
pub fn parse_default_model(value: &Value) -> Option<(String, String)> {
    let entry = value.get("defaultModel")?.as_object()?;
    let provider = entry
        .get("provider")
        .or_else(|| entry.get("providerId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    let model = entry
        .get("modelId")
        .or_else(|| entry.get("model"))
        .or_else(|| entry.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    Some((provider.to_owned(), model.to_owned()))
}

/// Map `u0` -> `$HOME/.aside/u/0`. Returns `None` for unparseable ids.
fn account_dir(home: &Path, account_id: &str) -> Option<PathBuf> {
    let digits = account_id.strip_prefix('u')?;
    if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(home.join(".aside").join("u").join(digits))
}

fn full_reasoning_ladder() -> Vec<ReasoningLevel> {
    vec![
        ReasoningLevel::Off,
        ReasoningLevel::Minimal,
        ReasoningLevel::Low,
        ReasoningLevel::Medium,
        ReasoningLevel::High,
        ReasoningLevel::XHigh,
        ReasoningLevel::Max,
    ]
}

fn discovered_row(provider: &str, display: &str, model_id: &str) -> Model {
    Model {
        id: format!("{provider}/{model_id}"),
        label: model_id.into(),
        description: Some(format!("{display} via Aside")),
        reasoning_levels: full_reasoning_ladder(),
        options: vec![effort_option(), permission_option()],
    }
}

fn account_option(ids: &[String]) -> ModelOption {
    ModelOption {
        id: "account".into(),
        label: "Account".into(),
        choices: ids
            .iter()
            .map(|id| ModelOptionChoice {
                id: id.clone(),
                label: id.clone(),
            })
            .collect(),
        default_choice: ids.first().cloned().unwrap_or_default(),
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
        Self::global_args_with_accounts(request, None)
    }

    fn global_args_with_accounts(
        request: &RunRequest,
        signed_in: Option<&[String]>,
    ) -> Vec<String> {
        let mut args = Vec::new();
        let selected_model = request.model.as_deref().unwrap_or(DEFAULT_MODEL);
        let configured_speed = request.model_options.get("speed").and_then(option_string);
        let is_slash_model = selected_model.contains('/');
        if selected_model == FAST_MODEL || configured_speed == Some(FAST_MODEL) {
            args.extend(["--speed".into(), FAST_MODEL.into()]);
        } else if is_slash_model {
            // Discovered rows use the `provider/model` slash form. It overrides
            // `--provider`, so never send `-p` alongside `-m`.
            args.extend(["-m".into(), selected_model.into()]);
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
        // Host discovery is intentionally not performed (remote hosts are not
        // probed); an explicit host value is passed through untouched.
        for id in ["provider", "host"] {
            if is_slash_model && id == "provider" {
                continue;
            }
            if let Some(value) = option(id) {
                args.extend([format!("--{id}"), value.into()]);
            }
        }
        if let Some(account) = option("account") {
            match signed_in {
                Some(ids) if !ids.is_empty() => {
                    if ids.iter().any(|id| id == account) {
                        args.extend(["--account".into(), account.into()]);
                    } else {
                        tracing::debug!(
                            target: "zeron_harness::aside",
                            "skipping unknown aside account option"
                        );
                    }
                }
                Some(_) => {
                    tracing::debug!(
                        target: "zeron_harness::aside",
                        "skipping aside account option (no signed-in accounts discovered)"
                    );
                }
                None => {
                    // Discovery did not run or failed: forward without
                    // validation rather than hard-erroring the run.
                    args.extend(["--account".into(), account.into()]);
                }
            }
        }
        args
    }

    /// Blocking-free async discovery of signed-in account ids (Codex-style:
    /// live per call, 10s timeout, failure returns `None`, never cached).
    /// Only the account id + signed-in boolean are parsed; emails are ignored.
    async fn discover_accounts(&self) -> Option<Vec<String>> {
        let executable = match self.resolve_executable() {
            Ok(executable) => executable,
            Err(error) => {
                tracing::debug!(
                    target: "zeron_harness::aside",
                    "aside account discovery skipped (no executable): {error}"
                );
                return None;
            }
        };
        let mut command = Command::new(&executable);
        command.args(["account", "list"]);
        crate::compose_child_path(&mut command, &executable);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let output = match tokio::time::timeout(Duration::from_secs(10), command.output()).await
        {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                tracing::debug!(
                    target: "zeron_harness::aside",
                    "aside account list failed; using fallback catalog: {error}"
                );
                return None;
            }
            Err(_) => {
                tracing::debug!(
                    target: "zeron_harness::aside",
                    "aside account list timed out; using fallback catalog"
                );
                return None;
            }
        };
        if !output.status.success() {
            tracing::debug!(
                target: "zeron_harness::aside",
                "aside account list exited unsuccessfully; using fallback catalog"
            );
            return None;
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let signed_in = parse_account_list(&text)
            .into_iter()
            .filter(|(_, signed_in)| *signed_in)
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        Some(signed_in)
    }

    /// Live catalog probe: signed-in accounts -> `$HOME/.aside/u/{n}/`
    /// `models.json` + `settings.json`. Returns `(discovered rows, signed-in
    /// ids)`. Empty rows on any total failure (static fallback engages);
    /// failures are never cached. Reads ONLY provider name/id + model ids
    /// plus `defaultModel { provider, modelId }` for ordering; secret fields
    /// are never read and file contents are never logged.
    async fn discover_catalog(&self) -> (Vec<Model>, Vec<String>) {
        let Some(signed_in) = self.discover_accounts().await else {
            return (Vec::new(), Vec::new());
        };
        if signed_in.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let Some(home) = crate::executable::home_dir() else {
            tracing::debug!(
                target: "zeron_harness::aside",
                "aside catalog discovery skipped (no home dir)"
            );
            return (Vec::new(), signed_in);
        };
        let mut pairs: Vec<(String, String, String)> = Vec::new();
        let mut first_default: Option<(String, String)> = None;
        for account_id in &signed_in {
            let Some(dir) = account_dir(&home, account_id) else {
                tracing::debug!(
                    target: "zeron_harness::aside",
                    "skipping aside account with unparseable id"
                );
                continue;
            };
            let models_text = match tokio::fs::read_to_string(dir.join("models.json")).await {
                Ok(text) => text,
                Err(error) => {
                    tracing::debug!(
                        target: "zeron_harness::aside",
                        "skipping aside account with unreadable models.json: {error}"
                    );
                    continue;
                }
            };
            let models_value: Value = match serde_json::from_str(&models_text) {
                Ok(value) => value,
                Err(error) => {
                    tracing::debug!(
                        target: "zeron_harness::aside",
                        "skipping aside account with unparseable models.json: {error}"
                    );
                    continue;
                }
            };
            for (provider, display, model_ids) in parse_catalog_with_display(&models_value) {
                for model_id in model_ids {
                    // (true provider id, model id, display-only name).
                    pairs.push((provider.clone(), model_id, display.clone()));
                }
            }
            if first_default.is_none() {
                match tokio::fs::read_to_string(dir.join("settings.json")).await {
                    Ok(text) => match serde_json::from_str::<Value>(&text) {
                        Ok(settings) => {
                            if let Some(default) = parse_default_model(&settings) {
                                first_default = Some(default);
                            }
                        }
                        Err(error) => {
                            tracing::debug!(
                                target: "zeron_harness::aside",
                                "ignoring unparseable aside settings.json: {error}"
                            );
                        }
                    },
                    Err(error) => {
                        tracing::debug!(
                            target: "zeron_harness::aside",
                            "aside settings.json unreadable, continuing without default ordering: {error}"
                        );
                    }
                }
            }
        }
        if pairs.is_empty() {
            return (Vec::new(), signed_in);
        }
        // Sort by TRUE `{provider}/{model}` id form; display text never affects
        // ordering. Stable sort preserves first-seen display for duplicates.
        pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut deduped: Vec<(String, String, String)> = Vec::new();
        {
            let mut seen_pairs = std::collections::HashSet::new();
            for (provider, model_id, display) in pairs {
                if seen_pairs.insert((provider.clone(), model_id.clone())) {
                    deduped.push((provider, model_id, display));
                }
            }
        }
        let mut rows: Vec<Model> = Vec::new();
        let mut seen_ids = std::collections::HashSet::new();
        for (provider, model_id, display) in deduped {
            let id = format!("{provider}/{model_id}");
            if !seen_ids.insert(id.clone()) {
                continue;
            }
            rows.push(discovered_row(&provider, &display, &model_id));
        }
        if let Some((default_provider, default_model)) = first_default {
            // Promotion compares against the TRUE `{provider}/{model}` id form
            // (e.g. `bedrock-claude/<model>`), matching `settings.json`
            // `defaultModel { provider, modelId }`.
            let default_id = format!("{default_provider}/{default_model}");
            if let Some(index) = rows.iter().position(|row| row.id == default_id) {
                if index != 0 {
                    let row = rows.remove(index);
                    rows.insert(0, row);
                }
            }
        }
        (rows, signed_in)
    }

    async fn resolved_global_args(&self, request: &RunRequest) -> Vec<String> {
        match self.discover_accounts().await {
            Some(ids) => Self::global_args_with_accounts(request, Some(&ids)),
            None => Self::global_args(request),
        }
    }

    async fn spawn_mcp(
        &self,
        request: &RunRequest,
    ) -> Result<(PathBuf, Child, RpcClient), HarnessError> {
        let executable = self.resolve_executable()?;
        let mut command = Command::new(&executable);
        command.args(self.resolved_global_args(request).await).arg("mcp");
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
        command.args(self.resolved_global_args(request).await);
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
        // Codex-style live discovery per call with static fallback; failures
        // are never cached so reopening the picker retries.
        let (discovered, signed_in) = self.discover_catalog().await;
        let mut catalog = static_models();
        catalog.extend(discovered);
        if !signed_in.is_empty() {
            let option = account_option(&signed_in);
            for model in &mut catalog {
                model.options.push(option.clone());
            }
        }
        Ok(catalog)
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
        let mut values: HashMap<String, OsString> = HashMap::new();
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

    #[test]
    fn account_list_parser_filters_signed_out_and_ignores_emails() {
        let text = "\
* u0  redacted  signed in  profiles: Profile 0
provider: redacted
  u1  redacted  signed out
  u2  redacted  signed in  profiles: Profile 0
not an account line
";
        assert_eq!(
            parse_account_list(text),
            vec![
                ("u0".to_owned(), true),
                ("u1".to_owned(), false),
                ("u2".to_owned(), true),
            ]
        );
        assert!(parse_account_list("").is_empty());
        assert!(parse_account_list("provider: redacted\n").is_empty());
    }

    #[test]
    fn catalog_parser_ignores_secret_adjacent_fields() {
        let value = json!({
            "providers": [
                {
                    "id": "fake-alpha",
                    "apiKey": "FAKE-SECRET-DO-NOT-USE",
                    "authHeader": "FAKE-SECRET-DO-NOT-USE",
                    "baseUrl": "https://fake.example.invalid",
                    "models": [
                        {"id": "fake-alpha-model-b", "apiKey": "FAKE"},
                        {"id": "fake-alpha-model-a", "displayName": "Fake A"},
                        "fake-alpha-model-c"
                    ]
                },
                {
                    "name": "fake-beta",
                    "apiKey": "FAKE-SECRET-DO-NOT-USE",
                    "models": [
                        {"modelId": "fake-beta-model-1"},
                        {"model": "fake-beta-model-2"},
                        {"name": "fake-beta-model-3"}
                    ]
                },
                {"id": "fake-empty", "models": []},
                {"models": [{"id": "orphan"}]}
            ]
        });
        let catalog = parse_models_catalog(&value);
        assert_eq!(catalog.len(), 2);
        assert_eq!(catalog[0].0, "fake-alpha");
        assert_eq!(
            catalog[0].1,
            vec![
                "fake-alpha-model-b",
                "fake-alpha-model-a",
                "fake-alpha-model-c"
            ]
        );
        assert_eq!(catalog[1].0, "fake-beta");
        assert_eq!(
            catalog[1].1,
            vec![
                "fake-beta-model-1",
                "fake-beta-model-2",
                "fake-beta-model-3"
            ]
        );
    }

    #[test]
    fn default_model_parser_reads_only_provider_and_model() {
        let value = json!({
            "defaultModel": {
                "provider": "fake-beta",
                "modelId": "fake-beta-model-2",
                "thinkingLevel": "whatever",
                "fastMode": false
            }
        });
        assert_eq!(
            parse_default_model(&value),
            Some(("fake-beta".to_owned(), "fake-beta-model-2".to_owned()))
        );
        assert_eq!(parse_default_model(&json!({})), None);
        assert_eq!(
            parse_default_model(&json!({"defaultModel": {"provider": "x"}})),
            None
        );
    }

    #[test]
    fn account_dir_maps_u_prefix_to_digits() {
        let home = std::path::Path::new("/tmp/fake-home");
        assert_eq!(
            account_dir(home, "u0"),
            Some(home.join(".aside").join("u").join("0"))
        );
        assert_eq!(
            account_dir(home, "u12"),
            Some(home.join(".aside").join("u").join("12"))
        );
        assert_eq!(account_dir(home, "u"), None);
        assert_eq!(account_dir(home, "ux"), None);
        assert_eq!(account_dir(home, "0"), None);
        assert_eq!(account_dir(home, ""), None);
    }

    #[test]
    fn slash_model_uses_short_flag_and_suppresses_provider() {
        let mut request = RunRequest {
            prompt: "p".into(),
            harness: None,
            model: Some("fake-beta/fake-beta-model-1".into()),
            reasoning: None,
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
            .insert("provider".into(), "fake-beta".into());
        request.model_options.insert("host".into(), "local".into());
        assert_eq!(
            AsideHarness::global_args(&request),
            vec!["-m", "fake-beta/fake-beta-model-1", "--host", "local"]
        );
    }

    #[test]
    fn account_option_validates_membership_only_when_discovery_succeeded() {
        let mut request = RunRequest {
            prompt: "p".into(),
            harness: None,
            model: Some("default".into()),
            reasoning: None,
            model_options: Map::new(),
            cwd: String::new(),
            sandbox: zeron_proto::SandboxLevel::ReadOnly,
            auto_approve: false,
            resume: None,
            attachments: Vec::new(),
            worktree: None,
        };
        request.model_options.insert("account".into(), "u0".into());
        // No discovery (None): forward without validation, never hard-error.
        assert!(AsideHarness::global_args(&request).contains(&"--account".to_owned()));
        // Discovery succeeded with membership: forward.
        assert!(AsideHarness::global_args_with_accounts(
            &request,
            Some(&["u0".to_owned(), "u2".to_owned()])
        )
        .contains(&"u0".to_owned()));
        // Discovery succeeded without membership: skip, never hard-error.
        let filtered = AsideHarness::global_args_with_accounts(&request, Some(&["u2".to_owned()]));
        assert!(!filtered.contains(&"--account".to_owned()));
        assert!(!filtered.contains(&"u0".to_owned()));
        // Discovery succeeded with zero accounts: skip.
        let empty =
            AsideHarness::global_args_with_accounts(&request, Some(&[] as &[String]));
        assert!(!empty.contains(&"--account".to_owned()));
    }
}
