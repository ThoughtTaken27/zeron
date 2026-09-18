//! AsideHarness integration tests against the fake public CLI surface.

#![cfg(unix)]

use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};

use zeron_harness::{AsideHarness, CancellationToken, Harness, RunControls, SteerMessage};
use zeron_proto::{AgentEvent, DoneStatus, HarnessId, RunRequest, SandboxLevel};

fn fixture_path() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fake-aside.sh");
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755));
    path
}

fn request(prompt: &str) -> RunRequest {
    RunRequest {
        prompt: prompt.into(),
        harness: None,
        model: Some("fast".into()),
        reasoning: None,
        model_options: serde_json::Map::new(),
        cwd: String::new(),
        sandbox: SandboxLevel::ReadOnly,
        auto_approve: false,
        resume: None,
        attachments: Vec::new(),
        worktree: None,
    }
}

fn set_env(key: &str, value: impl AsRef<std::ffi::OsStr>) {
    unsafe { std::env::set_var(key, value) }
}

fn remove_env(key: &str) {
    unsafe { std::env::remove_var(key) }
}

fn home_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(())).lock().unwrap()
}

struct HomeGuard {
    original: Option<std::ffi::OsString>,
}

impl HomeGuard {
    fn set_to(path: &std::path::Path) -> Self {
        let original = std::env::var_os("HOME");
        set_env("HOME", path);
        Self { original }
    }
}

impl Drop for HomeGuard {
    fn drop(&mut self) {
        match self.original.take() {
            Some(value) => set_env("HOME", value),
            None => remove_env("HOME"),
        }
    }
}

fn make_controls() -> (RunControls, mpsc::Sender<SteerMessage>, CancellationToken) {
    let (steer_tx, steer_rx) = mpsc::channel(8);
    let interrupt = CancellationToken::new();
    let controls = RunControls {
        request_input: Box::new(|_| {
            let (_tx, rx) = oneshot::channel();
            rx
        }),
        steering: steer_rx,
        interrupt: interrupt.clone(),
    };
    (controls, steer_tx, interrupt)
}

async fn run_to_end(
    harness: &AsideHarness,
    request: RunRequest,
    controls: RunControls,
) -> Vec<AgentEvent> {
    let stream = harness.run(request, controls).await.expect("run starts");
    tokio::time::timeout(
        Duration::from_secs(5),
        stream
            .map(|event| event.expect("stream event"))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("run finishes")
}

#[tokio::test]
async fn fake_cli_covers_detection_arguments_result_error_resume_steer_and_interrupt() {
    let _home_guard_lock = home_lock();
    // Isolate dynamic catalog discovery: with an empty HOME, `aside account
    // list` (via the fake executable below) yields no signed-in accounts, so
    // the static default/fast fallback engages deterministically even on
    // machines with a real ~/.aside catalog.
    let empty_home = tempfile::tempdir().unwrap();
    let _home_guard = HomeGuard::set_to(empty_home.path());
    let fixture = fixture_path();
    let temp = tempfile::tempdir().unwrap();
    let log = temp.path().join("calls.log");
    set_env("ASIDE_FAKE_LOG", &log);

    // Detection honors the explicit supported override and rejects a missing
    // override without spawning a process.
    set_env("ASIDE_EXECUTABLE", &fixture);
    assert_eq!(
        zeron_harness::aside::resolve_aside_executable(),
        Some(fixture.clone())
    );
    let missing = temp.path().join("missing-aside");
    assert!(!AsideHarness::new().with_executable(missing).installed());
    remove_env("ASIDE_EXECUTABLE");

    let harness = AsideHarness::new().with_executable(fixture.clone());
    assert_eq!(harness.id(), HarnessId::Aside);
    assert!(harness.supports_steering());
    assert!(harness.deterministic_turn_end());
    let models = harness.models().await.unwrap();
    assert_eq!(
        models
            .iter()
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>(),
        vec!["default", "fast"]
    );

    set_env("ASIDE_FAKE_SCENARIO", "happy");
    let mut happy = request("open the page");
    happy.reasoning = Some(zeron_proto::ReasoningLevel::High);
    happy
        .model_options
        .insert("permission".into(), "full-access".into());
    happy
        .model_options
        .insert("provider".into(), "openai".into());
    happy.model_options.insert("host".into(), "local".into());
    let (controls, _steer, _interrupt) = make_controls();
    let events = run_to_end(&harness, happy, controls).await;
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::TextDelta { text } if text == "finished"))
    );
    assert!(events.iter().any(|event| matches!(event, AgentEvent::Done { status: DoneStatus::Completed, session_id: Some(id), .. } if id == "ses-happy")));

    set_env("ASIDE_FAKE_SCENARIO", "error");
    let (error_controls, _steer, _interrupt) = make_controls();
    let events = run_to_end(&harness, request("fail"), error_controls).await;
    assert!(events.iter().any(|event| matches!(event, AgentEvent::Done { status: DoneStatus::Errored, error: Some(error), .. } if error == "agent failed")));

    set_env("ASIDE_FAKE_SCENARIO", "resume");
    let mut follow_up = request("continue");
    follow_up.resume = Some("ses-happy".into());
    let (resume_controls, _steer, _interrupt) = make_controls();
    let events = run_to_end(&harness, follow_up, resume_controls).await;
    assert!(events.iter().any(|event| matches!(event, AgentEvent::Done { status: DoneStatus::Completed, session_id: Some(id), .. } if id == "ses-resumed")));

    set_env("ASIDE_FAKE_SCENARIO", "hold");
    let mut live = request("keep working");
    live.resume = Some("ses-live".into());
    let (live_controls, steer, interrupt) = make_controls();
    let stream = harness
        .with_graces(Duration::from_millis(50), Duration::from_millis(50))
        .run(live, live_controls)
        .await
        .unwrap();
    steer
        .send(SteerMessage {
            prompt: "change direction".into(),
            message_id: Some("steer-1".into()),
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    interrupt.cancel();
    let events = tokio::time::timeout(
        Duration::from_secs(5),
        stream.map(|event| event.unwrap()).collect::<Vec<_>>(),
    )
    .await
    .expect("interrupted run finishes");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::Steered { .. }))
    );
    assert!(events.iter().any(|event| matches!(event, AgentEvent::Done { status: DoneStatus::Interrupted, session_id: Some(id), .. } if id == "ses-live")));

    let calls = std::fs::read_to_string(log).unwrap();
    assert!(
        calls.contains(
            "--speed fast --effort high --permission full-access --provider openai --host local mcp"
        ),
        "{calls}"
    );
    assert!(calls.contains("--speed fast mcp"), "{calls}");
    assert!(calls.contains(r#"session_id":"ses-happy"#), "{calls}");
    assert!(
        calls.contains("session steer ses-live change direction"),
        "{calls}"
    );
    assert!(calls.contains("session stop ses-live"), "{calls}");

    remove_env("ASIDE_FAKE_LOG");
    remove_env("ASIDE_FAKE_SCENARIO");
}

#[test]
fn account_list_parser_filters_signed_out_rows() {
    let text = "* u0  redacted  signed in  profiles: Profile 0\nprovider: redacted\nu1  redacted  signed out\n";
    assert_eq!(
        zeron_harness::aside::parse_account_list(text),
        vec![("u0".to_owned(), true), ("u1".to_owned(), false)]
    );
    // Signed-in filtering is the caller's job; the parser preserves both.
    let signed_in: Vec<_> = zeron_harness::aside::parse_account_list(text)
        .into_iter()
        .filter(|(_, signed_in)| *signed_in)
        .collect();
    assert_eq!(signed_in, vec![("u0".to_owned(), true)]);
}

#[test]
fn catalog_parser_ignores_secret_adjacent_fields() {
    let value = serde_json::json!({
        "providers": [
            {
                "id": "fake-alpha",
                "apiKey": "FAKE-SECRET-DO-NOT-USE",
                "authHeader": "FAKE-SECRET-DO-NOT-USE",
                "baseUrl": "https://fake.example.invalid",
                "models": [{"id": "fake-model-a"}, {"id": "fake-model-b"}]
            },
            {
                "name": "fake-beta",
                "apiKey": "FAKE-SECRET-DO-NOT-USE",
                "models": [{"modelId": "fake-model-1"}]
            }
        ]
    });
    let catalog = zeron_harness::aside::parse_models_catalog(&value);
    assert_eq!(catalog.len(), 2);
    assert_eq!(catalog[0].0, "fake-alpha");
    assert_eq!(catalog[0].1.len(), 2);
    // Secrets never leak into model ids.
    for (_, ids) in &catalog {
        for id in ids {
            assert!(!id.contains("FAKE-SECRET"));
            assert!(!id.contains("fake.example"));
        }
    }
}

#[tokio::test]
async fn dynamic_catalog_imports_models_orders_default_and_routes_account() {
    let _lock = home_lock();
    let home = tempfile::tempdir().unwrap();
    let account_dir = home.path().join(".aside").join("u").join("0");
    std::fs::create_dir_all(&account_dir).unwrap();
    // Small fake fixture: 2 providers x 2 models, plus secret-adjacent fields
    // with fake values to prove they are ignored. No real user data.
    std::fs::write(
        account_dir.join("models.json"),
        serde_json::json!({
            "providers": [
                {
                    "id": "fake-alpha",
                    "apiKey": "FAKE-SECRET-DO-NOT-USE",
                    "authHeader": "FAKE-SECRET-DO-NOT-USE",
                    "baseUrl": "https://fake.example.invalid",
                    "models": [{"id": "fake-model-b"}, {"id": "fake-model-a"}]
                },
                {
                    "id": "fake-beta",
                    "apiKey": "FAKE-SECRET-DO-NOT-USE",
                    "models": [{"id": "fake-model-2"}, {"id": "fake-model-1"}]
                }
            ]
        })
        .to_string(),
    )
    .unwrap();
    // defaultModel promotes its row to index 2 (right after default/fast).
    std::fs::write(
        account_dir.join("settings.json"),
        serde_json::json!({
            "defaultModel": {
                "provider": "fake-beta",
                "modelId": "fake-model-2",
                "thinkingLevel": "whatever",
                "fastMode": false
            },
            "modelCategories": {"deep": [], "fast": [], "standard": [], "visual": []},
            "customModels": []
        })
        .to_string(),
    )
    .unwrap();

    // Fake `aside` that answers `account list` with one signed-in account.
    let bin = tempfile::tempdir().unwrap();
    let fake = bin.path().join("fake-aside-accounts.sh");
    std::fs::write(
        &fake,
        "#!/bin/sh\nset -eu\ncase \" $* \" in\n*\" account list \"*)\nprintf '%s\\n' '* u0  redacted  signed in  profiles: Profile 0'\nprintf '%s\\n' 'provider: redacted'\nexit 0\n;;\nesac\nexit 0\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755));
    }

    let _home_guard = HomeGuard::set_to(home.path());
    let harness = AsideHarness::new().with_executable(fake.clone());
    let models = harness.models().await.unwrap();
    assert_eq!(models.len(), 2 + 4, "default/fast + 2 providers x 2 models");
    assert_eq!(models[0].id.as_str(), "default");
    assert_eq!(models[1].id.as_str(), "fast");
    // Promotion: defaultModel row moves to index 2.
    assert_eq!(models[2].id.as_str(), "fake-beta/fake-model-2");
    // Remaining discovered rows sorted by (provider, model).
    let rest: Vec<_> = models[3..].iter().map(|m| m.id.as_str()).collect();
    assert_eq!(
        rest,
        vec![
            "fake-alpha/fake-model-a",
            "fake-alpha/fake-model-b",
            "fake-beta/fake-model-1",
        ]
    );
    // Discovered rows carry provider-via description + full ladder + options.
    for model in &models[2..] {
        assert_eq!(model.label.as_str(), model.id.split('/').nth(1).unwrap());
        assert!(model.description.as_deref().unwrap().ends_with(" via Aside"));
        assert!(model.reasoning_levels.contains(&zeron_proto::ReasoningLevel::Max));
        assert!(model.options.iter().any(|o| o.id == "effort"));
        assert!(model.options.iter().any(|o| o.id == "permission"));
    }
    // Account routing: every row (static + discovered) has the option.
    for model in &models {
        let option = model
            .options
            .iter()
            .find(|o| o.id == "account")
            .expect("account option present");
        assert_eq!(
            option.choices.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
            vec!["u0"]
        );
        assert_eq!(option.default_choice.as_str(), "u0");
    }
}
