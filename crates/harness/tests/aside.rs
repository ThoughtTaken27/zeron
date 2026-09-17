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
