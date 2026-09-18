# Aside CLI integration

Zeron's Aside harness is a macOS-first adapter for the official `aside` CLI.
It runs Aside on the selected Zeron execution host; iOS is a remote control and
catalog mirror, not an Aside runtime.

## Requirements

- Install Aside Browser from [aside.com/download](https://aside.com/download).
- Use macOS 15 or newer for the supported macOS-first setup.
- Open Aside, sign in, and leave the Aside daemon available.
- Keep macOS Keychain access available. Aside's account, built-in model, and
  provider behavior depends on the app's credential storage; Zeron does not read
  or replace those credentials.
- Install the official CLI, then verify it with `aside --version`.

The app and CLI are separate components, but the CLI is not a standalone
replacement for Aside's browser, daemon, or keychain-backed account state.

## Official CLI installation and paths

The official macOS installer is:

```sh
curl -fsSL https://releases.aside.com/install.sh | bash
```

The installer places the CLI app at:

```text
~/.aside/cli/Aside CLI.app/Contents/MacOS/aside
```

and creates the command link at:

```text
~/.local/bin/aside
```

The adapter searches `ASIDE_EXECUTABLE` first, then `PATH` and the login-shell
PATH, the installer paths above, and the packaged browser fallback:

```text
/Applications/Aside.app/Contents/MacOS/Aside
```

Set `ASIDE_EXECUTABLE` when the CLI is installed somewhere else or when testing
with a fixture:

```sh
ASIDE_EXECUTABLE="$HOME/.local/bin/aside" zeron
```

The override must name an executable file. It does not change Aside's account
or credential storage.

## Transport and lifecycle

Zeron spawns the public `aside` CLI directly for each turn (native CLI
transport — the MCP server is no longer used). It does not use private app,
daemon, or browser protocols.

The adapter supports:

- start a task with `aside <routing flags> exec <prompt>` (first turn);
  model routing (`-m`/`--speed`/`--effort`/`--permission`/`--provider`/
  `--host`/`--account`) is real here, applied at session creation;
- resume with `aside [--account] session resume <session-id> <prompt>`;
  resume accepts only `--account` — the session keeps its original model,
  so no model/effort flags are sent;
- steer with `aside session steer <session-id> <prompt>`; the documented command interrupts the current step and replaces it.
- stop with `aside session stop <session-id>`;
- `default` and `fast` routing;
- documented effort, permission, provider, and host flags when an explicit
  value is already present in the run configuration.

Aside's CLI also exposes queue, archive, delete, account, host, and update
commands. Queue, archive, and delete remain CLI-only in this adapter unless a
future wire integration gives them durable Zeron lifecycle semantics.

Aside's CLI runs each turn to completion. It returns the completed reply
rather than a stable stream of intermediate agent or tool events. Zeron
therefore emits the completed text result and terminal status only; it does
not promise streaming tool events, token deltas, or live progress from Aside.

Stdout is the reply text only. The session id arrives on stderr as
`created new session: <id>` (first turn) or
`continuing existing session: <id>` (resume), possibly wrapped in ANSI
escapes alongside other log lines. Zeron strips ANSI escapes, parses the
LAST such line, emits `SessionStarted` first so the engine records the
resume id, and reports a loud error (never a silent new session) when
neither a parsed id nor a resumed session exists. A non-zero exit is a loud
terminal error carrying the stderr tail.

The session id is only known once the turn's child process exits.
Consequently, in-turn steering is available for a resumed or previously
persisted Aside session; the first-ever turn cannot be steered mid-flight
and reports that limitation instead of guessing a session.

## Models and options

The Zeron catalog is dynamically imported from the user's real Aside config
on every `models()` call (Codex-style live discovery, never cached), with a
static `default`/`fast` fallback when discovery yields nothing:

| ID | Label | Meaning |
| --- | --- | --- |
| `default` | Default | Aside's normal model routing |
| `fast` | Fast | Aside's faster routing, which may use extra credit |
| `<provider>/<model>` | `<model>` | Discovered row, described as `<provider> via Aside` |

Sources (read-only, no writes, no network):

- `aside account list` (10s timeout) for signed-in account ids only. There is
  no `--json` flag; only the account id (`u` + digits) and the signed-in
  boolean are parsed from each line — emails are redacted/ignored. Failure
  (timeout, spawn error, non-zero exit) falls back to the static catalog with
  a debug log; failures are never cached.
- `~/.aside/u/{digits}/models.json` per signed-in account (`u0` -> `0`;
  unparseable ids skipped). Only the top-level `providers` array (or map) is
  read: provider `name`/`id` plus each model's id-ish keys
  (`id`/`modelId`/`model`/`name`). Secret-adjacent fields (`apiKey`,
  `authHeader`, `baseUrl`, …) are never read and file contents are never
  logged. Missing/unparseable files skip that account with a debug log.
- `~/.aside/u/{digits}/settings.json` `defaultModel { provider, modelId }`
  (non-secret) is used ONLY to order the catalog: when it matches a
  discovered row, that row moves to index 2 (right after `default`/`fast`).
  No other semantics are inferred (`thinkingLevel`/`fastMode` ignored).

Discovered rows are sorted by `(provider, model)`, deduplicated by
`provider/model` id, and advertise no reasoning ladder (empty, like the
static rows) with the same `effort` + `permission` options as the static
rows. Remote hosts are 403-blocked except `local`; no host discovery is
performed — the existing host passthrough is unchanged.

Account routing: every row (static + discovered) gains an `account` picker
option ONLY when at least one signed-in account is discovered. Its choices
are the signed-in ids with the first as default; with zero discovered the
option is omitted entirely (as before). At spawn, `model_options["account"]`
is forwarded as `--account <id>`; membership is validated against that call's
freshly discovered ids and unknown values are skipped with a debug log —
never a hard error. When discovery fails, the value forwards unvalidated
rather than breaking the run.

Model routing for discovered rows: when `request.model` contains `/`, the
first-turn spawn passes `-m <provider/model>` (slash form overrides
`--provider`, so `-p` is not also sent). Resume turns never send `-m`
(or `--effort`/`--speed`): the session keeps its original model.
`default`/`fast`/other behavior is unchanged
(`--speed fast`, `--model <id>`, `--effort`, `--permission`, `--provider`,
`--host`, `--account`).

The iOS peer's fallback mirrors this catalog: `default`/`fast` with no
reasoning ladder; the wire delivers the live catalog there.

No entry advertises a Reasoning ladder, so the picker's Reasoning row stays
hidden for Aside. Thinking strength is driven by the single explicit `effort`
option (a stored Reasoning level from an older chat is ignored, never an
error). The complete CLI effort choices are:

```text
off, minimal, low, medium, high, xhigh, max, ultrabrowse
```

The `ultrabrowse` choice is representable only through the `effort` option
(it has no ReasoningLevel equivalent), which is why `effort` is the surviving
knob. The `effort` option also includes `default`, with `default` as its
default choice; `default` omits `--effort` so Aside decides.

The `permission` option uses `ask`, `guard`, and `full-access`, with `guard` as
the default. Host names are not picker choices: they are device-specific
strings passed through untouched. Provider/model combinations appear as
discovered `<provider>/<model>` rows, and signed-in account ids appear as an
`account` picker option whenever discovery finds at least one.

The CLI still accepts explicit model/provider selections such as
`-m fake-provider/fake-model` and `--provider fake-provider`. Discovered `provider/model`
rows route via `-m`; the `account` choice is validated against the run's fresh
discovery (unknown values skipped, never a hard error). Built-in Aside models
require an active Aside sign-in. Configure provider credentials in Aside
rather than in Zeron.

## Remote hosts

Aside's CLI accepts `--host local`, a remote host ID, or a host name. The
adapter forwards an explicit host value when one is supplied. Separately,
Zeron's iOS host picker selects the desktop device that owns the run and its
workspace; that device must have the Aside app/daemon and CLI available.

Remote execution is therefore supported only when both host layers are
configured: the Zeron execution device must be reachable, and Aside must know
the requested local or remote session host. A catalog lookup does not discover
or validate remote host names.

## Source boundaries

- [Aside developer tools](https://docs.aside.com/help/developers) documents
  CLI installation, `exec`, session resume, accounts, host selection, and
  REPL usage.
- [Aside AI providers](https://docs.aside.com/help/ai) documents built-in,
  subscription, and API-key provider categories.
- The live CLI's `aside exec --help` is authoritative for the installed
  version's flags and choices.

When the CLI, app, daemon, or keychain is unavailable, Zeron reports Aside as
not installed instead of trying to bypass those dependencies.
