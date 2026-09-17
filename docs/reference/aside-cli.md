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

Zeron starts the public `aside mcp` command and speaks newline-delimited MCP
JSON-RPC over stdin/stdout. It does not use private app, daemon, or browser
protocols.

The adapter supports:

- start a task through the MCP `exec` tool;
- resume by passing the existing Aside `session_id` to `exec`;
- steer with `aside session steer <session-id> <prompt>`; the documented command interrupts the current step and replaces it.
- stop with `aside session stop <session-id>`;
- `default` and `fast` routing;
- documented effort, permission, provider, and host flags when an explicit
  value is already present in the run configuration.

Aside's CLI also exposes queue, archive, delete, account, host, and update
commands. Queue, archive, and delete remain CLI-only in this adapter unless a
future wire integration gives them durable Zeron lifecycle semantics.

Aside MCP is completion-oriented. It returns the completed tool result rather
than a stable stream of intermediate agent or tool events. Zeron therefore
emits the completed text result and terminal status only; it does not promise
streaming tool events, token deltas, or live progress from Aside.

## Models and options

Aside does not expose a stable public model-catalog API for this integration.
The Zeron catalog is intentionally static and contains two routing entries:

| ID | Label | Meaning |
| --- | --- | --- |
| `default` | Default | Aside's normal model routing |
| `fast` | Fast | Aside's faster routing, which may use extra credit |

Both entries advertise the Zeron picker ladder from `off` through `max`. The complete CLI effort choices are:

```text
off, minimal, low, medium, high, xhigh, max, ultrabrowse
```

The `ultrabrowse` choice is represented through the explicit `effort` option,
while the shared reasoning ladder includes `off` through `max`. The `effort`
option also includes `default`, with `default` as its default choice.

The `permission` option uses `ask`, `guard`, and `full-access`, with `guard` as
the default. Provider names and host names are not static picker choices:
they are account- and device-specific strings, so the iOS catalog does not
pretend to enumerate them.

The CLI still accepts explicit model/provider selections such as
`--model openai/gpt-5.6-sol` and `--provider openai`. Zeron does not validate
those values against a dynamic catalog, and built-in Aside models require an
active Aside sign-in. Configure provider credentials in Aside rather than in
Zeron.

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
  CLI installation, `exec`, MCP, accounts, host selection, and REPL usage.
- [Aside AI providers](https://docs.aside.com/help/ai) documents built-in,
  subscription, and API-key provider categories.
- The live CLI's `aside exec --help` is authoritative for the installed
  version's flags and choices.

When the CLI, app, daemon, or keychain is unavailable, Zeron reports Aside as
not installed instead of trying to bypass those dependencies.
