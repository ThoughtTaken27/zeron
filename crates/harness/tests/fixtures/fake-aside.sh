#!/bin/sh
set -eu

if [ -n "${ASIDE_FAKE_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$ASIDE_FAKE_LOG"
fi

case " $* " in
  *" session steer "*) exit 0 ;;
  *" session stop "*) exit 0 ;;
esac

# Account discovery: no signed-in accounts, so the static default/fast
# fallback engages deterministically.
case " $* " in
  *" account list "*) exit 0 ;;
esac

# Native CLI shape: stdout is the reply text only; stderr carries the session
# line (`created new session: <id>` / `continuing existing session: <id>`).
# All ids fake.
scenario=${ASIDE_FAKE_SCENARIO:-happy}

case " $* " in
  *" session resume "*)
    case "$scenario" in
      hold)
        while :; do sleep 1; done
        ;;
      error)
        printf '%s\n' "agent failed" >&2
        exit 1
        ;;
      *)
        printf '%s\n' "follow-up complete"
        printf '%s\n' "continuing existing session: ses-resumed" >&2
        exit 0
        ;;
    esac
    ;;
esac

case " $* " in
  *" exec "*)
    case "$scenario" in
      hold)
        while :; do sleep 1; done
        ;;
      error)
        printf '%s\n' "agent failed" >&2
        exit 1
        ;;
      ansi)
        printf '\033[32mfinished\033[0m\n'
        printf '\033[2mcreated new session: ses-ansi\033[0m\n' >&2
        exit 0
        ;;
      *)
        printf '%s\n' "finished"
        printf '%s\n' "created new session: ses-happy" >&2
        exit 0
        ;;
    esac
    ;;
esac

exit 0
