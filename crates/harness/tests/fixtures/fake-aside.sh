#!/bin/sh
set -eu

if [ -n "${ASIDE_FAKE_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$ASIDE_FAKE_LOG"
fi

case " $* " in
  *" session steer "*) exit 0 ;;
  *" session stop "*) exit 0 ;;
esac

case " $* " in
  *" mcp ") ;;
  *) exit 0 ;;
esac

# Mirrors the real `aside mcp` exec result shape: the session id is EMBEDDED
# as the first line of content[0].text (`session_id: <id>`, blank line, reply
# body). There is no separate structured session-id field. All ids fake.
scenario=${ASIDE_FAKE_SCENARIO:-happy}
while IFS= read -r line; do
  if [ -n "${ASIDE_FAKE_LOG:-}" ]; then
    printf 'stdin %s\n' "$line" >> "$ASIDE_FAKE_LOG"
  fi
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26","capabilities":{},"serverInfo":{"name":"aside","version":"fake"}}}'
      ;;
    *'"method":"tools/call"'*)
      case "$scenario" in
        happy)
          printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"session_id: ses-happy\n\nfinished"}],"isError":false}}'
          exit 0
          ;;
        resume)
          printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"session_id: ses-resumed\n\nfollow-up complete"}],"isError":false}}'
          exit 0
          ;;
        error)
          printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"isError":true,"content":[{"type":"text","text":"session_id: ses-error\n\nagent failed"}]}}'
          exit 0
          ;;
        hold)
          while :; do sleep 1; done
          ;;
      esac
      ;;
  esac
done
