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
          printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"structuredContent":{"session_id":"ses-happy","result":"finished"},"content":[{"type":"text","text":"finished"}]}}'
          exit 0
          ;;
        resume)
          printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"structuredContent":{"sessionId":"ses-resumed","result":"follow-up complete"}}}'
          exit 0
          ;;
        error)
          printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"isError":true,"content":[{"type":"text","text":"agent failed"}]}}'
          exit 0
          ;;
        hold)
          while :; do sleep 1; done
          ;;
      esac
      ;;
  esac
done
