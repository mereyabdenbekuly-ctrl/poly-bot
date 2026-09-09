#!/bin/zsh
set -euo pipefail

readonly project="/Users/admin/Documents/ChatGPT/poly-bot"

# LaunchAgents inherit the user's GUI environment. Start Polybot from a clean
# environment so unrelated desktop/API credentials are not visible to it.
exec /usr/bin/env -i \
  HOME="/Users/admin" \
  PATH="$project/.venv/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" \
  PYTHONUNBUFFERED="1" \
  POLYBOT_PAPER_MAX_EVENTS="8" \
  TMPDIR="${TMPDIR:-/tmp}" \
  "$project/.venv/bin/polybot" run \
    --interval 300 \
    --paper
