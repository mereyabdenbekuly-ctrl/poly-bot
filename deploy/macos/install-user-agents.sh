#!/bin/zsh
set -euo pipefail

readonly project="/Users/admin/Documents/ChatGPT/poly-bot"
readonly destination="$HOME/Library/LaunchAgents"
readonly domain="gui/$(id -u)"

mkdir -p "$destination" "$project/data"

for label in com.polybot.observer com.polybot.dashboard; do
  source_plist="$project/deploy/macos/$label.plist"
  destination_plist="$destination/$label.plist"
  plutil -lint "$source_plist"
  launchctl bootout "$domain/$label" >/dev/null 2>&1 || true
  sleep 2
  install -m 600 "$source_plist" "$destination_plist"
  for attempt in 1 2 3 4 5; do
    if launchctl bootstrap "$domain" "$destination_plist"; then
      break
    fi
    [[ "$attempt" == 5 ]] && exit 1
    sleep 2
  done
done

echo "Polybot LaunchAgents installed for user $(id -un)."
echo "Dashboard: http://127.0.0.1:8787"
