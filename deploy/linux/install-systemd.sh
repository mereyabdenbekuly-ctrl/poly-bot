#!/bin/sh
set -eu

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root." >&2
  exit 1
fi

project=/opt/polybot
state=/var/lib/polybot
config=/etc/polybot

if ! getent group polybot >/dev/null 2>&1; then
  groupadd --system polybot
fi

if ! getent passwd polybot >/dev/null 2>&1; then
  useradd --system --gid polybot --home-dir "$state" --shell /usr/sbin/nologin polybot
fi

if [ -e "$project/.env" ] || [ -L "$project/.env" ]; then
  echo "Refusing deployment while $project/.env exists; use $config/polybot.env only." >&2
  exit 1
fi

if [ ! -x "$project/.venv/bin/polybot" ] \
  || ! runuser -u polybot -- "$project/.venv/bin/polybot" --help >/dev/null 2>&1; then
  echo "Missing $project/.venv/bin/polybot; install the locked environment first." >&2
  exit 1
fi

if [ ! -f "$config/polybot.env" ]; then
  echo "Missing $config/polybot.env." >&2
  exit 1
fi

install -d -o polybot -g polybot -m 0750 \
  "$state" \
  "$state/backups" \
  "$state/forecasts/ecmwf-ifs025-json" \
  "$state/forecasts/ecmwf-open-data"
install -d -o root -g polybot -m 0750 "$config"

for unit in \
  polybot-observer.service \
  polybot-dashboard.service \
  polybot-backup.service \
  polybot-backup.timer \
  polybot-ecmwf-archive.service \
  polybot-ecmwf-archive.timer
do
  install -o root -g root -m 0644 "$project/deploy/linux/$unit" "/etc/systemd/system/$unit"
done

systemctl daemon-reload
echo "Units installed. Verify the restored state in $state, then enable/start"
echo "the observer, dashboard, and backup timer explicitly."
