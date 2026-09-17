# One-shot live pilot

This is a manually invoked, approval-gated executor. It is not connected to the
observer, a timer, or systemd. WeatherNext and shadow research cannot trigger it.

## Fixed limits

- one `BUY` intent for the lifetime of the pilot database;
- `FOK` only;
- BUY notional at most `$1.90`;
- conservative all-in authorization at most `$2.00`;
- collateral balance at most `$10.00`;
- no builder fee and no automatic token/collateral approval;
- the first prepared intent permanently reserves the one-shot slot, including
  after a restart, rejection, timeout, or an attempt to use a different intent.

## Supported account authorization

Use the official `polymarket-client` secure client. For this small pilot, the
operational default is the signer of a **dedicated pilot account/wallet** whose
collateral never exceeds `$10`. Do not use a primary wallet with unrelated
assets.

`direct_signer` supports an EOA and an existing Polymarket proxy/safe/deposit
wallet. `session_key` is also accepted, but only when that session signer has
already been authorized for an existing Deposit Wallet. Provisioning derives or
creates CLOB API credentials, but it never transfers funds, changes allowances,
signs an order, or posts an order.

Create the protected directory and provision through a hidden TTY prompt on the
VPS. Never put a private key or API key in chat, shell arguments, environment
variables, git, or command history.

```bash
sudo install -d -o polybot -g polybot -m 0700 /var/lib/polybot/live
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-pilot provision \
  --auth-mode direct_signer
```

The command prompts for the public account wallet address and, without echo,
for the signer private key. When `--relayer-address` is supplied, it also
prompts without echo for the Relayer API key. The resulting file is
`/var/lib/polybot/live/credentials.json`, owned by `polybot`, mode `0600`.
For an EOA that does not use a Relayer key, omit `--relayer-address`.

For a previously authorized Deposit Wallet session signer:

```bash
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-pilot provision \
  --auth-mode session_key \
  --wallet 0xDEPOSIT_WALLET
```

## Read-only verification

```bash
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-pilot account-check --json
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-pilot status --json
```

The account check authenticates but performs no signing, approval, transfer, or
order POST. It reports the wallet/signer addresses, wallet type, collateral
balance and allowances, counts of open orders/positions and first-page trades,
closed-only state, geoblock result, and the persistent one-shot gate.

## Preview and prepare

Only a fresh, non-superseded `v1` candidate with the exact still-current book
hash can be mirrored. This is either `PAPER_BUY`, or `OBSERVE` when its sole
reason is `ACTIVE_PAPER_EVENT_MONITOR_ONLY`: that state is the same computed v1
buy candidate downgraded only to prevent a duplicate **virtual** paper position.
Every other `OBSERVE`/`SKIP` remains ineligible. A preview is unsigned,
non-binding, and does not reserve the slot:

```bash
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-pilot preview \
  --decision-id DECISION_ID --json
```

Preparation repeats the authenticated read-only checks and writes an unsigned
intent. It does not reserve the one-shot slot, sign, or submit an order. The
slot becomes permanent only when `execute` starts:

```bash
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-pilot prepare \
  --decision-id DECISION_ID --json
```

The `execute` command exists only for a later separately approved exact intent.
It requires a private `0600` approval sidecar bound to the intent SHA-256. Do not
run it until the owner explicitly approves that exact prepared intent.

## Opt-in autonomous one-shot

`live-pilot auto-once` is a standing-authorization wrapper around the same
executor. It waits only for a **new** fresh eligible v1 candidate, prepares an
exact intent, rechecks geoblock/account/book/fee state, and submits at most one
FOK BUY. It does not remove any pilot limit: the wallet cap remains `$10`, the
BUY notional cap remains `$1.90`, all-in spend remains `$2.00`, and the
persistent one-shot gate is consumed before POST. A rejection, timeout, or
ambiguous response also consumes the one-shot slot and is never retried.

The command requires a private `0600` standing sidecar with this exact schema:

```json
{
  "kind": "polybot-live-auto-once-v1",
  "wallet": "0xDEPOSIT_WALLET",
  "strategy": "open-meteo-truncated-normal-v1",
  "side": "BUY",
  "order_type": "FOK",
  "max_orders": 1,
  "max_wallet_balance_usd": "10.00",
  "max_buy_notional_usd": "1.90",
  "max_total_spend_usd": "2.00",
  "min_probability_edge": "0.08",
  "min_expected_profit_usd": "0.25",
  "jurisdiction_confirmed": true,
  "expires_at_utc": "2026-09-18T12:00:00+00:00"
}
```

The sidecar is bound to the configured Deposit Wallet session key and is moved
to a `.used-*` path as soon as the one-shot gate is consumed. The optional
`polybot-live-auto-once.service` waits in the background but is never enabled by
the installation script; an operator must create the sidecar and start it
explicitly.

## Live-v2: bounded daily loop

`polybot live-v2` is separate from the legacy `live-pilot` one-shot. It shares
the provisioned credential file, but uses its own standing authorization and
`live_v2_attempts` journal; it does not reset or extend the legacy one-shot
gate.

Its risk envelope is fixed:

- only `FOK BUY` orders from fresh, authorized `v1` candidates;
- BUY notional at most `$1.90` and signed all-in spend at most `$2.00`;
- one reserved order attempt per `Asia/Almaty` local day. Reservation happens
  before signing/POST, so one attempt with a `$2.00` maximum spend
  conservatively bounds new daily risk to `$2.00`. A position closure consumes
  that local day's slot too, and any other wallet trade observed that day blocks
  a new attempt;
- one position at a time: any open order or open position makes the loop wait;
- dedicated-wallet collateral must be greater than zero and at most `$10.00`;
- an ambiguous submission is never retried automatically. Reconciliation that
  cannot prove a position or close marks `MANUAL_REVIEW`, and no new order is
  allowed while any active/manual-review attempt remains unresolved.

The loop requires an authorized Deposit Wallet `session_key` plus a private
mode-`0600` `/var/lib/polybot/live/live-v2-authorization.json`. The sidecar has
the exact fixed schema below; replace the wallet and timestamps, and choose the
two non-negative signal thresholds explicitly:

```json
{
  "kind": "polybot-live-v2-authorization-v1",
  "wallet": "0xDEPOSIT_WALLET",
  "strategy": "open-meteo-truncated-normal-v1",
  "side": "BUY",
  "order_type": "FOK",
  "one_position_at_a_time": true,
  "max_orders_per_day": 1,
  "daily_stop_loss_usd": "2.00",
  "daily_timezone": "Asia/Almaty",
  "max_wallet_balance_usd": "10.00",
  "max_buy_notional_usd": "1.90",
  "max_total_spend_usd": "2.00",
  "min_probability_edge": "0.08",
  "min_expected_profit_usd": "0.25",
  "jurisdiction_confirmed": true,
  "authorized_at_utc": "<RFC3339 UTC timestamp>",
  "expires_at_utc": "<later RFC3339 UTC timestamp>"
}
```

Candidates older than `authorized_at_utc` are ignored. The authorization is
re-read on every polling iteration, and an iteration at or after
`expires_at_utc` fails closed before considering a new order.

Inspect the journal or run one foreground wait/attempt with:

```bash
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-v2 status \
  --database /var/lib/polybot/polybot.sqlite3 --json
sudo -u polybot -H /opt/polybot/.venv/bin/polybot live-v2 run \
  --database /var/lib/polybot/polybot.sqlite3 \
  --credentials /var/lib/polybot/live/credentials.json \
  --authorization /var/lib/polybot/live/live-v2-authorization.json \
  --json
```

`deploy/linux/install-systemd.sh` installs but does not enable the opt-in
`polybot-live-v2.service`. After the database, credentials, and unexpired
authorization exist, manage it explicitly:

```bash
systemctl enable --now polybot-live-v2.service
systemctl status polybot-live-v2.service
journalctl -u polybot-live-v2.service -f
systemctl stop polybot-live-v2.service
```
