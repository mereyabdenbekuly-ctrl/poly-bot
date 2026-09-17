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
