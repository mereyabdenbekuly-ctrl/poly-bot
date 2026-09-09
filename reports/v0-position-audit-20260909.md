# Paper strategy v0 — immutable position audit

Audit date: **2026-09-09**. Frozen code tag: `paper-strategy-v0-20260909`
(`ff7d0dc1cff89c7d6075d9391e49b60dfa76e95c`). The pre-migration SQLite
snapshot is kept outside Git in `data/archive/` with a SHA-256 sidecar.

This report does not rewrite either signal with information added by v1.

## Identity and execution findings

Both v0 positions use the expected **YES** token. No YES/NO mix-up was found.
Across all post-entry snapshots, each position retained one asset ID and one
condition ID, with zero decision/snapshot mismatches.

| Event | Market | Outcome | Condition ID | YES token ID |
| --- | --- | --- | --- | --- |
| Chengdu 2026-09-09 | `4321968` | 24°C YES | `0xf25fb2c6a025dd1e9a03a35e6ea35443dd8f452654888599bfbb8ec27d47e3e3` | `23203752742729145861472334299435722685423796939365263117705956806788582421941` |
| Munich 2026-09-09 | `4321819` | 22°C YES | `0xcf856b1bd7b29d43df984e6bb1cbd1114095a74b0ddd22cf0ae73acc8bf7d288` | `92823148118057436349823021056130062265309983499485861461927331452990010714040` |

Opening liquidity was sufficient for the simulated crossing-limit size of five
shares:

- Chengdu: best ask `0.001`, available size `1832`;
- Munich: best ask `0.11`, available size `56.22`.

The v0 simulator used the SDK order-book minimum as a **limit-order share size**.
This must not be confused with the `amount` argument of a BUY market order,
which is a currency spend.

## Position-specific findings

### Chengdu 24°C

The entry was executable in the YES book, but it was logically invalid under
the v1 observation rule. The official station is ZUUU. An observation of 27°C
for the September 9 station-local day was already available before the paper
entry, so the final daily maximum could no longer be 24°C.

Current exit state at audit: no YES bids, therefore:

```text
immediately_sellable_shares = 0
full_exit_value             = unavailable
realized_pnl                = 0 (still unresolved)
```

The position remains a v0 historical record and follows the preselected
hold-to-official-resolution policy.

### Munich 22°C

The entry was made before the September 9 station-local day began. It was the
same YES token later marked at a best bid near `0.90`; the apparent gain was not
caused by using the NO token.

At the audited `0.90` bid, five shares were fully sellable:

```text
gross proceeds       = 5 × 0.90 = 4.50
estimated exit fee   = 0.0225
entry cash cost      = 0.55 + 0.024475 = 0.574475
hypothetical exit P&L = 4.50 - 0.0225 - 0.574475 = +3.903025
```

This is a hypothetical same-token bid-side mark, not realized paper P&L. The
old `$0.02` execution buffer is a risk allowance, not a cash expense, and must
not be subtracted from realized settlement accounting.

## Model/API accounting

Lifetime Astra usage at audit was `$0.09781`:

```text
Hong Kong + London earlier checks = $0.04777
Chengdu + Munich rule analysis     = $0.05004
total                              = $0.09781
```

The two opening scans reused cached rule interpretations, so their marginal
per-order API cost was zero. The v0 two-event experiment may subtract `$0.05004`
once; a project-lifetime report may subtract `$0.09781` once. These scopes must
not be combined or double-counted.

## Corrections required by v1

1. Persist outcome, token and condition identity explicitly.
2. Track `OPEN → AWAITING_RESULT → RESOLVED → PAPER_SETTLED`.
3. Never book a payout from `endDate`, an empty book or `closed` alone.
4. Persist same-token bid-side marks separately from realized P&L.
5. Poll and version EDDM/ZUUU station observations, including corrections.
6. Block brackets made impossible by observations already available at entry.
7. Treat unresolved rule/source ambiguity as a decision blocker.
8. Preserve v0 rows and start corrected signals under `strategy_version=v1`.

