# ECMWF IFS ENS adapter

`polybot.ecmwf.EcmwfIfsEnsAdapter` is a standalone, safe-by-default ECMWF Open
Data adapter. It is intentionally not wired into the scanner yet.

## Data contract

The adapter accepts only the following official Open Data fields:

- physics-based model: `ifs` (not AIFS);
- resolution: `0p25`;
- class: `od`;
- atmospheric ensemble stream: `enfo`;
- perturbed forecast type: `pf`;
- surface level: `sfc`;
- exactly all perturbed member numbers `1..50`;
- `mx2t3` by default for daily maxima, or explicit instantaneous `2t`.

The operational deterministic `oper/fc` field and the ensemble control `cf`
field are rejected as substitutes for an ensemble. Incomplete member sets are
`pending` before the publication deadline and `unavailable` afterwards.

The official `ecmwf-opendata` client documentation describes IFS, the `enfo`
stream, `pf`, the `number` selector with values 1 to 50, and `2t` in kelvin. It
also says not specifying `number` retrieves every ensemble member. This adapter
spells out all 50 members and validates each index and decoded message instead
of relying on that implicit behavior.

## Daily maximum semantics

An instantaneous `2t` value every three hours can miss the temperature peak
between output times. Therefore the production default is `mx2t3`: maximum
2-metre temperature over the preceding three-hour interval.

For a field with end step `N`, the adapter records the represented interval as
`(N-3h, N]`. `daily_max_steps()` returns the end steps that exactly tile a
requested `[day_start_utc, day_end_utc]` span, while `daily_maxima()` checks that
all member intervals are contiguous and cover exactly that span before taking
the maximum. A local day whose UTC boundaries do not align to the three-hour
IFS grid is rejected; the adapter never pulls in an adjacent day's hours
silently.

This implementation limits `mx2t3` to steps 3 through 144 in three-hour
increments. It does not silently fall back to instantaneous `2t` or use a
longer aggregation window beyond that horizon. For a future longer-horizon
extension, `mx2t6` needs its own explicit six-hour interval contract and tests.

## Batch and two-phase API

Do **not** call `fetch_point()` once per station in a scan. Global fields are
large. Use either:

```python
result = adapter.fetch_points(
    init_time_utc=run,
    steps=steps,
    points={
        "EDDM": (48.3538, 11.7861),
        "RJTT": (35.5494, 139.7798),
    },
)
```

or the explicit two-phase API:

```python
archived = adapter.fetch_archive(init_time_utc=run, steps=steps)
if archived.archive is not None:
    result = adapter.extract_points(
        archived.archive,
        points=all_station_coordinates,
    )
```

`fetch_archive()` downloads the 50 selected GRIB byte ranges once for each
model run and step. The coordinate-independent archive is reused after digest
verification. `extract_points()` opens each archived field once and extracts
all supplied station coordinates while that GRIB member is in memory. Adding
more stations therefore does not repeat network downloads.

`fetch_daily_max_points()` combines strict day-window step selection with the
batch API.

## Time provenance and availability

Each successful raw archive records distinct UTC timestamps:

- `init_time_utc`: model cycle start supplied by the caller;
- `published_at_utc`: HTTP `Last-Modified` from the official index and GRIB
  object; mismatched index/data publication times are rejected;
- `fetched_at_utc`: time the immutable raw archive completed;
- `decoded_at_utc`: time point scenarios were extracted.

Official client documentation says data become available between 7 and 9 hours
after model initialization, depending on the forecasting system and step.
`latest_conservative_init()` selects the newest 00/06/12/18 UTC cycle at least
nine hours old. Missing members/indexes are reported as `pending` until the
configurable deadline (10 hours by default), then `unavailable`. Transport,
decoder, provenance, and archive failures are always `unavailable`; no values
are fabricated.

## Immutable archive

The official newline-delimited `.index` is archived alongside concatenated
selected GRIB messages:

```text
<archive_root>/<content_sha256>/
├── manifest.json
├── step-003-mx2t3.index
├── step-003-mx2t3.grib2
└── ...
```

The adapter requires HTTP `206`, an exact `Content-Range`, bounded response
sizes, matching `Last-Modified`, all 50 members, and correct run metadata. It
refuses a server that ignores `Range`, preventing an accidental multi-gigabyte
whole-file download. SHA-256 digests cover both indexes and GRIB subsets. Files
are staged, fsynced, atomically renamed, and made read-only. Existing archives
are revalidated before reuse.

## Decoder dependency

`EcCodesPointDecoder` uses ECMWF ecCodes Python bindings. It verifies parameter,
`dataType=pf`, run initialization, end step, member number, units, and for
`mx2t3` the `stepType=max` and exact preceding three-hour interval. Kelvin is
converted to Celsius only after validation.

The repository's lightweight default dependencies do not include the native
library. Until ecCodes and the matching Python `eccodes` package are installed,
fetch/extraction returns `unavailable` without downloading fields.

## Future integration parameters

No scanner, storage, dashboard, or config files are modified. A future
integration should add explicit settings:

| Setting | Suggested default |
|---|---|
| `POLYBOT_ECMWF_ENABLED` | `false` |
| `POLYBOT_ECMWF_SOURCE_URL` | `https://data.ecmwf.int/forecasts` |
| `POLYBOT_ECMWF_ARCHIVE_ROOT` | `data/forecasts/ecmwf-ifs-ens` |
| `POLYBOT_ECMWF_PRODUCT` | `mx2t3` |
| `POLYBOT_ECMWF_PUBLICATION_DEADLINE_HOURS` | `10` |
| `POLYBOT_ECMWF_MAX_INDEX_BYTES` | `8388608` |
| `POLYBOT_ECMWF_MAX_GRIB_MESSAGE_BYTES` | `4194304` |

At the start of a scan, gather every eligible station and the union of required
steps, call the batch API once, then distribute scenarios by `point_id`. Never
mix initialization cycles or let `pending`/`unavailable` change existing v1
trading decisions until forecast-engine-v2 integration is explicitly enabled.

## Official sources

- [ECMWF Open Data dataset](https://www.ecmwf.int/en/forecasts/datasets/open-data)
- [ECMWF real-time Open Data documentation](https://confluence.ecmwf.int/display/DAC/ECMWF+open+data%3A+real-time+forecasts+from+IFS+and+AIFS)
- [Official `ecmwf-opendata` client and request examples](https://github.com/ecmwf/ecmwf-opendata)
- [ECMWF ecCodes](https://github.com/ecmwf/eccodes)
