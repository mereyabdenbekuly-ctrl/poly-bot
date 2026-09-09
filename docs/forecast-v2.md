# Forecast engine v2

`v2` is a shadow forecast/evaluation system. It does **not** change the current
`v1` paper strategy and cannot place live orders.

## Parallel versions

Every eligible event is persisted after all same-run market snapshots exist:

1. `open-meteo-truncated-normal-v1` — the immutable production baseline.
2. `ecmwf-ifs025-raw-ensemble-v1` — empirical distribution of 50 explicitly
   selected ECMWF IFS ENS members.
3. `forecast-engine-v2-station-intraday@1` — the same IFS scenarios after a
   station bias/spread correction and conditioning on observations already
   available at forecast issuance.
4. `weathernext-empirical-ensemble-v1` — only after an authorized 64-member
   WeatherNext export exists.

All versions store model provenance, issuance/fetch/cutoff times, scenarios,
full bracket distribution, rule-day timezone and rounding, exact market
snapshot IDs, and observation revision hashes. Rows are append-only.

## ECMWF sources

The project contains two complementary paths:

- `polybot.ecmwf.EcmwfIfsEnsAdapter` downloads the official ECMWF Open Data
  `enfo/pf` product, all 50 perturbed members, and archives index/GRIB byte
  ranges by SHA-256. Daily maxima use `mx2t3` and exact 3-hour windows.
- The autonomous five-minute shadow loop uses a lightweight response from the
  Open-Meteo Ensemble API with `models=ecmwf_ifs025`. The exact JSON response is
  content-addressed and immutable. Because that response does not expose the
  upstream initialization/publication timestamps, those fields stay null and
  the dashboard says provenance is incomplete. It is never represented as the
  official byte archive.

## Station correction

The first version deliberately uses a simple, inspectable method:

- calculate historical error `actual_max - raw_ensemble_mean`;
- use its mean as station bias;
- match ensemble spread to residual error spread, clamped to `[0.5, 2.5]`;
- require at least 5 resolved events for a station;
- otherwise use a pooled profile after 20 events;
- with less history, use identity correction and explicitly report
  `insufficient_history`.

Training queries only outcomes recorded **before** prediction issuance. This
prevents future leakage. Parameters are not retroactively written into old
predictions.

## Intraday update

The final maximum cannot fall below the maximum already observed at the exact
resolution station. Corrected scenarios and probability kernels are therefore
truncated at that observed floor. The archive also records current temperature,
three-hour change, wind/cloud fields when the primary observation payload
contains them, and remaining station-local day time.

Wind/cloud/trend coefficients are currently zero. They are displayed and
archived, but `feature_adjustment_applied=false` until an out-of-sample
comparison proves a benefit. This avoids declaring unvalidated features useful.

## Evaluation

Only officially resolved unique events enter quality metrics. The latest saved
prediction per event and slice is used, so 20 five-minute updates are not 20
independent observations. Reports include:

- daily maximum MAE in °C;
- exact winning-bracket accuracy;
- multiclass Brier score;
- top-label calibration bins and expected calibration error;
- event count and forecast coverage;
- separate lead-time and intraday slices and lead-time bins;
- station-specific reports.

A model is not promoted because of a single win or green tests. Promotion must
use a frozen validation period and a preselected primary metric; uncertainty
must make a random explanation implausible. Until then, v2 stays shadow-only.
