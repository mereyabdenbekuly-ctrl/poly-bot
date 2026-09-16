# Polybot

Безопасный MVP для исследования погодных рынков Polymarket:

- читает рынки и стаканы через официальный Python SDK `polymarket-client`;
- проверяет географическую доступность до любых торговых действий;
- нормализует правила только для узкого шаблона `Highest temperature ...`;
- при явном включении использует `gpt-6-astra` для кэшируемого аудита правил;
- получает ансамблевый прогноз Open-Meteo и строит экспериментальные вероятности;
- сохраняет версионируемые наблюдения точной станции из источника
  NOAA/WRH/Synoptic и сверяет их с Aviation Weather Center;
- считает исполнимую цену по глубине стакана, комиссию, минимальный ордер и EV;
- пишет все снимки и решения в SQLite;
- умеет открывать только **виртуальные** позиции.
- автономно работает без браузера, формирует отчёты через 60 и 120 минут и
  продолжает следующее окно без сброса бюджета;
- предоставляет локальный read-only dashboard;
- подключает WeatherNext 3 только как параллельный сравнительный источник после
  получения официального доступа, без подмены отсутствующих данных заглушками.

> Проект не гарантирует прибыль. Модель вероятностей пока не откалибрована на
> завершившихся событиях. По состоянию на 8 сентября 2026 года в репозитории
> намеренно отсутствует исполнитель реальных заявок и код с приватным ключом.

## Текущий контур

```text
Polymarket public API ─┐
                      ├─> rules audit ─> weather ensemble ─> probability
Open-Meteo ───────────┘                                         │
                                                                v
order-book depth ─> fee/risk checks ─> observe or paper order ─> SQLite
                             ▲
                       GPT-6 Astra
                    (optional + cached)
```

GPT-6 Astra не получает доступ к кошельку, не выбирает размер позиции и не
может изменить лимиты риска. Текст рынка передаётся модели как недоверенные
данные. Если аудит запрошен, но недоступен или неоднозначен, событие пропускается.

## Быстрый запуск

Требования: macOS/Linux, `uv`, Python 3.11+ (проект зафиксирован на Python 3.12).

```bash
cp .env.example .env
uv sync --all-groups
uv run polybot doctor
uv run polybot scan --max-events 2
```

Первый `scan` работает без OpenAI API и ничего не покупает. Он сохраняет
рыночные и погодные снимки в `data/polybot.sqlite3`.

### Включить аудит Astra

Откройте локальный `.env`, добавьте `OPENAI_API_KEY`, при необходимости укажите
OpenAI-совместимый `POLYBOT_OPENAI_BASE_URL` и включите аудит:

```env
OPENAI_API_KEY=...
POLYBOT_OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_FALLBACK_API_KEY=
POLYBOT_OPENAI_FALLBACK_BASE_URL=
POLYBOT_ASTRA_ENABLED=true
POLYBOT_ASTRA_REASONING_EFFORT=low
```

Если настроены fallback URL и ключ, бот сначала вызывает primary endpoint и
переходит на fallback только после ошибки primary. Ключи для двух endpoint могут
быть разными.

> Публичный `http://` endpoint запрещён и завершается fail-closed. Для постоянного
> запуска используйте `https://` либо числовой loopback URL (`127.0.0.1`/`::1`),
> за которым находится отдельный SSH-туннель. Никогда не передавайте этому
> endpoint seed-фразу или приватный ключ кошелька.

```bash
uv run polybot scan --astra --max-events 2
```

Не присылайте ключ в чат и не коммитьте `.env`. Разбор правил кэшируется по
SHA-256 полного текста и набора исходов. Перед вызовом резервируется бюджет в
SQLite, а после ответа записывается фактическое число входных/выходных токенов.
Общий лимит по умолчанию — `$5`.

Тарифы в `.env.example` взяты из исходного технического задания на
8 сентября 2026 года и сделаны настраиваемыми. Перед длительным запуском
сверьте их с официальной ценой вашего API-проекта.

### Включить paper trading

```bash
uv run polybot scan --astra --paper
uv run polybot status
```

`--paper` открывает не более одной виртуальной позиции на группу события.
Новая стратегия записывается как `v1`; старые строки `v0` не переписываются.
По умолчанию paper-режим проверяет до `8` ещё не занятых событий за цикл,
тогда как observe-режим сохраняет меньший лимит `2`. Активные события исключаются
из квоты поиска: они продолжают переоцениваться отдельно, но не мешают находить
новые кандидаты. Это увеличивает только исследовательский охват — лимиты риска,
одна активная позиция на событие и отсутствие live-executor не меняются.
Начальные лимиты:

| Ограничение | Значение |
| --- | ---: |
| Новых событий-кандидатов за paper-цикл | `8` |
| Риск на событие | `$2` |
| Общая открытая экспозиция | `$6` |
| Дневной stop-loss | `$2` |
| Общая просадка | `$5` |
| Минимальный probability edge | `8 п.п.` |
| Минимальный ожидаемый результат | `$0.25` |

Все суммы включают рассчитанную комиссию и execution buffer. Стоимость Astra
вычитается из EV кандидата для события.
Теоретически за цикл может открыться до восьми paper-позиций, но фактическое
число ограничивают квалификация сигнала, одна позиция на событие и общая
экспозиция `$6`.

Закрыть виртуальную позицию после официального результата:

```bash
uv run polybot settle MARKET_ID --won
# или
uv run polybot settle MARKET_ID --lost
```

### Непрерывный наблюдатель

Наблюдение без виртуальных сделок:

```bash
uv run polybot run --interval 300 --astra --max-events 2
```

Виртуальная торговля:

```bash
uv run polybot run --interval 300 --astra --paper
```

Команда `run` полностью автономна. После запуска она создаёт постоянное окно в
SQLite, выполняет цикл каждые пять минут и сохраняет:

- `STARTUP` при запуске;
- `INTERIM_60M` после 60 минут;
- `COMPLETE_120M` после 120 минут;
- затем начинает следующее окно и продолжает работать.

API-бюджет, позиции и накопленная статистика между окнами не обнуляются.
Paper-охват настраивается через `POLYBOT_PAPER_MAX_EVENTS` (по умолчанию `8`)
или явный `--max-events`; максимальное допустимое значение — `20`.

На текущем Mac установлены пользовательские LaunchAgents:

```bash
deploy/macos/install-user-agents.sh
```

Это **запуск после входа пользователя `admin`**, а не системный LaunchDaemon до
login. Закрытие браузера и терминала не влияет на работу. Mac должен оставаться
включённым, пользовательская сессия — активной, а sleep на питании — выключенным.

### Read-only dashboard

```bash
uv run polybot dashboard --host 127.0.0.1 --port 8787
```

Откройте `http://127.0.0.1:8787`. Страница только читает SQLite, обновляется
раз в 30 секунд и никогда не запускает анализ или торговое решение.

Полный сравнительный отчёт v1/ECMWF/v2 доступен отдельно по
`GET /api/comparison`. Ответ строится только из read-only SQLite-снимка и
кэшируется в памяти на 60 секунд, поэтому тяжёлые диагностические расчёты не
дублируются при одновременных запросах и не влияют на paper-цикл. Dashboard
показывает его краткий promotion gate: число парных завершённых событий,
состояние поправки v2 и долю sigma-зависимых сигналов.

### WeatherNext 3

По умолчанию отображается честный статус `disabled`. Чтобы показать
`access_pending`, не останавливая текущую стратегию:

```env
POLYBOT_WEATHERNEXT_ENABLED=true
POLYBOT_WEATHERNEXT_SURFACE=gcs_full_ensemble
```

После получения Google-доступа можно указать путь к авторизованному экспорту:

```env
POLYBOT_WEATHERNEXT_SNAPSHOT_PATH=/absolute/path/to/weathernext-snapshot.json
```

Файл должен содержать реальные максимумы сценариев и два времени:
`init_time_utc` и `received_at_utc`. WeatherNext сохраняется и сравнивается с
базовой вероятностью, но **не меняет решения стратегии v1**. Если доступ или
файл отсутствует, бот продолжает работу на текущих источниках.

Для официального полного 64-member Zarr-v3 bucket используется Requester Pays.
Укажите Google Cloud **Project ID** (например, `weather-508105`), а не API key:

```env
POLYBOT_WEATHERNEXT_GCS_PROJECT=weather-508105
POLYBOT_WEATHERNEXT_GCS_BUCKET=weathernext3_spatial
POLYBOT_WEATHERNEXT_GCS_PREFIX=weathernext_3_0_0/zarr
POLYBOT_WEATHERNEXT_STATISTICS_VARIABLE=station_head_temperature_2m
POLYBOT_WEATHERNEXT_STATISTICS_SNAPSHOT_PATH=/absolute/path/to/weathernext-statistics-snapshot.json
```

Для чтения Zarr v3 установите необязательную группу зависимостей:

```bash
uv sync --group weathernext
```

Проверка выполняет только ограниченный metadata-запрос:

```bash
uv run polybot weathernext check --json
```

Отдельная metadata-only диагностика объясняет большой raw-transfer estimate и
не читает тела chunk-объектов:

```bash
uv run polybot weathernext raw-estimate \
  --latitude 52.3086 --longitude 4.7639 --location "EHAM Amsterdam Schiphol" \
  --date 2026-09-15 --timezone Europe/Amsterdam --json \
  --output /absolute/path/to/weathernext-raw-estimate.json
```

Отчёт разделяет глобальный logical array, выбранные chunks, codecs, sharding и
фактические compressed object sizes. `global_uncompressed_array_bytes` и
целый shard не считаются обязательным transfer сами по себе; transfer unit
берётся из Zarr metadata. `metadata_only=true` и `payload_read=false` должны
оставаться неизменными.

Первый снимок загружается явно, а не во время каждого observer-цикла:

```bash
uv run polybot weathernext refresh \
  --latitude 48.3538 --longitude 11.7861 --location Munich \
  --date 2026-09-15 --timezone Europe/Berlin
```

Для официальной сводки WeatherNext statistics используйте отдельный bounded
refresh. Он сохраняет только mean/p10/p25/p50/p75/p90 для одной станции и
часов, помечает snapshot как `SUMMARY_ONLY` и не создаёт 64 synthetic members:

```bash
uv run polybot weathernext summary-refresh \
  --latitude 52.3086 --longitude 4.7639 --station-id EHAM \
  --location "EHAM Amsterdam Schiphol" \
  --date 2026-09-15 --timezone Europe/Amsterdam --hours 4
```

`--hours` удерживает ранние valid hours выбранного station-local окна и
фиксирует partial/bounded coverage в provenance. Чтение останавливается до
скачивания, если ожидаемые network bytes превышают
`POLYBOT_WEATHERNEXT_STATISTICS_READ_MAX_BYTES`.

#### Full ensemble: отдельная WeatherNext paper-стратегия

Полный 64-member путь отделён от v1 и запускается только через approval-gated
manifest. Сначала автономный preflight строит inventory выбранных станций,
одного выпуска и общих compressed chunks; это metadata-only операция и
`payload_read=false`. Большой global logical array (например, оценка 148 GiB)
не считается обязательным transfer: в манифесте отдельно указаны shape,
chunk-shape, codecs, object sizes, число объектов и точный ожидаемый
compressed transfer. Sharded stores блокируются, а `max_network_bytes`,
`max_objects` и `max_object_bytes` проверяются до body GET.

Для первого заранее выбранного испытания отдельный планировщик берёт только
реальные market/event IDs из завершённого discovery-цикла, оставляет цели, чьи
station-local сутки ещё не начались, резервирует измеренное время прежнего
448-object прохода и объединяет крупнейшую группу с полностью одинаковым
UTC-покрытием, которая успевает завершиться до начала суток:

```bash
uv run polybot weathernext first-full-trial-plan --json
```

`strictly_future` относится только к этому первому pre-day испытанию. Обычный
inventory по-прежнему сохраняет уже начавшиеся, но ещё не закончившиеся сутки:
они могут оцениваться отдельно как Intraday при наличии корректных наблюдений и
своевременного решения. Прошедшие сутки не превращаются задним числом в paper
results, а требование полного точного station-local покрытия не ослабляется.

Планировщик делает только listing/metadata/HEAD, записывает exact-limit manifest
под `first-full-trial/` и не создаёт approval. Hourly systemd timer запускает
только этот metadata-only путь. Когда оператор отдельно согласует точный SHA и
создаст sidecar, существующий manifest читается без предварительной
регенерации:

```bash
uv run polybot weathernext autonomous-refresh \
  --read-approved --require-strictly-future-targets \
  --manifest /var/lib/polybot/weathernext/full/first-full-trial/read-manifest.json \
  --approval /var/lib/polybot/weathernext/full/first-full-trial/read-approval.json \
  --json
```

До отдельного согласования `POLYBOT_WEATHERNEXT_FULL_REFRESH_ENABLED=false`.
Чтение идёт ровно по одному Zarr object за раз и останавливается при изменении
размера, generation/checksum, лимита или покрытия. Измеренная проба заняла около
49,7 секунды на один объект с учётом загрузки, decode и накладных расходов;
поэтому прежний план из 448 объектов оценивается примерно в 6,2 часа, а не в
два часа. План сохраняет рассчитанную продолжительность и ready-at timestamp.
Если exact metadata показывает, что выбранная группа всё же не успевает до
station-local midnight, executable manifest не публикуется и остаётся только
metadata-only статус ожидания следующей цели.

Результат — immutable snapshots с release/init provenance, UTC valid times,
координатами, единицами, идентификаторами всех 64 участников и настоящими
почасовыми траекториями. Уже сохранённый выпуск переиспользуется без повторной
загрузки. Observer на следующем цикле подхватывает index и сохраняет snapshot в
SQLite; решения, позиции, marks и P&L WeatherNext находятся в отдельном
ledger/dashboard и не входят в v1 exposure, σ или live executor.

По умолчанию refresh блокирует потенциально очень большой raw-запрос до
скачивания данных. В full-ensemble Zarr пространственные chunks содержат
глобальную сетку, поэтому точечная выборка может потребовать десятки или сотни
гигабайт. В этой конфигурации `--allow-large-read` намеренно не используется;
observer такие raw-запросы сам не запускает. Для объяснения transfer cost
используйте metadata-only `raw-estimate`, а для данных — bounded statistics
surface ниже.

Часовой пояс нужен для преобразования локальной даты станции в UTC. Снимок
сохраняет фактический `init_time` из Zarr; значения `station_head_temperature_2m`
переводятся из Kelvin в Celsius. WeatherNext остаётся shadow/read-only и не
включает реальные сделки.

Если самый новый выпуск уже потерял начало текущего station-local дня, preflight
не подменяет часы: он ищет ближайший более ранний выпуск, который покрывает все
цели полностью, и только затем строит точный compressed-size manifest. Если
такого выпуска нет, манифест остаётся `blocked_incomplete_coverage`.
После отдельного согласования лимитов можно сначала измерить один блок, не
публикуя snapshot:

```bash
uv run polybot weathernext autonomous-refresh \
  --read-approved --probe-one-block --json
```

Проба возвращает compressed bytes, decoded shape/bytes и длительность. Она
одноразовая; после receipt повторять её не нужно. Полный проход выполняется
только отдельной явно согласованной командой и публикует snapshot лишь после
проверки 64 участников и полного набора UTC-часов.

## Docker Compose

По умолчанию контейнер запускается только как observer:

```bash
cp .env.example .env
docker compose up --build -d
docker compose logs -f observer
```

Чтобы включить Astra в контейнере, задайте в `.env` ключ и
`POLYBOT_ASTRA_ENABLED=true`. Для paper trading измените `command` в
`compose.yaml`, добавив `--paper`.
Не размещайте контейнер в другой стране без повторной проверки `doctor`:
географическая проверка относится к фактическому IP процесса.

### Linux VPS (systemd)

Production-like paper deployment uses the checked-in units under `deploy/linux`.
They keep the observer in explicit `--paper` mode, bind the dashboard only to
`127.0.0.1:8787`, and retain twelve SQLite backups. The official raw ECMWF
archive timer is installed but must remain disabled until the operator has set
a storage-retention/free-space policy. The repository contains no live-order
executor.

Expected paths:

```text
/opt/polybot                    checkout and locked virtual environment
/etc/polybot/polybot.env        secrets/config, root:polybot mode 0640
/var/lib/polybot                SQLite, snapshots, forecasts, backups
```

The VPS environment file must use absolute state paths (the observer unit also
pins these at execution time):

```env
POLYBOT_DATABASE_PATH=/var/lib/polybot/polybot.sqlite3
POLYBOT_ECMWF_JSON_ARCHIVE_ROOT=/var/lib/polybot/forecasts/ecmwf-ifs025-json
POLYBOT_WEATHERNEXT_STATISTICS_SNAPSHOT_PATH=/var/lib/polybot/weathernext-statistics-snapshot.json
```

After installing the locked environment, run `deploy/linux/install-systemd.sh`
as root, restore a consistent SQLite backup, and only then enable the units:

```bash
install -o polybot -g polybot -m 0600 BACKUP.sqlite3 /var/lib/polybot/polybot.sqlite3
systemctl enable --now polybot-observer.service polybot-dashboard.service
systemctl enable --now polybot-backup.timer
```

Do not enable `polybot-ecmwf-archive.timer` merely as part of deployment: its
raw archive is intentionally immutable and currently has no automatic pruning.

Do not expose port 8787 publicly. Use a local SSH tunnel instead:

```bash
ssh -N -L 8787:127.0.0.1:8787 root@SERVER_IP
```

## Команды

```text
polybot doctor                 проверка SDK, сети, геоблока, SQLite и конфигурации
polybot scan [--astra]         один цикл; никаких реальных заявок
polybot scan --paper           один цикл с виртуальными позициями
polybot run --interval 300     непрерывный цикл
polybot dashboard              локальная read-only страница состояния
polybot status                 экспозиция, P&L и расходы API
polybot diagnostics [--json]   forecast-vs-trade и sigma-диагностика
polybot comparison [--json]    сравнительный отчёт v1 / ECMWF / v2
polybot settle ID --won|--lost ручной результат paper-позиции
```

К любой диагностической команде можно добавить `--json` там, где это указано
в `polybot --help`.

## Как считается сигнал

1. Для минимального размера ордера стакан проходится от лучшего ask к худшему.
2. Комиссия каждого fill считается как:

   ```text
   shares × fee_rate × (price × (1 - price)) ^ exponent
   ```

3. Бот загружает историю точной станции из URL расчёта рынка, использует
   станционный часовой пояс, сохраняет `observed_at_utc`, `first_seen_at_utc`,
   сырой payload и SHA-256 версии. Уже опубликованный максимум является жёсткой
   нижней границей: исход ниже него блокируется.
4. Aviation Weather Center используется как независимая сверка METAR. Разница
   температур блокирует новый вход; отсутствие или устаревание основного
   источника также блокирует вход.
5. Для температурного диапазона вероятность оценивается как смесь нормальных
   распределений вокруг участников ансамбля. `POLYBOT_WEATHER_ERROR_SIGMA_C`
   по умолчанию равен `1.5°C` и является **исследовательским допущением**, а не
   подтверждённой калибровкой.
6. Кандидат проходит только если минимальный crossing-limit ордер в долях
   помещается в лимит, стакан
   свежий и достаточно глубокий, edge не меньше 8 п.п., а ожидаемый результат
   после комиссии, buffer и стоимости Astra не меньше `$0.25`.
7. В группе температурных диапазонов выбирается максимум один лучший кандидат.

Paper-позиции используют состояния:

```text
OPEN → AWAITING_RESULT → RESOLVED → PAPER_SETTLED
```

`endDate`, `closed` или пустой стакан сами по себе не начисляют выплату.
Переоценка хранится отдельно и проходит только по bids того же токена с учётом
глубины. Settlement требует подтверждённого бинарного результата и совпадения
`market_id + condition_id + token_id + outcome`.

## Данные и воспроизводимость

SQLite содержит:

- `scan_runs` — циклы и ошибки;
- `market_snapshots` — полный нормализованный стакан каждого рынка;
- `weather_snapshots` — значения всех участников ансамбля и время получения;
- `observation_fetches` — полная диагностика каждого опроса станции;
- `station_observation_versions` — неизменяемые версии наблюдений и исправлений;
- `rule_cache` — структурированный результат Astra и стоимость;
- `api_usage` — резервы/фактический расход API;
- `decisions` — все причины `SKIP`, `OBSERVE` и `PAPER_BUY`;
- `paper_orders` — виртуальные позиции и итоговый P&L.
- `paper_marks` — гипотетический исполнимый выход по текущим bids;
- `paper_resolution_checks` и `paper_order_transitions` — доказательства и
  переходы lifecycle.
- `runtime_windows` и `runtime_reports` — автономные 60/120-минутные отчёты;
- `weathernext_snapshots` — только реально загруженные сравнительные данные.

Эти записи нужны для следующего этапа: дождаться 100–200 завершившихся групп,
сверить результаты, оценить Brier score/калибровку, учесть задержку исполнения и
только затем решать, существует ли преимущество.

## Проверки разработчика

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest --cov=polybot
```

## Почему live trading отсутствует

Реальный исполнитель нельзя безопасно добавлять до выполнения как минимум
следующих условий:

1. положительный out-of-sample результат после всех расходов;
2. калиброванные вероятности и устойчивость к худшему исполнению;
3. автоматическая сверка позиций и восстановление после перезапуска;
4. отдельный ограниченный кошелёк/session key;
5. ручной kill switch и отдельное явное подтверждение владельца.

До этого момента правильный результат программы часто будет: **не торговать**.

## Forecast engine v2 (shadow mode)

The current `v1` paper strategy remains immutable. In parallel, the autonomous
loop can archive the `v1` distribution, an explicitly selected 50-member ECMWF
IFS ENS distribution, a simple station/intraday corrected `v2`, and later the
64-member WeatherNext 3 export. None of these shadow versions changes a paper
or live decision.

Enable local collection with:

```bash
POLYBOT_ECMWF_ENABLED=true
POLYBOT_FORECAST_V2_ENABLED=true
```

The dashboard shows forecasts even when no position is opened. Forecast
coverage is measured against the explicit eligible-event registry immediately;
MAE, exact-bracket accuracy, Brier score and calibration appear only after an
official result exists. Rates include 95% uncertainty intervals, and fewer than
30 resolved unique events remain descriptive-only. See `docs/forecast-v1.md`,
`docs/forecast-v2.md` and `docs/ecmwf.md` for formulas and provenance limits.

For the paper trades that have already settled, run the read-only diagnostic:

```bash
polybot diagnostics
polybot diagnostics --json
```

It separates a wrong weather forecast from buying a different bracket than the
forecast's top bracket, and compares the historical signal under raw empirical
members, the immutable `sigma=1.5°C` v1 kernel, and a clearly labelled
descriptive sigma proxy. These counterfactuals never alter stored decisions.
The report also splits realized P&L by the strategy version recorded at entry;
trade fees remain included, while allocated order API cost is shown separately
so it is not charged twice.

Для автоматического сравнения трёх сохранённых погодных версий:

```bash
polybot comparison
polybot comparison --json
```

Отчёт использует один последний сохранённый checkpoint на событие в каждой
фазе, показывает coverage, MAE, точность диапазона, multiclass Brier, ECE и
парные разницы кандидата относительно v1 только внутри одной строки evaluation
registry. Разница времени выпуска прогнозов выводится явно. Срезы по lead-time
остаются описательными: фиксированного holdout-периода пока нет, поэтому
`v2_promoted=false` независимо от текущих чисел. Отдельный раздел проверяет
архивную `sigma=1.5°C`, raw members, observed-floor conditioning и clamped
контрфактуал, не меняя историю или paper-решения.

### Durable local state and source archive

The deployed macOS LaunchAgents keep the live SQLite database and forecast
archives outside the Git checkout:

```text
~/Library/Application Support/Polybot/polybot.sqlite3
~/Library/Application Support/Polybot/forecasts/
```

`com.polybot.backup` creates an integrity-checked SQLite backup every 15 minutes
and retains at most the latest 12 snapshots, with an additional free-space
guard. `com.polybot.ecmwf-archive` runs every six
hours and archives the official ECMWF Open Data IFS ENS `enfo/pf/mx2t3` byte
ranges for all 50 perturbed members. This raw official archive is shown
separately from the lightweight `IFS ENS via Open-Meteo` comparison feed; the
latter intentionally reports missing upstream run/publication metadata.
