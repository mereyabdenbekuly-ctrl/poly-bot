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

> Если endpoint начинается с `http://`, API-ключ и текст запросов передаются без
> TLS-шифрования. Для постоянного запуска используйте `https://` либо защищённый
> туннель. Никогда не передавайте этому endpoint seed-фразу или приватный ключ
> кошелька.

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
дублируются при одновременных запросах и не влияют на paper-цикл.

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
and retains the latest 96 snapshots. `com.polybot.ecmwf-archive` runs every six
hours and archives the official ECMWF Open Data IFS ENS `enfo/pf/mx2t3` byte
ranges for all 50 perturbed members. This raw official archive is shown
separately from the lightweight `IFS ENS via Open-Meteo` comparison feed; the
latter intentionally reports missing upstream run/publication metadata.
