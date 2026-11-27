# Medicine Polyglot High-Load

Курсова робота з дисципліни «Бази даних та інформаційні системи».
Тема: проєктування та розробка розподіленої високонавантаженої системи збору,
збереження, повнотекстового пошуку та моніторингу інформації.
Варіант №17: «Медичні симптоми та Ліки».

КПІ ім. Ігоря Сікорського, ФПМ, група КМ-42.
Автори: Запорожець А. А., Коваль Д. С., Бойко Д. А., Хайло А. Р.

## Про проєкт

Довідник лікарських препаратів (датасет Kaggle, 222 825 препаратів після очищення) з
повнотекстовим пошуком за інструкціями, фільтрацією, пошуком аналогів і панеллю моніторингу.
Дані зберігаються в поліглотному сховищі, кожна БД обслуговує свій тип запитів:

| Сховище | Що зберігає | Для яких запитів |
|---|---|---|
| MongoDB (3 шарди) | картки препаратів, журнал пошуку | картка за id, фільтрація, агрегація |
| Elasticsearch | `full_description` (показання, побічні ефекти, механізм дії) | повнотекстовий пошук |
| Neo4j | вузли Medicine, Disease, Symptom і ребра між ними | аналоги, «аналог без побічного ефекту X» |
| Redis | результати пошуку, ключ `config:rate` | кеш, керування швидкістю генератора |

Між генератором і записом у БД стоїть черга RabbitMQ.
Стек: Python 3.11+, asyncio, FastAPI, Pydantic, Docker.

**Поточний стан (10.10.2026).** Архітектура, класи й контракти готові. Тіла методів поки
`NotImplementedError`. Реалізовано: підготовку датасету, автоматичні тести архітектури,
генератор схем із коду та Docker-стенд (власний образ + MongoDB).

## Архітектура

Шарова + гексагональна архітектура (порти й адаптери) + CQRS. Три незалежні процеси:
**API** (читання), **generator** (публікує пакети в чергу), **ingest_worker** (пише з черги в БД).

```
presentation ──> application ──> domain <── infrastructure
   (REST, SSE)     (вхідні порти,   (сутності,   (адаптери БД,
                    обробники,       служби,      черга, логування)
                    DTO)             вихідні порти)
                                        ^
workers ── через порти ─────────────────┘      infrastructure/config/container.py
                                               збирає все разом
```

Правило: залежності спрямовані до домену, домен не імпортує нічого. Правило перевіряють
тести та скрипт `tools/arch_diagrams.py --check`.

Схеми згенеровано з коду (дата стоїть на кожній), каталог `docs/diagrams/`:

| Файл | Що показує | У звіті |
|---|---|---|
| `01_modules.png` | залежності між модулями | рис. 3.8 |
| `02_domain_model.png` | доменна модель | — |
| `03_ports_inbound.png`, `04_ports_outbound.png` | вхідні та вихідні порти | — |
| `05_search_slice.png` | зріз «Пошук препаратів» | рис. 3.4 |
| `06_ingest_slice.png` | зріз «Запис документів» | рис. 3.7 |
| `07_card_slice.png` | зріз «Картка препарату» (ланцюжок п. 4) | рис. 3.5 |

### Ланцюжок Controller → Facade → Service → Repository

| Роль | Приклад із завдання | У проєкті |
|---|---|---|
| Контролер | `MovieController` | `MedicineController`, `SearchController`, `AnaloguesController`, `RateController` |
| Фасад | `MovieFacade` | вхідні порти `ListMedicines`, `GetMedicineCard`, `CreateMedicine`, `UpdateMedicine`, `DeleteMedicine`… |
| Сервіси | `MovieInformationService`, `MoviePictureService` | обробники `*Handler` + доменні служби `QueryNormalizer`, `DocumentValidator`, `ShardKeyService` |
| Репозиторії | `MovieInformationRepository`, `MoviePictureRepository` | `DocumentRepository` (CRUD), `GraphRepository`, `SearchIndex`, `CacheStore`: окремий для кожного сховища |
| Реалізації | — | `MongoDocumentAdapter`, `Neo4jGraphAdapter`, `SearchIndexAdapter`, `RedisCacheAdapter` |

Окремого класу-фасаду немає: його роль виконують вхідні порти (детальніше: звіт, п. 3.6.2).

### Цифри (з `docs/diagrams/summary.json`)

61 модуль, 78 класів: 6 сутностей, 3 доменні служби, 10 вхідних і 6 вихідних портів,
10 обробників, 16 DTO, 6 адаптерів, 5 контролерів, 2 процеси; 17 реалізацій портів;
порушень правила залежностей і циклів немає.

## Структура репозиторію

```
app/
  domain/           сутності, доменні служби, вихідні порти, винятки
  application/      вхідні порти, commands/ і queries/ (CQRS), DTO, mappers.py
  infrastructure/   адаптери persistence/, messaging/, observability/; config/ = Container
  presentation/     rest/ (контролери), stream/ (SSE)
  main.py           точка входу API
workers/            generator.py, ingest_worker.py: окремі процеси
scripts/            process_dataset.py: підготовка датасету (Додаток А звіту)
tools/              arch_diagrams.py: схеми з коду + перевірка архітектури
tests/unit/         автоматичні тести архітектури (18)
docker/             Dockerfile, docker-compose.smoke.yml, smoke_check.py
docs/diagrams/      згенеровані схеми
prepared/           підготовлені файли для БД (не в git)
```

## Як запустити й перевірити

Потрібно: **Git**, **Docker Desktop** (на Windows працює через WSL 2), за бажанням **Python 3.11+**.
Усі команди виконуються в корені проєкту.

```
git clone https://github.com/ArtemHaylo/BD_coursework_medicine.git
cd BD_coursework_medicine
```

### 1. Docker-стенд: власний образ + MongoDB (п. 6 завдання)

Контейнер `app` (образ `medicine-app:0.1` з кодом проєкту) підключається до контейнера `mongo`,
записує препарат, читає його назад і порівнює. Логи пишуться в консоль і у файл `logs/smoke.log`
на вашому комп'ютері (bind mount), тому лишаються після видалення контейнерів.

```
docker compose -f docker/docker-compose.smoke.yml up --build --attach app --exit-code-from app
```

- `--build` збирає образ (перший раз кілька хвилин, далі можна без цього прапорця);
- `--attach app` показує лише логи нашого контейнера (MongoDB пише багато службових рядків);
- `--exit-code-from app` зупиняє все, коли `app` завершився, і повертає його код виходу.

Очікуваний вивід:

```
smoke-app  | ... | INFO    | MongoDB відповідає (спроба 1, uri=mongodb://mongo:27017)
smoke-app  | ... | INFO    | ЗАПИС: id=... name='smoke-test medicine'
smoke-app  | ... | INFO    | ЧИТАННЯ: id=... name='smoke-test medicine' price=100.0
smoke-app  | ... | INFO    | ПОРІВНЯННЯ: записане і прочитане збігаються
smoke-app  | ... | INFO    | У колекції smoke.checks тепер 1 запис(ів)
smoke-app exited with code 0
```

Перевірити, що логи пережили видалення контейнерів:

```
docker compose -f docker/docker-compose.smoke.yml down
Get-Content logs\smoke.log -Encoding UTF8      # Windows PowerShell
cat logs/smoke.log                             # Linux / macOS / Git Bash
```

Без `-Encoding UTF8` старий PowerShell показує кирилицю «кракозябрами»: файл при цьому правильний.
Час у логах за UTC. Під час повторного запуску лічильник записів зростає (2, 3, …): дані MongoDB
лежать в іменованому томі `mongo_data`. Прибрати все разом із даними:

```
docker compose -f docker/docker-compose.smoke.yml down -v
```

### 2. Тести

Варіант без встановлення Python, у тому самому образі (код проєкту монтується в контейнер):

```
docker run --rm -v "${PWD}:/app" medicine-app:0.1 python -m pytest tests/unit -q
```

`${PWD}` працює в PowerShell і bash; у `cmd` замініть на `%cd%`.

Варіант з локальним Python:

```
python -m venv .venv
.venv\Scripts\Activate.ps1          # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/unit -q
```

Очікуваний результат: `18 passed`. Тести не потребують запущених БД.

| Тест | Що гарантує |
|---|---|
| `test_domain.py` | домен не імпортує жодного зовнішнього шару; сутності й порти коректні |
| `test_application.py` | `application` не імпортує `infrastructure`, `presentation`, `workers`; кожен обробник реалізує вхідний порт |
| `test_infrastructure.py` | кожен адаптер реалізує всі методи свого порту; адаптери не імпортують `application` і `presentation` |
| `test_presentation.py` | усі REST-маршрути зареєстровані; контролери не імпортують `infrastructure` |
| `test_workers.py` | воркери створюються з портів і не залежать від конкретних адаптерів |
| `test_diagrams.py` | немає порушень шарів і циклів; кожен порт має реалізацію; схеми будуються з коду з датою |

### 3. Схеми архітектури з коду

```
python tools/arch_diagrams.py --check                          # лише перевірка, код 1 при порушеннях
python tools/arch_diagrams.py --out docs/diagrams --date 10.10.2026
```

Потрібен лише Python; для PNG/SVG ще Graphviz (`dot`), без нього створюються `.dot` і `summary.json`.

### 4. Підготовка датасету

Датасет «250k Medicines Usage, Side Effects and Substitutes» (Kaggle) у git не зберігається.
Покладіть `medicine_dataset.csv` у корінь проєкту і запустіть:

```
python scripts/process_dataset.py
```

Скрипт виводить аудит (цифри розділу 2.1.1 звіту) і створює в `prepared/` файли для кожного
сховища: `medicines.jsonl`, `search_docs.jsonl`, `diseases.csv`, `symptoms.csv`, `treats.csv`,
`causes.csv`, `analogs.csv`, `search_queries.csv`. Генерація відсутніх полів відтворювана (`seed = 42`).

### 5. Етап 13: партиціонування, шардінг і бенчмарк запису

```
# спершу основний кластер (3 шарди) і дані, потім ще 2 шарди для порівняння
docker compose -f docker/docker-compose.mongo.yml up -d
python scripts/load_mongo.py --drop          # основні дані; ingest_log стає колекцією часових рядів
docker compose -f docker/docker-compose.mongo.yml -f docker/docker-compose.mongo.scale.yml up -d
python scripts/storage_sizes.py              # обсяги сховищ (чому шардується MongoDB)

python scripts/bench_sharding.py prepare     # 1,1 млн перемішаних документів у prepared/bench/
python scripts/bench_sharding.py run --shards 1
python scripts/bench_sharding.py run --shards 3
python scripts/bench_sharding.py run --shards 4 5
python scripts/bench_sharding.py report      # results/bench/summary.csv і throughput.png
```

Бенчмарк пише в колекцію `medicines_bench`, основну `medicines` не змінює. Для N шард
колекцію заздалегідь ділять на 4 x N чанків і розносять по колу; «1 шард» означає, що всі
чанки лежать на shard1rs, а інші шарди не використовуються. Записувачі - окремі процеси
(1, 4, 8), пакети по 1000 і 5000 документів, по 3 прогони, у підсумку - медіана.
Для графіка потрібен `pip install matplotlib`. Порядок важливий: якщо створити базу вже
на 5 шардах, основна колекція може лягти на shard4rs/shard5rs, а не на shard1-3, як у звіті.

### 6. Етап 14: один запит з усіх шард (map-reduce)

```
python scripts/mapreduce_demo.py                              # статистика за виробниками
python scripts/mapreduce_demo.py --group-by therapeutic_class
python -m uvicorn app.main:create_app --factory --reload      # вкладка «Аналітика по шардах»
```

Сервіс читає з `config.chunks`, які діапазони ключа лежать на кожній шарді, і паралельно
виконує на кожній шарді часткову агрегацію (map: кількість, сума цін, мінімум, максимум).
Застосунок зводить часткові підсумки (reduce) і лише потім рахує середні та частки.
Для перевірки той самий запит виконується вбудованою агрегацією MongoDB (у `explain` видно
`shardsPart` і `mergerPart`), і результати порівнюються. API: `GET /api/analytics/stats`.
На нешардованій базі (MongoDB Atlas у Render) уся колекція обробляється як одна частина.

## Контейнеризація

Docker + Docker Compose дають однакове середовище для всієї команди і дозволяють підняти
шардований кластер MongoDB разом з іншими БД на одному комп'ютері. Обґрунтування, схема
розгортання і результати стенда наведено в звіті, п. 3.10. Цільовий склад кластера:
`api`, `generator`, `ingest-worker` (один образ `medicine-app`), `mongos`, `configsvr`,
`shard1–3`, `elasticsearch`, `neo4j`, `redis`, `rabbitmq`. Кожна БД має іменований том, а логи
застосунку пишуться в `./logs`. Повний `docker-compose.yml` з'явиться разом із реалізацією адаптерів.


### 7. Етап 15. Повнотекстовий пошук Elasticsearch та кешування Redis

### Реалізована функціональність

- Повнотекстовий пошук за назвою та описом препарату.
- Ранжування результатів за релевантністю.
- Пріоритет точних збігів над нечіткими.
- Нечіткий пошук (fuzzy search).
- Підсвічування знайдених фрагментів тексту.
- Пакетний пошук із використанням search_after.
- Фільтрація за ціною та рецептурністю.
- Кешування результатів пошуку в Redis.
- Автоматичне видалення кешу за TTL (60 секунд).
- Обробка недоступності Redis без зупинки пошуку.
- Збереження метрик пошуку в MongoDB.
- REST API та окрема вкладка вебінтерфейсу.

### Архітектура

MongoDB використовується як основне сховище
інформації про лікарські засоби.

Elasticsearch використовується для повнотекстового
пошуку та ранжування документів.

Redis використовується для кешування результатів
повторюваних пошукових запитів.

Взаємодію між сховищами реалізовано відповідно
до принципів гексагональної архітектури через
вихідні порти та інфраструктурні адаптери.

### Запуск Elasticsearch та Redis

Передбачається, що MongoDB уже запущено
та заповнено підготовленими даними.

```bash
docker compose -f docker/docker-compose.search.yml up -d --wait
```

Ключ `--wait` чекає, доки обидва контейнери стануть `healthy`
(Elasticsearch стартує 20–60 с). Для Redis вимкнено запис на диск
і задано `maxmemory 256mb` з політикою `allkeys-lru`: це лише кеш,
тому старі ключі витісняються, а не накопичуються.

Перевірка стану контейнерів:

```bash
docker ps
```

Перевірка Redis:

```bash
docker exec medicine-redis redis-cli ping
```

### Локальні налаштування

У `config.yaml` адреси вказано через `127.0.0.1`:

```yaml
elastic:
  url: http://127.0.0.1:9200
redis:
  url: redis://127.0.0.1:6379
```

На Windows ім'я `localhost` спочатку перетворюється на IPv6-адресу `::1`,
а контейнери слухають лише `127.0.0.1`. Через це кожне підключення
до Redis чекало тайм-аут 2 с, пошук тривав понад 4 с і кеш не працював.
За потреби адреси можна перевизначити змінними `ELASTIC_URL` і `REDIS_URL`
у `.env`.

### Індексація документів

```bash
python scripts/load_elastic.py
```

Параметри запуску скрипту:

```bash
python scripts/load_elastic.py --help
```

У пошуковому індексі medicines зберігаються поля:

- medicine_id;
- name;
- full_description.

Під час локальної індексації було завантажено
222 825 документів.

### Запуск застосунку

```bash
python -m uvicorn app.main:create_app --factory --reload
```

### REST API

GET /api/search

Параметри запиту:

| Параметр | Опис |
|---|---|
| q | Текст пошукового запиту |
| price_max | Максимальна ціна |
| is_prescription | Рецептурність препарату |
| limit | Максимальна кількість результатів (1–100) |

Приклади запитів:

```http
GET /api/search?q=paracetamol&limit=10
GET /api/search?q=paracetmol&limit=10
GET /api/search?q=headache&price_max=150
GET /api/search?q=nausea&is_prescription=false
```

Відповідь містить список знайдених препаратів,
кількість повернутих результатів, оцінки
релевантності, підсвічені фрагменти тексту,
час обробки та ознаку використання кешу.

Фільтри `price_max` та `is_prescription` застосовуються після
отримання документів з MongoDB. Щоб вибірковий фільтр не змушував
переглядати весь індекс, обробник зупиняється після 2000 переглянутих
збігів Elasticsearch (`MAX_SCANNED_HITS`).

### Поведінка при відмові сервісів

| Ситуація | Результат |
|---|---|
| Redis недоступний | пошук працює без кешу (завжди MISS), помилка пишеться в лог |
| Elasticsearch недоступний або не відповів | `/api/search` повертає 503, вебінтерфейс показує повідомлення; каталог і аналітика працюють |
| Неіснуючий запит | порожній результат (0 препаратів), а не помилка |

На Render (MongoDB Atlas) Elasticsearch і Redis не розгорнуто,
тому вкладка повнотекстового пошуку там повертає 503;
решта сайту працює.


### Бенчмаркінг

Для порівняння продуктивності пошуку з використанням
кешу Redis та без нього:

```bash
python scripts/bench_search.py
```

Результати експерименту зберігаються у файлі:

`results/bench_search.csv`

Для побудови графіків на основі отриманих результатів:

```bash
python scripts/plot_search_benchmark.py
```

У результаті виконання скрипту створюються три графіки:

- `results/search_http_latency.png` — порівняння часу
  виконання HTTP-запитів;
- `results/search_handler_latency.png` — порівняння часу
  виконання SearchMedicinesHandler;
- `results/search_speedup.png` — коефіцієнт прискорення
  пошуку з використанням Redis.

Для кожного з п'яти пошукових запитів виконано
10 пар вимірювань Cache MISS та Cache HIT.

| Запит | Медіана MISS, мс | Медіана HIT, мс | Прискорення |
|---|---:|---:|---:|
| paracetamol | 26.01 | 6.85 | 3.80x |
| headache | 24.91 | 7.04 | 3.54x |
| nausea | 28.77 | 6.67 | 4.31x |
| bacterial infection | 22.99 | 6.36 | 3.62x |
| paracetmol | 73.01 | 6.06 | 12.04x |

За результатами експерименту:

- Середній час HTTP-запитів зменшився з 37.49 мс
  до 9.97 мс (на 73.4%).
- Прискорення HTTP-запитів за медіанним часом
  становить від 3.54 до 12.04 раза.
- Найбільше прискорення має запит з помилкою
  (`paracetmol`): без кешу Elasticsearch виконує
  два проходи — точний, а потім нечіткий.
- Усі вимірювання, зокрема без кешу, вкладаються
  у вимогу Latency < 100 мс.
- Результати пошуку з кешуванням та без нього
  є ідентичними в усіх 50 парах вимірювань.

Експеримент проведено в локальному середовищі
з використанням Docker Compose та послідовних
HTTP-запитів.


### Автоматизоване тестування

Запуск модульних тестів:

```bash
python -m pytest tests/unit -q
```

Запуск інтеграційних тестів пошуку:

```bash
python -m pytest tests/integration/test_search_integration.py -v
```

Запуск усіх тестів:

```bash
python -m pytest tests -q
```

У межах етапу 15 додано:

- 21 модульний тест;
- 5 інтеграційних тестів.

Інтеграційні тести використовують тимчасові
тестові дані та локальні екземпляри сховищ.
Якщо MongoDB, Elasticsearch або Redis не запущено,
відповідні тести пропускаються (skip), а не падають.

### Обмеження реалізації

- Elasticsearch та Redis розгорнуто локально
  за допомогою Docker Compose; на Render їх немає.
- Фільтри ціни та рецептурності застосовуються
  після пошуку, тому при дуже вибірковому фільтрі
  результатів може бути менше, ніж є насправді
  (межа — 2000 переглянутих збігів). Повноцінне
  рішення — зберігати ці поля в індексі
  Elasticsearch і фільтрувати в самому запиті.
- Коли Redis недоступний, кожен запит чекає
  тайм-аут підключення (2 с на читання і 2 с на запис кешу).
- Пакетний пошук розрахований на індекс, який
  не змінюється під час проходження пакетів.
- Кешовані результати можуть залишатися
  актуальними в Redis до завершення TTL,
  навіть якщо основні дані було змінено.


## Статус

- [x] Скелет каталогів і класи всіх шарів (domain, application, infrastructure, presentation, workers)
- [x] Підготовка датасету
- [x] Автоматичні тести архітектури та генератор схем з датою
- [x] Docker: власний образ і стенд app + MongoDB (запис, читання, логи на хості)
- [ ] Реалізація адаптерів і обробників
- [ ] Повний `docker-compose.yml` (3 шарди MongoDB, Elasticsearch, Neo4j, Redis, RabbitMQ)
- [ ] Веб-інтерфейс і генератор з керуванням швидкістю
- [ ] Навантажувальне тестування
- [ ] Фінальна схема взаємодії елементів
