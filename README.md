# TP Distribuidos – Grupo 15

## Cómo correr el sistema (resumen)

Requisitos: Docker con el plugin `docker compose` (y tu usuario en el grupo `docker`), `make` y
Python 3 en la máquina. Los datos van en `data/sample/<small|medium|large>_dataset/` (sólo `small`
viene en el repo) y lo esperado en `data/expected/<small|medium|large>/`.

    make switch SIZE=medium N=6   # 1) elige tamaño de dataset y escenario de réplicas (genera docker-compose.yaml)
    make build                    # 2) construye las imágenes (una por tipo de etapa; sólo hace falta si cambió el código)
    make system-up                # 3) levanta RabbitMQ + Gateways + etapas y las deja corriendo, sin clientes
    make run-clients              # 4) INICIA EL PROCESAMIENTO: vacía output/ y arranca los client_*
    make verify SIZE=medium       # 5) espera a los clientes y compara output/ con data/expected/medium
    make system-down              # 6) baja todo (también borra el volumen de la caché de imágenes)

- `SIZE` (small por defecto) tiene que ser el mismo en `make switch` y en `make verify`.
- `N` es el escenario (1..7, ver "Escenarios de réplicas"). Para limitar la CPU de RabbitMQ:
  `make switch SIZE=medium N=6 RABBIT_CPUS=1.0` (2.0 por defecto).
- Un solo comando para todo el ciclo: `make test-scenario SIZE=medium` (build, levantar, clientes,
  verificar y guardar logs en `logs/`). Deja el sistema corriendo; con `KEEP=0` lo baja al final.
- Un cliente más contra el sistema que sigue arriba: `make new-client C=client_0` (siempre con `C=`;
  sin eso se recrea todo el sistema). Después, `make verify`.
- `output/` lo vacía `make run-clients` (y `make test-scenario`) al empezar; `make system-down` no lo toca.
- Cada cliente escribe en `output/client_<i>/` (CSV de las 5 queries y `images/<sha256><ext>`).
- Los comandos `make run-clients` y `make new-client` son los que envían datos; `make verify` sólo
  espera y compara.

Herramientas para revisar una corrida:

    python3 tools/check_images.py              # que cada imagen del CSV exista, tenga extensión y no esté vacía
    sh tools/queue_depths.sh                   # mensajes por cola y CPU/memoria por contenedor (correrlo DURANTE la corrida: watch -n2 sh tools/queue_depths.sh)
    PYTHONPATH=src python3 tools/profile_year_in.py 200 1500   # costo de CPU por batch en el Gateway y en YearFilter

Variables útiles del cliente (por `docker compose --profile clients run --rm -e ... client_0` o
por argumentos): `BATCH_ROWS` (200), `BATCH_BYTES` (1 MiB), `PARQUET_READ_CHUNK` (200). De las etapas:
`PREFETCH` (10) para el prefetch de RabbitMQ, `IMAGE_CACHE_DIR` para la caché de imágenes.

Sin Docker (rápido, RabbitMQ falso): `make setup` y después `make test-scenarios`
(o `.venv/bin/pytest -q --ignore=tests/test_gateway_rabbit.py --ignore=tests/test_middleware.py --ignore=tests/test_querys.py`).

Notas:
- Todas las etapas parten de la imagen `tpdist/base` (`src/Dockerfile.base`), que fija Python 3.13.
  La versión importa: el conteo de palabras (Q5) usa `\w` y `lower()`, que dependen de la versión de
  Unicode de Python. Con otra versión algunos artículos de `large` cambian de conteo.
- Las imágenes se construyen una vez por tipo de etapa y las réplicas la comparten.


## Client y Gateway

Cómo correrlos (con RabbitMQ levantado: `make rabbit-up`):

    make setup
    make run-gateway     # terminal 1: escucha en :5000
    make run-client      # terminal 2: envía data/sample/medium_dataset y escribe output/

El cliente escribe `output/output_query_1.csv` … `output_query_5.csv` y `output/images/`
(las imágenes de Q2, nombradas por su sha256), que son los archivos que compara
`tests/test_querys.py`.

Variables de entorno del Gateway: `GATEWAY_ID` (1), `GATEWAY_PORT` (5000),
`GATEWAY_BIND_HOST` (0.0.0.0), `RABBIT_HOST` (localhost), `LOG_LEVEL` (INFO).
El cliente acepta las mismas opciones por argumentos (`python -m client --help`).

Prueba manual (cuatro terminales; el resto del sistema se reemplaza por `tools/fake_system.py`):

    make rabbit-up
    make run-gateway       # terminal 1
    make run-fake-system   # terminal 2: lee year_in y contesta con data/expected
    make run-client        # terminal 3

Tests: `make test-client-gateway` (sin broker) y `make test-gateway-rabbit` (con broker).
Detalle de la implementación en el documento de explicación entregado junto al código.

## Test de las queries

    make test

Construye, levanta todo el sistema con Docker Compose (`docker-compose.yaml`), espera a que `client_0`
termine de enviar y recibir los resultados en `output/` y corre `tests/test_querys.py`
(compara contra `data/expected/$(SIZE)`). El código de salida es el de pytest. `output/` se vacía al
empezar. El sistema queda corriendo; con `make test KEEP=0` se baja al final.

Para una sola query, con el sistema ya corrido y `output/` completo: `make test-query1` … `make test-query5`.

## Levantar y bajar el sistema

    make build         # imágenes (base + una por tipo de etapa), con caché de capas
    make system-up     # levanta el sistema sin clientes y espera a que estén healthy
    make run-clients   # arranca los clientes y empieza el procesamiento
    make system-down   # detiene y elimina contenedores, red y volúmenes

`make up` hace `build`, `system-up` y `run-clients` y sigue los logs (Ctrl+C corta los logs pero no
baja el sistema). `make down` equivale a `make system-down`. Logs sin seguir: `make logs`.

## LengthMapper, AvgReducer, Q5Filter y cierre de resultados (tareas 7 y 8)

| Etapa | Módulo | Qué hace |
|---|---|---|
| LengthMapper | `text_processing/length_mapper/length_mapper.py` | cuenta las palabras de cada artículo (con repeticiones); ≤2020 → suma y cantidad para el promedio, ≥2021 → Q5 |
| AvgReducer | `text_processing/avg_reducer/avg_reducer.py` | promedio por cliente (partición `crc32(client_id) % A`) |
| Q5Filter | `filters/q5_filter/q5_filter.py` | guarda en el spool los artículos que llegan antes del promedio; con el promedio conocido vacía el spool y filtra al vuelo; emite los de n > promedio |
| Spool | `common/utils/spool.py` | un archivo CSV por cliente (`/spool/q5_<k>/client_<gw>_<n>.csv` dentro del contenedor) |
| ResultTracker | `gateway/results_tracker.py` | el Gateway cuenta los EOF por consulta (1+1+P+1+K) y, al completarse, manda `done` al cliente |

Ya no hay Joiner: las etapas finales publican en `results_ex` con clave `gw_id`, el Gateway
consume `results_q.<gw_id>`, reenvía cada `result` al cliente y cuenta los EOF. Por eso el
Gateway necesita `WORD_PARTITIONS` (P) y `Q5_PARTITIONS` (K) como variables de entorno.

Nombres de colas/exchanges y formatos de mensaje: `src/common/topology.py`.

Tests (sin broker): `.venv/bin/pytest tests/test_length_avg_q5.py tests/test_spool.py tests/test_result_tracker.py tests/test_gateway.py`.

Prueba manual de Client + Gateway con RabbitMQ real: `make run-fake-system` (hace de resto del
sistema; levantar antes el Gateway, que declara `results_q.<gw_id>`).


## Escenarios de réplicas

`tools/scenarios.py` es la única fuente de verdad de "cuántas réplicas de cada proceso": de ahí
salen los `docker-compose` de cada caso (`scenarios/N.yaml`) y las corridas sin Docker
(`tests/real_flow.py`). Las variables que dependen de las cantidades (`CLEANER_REPLICAS`,
`LM_REPLICAS`, `WM_REPLICAS`, `AVG_PARTITIONS`, `Q5_PARTITIONS`, `WORD_PARTITIONS`) se calculan
solas, así nunca quedan desincronizadas con las réplicas levantadas.

La clave `gw` es la cantidad de Gateways: se levantan `gateway_0..gateway_<gw-1>` (con
`GATEWAY_ID` 1..gw) y el cliente `c` se conecta a `gateway_<c % gw>`. Cada Gateway recibe los
resultados de sus clientes por `results_q.<gw_id>` y cuenta sus EOF.

| N | Caso |
|---|------|
| 1 | Un cliente, una réplica de cada etapa |
| 2 | 3 clientes, una réplica de cada etapa |
| 3 | 3 clientes, filtros replicados (year, q1, q2, imágenes) |
| 4 | 3 clientes, rama de longitud/Q5 replicada (ac, lm, A, K) |
| 5 | 3 clientes, rama de palabras/Q3-Q4 replicada (ac, wm, P) |
| 6 | 3 clientes, todo replicado (incluye 2 Gateways) |
| 7 | 4 clientes repartidos en 2 Gateways (una réplica del resto) |
| 100+ | aleatorios (`make scenarios RANDOM_N=5 SEED=3`) |

Sin Docker (RabbitMQ falso, Wikimedia simulada):

    make test-scenarios                    # todos los fijos (1..7)
    make test-random RANDOM_N=10 SEED=3    # aleatorios

Con Docker (como `make switch` / `make test` de tp-coordinacion):

    make switch N=4                   # genera scenarios/*.yaml y copia el 4 a docker-compose.yaml
    make test-scenario                # build, levanta, corre los client_*, compara output/client_<i> (queda corriendo)
    make test-scenario KEEP=0         # igual, pero baja todo al final
    make switch SIZE=medium N=6 && make test-scenario SIZE=medium    # con medium_dataset / data/expected/medium
    make test-all-scenarios SIZE=small  # todos los escenarios fijos, uno tras otro, bajando cada uno

Cada cliente escribe en `output/client_<i>/` y se compara contra `data/expected/<size>/`.
Para otra combinación a mano: agregar una entrada a `SCENARIOS` en `tools/scenarios.py`.
