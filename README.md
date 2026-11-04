# TP Distribuidos – Grupo 15

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

Levanta todo el sistema con Docker Compose (`docker-compose.yaml`), espera a que `client_0`
termine de enviar `data/sample/medium_dataset` y recibir los resultados en `output/`, corre
`tests/test_querys.py` (compara contra `data/expected/medium`) y baja el sistema, sin
importar si los tests pasaron o fallaron. El código de salida es el de pytest.
`output/` se vacía al empezar.

Para una sola query, con el sistema ya corrido y `output/` completo: `make test-query1` … `make test-query5`.

## Levantar y bajar el sistema

    make up      # construye las imágenes y levanta todo; luego sigue los logs
    make down    # detiene y elimina los contenedores

`make up` crea `output/` si no existe, corre `docker compose up --build --remove-orphans --detach`
sobre `docker-compose.yaml` y deja corriendo `docker compose logs --follow` con los logs de todas
las réplicas. Ctrl+C corta los logs pero **no** baja el sistema. `client_0` envía
`data/sample/medium_dataset` y escribe los resultados en `output/`.

`make down` hace `stop -t 5` y `down`. Para ver los logs sin seguirlos: `make logs`.

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

    make switch N=4          # genera scenarios/*.yaml y copia el 4 a docker-compose.yaml
    make test-scenario       # up --build, espera a los client_*, compara output/client_<i> y baja todo
    make test-scenario SIZE=medium    # con medium_dataset / data/expected/medium

Cada cliente escribe en `output/client_<i>/` y se compara contra `data/expected/<size>/`.
Para otra combinación a mano: agregar una entrada a `SCENARIOS` en `tools/scenarios.py`.
