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

## LengthMapper, AvgReducer, Q5Filter y Joiner (tareas 7 y 8)

| Etapa | Módulo | Qué hace |
|---|---|---|
| LengthMapper | `text_processing/length_mapper.py` | cuenta palabras únicas; ≤2020 → promedio, ≥2021 → Q5 |
| AvgReducer | `text_processing/avg_reducer.py` | promedio por cliente (partición `crc32(client_id) % A`) |
| Q5Filter | `filters/q5_filter.py` | spool en disco; emite los artículos con n > promedio |
| Joiner | `results/joiner.py` | cuenta EOF por consulta (1+1+P+1+K), reenvía al Gateway y manda `done` |

Nombres de colas/exchanges y formatos de mensaje: `src/common/topology.py`.

Tests (sin broker): `make test-stages`.

Prueba manual con RabbitMQ (3 terminales):

    make rabbit-up
    make run-stages        # terminal 1: 2 LengthMapper, 2 AvgReducer, 2 Q5Filter, 1 Joiner
    make demo-watch        # terminal 2: lee out_q.1 y verifica (espera hasta 30 s)
    make demo-feed         # terminal 3: publica tokens, EOF y resultados Q1-Q4 simulados

Resultado esperado: `Q5 n = [6, 8]` y `=> OK`. Variables de cada etapa: ver `main()` del módulo.

