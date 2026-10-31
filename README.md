# TP Distribuidos – Grupo 15

## Client y Gateway

Cómo correrlos (con RabbitMQ levantado: `make rabbit-up`):

    make setup
    make run-gateway     # terminal 1: escucha en :5000
    make run-client      # terminal 2: envía data/sample/small_dataset y escribe output/

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
