# TP_Distribuidos_G15

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
