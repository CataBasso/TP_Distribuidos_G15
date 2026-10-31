#!/bin/bash
# Levanta en segundo plano las etapas de las tareas 7 y 8 con la configuración de la demo:
#   2 LengthMapper, A=2 AvgReducer, K=2 Q5Filter, 1 Joiner (gw 1, P=2). Los EOF del
#   AbstractCleaner simulado son 2 (CLEANER_REPLICAS=2). Ctrl-C los detiene a todos.
# Uso: ./tools/run_stages.sh        (desde la raíz del repo, con RabbitMQ andando)
set -e
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
export PYTHONPATH=src RABBIT_HOST=${RABBIT_HOST:-localhost}
export CLEANER_REPLICAS=2 LM_REPLICAS=2 AVG_PARTITIONS=2 Q5_PARTITIONS=2 WORD_PARTITIONS=2
pids=()
trap 'kill "${pids[@]}" 2>/dev/null' EXIT INT TERM
start() { "$@" & pids+=($!); }
# primero los consumidores del final de la cadena: así cada cola existe antes de que se publique
start env GW_ID=1 $PY -m results.joiner
for k in 0 1; do start env PARTITION_ID=$k SPOOL_DIR=spool/q5_$k $PY -m filters.q5_filter; done
for a in 0 1; do start env PARTITION_ID=$a $PY -m text_processing.avg_reducer; done
for r in 0 1; do start env REPLICA_ID=$r $PY -m text_processing.length_mapper; done
echo "etapas levantadas (pids: ${pids[*]}); Ctrl-C para detenerlas"
wait
