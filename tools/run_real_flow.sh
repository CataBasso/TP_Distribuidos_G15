set -e
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
export PYTHONPATH=src
export MOM_HOST=${RABBIT_HOST:-localhost} RABBIT_HOST=${RABBIT_HOST:-localhost}
export CLEANER_REPLICAS=1 LM_REPLICAS=2 AVG_PARTITIONS=2 Q5_PARTITIONS=2 WORD_PARTITIONS=1
pids=()
trap 'kill "${pids[@]}" 2>/dev/null' EXIT INT TERM
start() { "$@" & pids+=($!); }
# Orden: primero el final de la cadena (así cada cola existe antes de que se publique en ella)
start env GW_ID=1 $PY -m results.joiner
for k in 0 1; do start env PARTITION_ID=$k SPOOL_DIR=spool/q5_$k $PY -m filters.q5_filter; done
for a in 0 1; do start env PARTITION_ID=$a $PY -m text_processing.avg_reducer; done
for r in 0 1; do start env REPLICA_ID=$r $PY -m text_processing.length_mapper; done
start $PY -m devtools.stand_ins words
start env ID=0 $PY -m devtools.stand_ins cleaner
start env ID=0 INPUT_QUEUE=img_in $PY -m image.image_getter
start env ID=0 INPUT_QUEUE=q1_in $PY -m filters.query1
start env ID=0 INPUT_QUEUE=q2_in OUTPUT_QUEUE=img_in $PY -m filters.query2
start env ID=0 INPUT_QUEUE=year_in Q1_QUEUE=q1_in Q2_QUEUE=q2_in NORM_QUEUE=norm_in $PY -m filters.year_filter
start env GATEWAY_ID=1 GATEWAY_PORT=${GATEWAY_PORT:-5000} $PY -m gateway
echo "sistema levantado (pids: ${pids[*]}). En otra terminal: make run-client"
wait
