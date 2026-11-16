docker compose exec -T rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers 2>/dev/null
echo; docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}'
