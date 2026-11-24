from common.batch_stage import BatchStage
from common.ft.ids import msg_id
from common.protocol.messages import eof_message, result_message, with_id
from common.topology import Q1_CTRL, RESULTS_EX, results_key

STAGE = "q1_filter"
QUERY = 1
ENGLISH = "en" 

def rows_in_english(rows):
    english_rows = []
    for r in rows:
        if r.get("lang") == ENGLISH:
            english_rows.append({"date_modified": r["date_modified"], "url": r["url"]})
    return english_rows

class Q1Filter(BatchStage):
    name = STAGE
    ctrl_exchange = Q1_CTRL

    def __init__(self, bus, replica_id, input_queue="q1_in", state_dir=None):
        self.input_queue = input_queue
        super().__init__(bus, replica_id, state_dir)

    def process_batch(self, client_id, message):
        out = result_message(client_id, QUERY, rows_in_english(message["rows"]), origin=self.replica_id)
        self.publish(RESULTS_EX, with_id(out, msg_id(client_id, STAGE, message["batch_id"])), key=results_key(client_id))

    def publish_eof(self, client_id, total):
        eof = eof_message(client_id, origin=self.replica_id)
        eof["query"] = QUERY
        self.publish(RESULTS_EX, with_id(eof, msg_id(client_id, STAGE + "-eof")), key=results_key(client_id))

def main():
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage, state_dir
    rid = env_int("ID")
    run_stage(Q1Filter(RabbitBus(broker_host()), rid, env_str("INPUT_QUEUE", "q1_in"),state_dir(f"q1_filter_{rid}")))

if __name__ == "__main__":
    main()
