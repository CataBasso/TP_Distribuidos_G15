from common.batch_stage import BatchStage
from common.ft.ids import msg_id
from common.protocol.messages import batch_message, stage_eof_message, with_id
from common.topology import YEAR_CTRL
from filters.year_filter.year_logic import split_rows

STAGE = "year_filter"

class YearFilter(BatchStage):
    name = STAGE
    ctrl_exchange = YEAR_CTRL


    def __init__(self, bus, replica_id, input_queue="year_in", output_queues=("q1_in", "q2_in", "norm_in"),state_dir=None):
        self.input_queue = input_queue
        self.output_queues = output_queues
        super().__init__(bus, replica_id, state_dir)

    def declare_outputs(self):
        for queue in self.output_queues:
            self.bus.declare(queue)

    def process_batch(self, client_id, message):
        batch_id = message["batch_id"]
        parts = split_rows(message["rows"])
        for part, (queue, rows) in enumerate(zip(self.output_queues, parts)):
            out = batch_message(client_id, batch_id, rows, origin=self.replica_id)
            self.publish("", with_id(out, msg_id(client_id, STAGE, batch_id, part)), key=queue)

    def publish_eof(self, client_id, total):
        for part, queue in enumerate(self.output_queues):
            eof = stage_eof_message(client_id, STAGE, self.replica_id, total=total)
            self.publish("", with_id(eof, msg_id(client_id, STAGE + "-eof", 0, part)), key=queue)

def main():
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage, state_dir
    rid = env_int("ID")
    outputs = (env_str("Q1_QUEUE", "q1_in"), env_str("Q2_QUEUE", "q2_in"), env_str("NORM_QUEUE", "norm_in"))
    run_stage(YearFilter(RabbitBus(broker_host()), rid, input_queue=env_str("INPUT_QUEUE", "year_in"),output_queues=outputs, state_dir=state_dir(f"year_filter_{rid}")))

if __name__ == "__main__":
    main()
