import json
import logging

from common.batch_stage import BatchStage
from common.ft.ids import msg_id
from common.protocol.messages import batch_message, stage_eof_message, with_id
from common.topology import Q2_CTRL

STAGE = "q2_filter"
BIOGRAPHY_SECTIONS = {"biography", "biographie"}  

def section_names(sections):
    result = set()
    for part in json.loads(sections):
        if part.get("type") == "section" and part.get("name"):
            result.add(part["name"].strip().lower())
    return result

def biography_rows(rows):
    result = []
    for row in rows:
        try:
            if BIOGRAPHY_SECTIONS & section_names(row["sections"]):
                result.append({"name": row["name"],"url": row["url"],"image_url": row["image_url"]})
        except (KeyError, TypeError, ValueError) as e:
            logging.warning("fila inválida (%s: %s), se descarta", type(e).__name__, e)
    return result

class Q2Filter(BatchStage):
    name = STAGE
    ctrl_exchange = Q2_CTRL

    def __init__(self, bus, replica_id, input_queue="q2_in", output_queue = "img_in", state_dir=None):
        self.input_queue = input_queue
        self.output_queue = output_queue
        super().__init__(bus, replica_id, state_dir)

    def declare_outputs(self):
        self.bus.declare(self.output_queue)

    def process_batch(self, client_id, message):
        batch_id = message['batch_id']
        out = batch_message(client_id, batch_id, biography_rows(message["rows"]), origin=self.replica_id)
        self.publish("", with_id(out, msg_id(client_id, STAGE, batch_id)), key=self.output_queue)

    def publish_eof(self, client_id, total):
        eof = stage_eof_message(client_id, STAGE, self.replica_id, total=total)
        self.publish("", with_id(eof, msg_id(client_id, STAGE + "-eof")), key=self.output_queue)

def main():
    from common.bus import RabbitBus
    from common.runner import broker_host, env_int, env_str, run_stage, state_dir
    rid = env_int("ID")
    run_stage(Q2Filter(RabbitBus(broker_host()), rid, env_str("INPUT_QUEUE", "q2_in"),
                       env_str("OUTPUT_QUEUE", "img_in"), state_dir(f"q2_filter_{rid}")))

if __name__ == "__main__":
    main()

