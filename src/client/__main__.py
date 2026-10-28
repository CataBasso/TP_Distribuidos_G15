import argparse
import logging
import os
import signal
import sys

from .client import Client, ClientError
from .dataset import DEFAULT_BATCH_ROWS, DEFAULT_BATCH_BYTES


def main(argv=None) -> int:
    env = os.environ.get
    p = argparse.ArgumentParser(description="Cliente del análisis de artículos de Wikipedia")
    p.add_argument("--gateway-host", default=env("GATEWAY_HOST", "localhost"))
    p.add_argument("--gateway-port", type=int, default=int(env("GATEWAY_PORT", "5000")))
    p.add_argument("--data-dir", default=env("DATA_DIR", "data/sample/small_dataset"))
    p.add_argument("--output-dir", default=env("OUTPUT_DIR", "output"))
    p.add_argument("--batch-rows", type=int, default=int(env("BATCH_ROWS", DEFAULT_BATCH_ROWS)))
    p.add_argument("--batch-bytes", type=int, default=int(env("BATCH_BYTES", DEFAULT_BATCH_BYTES)))
    p.add_argument("--log-level", default=env("LOG_LEVEL", "INFO"))
    args = p.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    client = Client(args.gateway_host, args.gateway_port, args.data_dir, args.output_dir,
                    args.batch_rows, args.batch_bytes)
    signal.signal(signal.SIGTERM, lambda *_: client.stop())
    try:
        client.run()
    except ClientError as e:
        logging.getLogger("client").error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
