
from .messages import (
    BATCH,
    EOF,
    ABORT,
    RESULT,
    DONE,
    WELCOME,
    ERROR,
    batch_message,
    eof_message,
    abort_message,
    result_message,
    done_message,
    welcome_message,
    error_message,
)
from .framing import FrameError, send_frame, recv_frame


def send_msg(sock, message):
    send_frame(sock, message)


def recv_msg(sock):
    return recv_frame(sock)


def send_batch(sock, batch_id, rows):
    send_msg(sock, {
        "type": "batch",
        "batch_id": batch_id,
        "rows": rows,
    })


def send_eof(sock):
    send_msg(sock, {"type": "eof"})


def send_abort(sock, client_id):
    send_msg(sock, abort_message(client_id))


def send_result(sock, client_id, query, rows, origin=0):
    send_msg(
        sock,
        result_message(client_id, query, rows, origin),
    )


def send_done(sock, client_id):
    send_msg(sock, done_message(client_id))


def send_welcome(sock, client_id):
    send_msg(sock, welcome_message(client_id))


def send_error(sock, code, message, client_id=None):
    send_msg(sock, error_message(code, message, client_id))
