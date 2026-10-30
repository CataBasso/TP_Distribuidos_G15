
from . import external_serializer as serializer

# Mensajes externos: Client <-> Gateway
BATCH = 1
EOF = 2
ABORT = 3
RESULT = 4
DONE = 5
WELCOME = 6
ERROR = 7

# Mensajes internos: etapas del pipeline
ARTICLE = 20
FILTERED_ARTICLE = 21
TOKENS = 22
WORD_COUNTS = 23
PARTIAL_TOP = 24
AVERAGE_DATA = 25
AVERAGE_READY = 26
Q5_ARTICLE = 27


# EOF interno
STAGE_EOF = 28

# Coordinacion entre replicas
CONTROL = 29

TYPE_NAMES = {
    BATCH: "batch",
    EOF: "eof",
    ABORT: "abort",
    RESULT: "result",
    DONE: "done",
    WELCOME: "welcome",
    ERROR: "error",
    ARTICLE: "article",
    FILTERED_ARTICLE: "filtered_article",
    TOKENS: "tokens",
    WORD_COUNTS: "word_counts",
    PARTIAL_TOP: "partial_top",
    AVERAGE_DATA: "average_data",
    AVERAGE_READY: "average_ready",
    Q5_ARTICLE: "q5_article",
    STAGE_EOF: "stage_eof",
    CONTROL: "control",
}

NAME_TYPES = {name: code for code, name in TYPE_NAMES.items()}

# Columnas que escribe el cliente para cada consulta
RESULT_COLUMNS = {
    1: ("date_modified", "url"),
    2: ("name", "url", "image"),
    3: ("word",),
    4: ("word", "count"),
    5: ("url", "abstract_word_count"),
}

# Campos obligatorios por tipo
REQUIRED_FIELDS = {
    BATCH: ("batch_id", "rows"),
    EOF: (),
    ABORT: (),
    RESULT: ("query", "rows"),
    DONE: (),
    WELCOME: ("client_id",),
    ERROR: ("code", "message"),
    ARTICLE: ("article",),
    FILTERED_ARTICLE: ("query", "article"),
    TOKENS: ("url", "lang", "words", "period"),
    WORD_COUNTS: ("counts",),
    PARTIAL_TOP: ("counts",),
    AVERAGE_DATA: ("sum", "count"),
    AVERAGE_READY: ("average",),
    Q5_ARTICLE: ("url", "word_count"),
    STAGE_EOF: ("stage", "origin"),
    CONTROL: ("stage", "replica", "count"),
}

# Campos adicionales que pueden aparecer en cada mensaje
OPTIONAL_FIELDS = {
    BATCH: ("client_id", "origin"),
    EOF: ("client_id", "origin", "query", "stage", "total"),
    ABORT: ("client_id",),
    RESULT: ("client_id", "origin"),
    DONE: ("client_id",),
    WELCOME: (),
    ERROR: ("client_id",),
    ARTICLE: ("client_id", "batch_id", "origin"),
    FILTERED_ARTICLE: ("client_id", "batch_id", "origin"),
    TOKENS: ("client_id", "batch_id", "origin"),
    WORD_COUNTS: ("client_id", "origin"),
    PARTIAL_TOP: ("client_id", "origin"),
    AVERAGE_DATA: ("client_id", "batch_id", "origin"),
    AVERAGE_READY: ("client_id",),
    Q5_ARTICLE: ("client_id", "batch_id", "origin"),
    STAGE_EOF: ("client_id", "total"),
    CONTROL: ("client_id", "total"),
}


def client_id_of(message):
    gw_id, number = message["client_id"]
    return int(gw_id), int(number)


def validate_message(message):
    if not isinstance(message, dict):
        raise ValueError("el mensaje debe ser un diccionario")

    msg_type = message.get("type")
    if isinstance(msg_type, str):
        msg_type = NAME_TYPES.get(msg_type)
    if not isinstance(msg_type, int) or isinstance(msg_type, bool):
        raise ValueError("tipo de mensaje inválido")
    if msg_type not in TYPE_NAMES:
        raise ValueError(f"tipo de mensaje desconocido: {msg_type}")

    if "type" not in message:
        raise ValueError("mensaje sin tipo")

    for field in REQUIRED_FIELDS[msg_type]:
        if field not in message:
            raise ValueError(f"falta el campo obligatorio '{field}'")

    allowed = (
        {"type"}
        | set(REQUIRED_FIELDS[msg_type])
        | set(OPTIONAL_FIELDS[msg_type])
    )
    extra = set(message) - allowed
    if extra:
        raise ValueError(f"campos no permitidos: {sorted(extra)}")

    if "client_id" in message:
        client_id = message["client_id"]
        if (
            not isinstance(client_id, (tuple, list))
            or len(client_id) != 2
            or any(
                not isinstance(part, int) or isinstance(part, bool)
                or part < 0
                for part in client_id
            )
        ):
            raise ValueError("client_id debe ser (gw_id, n)")

    if msg_type == RESULT:
        query = message["query"]
        if query not in RESULT_COLUMNS:
            raise ValueError("consulta inválida")
        if not isinstance(message["rows"], list):
            raise ValueError("rows debe ser una lista")

    if msg_type == BATCH and not isinstance(message["rows"], list):
        raise ValueError("rows debe ser una lista")

    for field in ("total", "count"):
        if field in message:
            value = message[field]
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{field} debe ser un entero >= 0")
            
    if msg_type == ERROR:
        if not isinstance(message["code"], str):
            raise ValueError("code debe ser un string")
        if not isinstance(message["message"], str):
            raise ValueError("message debe ser un string")

    return message


def encode(message):
    validate_message(message)

    msg_type = message["type"]
    if isinstance(msg_type, str):
        msg_type = NAME_TYPES[msg_type]

    payload = dict(message)
    payload.pop("type")
    return (
        serializer.serialize_uint32(msg_type)
        + serializer.serialize_value(payload)
    )


def decode(data):
    if len(data) < serializer.UINT32_SIZE:
        raise ValueError("mensaje incompleto")

    msg_type = serializer.deserialize_uint32(data[:4])
    if msg_type not in TYPE_NAMES:
        raise ValueError(f"tipo de mensaje desconocido: {msg_type}")

    payload = serializer.deserialize_value(data[4:])
    if not isinstance(payload, dict):
        raise ValueError("el payload debe ser un diccionario")

    message = {"type": TYPE_NAMES[msg_type], **payload}
    validate_message(message)
    return message


def batch_message(client_id, batch_id, rows, origin=0):
    return {
        "type": "batch",
        "client_id": tuple(client_id),
        "batch_id": batch_id,
        "origin": origin,
        "rows": rows,
    }


def eof_message(client_id, origin=0, query=None, total=None):
    message = {
        "type": "eof",
        "client_id": tuple(client_id),
        "origin": origin,
    }
    if query is not None:
        message["query"] = query
    if total is not None:
        message["total"] = total
    return message

def abort_message(client_id):
    return {"type": "abort", "client_id": tuple(client_id)}


def result_message(client_id, query, rows, origin=0):
    return {
        "type": "result",
        "client_id": tuple(client_id),
        "query": query,
        "rows": rows,
        "origin": origin,
    }


def done_message(client_id):
    return {"type": "done", "client_id": tuple(client_id)}


def welcome_message(client_id):
    return {"type": "welcome", "client_id": tuple(client_id)}


def error_message(code, message, client_id=None):
    result = {"type": "error", "code": code, "message": message}
    if client_id is not None:
        result["client_id"] = tuple(client_id)
    return result

def article_message(article, client_id, batch_id, origin=0):
    return {
        "type": "article",
        "client_id": tuple(client_id),
        "batch_id": batch_id,
        "origin": origin,
        "article": article,
    }


def filtered_article_message(
    query, article, client_id, batch_id, origin=0
):
    return {
        "type": "filtered_article",
        "query": query,
        "article": article,
        "client_id": tuple(client_id),
        "batch_id": batch_id,
        "origin": origin,
    }


def tokens_message(url, lang, words, period, client_id, origin=0):
    return {
        "type": "tokens",
        "url": url,
        "lang": lang,
        "words": words,
        "period": period,
        "client_id": tuple(client_id),
        "origin": origin,
    }


def word_counts_message(counts, client_id, origin=0):
    return {
        "type": "word_counts",
        "counts": counts,
        "client_id": tuple(client_id),
        "origin": origin,
    }


def partial_top_message(counts, client_id, origin=0):
    return {
        "type": "partial_top",
        "counts": counts,
        "client_id": tuple(client_id),
        "origin": origin,
    }


def average_data_message(
    client_id, batch_id, total, count, origin=0
):
    return {
        "type": "average_data",
        "client_id": tuple(client_id),
        "batch_id": batch_id,
        "sum": total,
        "count": count,
        "origin": origin,
    }


def average_ready_message(client_id, average):
    return {
        "type": "average_ready",
        "client_id": tuple(client_id),
        "average": average,
    }


def q5_article_message(
    client_id, url, word_count, batch_id, origin=0
):
    return {
        "type": "q5_article",
        "client_id": tuple(client_id),
        "url": url,
        "word_count": word_count,
        "batch_id": batch_id,
        "origin": origin,
    }


def stage_eof_message(client_id, stage, origin, total=None):
    message = {
        "type": "stage_eof",
        "client_id": tuple(client_id),
        "stage": stage,
        "origin": origin,
    }
    if total is not None:
        message["total"] = total
    return message

def control_message(client_id, stage, replica, count, total=None):
    message = {
        "type": "control",
        "client_id": tuple(client_id),
        "stage": stage,
        "replica": replica,
        "count": count,
    }
    if total is not None:
        message["total"] = total
    return message