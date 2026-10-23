
from .messages import encode, decode


def serialize(message):
    """Serializa un mensaje interno para publicarlo en RabbitMQ."""
    return encode(message)


def deserialize(body):
    """Deserializa una publicación recibida de RabbitMQ."""
    return decode(body)


def serialize_article(article, client_id, batch_id, origin=0):
    return serialize({
        "type": "article",
        "client_id": tuple(client_id),
        "batch_id": batch_id,
        "origin": origin,
        "article": article,
    })


def deserialize_article(body):
    message = deserialize(body)
    if message["type"] != "article":
        raise ValueError("se esperaba un mensaje ARTICLE")
    return message["article"]


def serialize_eof(client_id, origin=0, stage=None):
    message = {
        "type": "eof",
        "client_id": tuple(client_id),
        "origin": origin,
    }
    if stage is not None:
        message["stage"] = stage
    return serialize(message)


def serialize_abort(client_id):
    return serialize({
        "type": "abort",
        "client_id": tuple(client_id),
    })
