"""
Mensajes del sistema (CONTRATO PROVISORIO del Gateway/Client).

Este módulo contiene solo lo que necesitan el Client y el Gateway. El paquete
common.protocol completo cuando  exista, estos nombres y formatos 
deben mantenerse o actualizarse en un solo lugar.

Todos los mensajes son diccionarios JSON. Los mensajes que viajan por RabbitMQ llevan el
encabezado común H = {client_id, batch_id, origin} y client_id = [gw_id, n]:
    gw_id  -> identifica al Gateway dueño del cliente (clave de ruteo de results_ex)
    n      -> identifica la conexión del cliente dentro de ese Gateway

Por RabbitMQ (Gateway -> year_in):
    batch  {type, client_id, batch_id, origin, rows}
    eof    {type, client_id, origin}
Por RabbitMQ (Gateway -> abort_ex, fanout):
    abort  {type, client_id}
Por RabbitMQ (Joiner -> out_q.<g>):
    result {type, client_id, query, rows}
    done   {type, client_id}

Por TCP (Client <-> Gateway) viajan los mismos tipos sin client_id (lo agrega el Gateway),
más 'welcome' y 'error'.
"""
import json

#Tipos de mensaje
BATCH = "batch"
EOF = "eof"
ABORT = "abort"
RESULT = "result"
DONE = "done"
WELCOME = "welcome"
ERROR = "error"

#Columnas de cada query en las filas de un 'result' (y en el CSV del cliente).
#Q2 lleva la imagen en base64 en 'image'; el cliente la guarda y escribe su sha256.
RESULT_COLUMNS = {
    1: ("date_modified", "url"),
    2: ("name", "url", "image"),
    3: ("word",),
    4: ("word", "count"),
    5: ("url", "abstract_word_count"),
}

#Convierte el diccionario a bytes 
def encode(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def decode(body: bytes) -> dict:
    message = json.loads(body.decode("utf-8"))
    if not isinstance(message, dict) or "type" not in message:
        raise ValueError("mensaje sin 'type'")
    return message

#client_id como tupla (gw_id, n) para usar de clave
def client_id_of(message: dict) -> tuple:
    gw_id, n = message["client_id"]
    return int(gw_id), int(n)

#Arma el mensaje que el Gateway publica en la cola yeaer_in
def batch_message(client_id, batch_id: int, origin: int, rows: list) -> dict:
    return {"type": BATCH, "client_id": list(client_id), "batch_id": batch_id,
            "origin": origin, "rows": rows}

#Aviso de que el cliente no tiene mas datos
def eof_message(client_id, origin: int) -> dict:
    return {"type": EOF, "client_id": list(client_id), "origin": origin}

#Cliente se fue. Se libera su estado
def abort_message(client_id) -> dict:
    return {"type": ABORT, "client_id": list(client_id)}

#Fragmento de resultado
def result_message(client_id, query: int, rows: list) -> dict:
    return {"type": RESULT, "client_id": list(client_id), "query": query, "rows": rows}

#Se entregó todo
def done_message(client_id) -> dict:
    return {"type": DONE, "client_id": list(client_id)}
