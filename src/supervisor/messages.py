ELECTION = "ELECTION"
OK = "OK"
COORDINATOR = "COORDINATOR"
HELLO = "HELLO"     

def encode(message):
    type = message['type']
    id = message['id']
    return f"{type} {id}".encode()

def decode(data):
    data_dict = data.decode().split()
    if len(data_dict) != 2:
        raise ValueError("se esperaban 2 campos")
    
    type = data_dict[0]
    id = int(data_dict[1])
    if type not in (ELECTION, OK, COORDINATOR, HELLO):
        raise ValueError(f"tipo desconocido: {type}")

    return {'type': type, 'id': id}


def election(sender_id):
    return {'type': ELECTION, 'id': sender_id}


def ok(sender_id):
    return {'type': OK, 'id': sender_id}


def coordinator(sender_id):
    return {'type': COORDINATOR, 'id': sender_id}


def hello(sender_id):
    return {'type': HELLO, 'id': sender_id}
