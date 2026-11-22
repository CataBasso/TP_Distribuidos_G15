#Construye el msg_id con el client_id, la etapa que genera el mensaje, el batch_id y la particion de destino
def msg_id(client_id, stage, batch_id=0, part=0) -> str:
    return f"{int(client_id[0])}.{int(client_id[1])}/{stage}/{batch_id}/{part}"

#Convierte la tupla (gateway, n) en texto
def client_key(client_id) -> str:
    return f"{int(client_id[0])}.{int(client_id[1])}"

#Inverso a client_key
def parse_client_key(key: str) -> tuple:
    gw, n = key.split(".")
    return int(gw), int(n)
