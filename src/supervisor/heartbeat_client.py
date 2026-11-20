import logging
import socket
import time

RECV_SIZE = 1024   

class HeartbeatClient:

    def __init__(self, port, ping_timeout, stall_limit):
        self.port = port
        self.ping_timeout = ping_timeout
        self.stall_limit = stall_limit        # progreso para considerar vivo al nodo (permite detectar nodos colgados)
        self.seq = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def ping(self, nodes):
        self.seq += 1
        self._send_pings(nodes, self.seq)
        pongs = self._collect_pongs(nodes, self.seq)
        pong_results = {}
        pong_results = {}
        for node in nodes:
            if node in pongs and self._is_alive(pongs[node]):
                pong_results[node] = True
            else:
                pong_results[node] = False
        return pong_results
    
    def close(self):
        self.sock.close()

    def _address_of(self, node):
        return (node, self.port)

    def _send_pings(self, nodes, seq):
        message = f"PING {seq}".encode()
        for node in nodes:
            try:
                self.sock.sendto(message, self._address_of(node))
            except OSError as e:
                logging.debug("no se pudo mandar PING a '%s': %s", node, e)

    def _collect_pongs(self, nodes, seq):
        wanted = set(nodes)
        pongs = {}
        deadline = time.monotonic() + self.ping_timeout

        while len(pongs) < len(wanted):
            left = deadline - time.monotonic()
            if left <= 0:
                break
            self.sock.settimeout(left)
            try:
                data, _ = self.sock.recvfrom(RECV_SIZE)
            except socket.timeout:
                break
            except OSError as e:                
                logging.debug("error leyendo PONG: %s", e)
                break

            try:
                pong_seq, node, progress = self._parse_pong(data)
            except ValueError as e:
                logging.debug("PONG ignorado (%s): %r", e, data[:100])
                continue

            if pong_seq != seq or node not in wanted:
                continue                        # de otro ciclo o de un nodo que no se pidio
            pongs[node] = progress

        return pongs

    @staticmethod
    def _parse_pong(data):
        parts = data.decode().split()          
        if len(parts) != 4 or parts[0] != "PONG":
            raise ValueError("no es un PONG")
        return int(parts[1]), parts[2], float(parts[3])

    def _is_alive(self, progress):
        return progress < self.stall_limit
