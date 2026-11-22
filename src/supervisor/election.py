
from . import messages

IDLE = "IDLE"                          # no hay elección en curso
ELECTING = "ELECTING"                  # mandé ELECTION y espero algún OK
WAITING_COORDINATOR = "WAITING"        # alguien mayor contestó OK: espero su COORDINATOR


class Election:

    def __init__(self, supervisor_id, peers, election_timeout, coordinator_timeout, send):
        self.id = supervisor_id
        self.peers = peers                          # {id: nombre} de los OTROS Supervisores
        self.election_timeout = election_timeout
        self.coordinator_timeout = coordinator_timeout
        self.send = send                            # send(peer_id, mensaje: dict)
        self.state = IDLE
        self.leader = None                          # id del líder actual, o None si no se sabe
        self.deadline = None                        # instante en que vence la espera actual

    def start_election(self, now):
        self.state = ELECTING
        higher_peers = self._higher_peers()
        if not higher_peers:
            self._become_leader()
        else:
            self.deadline = now + self.election_timeout
            for peer_id in higher_peers:
                self.send(peer_id, messages.election(self.id))
    
    def on_message(self, message, now):
        if message['type'] == messages.ELECTION:
            self.send(message['id'], messages.ok(self.id))
            self.start_election(now)
        elif message['type'] == messages.OK and self.state == ELECTING:
            self.state = WAITING_COORDINATOR
            self.deadline = now + self.coordinator_timeout
        elif message['type'] == messages.COORDINATOR:
            if message['id'] >= self.id:
                self.state = IDLE
                self.deadline = None
                self.leader = message['id']
            else:
                self.start_election(now)
        
 
    def tick(self, now):
        if self.state == ELECTING and now >= self.deadline:
            self._become_leader()
        if self.state == WAITING_COORDINATOR and now >= self.deadline:
            self.start_election(now)
            
    def is_leader(self):
        return self.leader == self.id

    def _higher_peers(self):
        higher_peers = []
        for peer_id in self.peers:
            if peer_id > self.id:
                higher_peers.append(peer_id)
        return higher_peers

    def _become_leader(self):
        self.leader = self.id
        self.state = IDLE
        self.deadline = None
        for peer_id in self.peers:
            self.send(peer_id, messages.coordinator(self.id))
