import os
import queue
import socket
import threading
import time

from common import safe_socket
from common.health import HealthResponder
from common.logger import logger
from common.logger.logger import LogResult
from . import messages
from .election import IDLE, Election
from .heartbeat_client import HeartbeatClient
from .monitor import Monitor
from .peers import MAX_MESSAGE_SIZE, PeerHandler
from .restarter import Restarter

POLL_STEP = 0.1    
ACCEPT_TIMEOUT = 0.5      
HANDSHAKE_TIMEOUT = 1.0  
INITIAL_CONNECT_WAIT = 4.0  

class Supervisor:

    def __init__(self, config, heartbeat_client=None, restarter=None):
        self.config = config
        self.client = heartbeat_client or HeartbeatClient(config.heartbeat_port, config.ping_timeout, config.stall_limit)
        self.restarter = restarter or Restarter()
        self.responder = HealthResponder(config.node_name, config.heartbeat_port)   # contesta los PING de los demás
        self.election = Election(config.supervisor_id, config.peers, config.election_timeout,config.coordinator_timeout, send=self._send_election)
        self.node_monitor = Monitor(config.targets, config.miss_limit)   
        self.leader_monitor = None           
        self.events = queue.SimpleQueue()     
        self.peer_handlers = {peer_id: PeerHandler(peer_id, self.events) for peer_id in config.peers}
        self.listener = None                 
        self.restarted = queue.SimpleQueue()  
        self.restarting = set()              
        self.was_leader = False
        self.stop_event = threading.Event()

    def run(self):
        logger.info("supervisor_started", LogResult.in_progress, "id", self.config.supervisor_id, "targets", len(self.config.targets), "peers", sorted(self.config.peers))
        try:
            self.responder.start()
            self._start_network()
            self._wait_for_peers(INITIAL_CONNECT_WAIT)
            self.election.start_election(time.monotonic())
            while not self.stop_event.is_set():
                started = time.monotonic()
                try:
                    self.run_cycle(started)
                except Exception as e:               # un error en un ciclo no deja al sistema sin supervisor
                    logger.error("supervisor_cycle", LogResult.fail, "error", repr(e))
                self._wait_until(started + self.config.ping_interval)
        finally:
            self.close()
            logger.info("supervisor_stopped", LogResult.success, "id", self.config.supervisor_id)

    def _wait_for_peers(self, timeout):
        deadline = time.monotonic() + timeout
        while not all(h.is_connected() for h in self.peer_handlers.values()):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self.stop_event.wait(min(POLL_STEP / 2, remaining)):
                break
        missing = sorted(pid for pid, h in self.peer_handlers.items() if not h.is_connected())
        if missing:
            logger.warning("peers_missing_at_start", LogResult.fail, "peers", missing)

    def stop(self):
        self.stop_event.set()

    def close(self):
        try:
            self._close_network()
        finally:                                # si falla una cosa, igual se cierran las otras
            try:
                self.client.close()
            finally:
                self.responder.close()

    def run_cycle(self, now):
        self.responder.touch()
        self._drain_restarted()
        self._sync_role()
        if self.election.is_leader():
            self._lead(now)
        else:
            self._follow(now)

    def _wait_until(self, deadline):
        while True:
            now = time.monotonic()
            try:
                self._poll_election(now)
            except Exception as e:          
                logger.error("election_poll", LogResult.fail, "error", repr(e))
            if now >= deadline:
                return
            if self.stop_event.wait(min(POLL_STEP, deadline - now)):
                return

    def _poll_election(self, now):
        self.responder.touch()
        self._receive_election_messages(now)
        self.election.tick(now)
        self._sync_role()

    def _start_network(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("", self.config.election_port))
            listener.listen()
            listener.settimeout(ACCEPT_TIMEOUT)
        except OSError:
            listener.close()
            raise                             
        self.listener = listener
        threading.Thread(target=self._accept_lower_peer_connections, daemon=True).start()
        self._connect_to_higher_peers()

    def _close_network(self):
        self.stop_event.set()
        if self.listener is not None:
            self.listener.close()
        for handler in self.peer_handlers.values():
            handler.close()

    def _election_address(self, peer_id):
        return (self.config.peers[peer_id], self.config.election_port)

    def _accept_lower_peer_connections(self):
        while not self.stop_event.is_set():
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:                   
                return
            peer_id = self._read_hello(sock)
            if peer_id is None:
                sock.close()
                continue
            self.peer_handlers[peer_id].accept(sock)

    def _read_hello(self, sock):
        try:
            sock.settimeout(HANDSHAKE_TIMEOUT)
            data = safe_socket.recv_packet(sock, max_size=MAX_MESSAGE_SIZE)
            hello = messages.decode(data) if data is not None else None
            sock.settimeout(None)
        except (OSError, safe_socket.FrameError, ValueError) as e:
            logger.debug("peer_handshake", LogResult.fail, "error", e)
            return None
        if hello is None or hello['type'] != messages.HELLO or hello['id'] not in self.peer_handlers:
            logger.debug("peer_handshake", LogResult.fail, "error", "invalid_hello", "hello", hello)
            return None
        return hello['id']

    def _connect_to_higher_peers(self):
        for peer_id, handler in self.peer_handlers.items():
            if peer_id > self.config.supervisor_id:
                handler.connect(lambda peer_id=peer_id: self._election_address(peer_id), self.config.supervisor_id)

    def _receive_election_messages(self, now):
        while True:
            try:
                kind, peer_id, message = self.events.get_nowait()
            except queue.Empty:
                return
            if kind == PeerHandler.EventType.MESSAGE:
                self.election.on_message(message, now)
            elif kind == PeerHandler.EventType.DISCONNECT:
                self._on_peer_disconnect(peer_id, now)

    def _on_peer_disconnect(self, peer_id, now):
        if peer_id == self.election.leader and not self.election.is_leader():
            self.election.start_election(now)

    def _send_election(self, peer_id, message):
        if not self.peer_handlers[peer_id].send(messages.encode(message)):
            logger.debug("election_send", LogResult.fail, "peer_id", peer_id, "type", message['type'], "reason", "no_connection")

    def _sync_role(self):
        is_leader = self.election.is_leader()
        if is_leader != self.was_leader:
            if is_leader:
                logger.info("became_leader", LogResult.success, "id", self.config.supervisor_id)
            else:
                logger.info("ceased_leader", LogResult.success, "id", self.config.supervisor_id)
            self.node_monitor.reset()
            self.leader_monitor = None
            self.was_leader = is_leader

    def _lead(self, now):
        pinged = self.client.ping(self.config.targets)
        miss_limit_reached = self.node_monitor.update(pinged)
        for node in miss_limit_reached:
            logger.warning("node_declared_dead", LogResult.fail, "node", node)
            self._launch_restart(node)

    def _follow(self, now):
        if self.election.leader is None or self.election.state != IDLE or self.election.is_leader():
            return
        leader = self.config.peers[self.election.leader]
        if self.leader_monitor is None or self.leader_monitor.nodes != [leader]:
            self.leader_monitor = Monitor([leader], self.config.leader_miss_limit)
        pinged = self.client.ping([leader])
        if self.leader_monitor.update(pinged):
            logger.warning("leader_declared_dead", LogResult.fail, "leader", leader)
            self.leader_monitor = None          
            self.election.start_election(now)

    def _launch_restart(self, node):
        if node in self.restarting:
            return
        self.restarting.add(node)
        threading.Thread(target=self._restart_worker, args=(node,), daemon=True).start()

    def _restart_worker(self, node):
        try:
            self.restarter.restart(node)
        except Exception as e:                     
            logger.error("node_restart", LogResult.fail, "node", node, "error", repr(e))
        finally:                                
            self.restarted.put(node)

    def _drain_restarted(self):
        while True:
            try:
                node = self.restarted.get_nowait()
            except queue.Empty:
                return
            self.restarting.discard(node)
            self.node_monitor.mark_restarted(node)
