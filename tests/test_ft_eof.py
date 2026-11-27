from common.ft.batch_eof import BatchEof
from common.ft.ids import client_key
from common.utils.spool import BlockSpool

C = (1, 7)


# ---------------------------------------------------------------- BatchEof
class Red:
    """Bus de control entre réplicas de una etapa (en memoria)."""
    def __init__(self):
        self.members, self.out = [], []

    def send(self, replica):
        """Devuelve la función `send_control` de una réplica: entrega el mensaje a todas las demás."""
        def _s(msg):
            for m in self.members:
                if m is not replica:
                    m.on_control(msg)
        return _s


def replicas(n, tmp_path=None, emit_all=False):
    """Arma n réplicas de BatchEof conectadas entre sí. Devuelve (réplicas, lista de EOF emitidos)."""
    red, eofs = Red(), []
    out = []
    for r in range(n):
        d = str(tmp_path / f"r{r}") if tmp_path else None
        be = BatchEof("etapa", r, None, lambda cid, total, r=r: eofs.append((r, cid, total)),
                      state_dir=d, emit_all=emit_all, fsync="never")
        be.send_control = red.send(be)
        red.members.append(be)
        out.append(be)
    return out, eofs


def test_eof_se_emite_una_sola_vez_cuando_la_union_de_las_replicas_cubre_todo():
    """Dos réplicas se reparten los batches: el EOF sale una vez, de la que lo consumió."""
    (a, b), eofs = replicas(2)
    a.batch_done(C, 0); b.batch_done(C, 1); a.batch_done(C, 2)
    a.eof_seen(C, 4)                                    # la réplica a consumió el EOF: total=4
    assert eofs == []                                   # falta el batch 3
    b.batch_done(C, 3)
    assert eofs == [(0, C, 4)]                          # emite quien tenía el EOF, una vez
    assert a.is_closed(C) and b.is_closed(C)


def test_reentregas_no_adelantan_el_eof():
    """Procesar el mismo batch dos veces no cuenta doble: no se termina antes de tiempo."""
    (a, b), eofs = replicas(2)
    a.eof_seen(C, 3)
    for _ in range(5):
        a.batch_done(C, 0)                              # el mismo batch reentregado
    b.batch_done(C, 1)
    assert eofs == []
    a.batch_done(C, 2)
    assert len(eofs) == 1


def test_emit_all_hace_que_cada_replica_emita_su_eof():
    """Con emit_all (mappers) todas las réplicas emiten su propio EOF."""
    (a, b), eofs = replicas(2, emit_all=True)
    a.batch_done(C, 0); b.batch_done(C, 1)
    a.eof_seen(C, 2)
    assert sorted(r for r, _c, _t in eofs) == [0, 1]


def test_la_replica_que_cae_recupera_su_cobertura_del_disco_y_el_eof_se_completa(tmp_path):
    """Una réplica "muere" y se recrea con el mismo directorio: recupera lo suyo y termina el cliente."""
    (a, b), eofs = replicas(2, tmp_path)
    a.batch_done(C, 0)
    b.batch_done(C, 1)
    a.eof_seen(C, 3)
    # a cae y vuelve: lo persistido (own/hold/total) se recupera del WAL
    a2 = BatchEof("etapa", 0, b.on_control, lambda cid, total: eofs.append((0, cid, total)),
                  state_dir=str(tmp_path / "r0"), fsync="never")
    assert a2.own[client_key(C)] == {0} and client_key(C) in a2.holds
    b.send_control = lambda m: a2.on_control(m)
    a2.send_control = b.on_control
    a2.resync()                                         # pide a las otras réplicas lo que no persistió (others)
    a2.batch_done(C, 2)                                 # el batch que faltaba (reentregado)
    assert eofs and eofs[-1][2] == 3


def test_una_replica_que_vuelve_re_anuncia_lo_ya_terminado_aunque_el_sync_del_par_se_haya_perdido(tmp_path):
    """Regresión (cuelgue en test_cada_etapa_cae_varias_veces): a pidió `sync` mientras b estaba
    caída y el pedido se perdió. Al volver, b tiene que anunciar por su cuenta que ya cubrió el
    cliente; si no, a esperaría para siempre."""
    (a, b), eofs = replicas(2, tmp_path)
    a.eof_seen(C, 4)                    # a consumió el EOF (total 4) y procesó los batches 1 y 3
    a.batch_done(C, 1); a.batch_done(C, 3)
    b.batch_done(C, 0); b.batch_done(C, 2)   # b procesó 0 y 2: la unión cubre todo
    a.others.clear()                    # a no se enteró (b estaba caída cuando avisó): a sigue pendiente
    a.done.pop(client_key(C), None)
    assert client_key(C) in b.done      # b ya terminó el cliente
    assert not a.own.get(client_key(C), set()) >= {0, 1, 2, 3}
    b.resync()                          # b reinicia y re-anuncia su cobertura
    assert client_key(C) in a.done      # a se entera, completa y termina


def test_abort_olvida_al_cliente_y_los_mensajes_tardios_se_ignoran():
    """Tras un abort se borra el estado del cliente y los mensajes posteriores se descartan."""
    (a, b), eofs = replicas(2)
    a.batch_done(C, 0)
    a.abort(C)
    a.eof_seen(C, 1)
    assert eofs == [] and a.is_closed(C)


# ---------------------------------------------------------------- BlockSpool
def test_blockspool_sobrevive_a_un_reinicio_y_descarta_la_cola_rota(tmp_path):
    s = BlockSpool(tmp_path, fsync="never")
    s.append(C, "m0", 0, [["u0", 5]])
    s.append(C, "m1", 1, [["u1", 6]])
    s.close()
    path = s.path(C)
    path.write_bytes(path.read_bytes()[:-2])            # cola rota
    s2 = BlockSpool(tmp_path, fsync="never")            # un proceso nuevo NO borra el spool
    assert s2.clients() == [C]
    assert s2.read(C) == [("m0", 0, [["u0", 5]])]
    s2.delete(C)
    assert not s2.exists(C)
