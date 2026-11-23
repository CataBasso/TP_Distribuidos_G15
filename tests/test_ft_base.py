import pytest

from common.ft import failpoints
from common.ft.coverage import covers, from_ranges, to_ranges
from common.ft.ids import client_key, msg_id, parse_client_key
from common.ft.journal import Journal, read_frames
from common.ft.store import StateStore

C = (1, 7)


# ---------------------------------------------------------------- failpoints
@pytest.fixture
def fp():
    failpoints.set_handler(lambda: (_ for _ in ()).throw(failpoints.SimulatedCrash()))
    yield failpoints
    failpoints.set_handler(None)
    failpoints.configure("")


def test_failpoint_por_cuenta_dispara_solo_en_la_n_esima_vez(fp):
    fp.configure("p:3")
    fp.hit("p"); fp.hit("p")
    with pytest.raises(fp.SimulatedCrash):
        fp.hit("p")
    fp.hit("p")                                        # una sola vez


def test_failpoint_sin_configurar_no_hace_nada(fp):
    fp.configure("")
    for _ in range(100):
        fp.hit("cualquiera")


def test_failpoint_probabilistico_con_semilla_es_reproducible(fp):
    def run():
        fp.configure("p@0.3", seed=4)
        out = []
        for _ in range(50):
            try:
                fp.hit("p"); out.append(0)
            except fp.SimulatedCrash:
                out.append(1)
        return out
    assert run() == run() and 0 < sum(run()) < 50


def test_failpoint_con_target_solo_afecta_a_los_hilos_con_ese_prefijo(fp):
    import threading
    fp.configure("p:1", target=("etapa_a",))
    fp.hit("p")                                        # hilo principal: ignorado
    seen = []

    def body():
        try:
            fp.hit("p")
        except fp.SimulatedCrash:
            seen.append("murio")
    t = threading.Thread(target=body, name="etapa_a-0")
    t.start(); t.join()
    assert seen == ["murio"]


def test_failpoint_por_cuenta_no_vuelve_a_disparar_despues_del_reinicio(fp, tmp_path, monkeypatch):
    """Con STATE_DIR (volumen del contenedor) el failpoint dispara una sola vez aunque el proceso
    vuelva a arrancar con la misma variable FAILPOINTS: si no, el Supervisor lo reiniciaría en bucle."""
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    fp.configure("p:2")
    fp.hit("p")
    with pytest.raises(fp.SimulatedCrash):
        fp.hit("p")
    fp.configure("p:2")                                  # "el proceso arranca de nuevo"
    for _ in range(5):
        fp.hit("p")                                      # ya no dispara
    fp.configure("p@1.0")                                # los probabilísticos sí siguen (caos)
    with pytest.raises(fp.SimulatedCrash):
        fp.hit("p")


# ---------------------------------------------------------------- ids y cobertura
def test_msg_id_es_determinista_y_no_depende_de_la_replica():
    assert msg_id((2, 9), "lm", 5, 1) == msg_id((2, 9), "lm", 5, 1) == "2.9/lm/5/1"
    assert msg_id((2, 9), "lm", 5, 1) != msg_id((2, 9), "lm", 5, 0)
    assert parse_client_key(client_key((2, 9))) == (2, 9)


def test_rangos_ida_y_vuelta_y_cobertura():
    ids = {0, 1, 2, 5, 6, 9}
    assert to_ranges(ids) == [[0, 2], [5, 6], [9, 9]]
    assert from_ranges(to_ranges(ids)) == ids
    assert covers({0, 1, 2}, 3) and not covers({0, 2}, 3) and covers(set(), 0)
    assert covers({0, 1, 2, 7}, 3)                      # ids de más (reentregas) no molestan


# ---------------------------------------------------------------- journal
def test_journal_recupera_lo_escrito_y_snapshot_compacta(tmp_path):
    j = Journal(tmp_path, fsync="never", snapshot_every=3)
    assert j.recover() == (None, [])
    for i in range(3):
        j.append(["x", i])
    assert j.should_snapshot()
    j.snapshot({"n": 3})
    j.append(["x", 3])
    j.close()
    state, wal = Journal(tmp_path, fsync="never").recover()
    assert state == {"n": 3} and wal == [["x", 3]]


def test_journal_descarta_la_cola_rota_de_una_escritura_a_medias(tmp_path):
    j = Journal(tmp_path, fsync="never")
    j.recover()
    for i in range(4):
        j.append(["r", i])
    j.close()
    wal = tmp_path / "wal.0.bin"
    data = wal.read_bytes()
    wal.write_bytes(data[:-3])                          # el proceso murió a mitad del último registro
    _, recs = Journal(tmp_path, fsync="never").recover()
    assert recs == [["r", 0], ["r", 1], ["r", 2]]
    j2 = Journal(tmp_path, fsync="never")
    j2.recover(); j2.append(["r", 99]); j2.close()      # y se puede seguir escribiendo sin corromper
    assert [r[1] for r in Journal(tmp_path, fsync="never").recover()[1]] == [0, 1, 2, 99]


def test_journal_detecta_un_registro_corrupto_por_crc(tmp_path):
    j = Journal(tmp_path, fsync="never")
    j.recover()
    j.append(["a", 1]); j.append(["b", 2]); j.close()
    wal = tmp_path / "wal.0.bin"
    raw = bytearray(wal.read_bytes())
    raw[-1] ^= 0xFF
    wal.write_bytes(bytes(raw))
    recs, _ = read_frames(wal)
    assert recs == [["a", 1]]


def test_wal_viejo_no_se_reaplica_si_la_caida_fue_entre_el_snapshot_y_el_borrado(tmp_path):
    j = Journal(tmp_path, fsync="never")
    j.recover()
    j.append(["a", 1])
    old = (tmp_path / "wal.0.bin").read_bytes()
    j.snapshot({"estado": "con a"})
    j.close()
    (tmp_path / "wal.0.bin").write_bytes(old)           # el WAL viejo quedó (no llegó a borrarse)
    state, recs = Journal(tmp_path, fsync="never").recover()
    assert state == {"estado": "con a"} and recs == []   # NO se aplica dos veces
    assert not (tmp_path / "wal.0.bin").exists()


# ---------------------------------------------------------------- StateStore
class Contador:
    def __init__(self, d):
        self.n = {}
        self.store = StateStore(d, self.apply, lambda: dict(self.n), self.load, fsync="never",
                                snapshot_every=5)
        self.store.recover()

    def apply(self, r):
        self.n[r[0]] = self.n.get(r[0], 0) + r[1]

    def load(self, s):
        self.n = dict(s)


def test_store_reconstruye_el_estado_tras_reiniciar(tmp_path):
    a = Contador(tmp_path)
    for i in range(12):
        a.store.commit(["k", 1])
        a.store.checkpoint()
    a.store.commit(["z", 5])
    b = Contador(tmp_path)                              # "reinicio": mismo directorio
    assert b.n == {"k": 12, "z": 5}


def test_store_sin_directorio_es_solo_memoria():
    a = Contador(None)
    a.store.commit(["k", 2])
    assert a.n == {"k": 2}
