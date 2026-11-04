"""ResultTracker: conteo de EOF por consulta (1 + 1 + P + 1 + K), sin sockets ni broker."""
import threading

import pytest

from common.topology import expected_result_eofs
from gateway.results_tracker import ResultTracker

P, K = 3, 2
C = (1, 1)


def tracker():
    return ResultTracker(expected_result_eofs(P, K))


def all_eofs(skip=()):
    plan = [(1, 0), (2, 0), (4, 0)] + [(3, p) for p in range(P)] + [(5, k) for k in range(K)]
    return [e for e in plan if e not in skip]


def feed(t, client, eofs):
    return [t.add(client, q, o) for q, o in eofs]


def test_completa_solo_con_el_ultimo_eof():
    t = tracker()
    done = feed(t, C, all_eofs())
    assert done[:-1] == [False] * (len(done) - 1) and done[-1] is True


def test_falta_un_eof_de_q3():
    t = tracker()
    assert not any(feed(t, C, all_eofs(skip={(3, 2)})))
    assert t.pending(C)[3] == (2, P)
    assert t.add(C, 3, 2) is True


def test_falta_un_eof_de_q5():
    t = tracker()
    assert not any(feed(t, C, all_eofs(skip={(5, 1)})))
    assert t.pending(C)[5] == (1, K)


def test_eof_repetido_del_mismo_origen_no_cuenta_dos_veces():
    t = tracker()
    feed(t, C, all_eofs(skip={(3, 1), (3, 2)}))
    assert t.add(C, 3, 0) is False and t.add(C, 3, 0) is False
    assert t.pending(C)[3] == (1, P)


def test_clientes_concurrentes_se_cuentan_por_separado():
    t = tracker()
    c2 = (1, 2)
    assert feed(t, C, all_eofs())[-1] is True
    assert not any(feed(t, c2, all_eofs(skip={(2, 0)})))
    assert t.add(c2, 2, 0) is True


def test_consulta_desconocida_es_un_error():
    with pytest.raises(ValueError):
        tracker().add(C, 9, 0)
    with pytest.raises(ValueError):
        tracker().add(C, None, 0)


def test_forget_descarta_el_estado():
    t = tracker()
    feed(t, C, all_eofs(skip={(5, 0)}))
    t.forget(C)
    assert t.pending(C)[1] == (0, 1)
    assert t.add(C, 5, 0) is False                      # empieza de cero


def test_el_origen_de_q1_y_q2_puede_ser_cualquier_replica():
    """Q1Filter e ImageGetter mandan eof(query, origin=réplica): cuenta 1 sea cual sea."""
    t = tracker()
    eofs = [(1, 5), (2, 3)] + [(3, p) for p in range(P)] + [(4, 0)] + [(5, k) for k in range(K)]
    assert feed(t, C, eofs)[-1] is True


def test_un_cliente_completo_no_deja_estado():
    t = tracker()
    feed(t, C, all_eofs())
    assert t.pending(C)[1] == (0, 1)


def test_es_seguro_con_hilos():
    t = tracker()
    results = []

    def worker(origins):
        for q, o in origins:
            results.append(t.add(C, q, o))
    plan = all_eofs()
    threads = [threading.Thread(target=worker, args=(plan[i::4],)) for i in range(4)]
    for th in threads: th.start()
    for th in threads: th.join()
    assert results.count(True) == 1                     # exactamente un hilo ve el cierre
