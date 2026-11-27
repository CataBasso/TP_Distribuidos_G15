"""Pruebas de tolerancia a fallas de punta a punta (ver tests/ft_harness.py y docs/FALLAS.md).

Cada caso mata una etapa (o el Gateway) en un punto exacto del código con un failpoint, la
reinicia con el mismo estado en disco y verifica que los CSV de todos los clientes sean idénticos
a los esperados: nada perdido, nada duplicado."""
import pytest

import scenarios as scn
from ft_harness import Cluster, run_case

SINGLE = scn.full(dict(clients=2))
REPLICATED = scn.full(dict(clients=2, year=2, q1=2, q2=2, img=2, ac=2, lm=2, wm=2, P=2, A=2, K=2))

GENERIC = ["before_persist:2", "after_persist:2", "after_publish:3", "before_ack:2",
           "after_ack:2", "after_eof_publish:1"]
# Etapas ya migradas a la librería de FT. YearFilter, Q1, Q2 e ImageGetter (Victoria, tarea 18) todavía
# no persisten estado: cuando las migre, sumar "year_0", "q1_0", "q2_0", "img_0" a esta lista.
STAGES = ["ac_0", "lm_0", "wm_0", "wr_0", "avg_0", "top", "q5_0"]


def fails(c):
    return f"terminó={c.finished} errores={c.errors} reinicios={c.restarts} {c.report()}"


@pytest.mark.parametrize("failpoint", GENERIC)
@pytest.mark.parametrize("stage", STAGES)
def test_una_etapa_cae_en_un_punto_exacto_y_el_resultado_no_cambia(stage, failpoint):
    good, c = run_case(SINGLE, failpoint, target=(stage,))
    assert good, fails(c)


@pytest.mark.parametrize("stage", ["ac_0", "lm_0", "wm_0", "wr_0", "q5_0"])
def test_con_replicas_una_cae_y_vuelve_sin_alterar_el_resultado(stage):
    good, c = run_case(REPLICATED, "before_ack:2", target=(stage,))
    assert good, fails(c)


@pytest.mark.parametrize("failpoint", ["after_spool_append:2", "during_replay:1"])
def test_q5_cae_con_el_spool_a_medio_escribir_o_a_medio_reemitir(failpoint):
    good, c = run_case(SINGLE, failpoint, target=("q5_0",))
    assert good, fails(c)


@pytest.mark.parametrize("failpoint", ["gw_after_publish_batch:2", "gw_after_publish_eof:1",
                                       "after_persist:3"])
def test_el_gateway_cae_y_el_cliente_reanuda_la_sesion(failpoint):
    good, c = run_case(SINGLE, failpoint, target=("gw1",))
    assert good, fails(c)
    assert c.restarts.get("gw_0", 0) >= 1


def test_el_gateway_cae_varias_veces_en_la_misma_corrida():
    good, c = run_case(SINGLE, "gw_after_publish_batch@0.15", target=("gw1",), timeout=180)
    assert good, fails(c)


def test_cada_etapa_cae_varias_veces():
    good, c = run_case(REPLICATED, "before_ack@0.03,after_persist@0.03", timeout=240, seed=5)
    assert good, fails(c)
    assert sum(c.restarts.values()) >= 2


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_caos_aleatorio_matando_procesos(seed):
    good, c = run_case(REPLICATED, chaos=0.04, seed=seed, timeout=240)
    assert good, fails(c)
