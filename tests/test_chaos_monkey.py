import random
import sys

sys.path.insert(0, "tools")
import chaos_monkey as cm  # noqa: E402

NAMES = ["rabbitmq", "client_0", "gateway_0", "gateway_1", "q5_filter_0", "q5_filter_1",
         "length_mapper_0", "top_merger", "year_filter_0", "q1_filter_0"]


def pick(scn, seed=0, all_=False):
    return cm.pick_victims(scn, NAMES, random.Random(seed), all_)


def test_nunca_mata_a_rabbit_ni_a_los_clientes():
    for seed in range(50):
        for scn in ("random", "storm"):
            assert not {"rabbitmq", "client_0"} & set(pick(scn, seed, True))


def test_por_defecto_no_toca_las_etapas_que_todavia_no_persisten():
    for seed in range(50):
        assert not any(n.startswith(cm.NOT_YET_FT) for n in pick("random", seed))
    assert any(n.startswith("year_filter_") for s in range(50) for n in pick("random", s, True))


def test_stage_mata_a_todas_las_replicas_del_prefijo():
    assert sorted(pick("stage:q5_filter")) == ["q5_filter_0", "q5_filter_1"]


def test_gateway_mata_solo_gateways():
    for seed in range(20):
        assert pick("gateway", seed)[0].startswith("gateway_")


def test_storm_mata_a_la_mitad():
    assert len(pick("storm", 1)) == len(cm.eligible(NAMES)) // 2


def test_dry_run_no_ejecuta_docker():
    calls = []
    cm.kill(["x"], dry_run=True, runner=calls.append)
    assert calls == []
    cm.kill(["x"], runner=calls.append)
    assert calls == [["kill", "-s", "KILL", "x"]]
