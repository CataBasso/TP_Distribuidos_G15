import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, "tools")
import scenarios as scn  

pytestmark = pytest.mark.skipif(not Path("data/sample/small_dataset").exists(),
                                reason="faltan los datos de ejemplo")


def env_of(text, service):
    block = re.search(rf"^  {service}:\n(.*?)(?=^  \S|\Z)", text, re.S | re.M).group(1)
    env = re.search(r"environment:\n((?:    - .*\n)+)", block).group(1)
    return dict(l[6:].split("=", 1) for l in env.splitlines())


def test_compose_counts_match_replicas():
    s = scn.full(dict(clients=2, ac=2, lm=3, wm=2, P=2, A=2, K=3))   
    text = scn.compose(s)
    names = re.findall(r"^  (\w+):$", text.split("\nvolumes:")[0], re.M)
    count = lambda prefix: len([n for n in names if re.fullmatch(prefix + r"_\d+", n)])
    assert (count("abstract_cleaner"), count("length_mapper"), count("avg_reducer"),
            count("q5_filter"), count("word_mapper"), count("word_reducer"), count("client")) \
        == (2, 3, 2, 3, 2, 2, 2)
    assert env_of(text, "length_mapper_2") == {
        "ID": "2", "MOM_HOST": "rabbitmq", "CLEANER_REPLICAS": "2", "AVG_PARTITIONS": "2",
        "Q5_PARTITIONS": "3", "PYTHONUNBUFFERED": "1", "STATE_DIR": "/state",
        "FAILPOINTS": "${FP_LENGTH_MAPPER_2:-}"}
    assert env_of(text, "avg_reducer_1")["LM_REPLICAS"] == "3"
    assert env_of(text, "word_reducer_0")["WM_REPLICAS"] == "2"
    gw = env_of(text, "gateway_0")                     
    assert gw["WORD_PARTITIONS"] == "2" and gw["Q5_PARTITIONS"] == "3"
    assert "joiner" not in names
    assert env_of(text, "top_merger")["WORD_PARTITIONS"] == "2"


@pytest.mark.timeout(180)
@pytest.mark.parametrize("n", [1, 6, 7])
def test_scenario_in_memory_matches_expected(n):
    sys.path.insert(0, "tests")
    import real_flow
    s = scn.full(scn.SCENARIOS[n][1])
    finished, _, out = real_flow.run(s, timeout=150)
    assert finished and real_flow.check(out, s["clients"])


def test_varios_gateways_en_el_compose():
    s = scn.full(dict(clients=5, gw=3, P=2, K=3))
    text = scn.compose(s)
    names = re.findall(r"^  (\w+):$", text.split("\nvolumes:")[0], re.M)
    assert [n for n in names if n.startswith("gateway")] == ["gateway_0", "gateway_1", "gateway_2"]
    for g in range(3):
        env = env_of(text, f"gateway_{g}")
        assert env["GATEWAY_ID"] == str(g + 1) and env["GATEWAY_PORT"] == "5000"
        assert env["WORD_PARTITIONS"] == "2" and env["Q5_PARTITIONS"] == "3"
    for c in range(5):
        assert env_of(text, f"client_{c}")["GATEWAY_HOST"] == f"gateway_{c % 3}"
    block = re.search(r"^  client_0:\n(.*?)(?=^  \S|\Z)", text, re.S | re.M).group(1)
    assert all(f"- gateway_{g}" in block for g in range(3))


def test_un_solo_gateway_por_defecto_y_ids_de_los_fijos():
    text = scn.compose(scn.full({}))
    assert re.findall(r"^  (gateway\w*):$", text.split("\nvolumes:")[0], re.M) == ["gateway_0"]
    assert env_of(text, "client_0")["GATEWAY_HOST"] == "gateway_0"
    assert list(scn.SCENARIOS) == [1, 2, 3, 4, 5, 6, 7]
    assert scn.SCENARIOS[7][1]["gw"] >= 2 and scn.SCENARIOS[6][1]["gw"] >= 2


def test_solo_las_etapas_con_estado_persistente_tienen_volumen_y_todas_se_reinician_solas():
    s = scn.full(dict(clients=1, gw=2, lm=2))
    text = scn.compose(s)
    services = text.split("\nvolumes:")[0]
    names = re.findall(r"^  (\w+):$", services, re.M)
    assert not [n for n in names if n.startswith("supervisor")]          # el Supervisor todavía no existe
    for n in ("gateway_0", "length_mapper_1", "top_merger", "q5_filter_0", "abstract_cleaner_0"):
        blk = re.search(rf"^  {n}:\n(.*?)(?=^  \S|\Z)", text, re.S | re.M).group(1)
        assert f"{n}_state:/state" in blk
        assert re.search(rf"^  {n}_state:$", text, re.M)
    for n in ("year_filter_0", "q1_filter_0", "q2_filter_0", "image_getter_0"):
        blk = re.search(rf"^  {n}:\n(.*?)(?=^  \S|\Z)", text, re.S | re.M).group(1)
        assert "/state" not in blk                                        # todavía no migradas (Victoria #18)
    for n in names:
        if n.startswith("client_"):
            continue
        blk = re.search(rf"^  {n}:\n(.*?)(?=^  \S|\Z)", text, re.S | re.M).group(1)
        assert "restart: unless-stopped" in blk, n


def test_los_volumenes_de_estado_no_se_repiten_en_ningun_escenario():
    """Docker Compose rechaza claves repetidas en un mapping: cada volumen se declara una sola vez."""
    for n in scn.SCENARIOS:
        text = scn.compose(scn.full(scn.SCENARIOS[n][1]))
        vols = text.split("\nvolumes:")[1]
        decl = re.findall(r"^  (\w+):$", vols, re.M)
        assert len(decl) == len(set(decl)), (n, [d for d in decl if decl.count(d) > 1])
