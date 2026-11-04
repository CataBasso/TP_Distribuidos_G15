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
    names = re.findall(r"^  (\w+):$", text, re.M)
    count = lambda prefix: len([n for n in names if re.fullmatch(prefix + r"_\d+", n)])
    assert (count("abstract_cleaner"), count("length_mapper"), count("avg_reducer"),
            count("q5_filter"), count("word_mapper"), count("word_reducer"), count("client")) \
        == (2, 3, 2, 3, 2, 2, 2)
    assert env_of(text, "length_mapper_2") == {
        "ID": "2", "MOM_HOST": "rabbitmq", "CLEANER_REPLICAS": "2", "AVG_PARTITIONS": "2",
        "Q5_PARTITIONS": "3", "PYTHONUNBUFFERED": "1"}
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
    names = re.findall(r"^  (\w+):$", text, re.M)
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
    assert re.findall(r"^  (gateway\w*):$", text, re.M) == ["gateway_0"]
    assert env_of(text, "client_0")["GATEWAY_HOST"] == "gateway_0"
    assert list(scn.SCENARIOS) == [1, 2, 3, 4, 5, 6, 7]
    assert scn.SCENARIOS[7][1]["gw"] >= 2 and scn.SCENARIOS[6][1]["gw"] >= 2
