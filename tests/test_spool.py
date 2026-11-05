"""Spool en CSV: ida y vuelta, entrecomillado, eviction de archivos abiertos y limpieza."""
import pytest

from common.utils.spool import Spool

C = (1, 1)


def test_ida_y_vuelta_devuelve_texto_en_orden(tmp_path):
    s = Spool(tmp_path)
    for i in range(5):
        s.append(C, [f"https://en.wikipedia.org/wiki/A{i}", i * 10])
    assert list(s.read(C)) == [[f"https://en.wikipedia.org/wiki/A{i}", str(i * 10)] for i in range(5)]


@pytest.mark.parametrize("url", [
    "https://en.wikipedia.org/wiki/Paris,_Texas",        # coma
    'https://x/wiki/Say_"hi"',                           # comillas
    "https://es.wikipedia.org/wiki/Ñandú",               # tildes
    "https://x/wiki/a\nb",                               # salto de línea (no debería pasar, pero no rompe)
    "",                                                  # vacío
])
def test_valores_con_caracteres_especiales(tmp_path, url):
    s = Spool(tmp_path)
    s.append(C, [url, 7])
    s.append(C, ["otra", 8])
    assert list(s.read(C)) == [[url, "7"], ["otra", "8"]]


def test_un_archivo_por_cliente(tmp_path):
    s = Spool(tmp_path)
    s.append((1, 1), ["a", 1])
    s.append((1, 2), ["b", 2])
    assert s.path((1, 1)).name == "client_1_1.csv" and s.path((1, 2)).name == "client_1_2.csv"
    assert list(s.read((1, 1))) == [["a", "1"]] and list(s.read((1, 2))) == [["b", "2"]]


def test_leer_un_cliente_sin_archivo_no_devuelve_nada(tmp_path):
    s = Spool(tmp_path)
    assert list(s.read(C)) == [] and not s.exists(C)


def test_delete_borra_el_archivo(tmp_path):
    s = Spool(tmp_path)
    s.append(C, ["a", 1])
    assert s.exists(C)
    s.delete(C)
    assert not s.exists(C)
    s.delete(C)                                           # borrar dos veces no falla


def test_se_puede_seguir_escribiendo_despues_de_leer(tmp_path):
    s = Spool(tmp_path)
    s.append(C, ["a", 1])
    assert list(s.read(C)) == [["a", "1"]]
    s.append(C, ["b", 2])
    assert list(s.read(C)) == [["a", "1"], ["b", "2"]]


def test_eviction_de_archivos_abiertos_no_pierde_datos(tmp_path):
    s = Spool(tmp_path, max_open=2)
    for round_ in range(3):
        for n in range(1, 6):                             # 5 clientes con solo 2 archivos abiertos
            s.append((1, n), [f"u{round_}", round_])
    assert len(s._open) <= 2
    for n in range(1, 6):
        assert list(s.read((1, n))) == [["u0", "0"], ["u1", "1"], ["u2", "2"]]


def test_wipe_borra_lo_de_una_corrida_anterior(tmp_path):
    viejo = Spool(tmp_path)
    viejo.append(C, ["a", 1])
    viejo.close()
    (tmp_path / "client_9_9.jsonl").write_text('["x", 1]\n')       # formato anterior
    nuevo = Spool(tmp_path)                                         # wipe=True por defecto
    assert not nuevo.exists(C) and not (tmp_path / "client_9_9.jsonl").exists()


def test_sin_wipe_conserva_los_archivos(tmp_path):
    viejo = Spool(tmp_path)
    viejo.append(C, ["a", 1])
    viejo.close()
    assert list(Spool(tmp_path, wipe=False).read(C)) == [["a", "1"]]


def test_append_many_equivale_a_varios_append_en_orden(tmp_path):
    s = Spool(tmp_path)
    s.append(C, ["a", 1])
    s.append_many(C, [["b", 2], ["c, con coma", 3]])
    assert list(s.read(C)) == [["a", "1"], ["b", "2"], ["c, con coma", "3"]]
