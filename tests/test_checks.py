import duckdb
import pytest

from catalog.checks import cnpj_is_valid, gtin_is_valid
from catalog.cli import SQL_DIR


@pytest.fixture(scope="module")
def sql():
    con = duckdb.connect()
    con.execute((SQL_DIR / "00_macros.sql").read_text())
    return con


@pytest.mark.parametrize(
    ("gtin", "ok"),
    [
        ("07891000258491", True),
        ("07891000258492", False),
        ("00000078903784", True),
        ("7891000258491", False),
        ("0789100025849X", False),
        (None, False),
    ],
)
def test_gtin(sql, gtin, ok):
    assert gtin_is_valid(gtin) is ok
    assert sql.execute("select gtin_is_valid(?)", [gtin]).fetchone()[0] is ok


@pytest.mark.parametrize(
    ("cnpj", "ok"),
    [
        ("68093095000179", True),
        ("68093095000178", False),
        ("60409075000152", True),
        ("11111111111111", False),
        ("6809309500017", False),
        (None, False),
    ],
)
def test_cnpj(sql, cnpj, ok):
    assert cnpj_is_valid(cnpj) is ok
    assert sql.execute("select cnpj_is_valid(?)", [cnpj]).fetchone()[0] is ok


def test_sql_and_python_agree_on_every_identifier_in_the_raw_data(sql):
    gtins = sql.execute(
        "select distinct gtin from read_csv('raw_data/infomix.tsv', delim='\t', all_varchar=true)"
        " union select distinct gtin from read_json('raw_data/cosmos.jl', format='newline_delimited')"
    ).fetchall()
    cnpjs = sql.execute(
        "select distinct cnpj from read_csv('raw_data/infomix.tsv', delim='\t', all_varchar=true)"
        " union select distinct cnpj from read_json('raw_data/cnpjs_receita_federal.jl', format='newline_delimited')"
    ).fetchall()
    for (g,) in gtins:
        assert sql.execute("select gtin_is_valid(?)", [g]).fetchone()[0] == gtin_is_valid(g), g
    for (c,) in cnpjs:
        assert sql.execute("select cnpj_is_valid(?)", [c]).fetchone()[0] == cnpj_is_valid(c), c
