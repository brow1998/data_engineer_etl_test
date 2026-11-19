import csv
import json
from pathlib import Path

import pytest

from catalog.cli import build

GOOD_GTIN, OTHER_GTIN, BAD_GTIN = "07891000258491", "07896040700276", "07891000258492"
MAKER, CLOSED_MAKER, STORE = "60409075000152", "68093095000179", "43235985000732"


def gs1(record_id, gtin, maker, status="OK", at="2019-01-01T00:00:00"):
    address = {"city": "SAO PAULO", "state": "SP"}
    return {
        "id": record_id,
        "gtin": gtin,
        "cnpj_manufacturer": maker,
        "created_at": at,
        "response": {"status": status, "gepirParty": {"partyDataLine": {"address": address}}},
    }


def cosmos(record_id, gtin, description, status="OK", at="2019-01-01T00:00:00"):
    return {"id": record_id, "gtin": gtin, "created_at": at, "response": {"status": status, "description": description}}


def company(record_id, cnpj, name, situacao="ATIVA"):
    return {
        "id": record_id,
        "cnpj": cnpj,
        "created_at": "2019-01-01T00:00:00",
        "response": {"nome": name, "situacao": situacao, "municipio": "SAO PAULO", "uf": "SP"},
    }


def write_jl(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    d = tmp_path / "raw"
    d.mkdir()
    write_jl(
        d / "gs1.jl",
        [
            gs1("00000000-0000-0000-0000-000000000001", GOOD_GTIN, MAKER, status="CONNECTION_ERROR"),
            gs1("00000000-0000-0000-0000-000000000002", GOOD_GTIN, MAKER, at="2019-02-01T00:00:00"),
            gs1("00000000-0000-0000-0000-000000000003", OTHER_GTIN, CLOSED_MAKER),
            gs1("00000000-0000-0000-0000-000000000004", BAD_GTIN, MAKER),
        ],
    )
    write_jl(
        d / "cosmos.jl",
        [
            cosmos("00000000-0000-0000-0000-000000000011", GOOD_GTIN, "OLD NAME"),
            cosmos("00000000-0000-0000-0000-000000000012", GOOD_GTIN, "CAFE 500G", at="2019-03-01T00:00:00"),
            cosmos("00000000-0000-0000-0000-000000000013", OTHER_GTIN, "SABAO"),
        ],
    )
    write_jl(
        d / "cnpjs_receita_federal.jl",
        [
            company("00000000-0000-0000-0000-000000000021", MAKER, "NESTLE BRASIL LTDA"),
            company("00000000-0000-0000-0000-000000000022", CLOSED_MAKER, "FROOTY", situacao="BAIXADA"),
        ],
    )
    (d / "infomix.tsv").write_text(
        "cnpj\tgtin\tcategory\n"
        f"{STORE}\t{GOOD_GTIN}\tcafes\n"  # line 2: complete
        f"{STORE}\t{GOOD_GTIN}\tbebidas\n"  # line 3: repeats line 2, other category
        f"{STORE}\t{OTHER_GTIN}\tlimpeza\n"  # line 4: maker is not active
        f"{STORE}\t{BAD_GTIN}\tcafes\n"  # line 5: invalid gtin
    )
    return d


def rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def test_builds_one_complete_product_with_its_receipts(raw, tmp_path):
    build(raw, tmp_path / "out")
    [product] = rows(tmp_path / "out" / "products.csv")
    assert product["gtin"] == GOOD_GTIN  # leading zero kept
    assert product["category"] == "CAFES"
    assert product["manufacturer_name"] == "NESTLE BRASIL LTDA"
    assert product["description"] == "CAFE 500G"  # latest successful lookup wins
    assert product["store_source_line"] == "2"
    assert product["gs1_record_id"].endswith("0002")
    assert product["company_record_id"].endswith("0021")
    assert product["cosmos_record_id"].endswith("0012")


def test_explains_every_store_product_that_did_not_make_it(raw, tmp_path):
    summary = build(raw, tmp_path / "out")
    assert summary["coverage"] == {"complete": 1, "manufacturer not active": 1}


def test_rejects_with_reasons(raw, tmp_path):
    build(raw, tmp_path / "out")
    rejected = {(r["source"], r["receipt"], r["reason"]) for r in rows(tmp_path / "out" / "rejected.csv")}
    assert ("store_products", "line 3", "repeats line 2 with another category") in rejected
    assert ("store_products", "line 5", "invalid gtin") in rejected
    assert ("gs1", "00000000-0000-0000-0000-000000000001", "lookup failed: CONNECTION_ERROR") in rejected
    assert ("gs1", "00000000-0000-0000-0000-000000000004", "invalid gtin") in rejected


def test_running_twice_produces_identical_files(raw, tmp_path):
    out = tmp_path / "out"
    build(raw, out)
    first = {p.name: p.read_bytes() for p in out.glob("*.csv")}
    build(raw, out)
    assert {p.name: p.read_bytes() for p in out.glob("*.csv")} == first
