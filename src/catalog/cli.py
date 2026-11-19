"""`catalog run`: rebuild the product catalog from the raw lookups and export it."""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).parent / "sql"
log = logging.getLogger("catalog")


def build(raw: Path, output: Path) -> dict:
    """Rebuild everything from raw. Same inputs, same outputs, byte for byte."""
    output.mkdir(parents=True, exist_ok=True)
    db_path = output / "catalog.duckdb"
    db_path.unlink(missing_ok=True)  # the warehouse is derived; a rebuild starts from nothing

    with duckdb.connect(str(db_path)) as con:
        raw_path = str(raw.resolve()).replace("'", "''")
        for script in sorted(SQL_DIR.glob("*.sql")):
            log.info("running %s", script.name)
            con.execute(script.read_text().replace("{raw}", raw_path))

        exports = {
            "products.csv": "select * from products",
            "rejected.csv": "select * from rejected order by source, receipt, reason",
            "coverage.csv": "select * from coverage",
        }
        for name, query in exports.items():
            tmp = output / f".{name}"
            con.execute(f"copy ({query}) to '{tmp}' (header, delimiter ',')")
            os.replace(tmp, output / name)

        summary = {
            "store_products": con.sql("select count(*) from store_products").fetchone()[0],
            "products": con.sql("select count(*) from products").fetchone()[0],
            "rejected": con.sql("select count(*) from rejected").fetchone()[0],
            "coverage": dict(con.sql("select outcome, store_products from coverage").fetchall()),
        }
    (output / "run_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="catalog", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="rebuild and export the catalog")
    run.add_argument("--raw", type=Path, default=Path("raw_data"), help="directory with the raw lookups")
    run.add_argument("--output", type=Path, default=Path("output"), help="where exports are written")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    summary = build(args.raw, args.output)
    log.info("store products: %d", summary["store_products"])
    for outcome, n in summary["coverage"].items():
        log.info("  %-38s %d", outcome, n)
    log.info("products exported: %d -> %s/products.csv", summary["products"], args.output)
    log.info("rejected raw rows: %d -> %s/rejected.csv", summary["rejected"], args.output)


if __name__ == "__main__":
    main()
