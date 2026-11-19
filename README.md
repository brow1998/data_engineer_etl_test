# catalog

[![ci](https://github.com/brow1998/data_engineer_etl_test/actions/workflows/ci.yml/badge.svg)](https://github.com/brow1998/data_engineer_etl_test/actions/workflows/ci.yml)

Product master data for retail price research. It takes the products stores report selling and
enriches each one with who makes it and what it is, using three external lookups:

| Source | File | What it answers |
|---|---|---|
| Store catalog | `raw_data/infomix.tsv` | Which store (CNPJ) sells which product (GTIN), in which category |
| GS1 | `raw_data/gs1.jl` | Which company (CNPJ) owns a GTIN |
| Receita Federal | `raw_data/cnpjs_receita_federal.jl` | A company's legal name, city, state and whether it is active |
| Cosmos | `raw_data/cosmos.jl` | A product's description |

The lookups are raw API responses, one per line, including the failures.

## Quick start

Needs [uv](https://docs.astral.sh/uv/). One command, from a clean checkout:

```bash
uv run catalog run
```

```text
INFO store products: 25677
INFO   complete                               17364
INFO   manufacturer unknown                   4934
INFO   no description                         3018
INFO   manufacturer not active                361
INFO products exported: 17364 -> output/products.csv
INFO rejected raw rows: 20683 -> output/rejected.csv
```

| Output | What's in it |
|---|---|
| `output/products.csv` | One row per store and product, with manufacturer, description and four receipts |
| `output/coverage.csv` | Every store product, counted by why it is or isn't in the catalog |
| `output/rejected.csv` | Every raw row that couldn't be used, with its record id or line, and the reason |
| `output/catalog.duckdb` | The whole build, every layer, for ad-hoc SQL |

## How it behaves

**Identifiers are validated, not trusted.** GTINs are checked against the GS1 mod-10 check digit
and CNPJs against their two mod-11 check digits. The rules live in SQL macros
([`00_macros.sql`](src/catalog/sql/00_macros.sql)) and in Python ([`checks.py`](src/catalog/checks.py));
a test runs both over every identifier in the raw data and fails if they ever disagree. GTINs stay
text end to end, so leading zeros survive.

**Retried lookups resolve to one answer.** The raw files hold every attempt, including connection
errors and not-found responses. For each GTIN or CNPJ, the latest successful response wins.

**Every row has a receipt.** Each product carries the store file line and the record ids of the
GS1, Receita Federal and Cosmos responses it was built from.

**Nothing disappears silently.** A store product missing from the catalog is counted under the
first thing it lacked. A raw row that was dropped is in `rejected.csv` with the reason, including
store lines that repeat an earlier one with a different category (the first line wins).

**Running it twice gives the same files.** The build starts from nothing on every run and sorts
its outputs. CI builds the real data twice and diffs the results.

## Layout

```text
src/catalog/sql/00_macros.sql     check-digit validation
src/catalog/sql/01_raw.sql        raw sources as tables
src/catalog/sql/02_rejected.sql   unusable rows and why
src/catalog/sql/03_staging.sql    one row per key, latest successful lookup
src/catalog/sql/04_products.sql   the catalog and its coverage
src/catalog/cli.py                runs the SQL in order and exports
tests/                            fixtures built in code; no network
```

## Tests

```bash
uv run pytest
```

## License

[MIT](LICENSE) covers the code in this repository. The data in `raw_data/` belongs to its authors.
