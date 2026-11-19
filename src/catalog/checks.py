"""Check-digit validation for the two identifiers everything here is joined on."""

from __future__ import annotations


def gtin_is_valid(gtin: str | None) -> bool:
    """GTIN-8/12/13/14, zero-padded to 14 digits, with a correct GS1 mod-10 check digit."""
    if not gtin or not gtin.isdigit() or len(gtin) != 14:
        return False
    body, check = gtin[:-1], int(gtin[-1])
    # From the right of the body, weights alternate 3, 1, 3, ...
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return (10 - total % 10) % 10 == check


def cnpj_is_valid(cnpj: str | None) -> bool:
    """Brazilian company id: 14 digits with two mod-11 check digits."""
    if not cnpj or not cnpj.isdigit() or len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    def digit(base: str) -> int:
        weights = list(range(len(base) - 7, 1, -1)) + list(range(9, 1, -1))
        rest = sum(int(d) * w for d, w in zip(base, weights, strict=True)) % 11
        return 0 if rest < 2 else 11 - rest

    first = digit(cnpj[:12])
    return first == int(cnpj[12]) and digit(cnpj[:12] + str(first)) == int(cnpj[13])
