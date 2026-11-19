-- Check digits, in SQL. catalog/checks.py has the same rules in Python; a test keeps them in step.

-- GTIN zero-padded to 14 digits, GS1 mod-10: body digits weighted 3,1,3,... from the right.
create or replace macro gtin_is_valid(g) as (
    coalesce(regexp_full_match(g, '[0-9]{14}'), false)
    and (10 - list_sum(list_transform(range(13),
            i -> (ascii(g[i + 1]) - 48) * (case when i % 2 = 0 then 3 else 1 end))) % 10) % 10
        = ascii(g[14]) - 48
);

-- One CNPJ check digit over `base` (12 or 13 digits), mod 11.
create or replace macro cnpj_check_digit(base) as (
    with weighted as (
        select list_sum(list_transform(range(length(base)),
            i -> (ascii(base[i + 1]) - 48)
                 * ([6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2][i + 14 - length(base)]))) % 11 as rest
    )
    select case when rest < 2 then 0 else 11 - rest end from weighted
);

create or replace macro cnpj_is_valid(c) as (
    coalesce(regexp_full_match(c, '[0-9]{14}'), false)
    and length(list_distinct(string_split(c, ''))) > 1
    and cnpj_check_digit(c[1:12]) = ascii(c[13]) - 48
    and cnpj_check_digit(c[1:13]) = ascii(c[14]) - 48
);
