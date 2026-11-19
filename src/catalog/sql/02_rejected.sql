-- Every row that cannot be used, with where it came from and why.
create or replace table rejected as
select 'gs1' as source, record_id as receipt, 'invalid gtin' as reason from raw_gs1 where not gtin_is_valid(gtin)
union all
select 'gs1', record_id, 'lookup failed: ' || coalesce(response.status, 'no status')
from raw_gs1 where gtin_is_valid(gtin) and coalesce(response.status, '') <> 'OK'
union all
select 'gs1', record_id, 'no manufacturer cnpj'
from raw_gs1 where gtin_is_valid(gtin) and response.status = 'OK' and not cnpj_is_valid(cnpj_manufacturer)
union all
select 'cosmos', record_id, 'invalid gtin' from raw_cosmos where not gtin_is_valid(gtin)
union all
select 'cosmos', record_id, 'lookup failed: ' || coalesce(response.status, 'no status')
from raw_cosmos where gtin_is_valid(gtin) and coalesce(response.status, '') <> 'OK'
union all
select 'companies', record_id, 'invalid cnpj' from raw_companies where not cnpj_is_valid(cnpj)
union all
select 'store_products', 'line ' || source_line, 'invalid gtin' from raw_store_products where not gtin_is_valid(gtin)
union all
select 'store_products', 'line ' || source_line, 'invalid store cnpj'
from raw_store_products where gtin_is_valid(gtin) and not cnpj_is_valid(cnpj);
