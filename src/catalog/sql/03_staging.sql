-- One row per key. Lookups were retried over time, so the latest successful answer wins.
create or replace table manufacturers_by_gtin as
select gtin, cnpj_manufacturer, record_id
from raw_gs1
where gtin_is_valid(gtin) and response.status = 'OK' and cnpj_is_valid(cnpj_manufacturer)
qualify row_number() over (partition by gtin order by created_at desc, record_id) = 1;

create or replace table descriptions_by_gtin as
select gtin, trim(response.description) as description, record_id
from raw_cosmos
where gtin_is_valid(gtin) and response.status = 'OK' and nullif(trim(response.description), '') is not null
qualify row_number() over (partition by gtin order by created_at desc, record_id) = 1;

create or replace table companies as
select
    cnpj,
    trim(response.nome)       as name,
    response.situacao         as status,
    trim(response.municipio)  as city,
    response.uf               as state,
    record_id
from raw_companies
where cnpj_is_valid(cnpj)
qualify row_number() over (partition by cnpj order by created_at desc, record_id) = 1;

-- One row per store and product. When the file lists a pair twice, the first line wins.
create or replace table store_products as
select cnpj as cnpj_store, gtin, upper(trim(category)) as category, source_line
from raw_store_products
where gtin_is_valid(gtin) and cnpj_is_valid(cnpj)
qualify row_number() over (partition by cnpj, gtin order by source_line) = 1;

insert into rejected
select 'store_products', 'line ' || r.source_line,
       'repeats line ' || s.source_line
       || case when upper(trim(r.category)) <> s.category then ' with another category' else '' end
from raw_store_products as r
join store_products as s on s.cnpj_store = r.cnpj and s.gtin = r.gtin and s.source_line <> r.source_line;
