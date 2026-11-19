-- Raw sources, read as-is. Identifiers stay text so leading zeros survive.
create or replace table raw_gs1 as
select id::varchar as record_id, gtin, cnpj_manufacturer, created_at, response
from read_json('{raw}/gs1.jl', format = 'newline_delimited', maximum_object_size = 10000000);

create or replace table raw_cosmos as
select id::varchar as record_id, gtin, created_at, response
from read_json('{raw}/cosmos.jl', format = 'newline_delimited');

create or replace table raw_companies as
select id::varchar as record_id, cnpj, created_at, response
from read_json('{raw}/cnpjs_receita_federal.jl', format = 'newline_delimited');

create or replace table raw_store_products as
select row_number() over () + 1 as source_line, cnpj, gtin, category
from read_csv('{raw}/infomix.tsv', delim = '\t', header = true, all_varchar = true);
