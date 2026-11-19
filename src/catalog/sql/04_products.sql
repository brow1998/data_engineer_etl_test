-- The deliverable: store products with an identified, active manufacturer and a description.
-- Each row keeps the receipts of the four records it was built from.
create or replace table products as
select
    s.cnpj_store,
    s.gtin,
    s.category,
    m.cnpj_manufacturer,
    c.name   as manufacturer_name,
    c.city   as manufacturer_city,
    c.state  as manufacturer_state,
    d.description,
    s.source_line   as store_source_line,
    m.record_id     as gs1_record_id,
    c.record_id     as company_record_id,
    d.record_id     as cosmos_record_id
from store_products as s
join manufacturers_by_gtin as m using (gtin)
join companies as c on c.cnpj = m.cnpj_manufacturer and c.status = 'ATIVA'
join descriptions_by_gtin as d using (gtin)
order by s.gtin, s.cnpj_store;

-- Why the other store products didn't make it, first missing piece wins.
create or replace table coverage as
select
    case
        when m.gtin is null then 'manufacturer unknown'
        when c.cnpj is null then 'manufacturer not in company registry'
        when c.status <> 'ATIVA' then 'manufacturer not active'
        when d.gtin is null then 'no description'
        else 'complete'
    end as outcome,
    count(*) as store_products
from store_products as s
left join manufacturers_by_gtin as m using (gtin)
left join companies as c on c.cnpj = m.cnpj_manufacturer
left join descriptions_by_gtin as d using (gtin)
group by all
order by store_products desc, outcome;
