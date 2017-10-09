select
    cities.id,
    cities.name as city_name,
    cities.iso_code as city_iso_code,
    regions.name as region_name,
    regions.iso_code as region_iso_code,
    countries.name as country_name,
    countries.alpha2 as country_alpha2,
    countries.alpha3 as country_alpha3,
from
    countries,
    regions,
    cities
where
    cities.country_id = countries.id
    and cities.region_id = regions.id