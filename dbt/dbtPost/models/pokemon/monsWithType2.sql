{{ config(materialized= 'view') }}

select * from {{ source('dbt','pokemon' )}} where type2 is not null

