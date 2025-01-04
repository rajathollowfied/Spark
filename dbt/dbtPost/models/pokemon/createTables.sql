{{ config(materialized= 'table') }}
CREATE TABLE IF NOT EXISTS dbt.pokemon (
    ID int,
    Name varchar(50),
    Type1 varchar(20),
    Type2 varchar(20),
    Total int,
    HP int,
    Attack int,
    Defense int,
    SpAtk int,
    SpDef int,
    Speed int,
    Generation int,
    Legendary boolean
)

TABLESPACE pg_default;