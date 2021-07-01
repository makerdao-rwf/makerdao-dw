with lending_assets_1 as (
    select i as ilk, block_number, dart as dart, null as rate
    from makermcd.vat_call_frob
    where dart <> 0.0
    union all
    select i as ilk, block_number, dart as dart, 0.0 as rate
    from makermcd.vat_call_grab
    where dart <> 0.0
    union all
    select i as ilk, block_number, null as dart, rate as rate 
    from makermcd.vat_call_fold
    where rate <> 0.0
),
-- Find the first usage of an ilk
ilks as (
    select ilk, min(block_number) as starting_use, max(block_number) as end_use
    from lending_assets_1
    group by ilk
),
ilks_2 as (
    select ilk, starting_use, max(end_use) over () as end_use
    from ilks
),
-- Generate one 'touch' per ilk per month to avoid holes
noop_filling as (
    select ilk, d as block_number, null::numeric as dart, null::numeric as rate, null::numeric as sf
    from ilks_2
    cross join generate_series(starting_use, end_use, 1000) d
),
rates as (
    select block_number, ilk, (data/10^27)^(3600*24*365) -1 as sf
    from makermcd.jug_call_file
),
lending_assets_1_with_filling as (
    select *, null::numeric as sf from lending_assets_1
    union all
    select * from noop_filling
    union all
    select ilk, block_number,  null::numeric as dart, null::numeric as rate, sf from rates
),
lending_assets_2 as (
    select ilk, block_number, 
        coalesce(1+sum(rate) over(partition by ilk order by block_number asc)/10^27,1) as rate,
        sum(dart) over(partition by ilk order by block_number asc)/10^18 as dart,
        sum(case when sf is not null then 1 else 0 end) over(partition by ilk order by block_number asc) as sf_grp,
        sf
    from lending_assets_1_with_filling 
),
with_rk as (
    select (block_number/10000)::int as dt,
        replace(encode(ilk, 'escape'), '\000', '') as collateral, 
        dart*rate as debt,
        max(sf) over(partition by ilk, sf_grp) as sf,
        row_number() over (partition by ilk, (block_number/10000)::int order by block_number desc) as rk
    from lending_assets_2
),
group_by as (
    select *, sf as rate, debt*sf as annual_revenues
    from with_rk
    where rk = 1
        and debt <> 0.0
),
group_by_cat as (
    select dt, 
        case when collateral like 'PSM%' then 'Stablecoins'
            when collateral in ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') then 'Stablecoins'
            when collateral like 'ETH-%' then 'ETH'
            when collateral like 'WBTC-%' then 'WBTC'
            when collateral like 'UNIV2%' then 'Liquidity Pools'
            when collateral like 'RWA%' then 'RWA'
            else 'Others' end as collateral,
            debt as asset,
            annual_revenues
    from group_by
)
select dt as dt,  collateral, sum(asset) as asset, sum(annual_revenues) as annual_revenues,  
    sum(annual_revenues)/sum(asset) as blended_rate
from group_by_cat
group by 1, 2
order by 1 desc, 2