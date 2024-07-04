with union_tbl as (
    select 
      to_char(sales.datetime, 'YYYY-MM-DD') as dt,
      sales.final_price as final_price
    from sales  
    join product  
    on product.id_1c = sales.product_id
    join shop
    on sales.shop_id = shop.id_1c
    where true 
    and product."name" = {{product}} 
    and {{date_range}}
    and {{shop}}
    [[and {{region}} != 'заглушка']]
    order by dt
),
min_max_price as (
    select min(final_price) min_cp, max(final_price) max_cp
    from union_tbl
),
range as (
    select 
        case when {{range}} = 0 then 0.01 else {{range}} end rng
),
gen_gr as (
    select generate_series(round(min_cp - 1) , max_cp + (select rng from range), (select rng from range)) as gr_price
    from min_max_price
),
unque_pos as (
	select min(dt) as dt , min(final_price) as final_price, count(*) as cnt
	from union_tbl
	group by dt, final_price
),
gr_union as (
    select *
    from unque_pos
    join gen_gr
    on unque_pos.final_price <= gen_gr.gr_price
    order by final_price, gr_price
),
range_add as (
    select
    	dt::date,
    	cnt as cnt_doubles,
    	min(gr_price) - {{range}} as range_from
    from gr_union
    group by
	    final_price, dt, cnt
	order by dt, range_from
),
cnt_2 as (
	select dt, sum(cnt_doubles) as cnt_all, range_from
	from range_add
	group by dt, range_from
	order by dt
),
rng_lag as (
	select *, lag(range_from) over () range_lag
	from cnt_2
),
bbb as (
	select dt, range_from, cnt_all, 
		case 
			when range_lag = range_from then 1 else 0
		end cc
	from rng_lag
),
pre_fin_tbl as (
	select 
		b1.dt, 
		b1.cnt_all, 
		b1.range_from, 
		b2.dt as gr_dt
	from bbb b1
	join bbb b2
	on b1.range_from = b2.range_from
	where b2.cc = 0 and b1.dt >= b2.dt
	order by b1.dt, b1.range_from
),
final_tbl as (
	select 
		dt, 
		cnt_all, 
		range_from, 
		max(gr_dt) gr_dt
	from pre_fin_tbl
	group by dt, cnt_all, range_from
	order by dt, range_from
),
final_grouping as (
	select 
		min(dt) as start_date,
		sum(cnt_all) as cnt,
		range_from
	from final_tbl
	group by range_from, gr_dt
	order by start_date, range_from
),
results as (
    select *, lead(start_date) over () as end_date, range_from + {{range}} as range_to
    from final_grouping
)
select
    start_date as "Дата начала",  
    coalesce(end_date, current_date) as "Дата окончания",
    coalesce(end_date, current_date) - start_date +1 as "Количество дней продаж",
    cnt as "Продано штук", 
    cnt / (coalesce(end_date, current_date) - start_date + 1) as "Штук/день",
    range_from as "Цена - от",
    range_to as "Цена - до"
from results
where true
    [[and coalesce(end_date, current_date) - start_date >= {{days_cnt}}]]
    [[and cnt >= {{cnt}}]]