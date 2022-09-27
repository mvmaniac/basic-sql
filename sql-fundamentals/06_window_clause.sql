/*********************************************************************************
	windows 절 실습
*********************************************************************************/

-- rows between unbounded preceding and current row
select p.*, sum(p.unit_price) over (order by p.unit_price) as unit_price_sum
from nw.products p
;

select p.*, sum(p.unit_price) over (order by p.unit_price rows between unbounded preceding and current row) as unit_price_sum
from nw.products p
;

select p.*, sum(p.unit_price) over (order by p.unit_price rows unbounded preceding) as unit_price_sum
from nw.products p
;

-- @formatter:off
-- 중앙합, 중앙 평균(Centered average)
select p.product_id
	 , p.product_name
	 , p.category_id
	 , p.unit_price
	 , sum(p.unit_price) over (partition by p.category_id order by p.unit_price rows between 1 preceding and 1 following) as unit_price_sum
	 , avg(p.unit_price) over (partition by p.category_id order by p.unit_price rows between 1 preceding and 1 following) as unit_price_avg
from nw.products p
-- @formatter:on
;

-- @formatter:off
-- rows between current row and unbounded following
select p.product_id
	 , p.product_name
	 , p.category_id
	 , p.unit_price
	 , sum(p.unit_price) over (partition by p.category_id order by p.unit_price rows between current row and unbounded following) as unit_price_sum
from nw.products p
-- @formatter:on
;

-- @formatter:off
-- range와 rows의 차이
with temp_01 as (
	select p.category_id, date_trunc('day', o.order_date) as ord_date, sum(oi.amount) as sum_by_daily_cat
	from nw.order_items oi
		inner join nw.orders o on o.order_id = oi.order_id
		inner join nw.products p on p.product_id = oi.product_id
	group by p.category_id, date_trunc('day', o.order_date)
	order by 1, 2
)
select t01.category_id
     , t01.ord_date::date
     , t01.sum_by_daily_cat
	 , sum(t01.sum_by_daily_cat) over (partition by t01.category_id order by t01.ord_date rows between 2 preceding and current row) as rows
	 , sum(t01.sum_by_daily_cat) over (partition by t01.category_id order by t01.ord_date range between interval '2' day preceding and current row) as range
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	이동평균 실습
*********************************************************************************/

-- @formatter:off
-- 3일 이동 평균 매출
with temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
)
select t01.ord_date
	 , t01.daily_sum
	 , avg(t01.daily_sum) over (order by t01.ord_date rows between 2 preceding and current row) as ma_3days
from temp_01 t01
-- @formatter:on
;

-- @formatter:off
-- 3일 중앙 평균 매출
with temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
)
select t01.ord_date
	 , t01.daily_sum
	 , avg(t01.daily_sum) over (order by t01.ord_date rows between 1 preceding and 1 following) as ca_3days
from temp_01 t01
-- @formatter:on
;

-- @formatter:off
-- N 이동 평균에서 맨 처음 N-1 개의 데이터의 경우 정확히 N이동 평균을 구할 수 없을 때 Null 처리 하기
with temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
)
select t01.ord_date
	, t01.daily_sum
	, avg(t01.daily_sum) over (order by t01.ord_date rows between 2 preceding and current row) as ma_3days_01
	, case
		when row_number() over (order by t01.ord_date) <= 2 then null
		else avg(t01.daily_sum) over (order by t01.ord_date rows between 2 preceding and current row)
	end as ma_3days_02
from temp_01 t01
-- @formatter:on
;

-- @formatter:off
-- 또는 아래와 같이 작성
with temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
)
, temp_02 as (
	select t01.ord_date
		, t01.daily_sum
		, avg(t01.daily_sum) over (order by t01.ord_date rows between 2 preceding and current row) as ma_3days_01
		, row_number() over (order by t01.ord_date) as rn
	from temp_01 t01
)
select ord_date
	, daily_sum
	, ma_3days_01
	, case
		when rn <= 2 then null
		else ma_3days_01
	end as ma_3days_02
from temp_02 t02
-- @formatter:on
;

-- 연속된 매출 일자에서 매출이 Null일때와 그렇지 않을 때의 Aggregate Analytic 결과 차이
with ref_days as (
	select generate_series('1996-07-04'::date, '1996-07-23'::date, '1 day'::interval)::date as ord_date
)
, temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
)
, temp_02 as (
	select rd.ord_date, t01.daily_sum as daily_sum
	from ref_days rd
		left outer join temp_01 t01 on rd.ord_date = t01.ord_date
)
select t02.ord_date
	, t02.daily_sum
	, avg(t02.daily_sum) over (order by t02.ord_date rows between 2 preceding and current row) as ma_3days
	, avg(coalesce(t02.daily_sum, 0)) over (order by t02.ord_date rows between 2 preceding and current row) as ma_3days_01
from temp_02 t02
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	range와 rows 적용 시 유의 사항
*********************************************************************************/

-- @formatter:off
-- range와 rows의 차이: order by 시 동일 row 처리 차이 - 1
select e.empno
	, e.deptno
	, e.sal
	, avg(e.sal) over (partition by e.deptno order by e.sal) as avg_default
	, avg(e.sal) over (partition by e.deptno order by e.sal range between unbounded preceding and current row) as avg_range
	, avg(e.sal) over (partition by e.deptno order by e.sal rows between unbounded preceding and current row) as avg_rows
	, sum(e.sal) over (partition by e.deptno order by e.sal) as sum_default
	, sum(e.sal) over (partition by e.deptno order by e.sal rows between unbounded preceding and current row) as sum_rows
from hr.emp e
-- @formatter:on
;

-- @formatter:off
-- range와 rows의 차이: order by 시 동일 row 처리 차이 - 2
select e.empno
	, e.deptno
	, e.sal
	, date_trunc('month', e.hiredate)::date as hiremonth
	, avg(e.sal) over (partition by e.deptno order by date_trunc('month', e.hiredate)) as avg_default
	, avg(e.sal) over (partition by e.deptno order by date_trunc('month', e.hiredate) range between unbounded preceding and current row) as avg_range
	, avg(e.sal) over (partition by e.deptno order by date_trunc('month', e.hiredate) rows between unbounded preceding and current row) as avg_rows
	, sum(e.sal) over (partition by e.deptno order by date_trunc('month', e.hiredate)) as sum_default
	, sum(e.sal) over (partition by e.deptno order by date_trunc('month', e.hiredate) rows between unbounded preceding and current row) as sum_rows
from hr.emp e
-- @formatter:on
;
