/*********************************************************************************
	작년 대비 동월 매출 비교, 작년 동월 대비 차이/비율/매출 성장 비율 추출
	step 1: 상품 카테고리 별 월별 매출액 추출
	step 2: step 1의 집합에서 12개월 이전 매출 데이터를 가져와서 현재 월과 매출 비교
*********************************************************************************/

-- @formatter:off
-- 작년 대비 동월 매출 비교, 작년 동월 대비 차이/비율/매출 성장 비율 추출
with temp01 as (
	select date_trunc('month', o.order_date)::date as month_day
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	group by date_trunc('month', o.order_date)::date
)
, temp02 as (
	select t01.month_day
		, t01.sum_amount as curr_amount
		, lag(month_day, 12) over (order by month_day) as prve_month_1year
		, lag(sum_amount, 12) over (order by month_day) as prev_amount_1year
	from temp01 t01
)
select t02.*
	, t02.curr_amount - t02.prev_amount_1year as diff_amount
	, 100.0 * t02.curr_amount / t02.prev_amount_1year as prev_pct
	, 100.0 * (t02.curr_amount - t02.prev_amount_1year) / prev_amount_1year as prev_growth_pct
from temp02 t02
where t02.prev_amount_1year is not null
-- @formatter:on
;

-- @formatter:off
-- 작년 대비 동분기 매출 비교, 작년  대비 차이/비율/매출 성장 비율 추출
with temp01 as (
	select date_trunc('quarter', o.order_date)::date as quarter_day
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	group by date_trunc('quarter', o.order_date)::date
)
, temp02 as (
	select t01.quarter_day
		, t01.sum_amount as curr_amount
		, lag(quarter_day, 4) over (order by quarter_day) as prev_quarter_1year
		, lag(sum_amount, 4) over (order by quarter_day) as prev_amount_1year
	from temp01 t01
)
select t02.*
	, t02.curr_amount - t02.prev_amount_1year as diff_amount
	, 100.0 * t02.curr_amount / t02.prev_amount_1year as prev_pct
	, 100.0 * (t02.curr_amount - t02.prev_amount_1year) / prev_amount_1year as prev_growth_pct
from temp02 t02
where t02.prev_quarter_1year is not null
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	카테고리 별 기준 월 대비 매출 비율 추이(aka 매출 팬 차트)
	step 1: 상품 카테고리 별 월별 매출액 추출
	step 2: step 1의 집합에서 기준 월이 되는 첫월의 매출액을
  			동일 카테고리에 모두 복제한 뒤 매출 비율을 계산
*********************************************************************************/

-- 카테고리 별 기준 월 대비 매출 비율 추이
-- @formatter:off
with temp_01 as (
	select c.category_name
		, to_char(date_trunc('month', o.order_date), 'yyyymm') as month_day
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
		inner join nw.products p on p.product_id = oi.product_id
		inner join nw.categories c on c.category_id = p.category_id
	where o.order_date between to_date('1996-07-01', 'yyyy-mm-dd') and to_date('1997-06-30', 'yyyy-mm-dd') -- 테스트 상 1년 데이터만 사용
	group by c.category_id, c.category_name, to_char(date_trunc('month', o.order_date), 'yyyymm')
)
select t01.*
	, first_value(t01.sum_amount) over (partition by t01.category_name order by t01.month_day) as base_amount
	, round(100.0 * t01.sum_amount / first_value(t01.sum_amount) over (partition by t01.category_name order by t01.month_day), 2) as base_pct
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	매출 Z 차트
*********************************************************************************/

-- @formatter:off
-- 매출 Z 차트
with temp01 as (
	-- 월별 매출
	select to_char(o.order_date, 'yyyymm') as year_month
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	group by to_char(o.order_date, 'yyyymm')
)
, temp02 as (
	select t01.year_month
		, substring(t01.year_month, 1, 4) as year
		, t01.sum_amount -- 월별 매출
		, sum(t01.sum_amount) over (partition by substring(t01.year_month, 1, 4) order by t01.year_month) as acc_amount -- 누적 매출
		, sum(t01.sum_amount) over (order by t01.year_month rows between 11 preceding and current row) as year_ma_amount -- 년간 이동 매출
	from temp01 t01
	-- where year_month between '199706' and '199805' 와 같이 사용하면 안됨. where절이 먼저 수행되므로 sum() analytics가 제대로 동작하지 않음
)
select t02.*
from temp02 t02
-- where year_month >= '199801' and year_month <= '199804' -- 월별로는 Z 차트 형식으로 나오지 않음 그러므로 아래와 같이 년단위로
-- where t02.year = '1997' -- 1년 단위로 보고 싶을 경우
-- @formatter:on
;
