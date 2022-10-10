/*********************************************************************************
	일/주/월/분기별 매출액 및 주문 건수
  	일(day), 주(week), 월(month), 분기(quarter) 별로 date_trunc 사용
*********************************************************************************/

-- 매출액 및 주문 건수
select date_trunc('day', o.order_date)::date as day
	 , sum(oi.amount) as sum_amount
	 , count(distinct o.order_id) as daily_ord_cnt
from nw.orders o
	inner join nw.order_items oi on o.order_id = oi.order_id
group by date_trunc('day', o.order_date)::date
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	일/주/월/분기별 상품별 매출액 및 주문 건수
  	일(day), 주(week), 월(month), 분기(quarter) 별로 date_trunc 사용
*********************************************************************************/

-- 상품별 매출액 및 주문건수
select date_trunc('day', o.order_date)::date as day
	 , oi.product_id
	 , sum(oi.amount) as sum_amount
	 , count(distinct o.order_id) as daily_ord_cnt
from nw.orders o
	inner join nw.order_items oi on o.order_id = oi.order_id
group by date_trunc('day', o.order_date)::date, oi.product_id
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	월별 상품카테고리별 매출액 및 주문 건수, 월 전체 매출액 대비 비율
	step 1: 상품 카테고리 별 월별 매출액 추출
	step 2: step 1의 집합에서 전체 매출액을 analytic으로 구한 뒤에 매출액 비율 계산
*********************************************************************************/
-- @formatter:off
-- 월별 상품카테고리별 매출액 및 주문 건수, 월 전체 매출액 대비 비율
with temp_01 as (
	select c.category_name
		, to_char(date_trunc('month', o.order_date), 'yyyymm') as month_day
		, sum(oi.amount) as sum_amount
		, count(distinct o.order_id) as monthly_ord_cnt
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
		inner join nw.products p on p.product_id = oi.product_id
		inner join nw.categories c on c.category_id = p.category_id
	group by c.category_id, c.category_name, to_char(date_trunc('month', o.order_date), 'yyyymm')
)
select t01.*
	, sum(t01.sum_amount) over (partition by t01.month_day) as month_tot_amount
	, round(t01.sum_amount / sum(t01.sum_amount) over (partition by t01.month_day), 3) as month_ratio
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	상품별 전체 매출액 및 해당 상품 카테고리 전체 매출액 대비 비율
  	해당 상품카테고리에서 매출 순위
	step 1: 상품별 전체 매출액을 구함
	step 2: step 1의 집합에서 상품 카테고리별 전체 매출액을 구하고, 비율과 매출 순위를 계산
*********************************************************************************/

-- @formatter:off
-- 상품별 전체 매출액 및 해당 상품 카테고리 전체 매출액 대비 비율
with temp_01 as (
	select oi.product_id
		, max(p.product_name) as product_name
		, max(c.category_name) as category_name
		, sum(oi.amount) as sum_amount
	from nw.order_items oi
		inner join nw.products p on p.product_id = oi.product_id
		inner join nw.categories c on c.category_id = p.category_id
	group by oi.product_id
)
select t01.product_name
	, t01.sum_amount as product_sales
	, t01.category_name
	, sum(t01.sum_amount) over (partition by t01.category_name) as cateogry_sales
	, round(t01.sum_amount / sum(t01.sum_amount) over (partition by t01.category_name), 3) as product_category_ratio
	, row_number() over (partition by t01.category_name order by t01.sum_amount desc) as product_rn
from temp_01 t01
order by t01.category_name, product_sales desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	동년도 월별 누적 매출 및 동일 분기 월별 누적 매출
	step 1: 월별 매출액을 구한다
	step 2: 월별 매출액 집합에 동일 년도의 월별 누적 매출과 동일 분기의 월별 누적 매출을 구함
*********************************************************************************/

-- @formatter:off
-- 동년도 월별 누적 매출 및 동일 분기 월별 누적 매출
with temp_01 as (
	select date_trunc('month', o.order_date)::date as month_day
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	group by date_trunc('month', o.order_date)::date
)
select t01.*
	, sum(t01.sum_amount) over (order by t01.month_day) as tot_amount -- 전체 누적 매출
	, sum(t01.sum_amount) over (partition by date_trunc('year', t01.month_day)::date order by t01.month_day) as cume_year_amount -- 년도별 누적 매출
	, sum(t01.sum_amount) over (partition by date_trunc('quarter', t01.month_day)::date order by t01.month_day) as cume_year_amount -- 분기별 누적 매출
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	5일 이동 평균 매출액 구하기
  	매출액의 경우 주로 1주일 이동 평균 매출을 구하나 데이터가 토,일 매출이 없음
*********************************************************************************/

-- @formatter:off
-- 5일 이동 평균 매출액 구하기
with temp_01 as (
	select date_trunc('day', o.order_date)::date as d_day
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	where o.order_date >= to_date('1996-07-08', 'yyyy-mm-dd')
	group by date_trunc('day', o.order_date)::date
)
select t01.d_day
	, t01.sum_amount
	, avg(t01.sum_amount) over (order by t01.d_day rows between 4 preceding and current row) as m_avg_5days
from temp_01 t01
-- @formatter:on
;

-- @formatter:off
-- 5일 이동 평균 매출액 구하되 5일을 채울 수 없는 경우는 Null로 표시
with temp_01 as (
	select date_trunc('day', o.order_date)::date as d_day
		, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	where o.order_date >= to_date('1996-07-08', 'yyyy-mm-dd')
	group by date_trunc('day', o.order_date)::date
)
, temp_02 as (
	select t01.d_day
		, t01.sum_amount
		, avg(t01.sum_amount) over (order by t01.d_day rows between 4 preceding and current row) as m_avg_5days
		, row_number() over (order by t01.d_day) as r_num
	from temp_01 t01
)
select t02.d_day
	, t02.sum_amount
	, t02.r_num
	, case
		when t02.r_num < 5 then null
		else t02.m_avg_5days
	end as m_avg_5days
from temp_02 t02
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	5일 이동 가중평균 매출액 구하기
  	당일 날짜에서 가까운 날짜일 수록 가중치를 증대
 	5일중 가장 먼 날짜는 매출액의 0.5,
  	중간 날짜 2, 3, 4는 매출액 그대로,
  	당일은 1.5 * 매출액으로 가중치 부여
*********************************************************************************/

-- @formatter:off
-- 5일 이동 가중평균 매출액 구하기
with temp_01 as (
	select date_trunc('day', o.order_date)::date as d_day
		, sum(oi.amount) as sum_amount
		, row_number() over (order by date_trunc('day', o.order_date)::date) as rnum
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	where o.order_date >= to_date('1996-07-08', 'yyyy-mm-dd')
	group by date_trunc('day', o.order_date)::date
)
, temp_02 as (
	select t01a.d_day
		, t01b.sum_amount
		, t01a.rnum
		, t01b.d_day as d_day_back
		, t01b.sum_amount as sum_amount_back
		, t01b.rnum as rnum_back
	from temp_01 t01a
		inner join temp_01 t01b on t01a.rnum between t01b.rnum and t01b.rnum + 4
		-- inner join temp_01 t01b on t01a.rnum between t01a.rnum - 4 and t01a.rnum
)
select t02.d_day
	, avg(t02.sum_amount_back) as m_avg_5days

	 -- sum을 건수인 5로 나누면 평균이 됨
	, sum(t02.sum_amount_back) / 5 as m_avg_5days_01

	-- 가중 이동 평균을 구하기 위해 가중치 값에 따라 sum을 구함
	, sum(case
		when t02.rnum - t02.rnum_back = 4 then 0.5 * t02.sum_amount_back
		when t02.rnum - t02.rnum_back in (3, 2, 1) then t02.sum_amount_back
		when t02.rnum - t02.rnum_back = 0 then 1.5 * t02.sum_amount_back
	end) as m_weighted_sum

	-- 위에서 구한 가중치 값에 따른 sum을 5로 나눠서 가중 이동 평균을 구함
	, sum(case
		when t02.rnum - t02.rnum_back = 4 then 0.5 * t02.sum_amount_back
		when t02.rnum - t02.rnum_back in (3, 2, 1) then t02.sum_amount_back
		when t02.rnum - t02.rnum_back = 0 then 1.5 * t02.sum_amount_back
	end) / 5 as m_w_avg_sum

	-- 5건이 안되는 초기 데이터는 삭제하기 위해서임
	, count(*) as cnt
from temp_02 t02
group by t02.d_day
having count(*) = 5
order by t02.d_day
-- @formatter:on
;


