/*********************************************************************************
	사용자별로 이전 주문이후 현주문까지 걸린 기간 및 걸린 기간의 Histogram 구하기
*********************************************************************************/

-- @formatter:off
-- 주문 테이블에서 이전 주문 이후 걸린 기간 구하기
with temp01 as (
	select o.order_id
		, o.customer_id
		, o.order_date
		, lag(o.order_date) over (partition by o.customer_id order by o.order_date) as prev_ord_date
	from nw.orders o
)
, temp02 as (
	select t01.order_id
		, t01.customer_id
	 	, t01.order_date
	    , t01.prev_ord_date
		, t01.order_date - t01.prev_ord_date as days_since_prev_order
	from temp01 t01
	where t01.prev_ord_date is not null
)
select t02.*
from temp02 t02
-- @formatter:on
;

-- @formatter:off
-- 이전 주문이후 걸린 기간의 Histogram 구하기
with temp01 as (
	select o.order_id
		, o.customer_id
		, o.order_date
		, lag(o.order_date) over (partition by o.customer_id order by o.order_date) as prev_ord_date
	from nw.orders o
)
, temp02 as (
	select t01.order_id
		, t01.customer_id
		, t01.order_date
		, t01.prev_ord_date
		, t01.order_date - t01.prev_ord_date as days_since_prev_order
	from temp01 t01
	where t01.prev_ord_date is not null
)
-- bin의 간격을 10으로 설정
select floor(t02.days_since_prev_order / 10.0) * 10 as bin, count(*) bin_cnt
from temp02 t02
group by floor(t02.days_since_prev_order / 10.0) * 10
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	월별 사용자 평균 주문 건수
*********************************************************************************/
-- @formatter:off
with temp_01 as (
    select o.customer_id, date_trunc('month', o.order_date)::date as month_day, count(*) as order_cnt
    from nw.orders o
    group by o.customer_id, date_trunc('month', o.order_date)::date
)
select t01.month_day, avg(t01.order_cnt), max(t01.order_cnt), min(t01.order_cnt)
from temp_01 t01
group by t01.month_day
order by t01.month_day
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	order별/고객별로 특정 상품 주문시 함께 가장 많이 주문된 다른 상품 추출하기
*********************************************************************************/

-- @formatter:off
-- 테스트용 테이블 기반에서 order별 특정 상품 주문시 함께 가장 많이 주문된 다른 상품 추출하기
with temp_01 as (
    select 'ord001' as order_id, 'A' as product_id
    union all
    select 'ord001', 'B'
    union all
    select 'ord001', 'C'
    union all
    select 'ord002', 'B'
    union all
    select 'ord002', 'D'
    union all
    select 'ord003', 'A'
    union all
    select 'ord003', 'B'
    union all
    select 'ord003', 'D'
)
, temp_02 as (
    select t01a.order_id, t01a.product_id as prod_01, t01b.product_id as prod_02
    from temp_01 t01a
        inner join temp_01 t01b on t01a.order_id = t01b.order_id
    where t01a.product_id != t01b.product_id
)
, temp_03 as (
    select t02.prod_01, t02.prod_02, count(*) as cnt
    from temp_02 t02
    group by t02.prod_01, t02.prod_02
    order by 1, 2, 3
)
, temp_04 as (
    select
        t03.prod_01
        , t03.prod_02
        , t03.cnt
        , row_number() over (partition by t03.prod_01 order by t03.cnt desc) as rnum
    from temp_03 t03
)
select t04.*
from temp_04 t04
where t04.rnum = 1
-- @formatter:on
;

-- @formatter:off
-- order별 특정 상품 주문시 함께 가장 많이 주문된 다른 상품 추출하기
with temp_01 as (
    -- order_items와 order_items를 order_id로 조인하면 M:M 조인되면서
    -- 개별 order_id별 주문 상품별로 연관된 주문 상품 집합을 생성
    select oi_a.order_id, oi_a.product_id as prod_01, oi_b.product_id as prod_02
    from ga.order_items oi_a
        inner join ga.order_items oi_b on oi_a.order_id = oi_b.order_id
    where oi_a.product_id != oi_b.product_id -- 동일 order_id로 동일 주문상품은 제외
)
, temp_02 as (
    -- prod_01 + prod_02 레벨로 group by 건수를 추출
    select t01.prod_01, t01.prod_02, count(*) as cnt
    from temp_01 t01
    group by t01.prod_01, t01.prod_02
)
, temp_03 as (
    select
        t02.prod_01
        , t02.prod_02
        , t02.cnt
         -- prod_01별로 가장 많은 건수를 가지는 prod_02를 찾기 위해 cnt가 높은 순으로 순위추출
        , row_number() over (partition by t02.prod_01 order by t02.cnt desc) as rnum
    from temp_02 t02
)
-- 순위가 1인 데이터만 별도 추출
select t03.prod_01, t03.prod_02, t03.cnt
from temp_03 t03
where t03.rnum = 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	고객별 RFM 구하기
    고객이 얼마나 최근에(Recency), 얼마나 자주(Frequency), 얼마나 많은 금액(Monetary)을
    사용했는지에 따라 고객을 분류하는 기법
*********************************************************************************/

-- @formatter:off
-- recency, frequency, monetary 각각에 5 ntile을 적용하여 고객별 RFM 구하기
with temp_01 as (
    select
        o.user_id
        , max(date_trunc('day', o.order_time)) :: date as max_ord_date
        , to_date('20161101', 'yyyymmdd') - max(date_trunc('day', o.order_time)) :: date as recency
        , count(distinct o.order_id) as frequency
        , sum(oi.prod_revenue) as monetary
    from ga.orders o
        inner join ga.order_items oi on o.order_id = oi.order_id
    group by o.user_id
)
select t01.*
    -- recency, frequency, money 각각을 5개 등급으로 나눔
    -- 1등급이 가장 높고, 5등급이 가장 낮음
    -- ntile로 하는 경우 데이터에 따라 분포 고르지 않기 떄문에 정확하지 않을 수 있음
    , ntile(5) over (order by recency rows between unbounded preceding and unbounded following) as recency_rank
    , ntile(5) over (order by frequency desc rows between unbounded preceding and unbounded following) as frequency_rank
    , ntile(5) over (order by monetary desc rows between unbounded preceding and unbounded following) as monetary_rank
from temp_01 t01
-- @formatter:on
;

-- @formatter:off
-- recency, frequency, monetary 고객별 RFM 구하기
with temp_01 as (
    select
        o.user_id
        , max(date_trunc('day', o.order_time)) :: date as max_ord_date
        , to_date('20161101', 'yyyymmdd') - max(date_trunc('day', o.order_time)) :: date as recency
        , count(distinct o.order_id) as frequency
        , sum(oi.prod_revenue) as monetary
    from ga.orders o
    inner join ga.order_items oi on o.order_id = oi.order_id
    group by o.user_id
)
, temp_02 as (
    -- recency, frequency, monetary 각각에 대해서 범위를 설정하고 이 범위에 따라 RFM 등급 할당
    select 'A' as grade, 1 as fr_rec, 14 as to_rec, 5 as fr_freq, 9999 as to_freq, 300.0 as fr_money, 999999.0 as to_money
	union all
	select 'B', 15, 50, 3, 4, 50.0, 299.999
	union all
	select 'C', 51, 99999, 1, 2, 0.0, 49.999
)
, temp_03 as (
    select
        t01.*
        , t02_a.grade as recency_grade
        , t02_b.grade as freq_grade
        , t02_c.grade as money_grade
    from temp_01 t01
    left join temp_02 t02_a on t01.recency between t02_a.fr_rec and t02_a.to_rec
    left join temp_02 t02_b on t01.frequency between t02_b.fr_freq and t02_b.to_freq
    left join temp_02 t02_c on t01.monetary between t02_c.fr_money and t02_c.to_money
)
select
    t03.*
	, case
        when t03.recency_grade = 'A' and t03.freq_grade in ('A', 'B') and t03.money_grade = 'A' then 'A'
        when t03.recency_grade = 'B' and t03.freq_grade = 'A' and t03.money_grade = 'A' then 'A'
        when t03.recency_grade = 'B' and t03.freq_grade in ('A', 'B', 'C') and t03.money_grade = 'B' then 'B'
        when t03.recency_grade = 'C' and t03.freq_grade in ('A', 'B') and t03.money_grade = 'B' then 'B'
        when t03.recency_grade = 'C' and t03.freq_grade = 'C' and t03.money_grade = 'A' then 'B'
        when t03.recency_grade = 'C' and t03.freq_grade = 'C' and t03.money_grade in ('B', 'C') then 'C'
        when t03.recency_grade in ('B', 'C') and t03.money_grade = 'C' then 'C'
        else 'C'
    end as total_grade
from temp_03 t03
-- @formatter:on
;