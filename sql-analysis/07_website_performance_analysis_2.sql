-- :current_date 값은 to_date('20161101', 'yyyymmdd')

/*********************************************************************************
	사용자 생성 날짜 별 일주일간 잔존율(Retention rate) 구하기
*********************************************************************************/

-- @formatter:off
-- 사용자 생성 날짜 별 일주일간 잔존율(Retention rate) 구하기
with temp_01 as (
    select gu.user_id
        , date_trunc('day', gu.create_time)::date as user_create_date
        , date_trunc('day', gs.visit_stime)::date as sess_visit_date
        , count(gs.*) as cnt
    from ga.ga_users gu
        left outer join ga.ga_sess gs on gu.user_id = gs.user_id
    where gu.create_time >= (:current_date - interval '8 days') and gu.create_time < :current_date
    group by gu.user_id, date_trunc('day', gu.create_time)::date, date_trunc('day', gs.visit_stime)::date
)
, temp_02 as (
    select t01.user_create_date
        , count(t01.*) as create_cnt
        -- d1 에서 d7 일자별 접속 사용자 건수 구하기
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '1 day' then 1 else 0 end) as d1_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '2 day' then 1 else 0 end) as d2_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '3 day' then 1 else 0 end) as d3_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '4 day' then 1 else 0 end) as d4_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '5 day' then 1 else 0 end) as d5_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '6 day' then 1 else 0 end) as d6_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '7 day' then 1 else 0 end) as d7_cnt

         -- 0이 아닌 null로 할 경우
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '1 day' then 1 end) as d1_cnt
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '2 day' then 1 end) as d2_cnt
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '3 day' then 1 end) as d3_cnt
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '4 day' then 1 end) as d4_cnt
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '5 day' then 1 end) as d5_cnt
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '6 day' then 1 end) as d6_cnt
--         , sum(case when t01.sess_visit_date = t01.user_create_date + interval '7 day' then 1 end) as d7_cnt
    from temp_01 t01
    group by t01.user_create_date
)
select t02.user_create_date
    , t02.create_cnt
     -- d1 에서 d7 일자별 잔존율 구하기
    , round(100.0 * t02.d1_cnt / t02.create_cnt, 2) as d1_ratio
    , round(100.0 * t02.d2_cnt / t02.create_cnt, 2) as d2_ratio
    , round(100.0 * t02.d3_cnt / t02.create_cnt, 2) as d3_ratio
    , round(100.0 * t02.d4_cnt / t02.create_cnt, 2) as d4_ratio
    , round(100.0 * t02.d5_cnt / t02.create_cnt, 2) as d5_ratio
    , round(100.0 * t02.d6_cnt / t02.create_cnt, 2) as d6_ratio
    , round(100.0 * t02.d7_cnt / t02.create_cnt, 2) as d7_ratio
from temp_02 t02
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	주별 잔존율(Retention rate) 및 주별 특정 채널 잔존율
*********************************************************************************/

-- @formatter:off
-- 사용자 생성 날짜 별 주별 잔존율(Retention rate) 구하기
with temp_01 as (
    select gu.user_id
        , date_trunc('week', gu.create_time)::date as user_create_date
        , date_trunc('week', gs.visit_stime)::date as sess_visit_date
        , count(gs.*) as cnt
    from ga.ga_users gu
        left outer join ga.ga_sess gs on gu.user_id = gs.user_id
    -- where gu.create_time >= (:current_date - interval '7 weeks') and gu.create_time < :current_date
    where gu.create_time >= to_date('20160912', 'yyyymmdd') and gu.create_time < to_date('20161101', 'yyyymmdd')
    group by gu.user_id, date_trunc('week', gu.create_time)::date, date_trunc('week', gs.visit_stime)::date
)
, temp_02 as (
    select t01.user_create_date
        , count(t01.*) as create_cnt
        -- w1 에서 w7까지 주단위 접속 사용자 건수 구하기
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '1 week' then 1 end) as w1_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '2 week' then 1 end) as w2_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '3 week' then 1 end) as w3_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '4 week' then 1 end) as w4_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '5 week' then 1 end) as w5_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '6 week' then 1 end) as w6_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '7 week' then 1 end) as w7_cnt
    from temp_01 t01
    group by t01.user_create_date
)
select t02.user_create_date
    , t02.create_cnt
     -- w1 에서 w7 주별 잔존율 구하기
    , round(100.0 * t02.w1_cnt / t02.create_cnt, 2) as w1_ratio
    , round(100.0 * t02.w2_cnt / t02.create_cnt, 2) as w2_ratio
    , round(100.0 * t02.w3_cnt / t02.create_cnt, 2) as w3_ratio
    , round(100.0 * t02.w4_cnt / t02.create_cnt, 2) as w4_ratio
    , round(100.0 * t02.w5_cnt / t02.create_cnt, 2) as w5_ratio
    , round(100.0 * t02.w6_cnt / t02.create_cnt, 2) as w6_ratio
    , round(100.0 * t02.w7_cnt / t02.create_cnt, 2) as w7_ratio
from temp_02 t02
order by 1
-- @formatter:on
;

-- @formatter:off
-- 주 단위 특정 채널 잔존율(Retention rate)
with temp_01 as (
    select gu.user_id
        , date_trunc('week', gu.create_time)::date as user_create_date
        , date_trunc('week', gs.visit_stime)::date as sess_visit_date
        , count(gs.*) as cnt
    from ga.ga_users gu
        left outer join ga.ga_sess gs on gu.user_id = gs.user_id
    -- where gu.create_time >= (:current_date - interval '7 weeks') and gu.create_time < :current_date
    where gu.create_time >= to_date('20160912', 'yyyymmdd') and gu.create_time < to_date('20161101', 'yyyymmdd')
    and gs.channel_grouping = 'Referral' -- Social, Organic Search, Direct, Referral
    group by gu.user_id, date_trunc('week', gu.create_time)::date, date_trunc('week', gs.visit_stime)::date
)
, temp_02 as (
    select t01.user_create_date
        , count(t01.*) as create_cnt
        -- w1 에서 w7까지 주단위 접속 사용자 건수 구하기
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '1 week' then 1 end) as w1_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '2 week' then 1 end) as w2_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '3 week' then 1 end) as w3_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '4 week' then 1 end) as w4_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '5 week' then 1 end) as w5_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '6 week' then 1 end) as w6_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '7 week' then 1 end) as w7_cnt
    from temp_01 t01
    group by t01.user_create_date
)
select t02.user_create_date
    , t02.create_cnt
     -- w1 에서 w7 주별 잔존율 구하기
    , round(100.0 * t02.w1_cnt / t02.create_cnt, 2) as w1_ratio
    , round(100.0 * t02.w2_cnt / t02.create_cnt, 2) as w2_ratio
    , round(100.0 * t02.w3_cnt / t02.create_cnt, 2) as w3_ratio
    , round(100.0 * t02.w4_cnt / t02.create_cnt, 2) as w4_ratio
    , round(100.0 * t02.w5_cnt / t02.create_cnt, 2) as w5_ratio
    , round(100.0 * t02.w6_cnt / t02.create_cnt, 2) as w6_ratio
    , round(100.0 * t02.w7_cnt / t02.create_cnt, 2) as w7_ratio
from temp_02 t02
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	(2016년 9월 12일 부터) 일주일간 생성된 사용자들에 대해
  	채널별 주 단위 잔존율(Retention rate)
*********************************************************************************/

-- @formatter:off
-- 채널별 주 단위 잔존율(Retention rate)
with temp_01 as (
	select gu.user_id
	    , gs.channel_grouping
        , date_trunc('week', gu.create_time)::date as user_create_date
        , date_trunc('week', gs.visit_stime)::date as sess_visit_date
        , count(gs.*) as cnt
    from ga.ga_users gu
        left outer join ga.ga_sess gs on gu.user_id = gs.user_id
    -- where gu.create_time >= (:current_date - interval '7 weeks') and gu.create_time < :current_date
    where gu.create_time >= to_date('20160912', 'yyyymmdd') and gu.create_time < to_date('20160919', 'yyyymmdd')
    group by gu.user_id, gs.channel_grouping, date_trunc('week', gu.create_time)::date, date_trunc('week', gs.visit_stime)::date
)
, temp_02 as (
    select t01.user_create_date
        , t01.channel_grouping
        , count(t01.*) as create_cnt
        -- w1 에서 w7까지 주단위 접속 사용자 건수 구하기.
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '1 week' then 1 end ) as w1_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '2 week' then 1 end) as w2_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '3 week' then 1 end) as w3_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '4 week' then 1 end) as w4_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '5 week' then 1 end) as w5_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '6 week' then 1 end) as w6_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '7 week' then 1 end) as w7_cnt
    from temp_01 t01
    group by user_create_date, channel_grouping
)
select t02.user_create_date
    , t02.channel_grouping
    , t02.create_cnt
    -- w1 에서 w7 주별 잔존율 구하기
    , round(100.0 * t02.w1_cnt / t02.create_cnt, 2) as w1_ratio
    , round(100.0 * t02.w2_cnt / t02.create_cnt, 2) as w2_ratio
    , round(100.0 * t02.w3_cnt / t02.create_cnt, 2) as w3_ratio
    , round(100.0 * t02.w4_cnt / t02.create_cnt, 2) as w4_ratio
    , round(100.0 * t02.w5_cnt / t02.create_cnt, 2) as w5_ratio
    , round(100.0 * t02.w6_cnt / t02.create_cnt, 2) as w6_ratio
    , round(100.0 * t02.w7_cnt / t02.create_cnt, 2) as w7_ratio
from temp_02 t02
order by 3 desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	7일간 생성된 총 사용자를 기반으로 총 잔존율을 구하고, 7일간 일별 잔존율을 함께 구하기
*********************************************************************************/

-- @formatter:off
-- 7일간 생성된 총 사용자를 기반으로 총 잔존율을 구하고, 7일간 일별 잔존율을 함께 구하기
with temp_01 as (
	select gu.user_id
        , date_trunc('day', gu.create_time)::date as user_create_date
        , date_trunc('day', gs.visit_stime)::date as sess_visit_date
        , count(gs.*) as cnt
    from ga.ga_users gu
        left outer join ga.ga_sess gs on gu.user_id = gs.user_id
    where gu.create_time >= (:current_date - interval '8 days') and gu.create_time < :current_date
    group by gu.user_id, date_trunc('day', gu.create_time)::date, date_trunc('day', gs.visit_stime)::date
)
, temp_02 as (
    select t01.user_create_date
        , count(t01.*) as create_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '1 day' then 1 end) as d1_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '2 day' then 1 end) as d2_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '3 day' then 1 end) as d3_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '4 day' then 1 end) as d4_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '5 day' then 1 end) as d5_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '6 day' then 1 end) as d6_cnt
        , sum(case when t01.sess_visit_date = t01.user_create_date + interval '7 day' then 1 end) as d7_cnt
    from temp_01 t01
    group by t01.user_create_date
)
-- 7일간 생성된 총 사용자를 기반으로 총 잔존율을 구하기
select 'All User' as user_create_date
    , sum(t02.create_cnt) as create_cnt
    , round(100.0 * sum(t02.d1_cnt) / sum(t02.create_cnt), 2) as d1_ratio
    , round(100.0 * sum(t02.d2_cnt) / sum(t02.create_cnt), 2) as d2_ratio
    , round(100.0 * sum(t02.d3_cnt) / sum(t02.create_cnt), 2) as d3_ratio
    , round(100.0 * sum(t02.d4_cnt) / sum(t02.create_cnt), 2) as d4_ratio
    , round(100.0 * sum(t02.d5_cnt) / sum(t02.create_cnt), 2) as d5_ratio
    , round(100.0 * sum(t02.d6_cnt) / sum(t02.create_cnt), 2) as d6_ratio
    , round(100.0 * sum(t02.d7_cnt) / sum(t02.create_cnt), 2) as d7_ratio
from temp_02 t02

union all

-- 7일간 일별 잔존율
select to_char(t02.user_create_date, 'yyyy-mm-dd') as user_create_date
    , t02.create_cnt
    , round(100.0 * t02.d1_cnt / t02.create_cnt, 2) as d1_ratio
    , round(100.0 * t02.d2_cnt / t02.create_cnt, 2) as d2_ratio
    , round(100.0 * t02.d3_cnt / t02.create_cnt, 2) as d3_ratio
    , round(100.0 * t02.d4_cnt / t02.create_cnt, 2) as d4_ratio
    , round(100.0 * t02.d5_cnt / t02.create_cnt, 2) as d5_ratio
    , round(100.0 * t02.d6_cnt / t02.create_cnt, 2) as d6_ratio
    , round(100.0 * t02.d7_cnt / t02.create_cnt, 2) as d7_ratio
from temp_02 t02

order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	전체 매출 전환율 및 일별, 월별 매출 전환율과 매출액
*********************************************************************************/

/*
   Unknown = 0. (홈페이지)
   Click through of product lists = 1, (상품 목록 선택)
   Product detail views = 2, (상품 상세 선택)
   Add product(s) to cart = 3, (카트에 상품 추가)
   Remove product(s) from cart = 4, (카트에서 상품 제거)
   Check out = 5, (결재 시작)
   Completed purchase = 6, (구매 완료)
   Refund of purchase = 7, (환불)
   Checkout options = 8 (결재 옵션 선택)
   이 중 1, 3, 4가 주로 EVENT로 발생. 0, 2, 5, 6은 주로 PAGE로 발생.
*/

-- @formatter:off
-- action_type별 hit_type에 따른 건수
select gsh.action_type
    , count(gsh.*) as action_cnt
    , sum(case when gsh.hit_type = 'PAGE' then 1 else 0 end)  as page_action_cnt
    , sum(case when gsh.hit_type = 'EVENT' then 1 else 0 end) as event_action_cnt
from ga.ga_sess_hits gsh
group by gsh.action_type
-- @formatter:on
;

-- @formatter:off
-- 전체 매출 전환율
with temp_01 as (
    select count(distinct gsh.sess_id) as purchase_sess_cnt
    from ga.ga_sess_hits gsh
    where gsh.action_type = '6'
)
, temp_02 as (
    select count(distinct gsh.sess_id) as sess_cnt
    from ga.ga_sess_hits gsh
)
select t01.purchase_sess_cnt
    , t02.sess_cnt
    , 100.0 * t01.purchase_sess_cnt / sess_cnt as sale_cv_rate
from temp_01 t01
    cross join temp_02 t02
-- @formatter:on
;

-- @formatter:off
-- 과거 1주일간 매출 전환률
with temp_01 as (
    select count(distinct gsh.sess_id) as purchase_sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gsh.action_type = '6'
    and gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
)
, temp_02 as (
    select count(distinct gsh.sess_id) as sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
)
select t01.purchase_sess_cnt
    , t02.sess_cnt
    , 100.0 * t01.purchase_sess_cnt / sess_cnt as sale_cv_rate
from temp_01 t01
    cross join temp_02 t02
-- @formatter:on
;

-- @formatter:off
-- 과거 1주일간 일별 매출 전환률 - 01
with temp_01 as (
    select date_trunc('day', gs.visit_stime)::date as cv_day
        , count(distinct gsh.sess_id) as purchase_sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gsh.action_type = '6'
    and gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
    group by date_trunc('day', gs.visit_stime)::date
)
, temp_02 as (
    select date_trunc('day', gs.visit_stime)::date as cv_day
        , count(distinct gsh.sess_id) as sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
    group by date_trunc('day', gs.visit_stime)::date
)
select t01.cv_day
    , t01.purchase_sess_cnt
    , t02.sess_cnt
    , 100.0 * t01.purchase_sess_cnt / sess_cnt as sale_cv_rate
from temp_01 t01
    inner join temp_02 t02 on t01.cv_day = t02.cv_day
-- @formatter:on
;

-- @formatter:off
-- 과거 1주일간 일별 매출 전환률 - 02
with temp_01 as (
    select date_trunc('day', gs.visit_stime)::date as cv_day
        , count(distinct gsh.sess_id) as sess_cnt
        , count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '7 days') and gs.visit_stime < :current_date
    group by date_trunc('day', gs.visit_stime)::date
)
select t01.cv_day
    , t01.purchase_sess_cnt
    , t01.sess_cnt
    , 100.0 * t01.purchase_sess_cnt / sess_cnt as sale_cv_rate
from temp_01 t01
-- @formatter:on
;

-- 과거 1주일간 일별 매출 전환률 및 매출액
with temp_01 as (select date_trunc('day', gs.visit_stime)::date                              as cv_day
                      , count(distinct gsh.sess_id)                                          as sess_cnt
                      , count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt
                 from ga.ga_sess_hits gsh
                          inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
                 where gs.visit_stime >= (:current_date - interval '7 days')
                   and gs.visit_stime < :current_date
                 group by date_trunc('day', gs.visit_stime)::date)
   , temp_02 as (select date_trunc('day', o.order_time)::date as ord_day
                      , sum(oi.prod_revenue)                  as sum_revenue
                 from ga.orders o
                          inner join ga.order_items oi on o.order_id = oi.order_id
                 where o.order_time >= (:current_date - interval '7 days')
                   and o.order_time < :current_date
                 group by date_trunc('day', o.order_time)::date)
select t01.cv_day
     , t02.ord_day
     , t01.sess_cnt
     , t01.purchase_sess_cnt
     , 100.0 * purchase_sess_cnt / sess_cnt    as sale_cv_rate
     , t02.sum_revenue
     , t02.sum_revenue / t01.purchase_sess_cnt as revenue_per_purchase_sess
from temp_01 t01
         left outer join temp_02 t02 on t01.cv_day = t02.ord_day
;

-- 월별 매출 전환률과 매출액
with temp_01 as (select date_trunc('month', gs.visit_stime)::date                            as cv_month
                      , count(distinct gsh.sess_id)                                          as sess_cnt
                      , count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as purchase_sess_cnt
                 from ga.ga_sess_hits gsh
                          inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
                 group by date_trunc('month', gs.visit_stime)::date)
   , temp_02 as (select date_trunc('month', o.order_time)::date as ord_month
                      , sum(oi.prod_revenue)                    as sum_revenue
                 from ga.orders o
                          inner join ga.order_items oi on o.order_id = oi.order_id
                 group by date_trunc('month', o.order_time)::date)
select t01.cv_month
     , t02.ord_month
     , t01.sess_cnt
     , t01.purchase_sess_cnt
     , 100.0 * purchase_sess_cnt / sess_cnt    as sale_cv_rate
     , t02.sum_revenue
     , t02.sum_revenue / t01.purchase_sess_cnt as revenue_per_purchase_sess
from temp_01 t01
         left outer join temp_02 t02 on t01.cv_month = t02.ord_month
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	채널별 월별 매출 전환율과 매출액
*********************************************************************************/

-- @formatter:off
-- 채널별 월별 매출 전환율과 매출액
with temp_01 as (
    select gs.channel_grouping
        , date_trunc('month', gs.visit_stime)::date as cv_month
        , count(distinct gsh.sess_id) as sess_cnt
        , count(distinct case when gsh.action_type = '6' then gsh.sess_id end) as pur_sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    group by gs.channel_grouping, date_trunc('month', gs.visit_stime)::date
)
, temp_02 as (
    select gs.channel_grouping
        , date_trunc('month', o.order_time)::date as ord_month
        , sum(oi.prod_revenue) as sum_revenue
    from ga.ga_sess gs
        inner join ga.orders o on gs.sess_id = o.sess_id
        inner join ga.order_items oi on o.order_id = oi.order_id
    group by gs.channel_grouping, date_trunc('month', o.order_time)::date
)
select t01.channel_grouping
    , t01.cv_month
    , t01.pur_sess_cnt
    , t01.sess_cnt
    , round(100.0 * t01.pur_sess_cnt / t01.sess_cnt, 2) as sale_cv_rate
    , t02.ord_month
    , round(t02.sum_revenue::numeric, 2) as sum_revenue
    , round(t02.sum_revenue::numeric / t01.pur_sess_cnt, 2) as rev_per_pur_sess
from temp_01 t01
    left outer join temp_02 t02 on t01.channel_grouping = t02.channel_grouping and t01.cv_month = t02.ord_month
order by 1, 2
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	월별 신규 사용자의 매출 전환율
*********************************************************************************/

-- @formatter:off
-- 월별 신규 사용자 건수
with temp_01 as (
    select gs.sess_id
        , gs.user_id
        , gs.visit_stime
        , gu.create_time
        , case
            when date_trunc('day', gu.create_time)::date >= date_trunc('month', visit_stime)::date
                and date_trunc('day', gu.create_time)::date < date_trunc('month', visit_stime)::date + interval '1 month'
            then 1
            else 0
        end as is_monthly_new_user
    from ga.ga_sess gs
        inner join ga.ga_users gu on gs.user_id = gu.user_id
)
select date_trunc('month', t01.visit_stime)::date
     , count(t01.*) as sess_cnt
     , count(distinct t01.user_id) as user_cnt
     , sum(case when t01.is_monthly_new_user = 1 then 1 end) as new_user_sess_cnt
     , count(distinct case when t01.is_monthly_new_user = 1 then t01.user_id end) as new_user_cnt
from temp_01 t01
group by date_trunc('month', t01.visit_stime)::date
-- @formatter:on
;

-- @formatter:off
-- 월별 신규 사용자의 매출 전환율 - 01
with temp_01 as (
    select gs.sess_id
        , gs.user_id
        , gs.visit_stime
        , gu.create_time
        , case
            when date_trunc('day', gu.create_time)::date >= date_trunc('month', visit_stime)::date
                and date_trunc('day', gu.create_time)::date < date_trunc('month', visit_stime)::date + interval '1 month'
            then 1
        else 0
        end as is_monthly_new_user
    from ga.ga_sess gs
    inner join ga.ga_users gu on gs.user_id = gu.user_id
)
-- 매출 전환한 월별 신규 생성자 세션 건수
, temp_02 as (
    select date_trunc('month', t01.visit_stime)::date as cv_month
        , count(distinct gsh.sess_id) as purchase_sess_cnt
    from temp_01 t01
        inner join ga.ga_sess_hits gsh on t01.sess_id = gsh.sess_id
    where t01.is_monthly_new_user = 1
    and gsh.action_type = '6'
    group by date_trunc('month', t01.visit_stime)::date
)
-- 월별 신규 생성자 세션 건수
, temp_03 as (
    select date_trunc('month', t01.visit_stime)::date as cv_month
        , sum(case when t01.is_monthly_new_user = 1 then 1 else 0 end) as monthly_nuser_sess_cnt
    from temp_01 t01
    group by date_trunc('month', t01.visit_stime)::date
)
select t02.cv_month
    , t02.purchase_sess_cnt
    , t03.monthly_nuser_sess_cnt
    , 100.0 * t02.purchase_sess_cnt / t03.monthly_nuser_sess_cnt as sale_cv_rate
from temp_02 t02
    inner join temp_03 t03 on t02.cv_month = t03.cv_month
order by 1
-- @formatter:on
;

-- @formatter:off
-- 매출 전환한 월별 신규 생성자 세션 건수와 월별 신규 생성자 세션 건수를 같이 구함
-- 월별 신규 사용자의 매출 전환율 - 02
with temp_01 as (
    select gs.sess_id
        , gs.user_id
        , gs.visit_stime
        , gu.create_time
        , case
            when date_trunc('day', gu.create_time)::date >= date_trunc('month', visit_stime)::date
                and date_trunc('day', gu.create_time)::date < date_trunc('month', visit_stime)::date + interval '1 month'
            then 1
            else 0
        end as is_monthly_new_user
    from ga.ga_sess gs
    inner join ga.ga_users gu on gs.user_id = gu.user_id
)
-- 매출 전환한 월별 신규 생성자 세션 건수와 월별 신규 생성자 세션 건수를 같이 구함.
, temp_02 as (
    select date_trunc('month', t01.visit_stime)::date as cv_month
        , count(distinct case when t01.is_monthly_new_user = 1 and gsh.action_type = '6' then t01.sess_id end) as purchase_sess_cnt
        , count(distinct case when t01.is_monthly_new_user = 1 then t01.sess_id end) as monthly_nuser_sess_cnt
    from temp_01 t01
        inner join ga.ga_sess_hits gsh on t01.sess_id = gsh.sess_id
    group by date_trunc('month', t01.visit_stime)::date
)
select t02.cv_month
    , t02.purchase_sess_cnt
    , t02.monthly_nuser_sess_cnt
    , 100.0 * t02.purchase_sess_cnt / t02.monthly_nuser_sess_cnt as sale_cv_rate
from temp_02 t02
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	전환 퍼널(conversion funnel) 구하기
*********************************************************************************/

/*
   Unknown = 0. (홈페이지)
   Click through of product lists = 1, (상품 목록 선택)
   Product detail views = 2, (상품 상세 선택)
   Add product(s) to cart = 3, (카트에 상품 추가)
   Remove product(s) from cart = 4, (카트에서 상품 제거)
   Check out = 5, (결재 시작)
   Completed purchase = 6, (구매 완료)
   Refund of purchase = 7, (환불)
   Checkout options = 8 (결재 옵션 선택)
   이 중 1, 3, 4가 주로 EVENT로 발생. 0, 2, 5, 6은 주로 PAGE로 발생.
*/

select *
from ga.ga_sess_hits
where sess_id = 'S0213506'
order by hit_seq
;

-- @formatter:off
-- 1주일간 세션 히트 데이터에서 세션별로 action_type의 중복 hit를 제거하고 세션별 고유한 action_type만 추출
drop table if exists ga.temp_funnel_base;

create table ga.temp_funnel_base
as
select t.*
from (
    select gsh.*
    , gs.visit_stime
    , gs.channel_grouping
    , row_number() over (partition by gsh.sess_id, gsh.action_type order by gsh.hit_seq) as action_seq
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (to_date('2016-10-31', 'yyyy-mm-dd') - interval '7 days')
    and gs.visit_stime < to_date('2016-10-31', 'yyyy-mm-dd')
) t
where t.action_seq = 1
-- @formatter:on
;

-- @formatter:off
-- action_type 전환 퍼널 세션 수 구하기(FLOW를 순차적으로 수행한 전환 퍼널)
-- action_type 0 -> 1 -> 2 -> 3 -> 5 -> 6 순으로의 전환 퍼널 세션 수를 구함.
with temp_act_0 as (
    select tfb.sess_id, tfb.hit_type, tfb.action_type
    from ga.temp_funnel_base tfb
    where tfb.action_type = '0'
)
, temp_hit_02 as (
    select ta0.sess_id as home_sess_id
        , tfb1.sess_id as plist_sess_id
        , tfb2.sess_id as pdetail_sess_id
        , tfb3.sess_id as cart_sess_id
        , tfb4.sess_id as check_sess_id
        , tfb5.sess_id as pur_sess_id
    from temp_act_0 ta0
    left outer join ga.temp_funnel_base tfb1 on (ta0.sess_id = tfb1.sess_id and tfb1.action_type = '1')
    left outer join ga.temp_funnel_base tfb2 on (tfb1.sess_id = tfb2.sess_id and tfb2.action_type = '2')
    left outer join ga.temp_funnel_base tfb3 on (tfb2.sess_id = tfb3.sess_id and tfb3.action_type = '3')
    left outer join ga.temp_funnel_base tfb4 on (tfb3.sess_id = tfb4.sess_id and tfb4.action_type = '5')
    left outer join ga.temp_funnel_base tfb5 on (tfb4.sess_id = tfb5.sess_id and tfb5.action_type = '6')
)
select count(th02.home_sess_id) as home_sess_cnt
    , count(th02.plist_sess_id) as plist_sess_cnt
    , count(th02.pdetail_sess_id) as pdetail_sess_cnt
    , count(th02.cart_sess_id) as cart_sess_cnt
    , count(th02.check_sess_id) as check_sess_cnt
    , count(th02.pur_sess_id) as purchase_sess_cnt
from temp_hit_02 th02
-- @formatter:on
;

-- @formatter:off
-- action_type 전환 퍼널 세션 수 구하기(FLOW를 스킵한 세션까지 포함한 전환 퍼널)
with temp_01 as (
    select count(tfb.sess_id) as home_sess_cnt
    from ga.temp_funnel_base tfb
    where tfb.action_type = '0'
)
, temp_02 as (
    select count(tfb.sess_id) as plist_sess_cnt
    from ga.temp_funnel_base tfb
    where tfb.action_type = '1'
)
, temp_03 as (
    select count(tfb.sess_id) as pdetail_sess_cnt
    from ga.temp_funnel_base tfb
    where tfb.action_type = '2'
)
, temp_04 as (
    select count(tfb.sess_id) as cart_sess_cnt
    from ga.temp_funnel_base tfb
    where tfb.action_type = '3'
)
, temp_05 as (
    select count(tfb.sess_id) as check_sess_cnt
    from ga.temp_funnel_base tfb
    where tfb.action_type = '5'
)
, temp_06 as (
    select count(tfb.sess_id) as purchase_sess_cnt
    from ga.temp_funnel_base tfb
    where tfb.action_type = '6'
)
select t01.home_sess_cnt
    , t02.plist_sess_cnt
    , t03.pdetail_sess_cnt
    , t04.cart_sess_cnt
    , t05.check_sess_cnt
    , t06.purchase_sess_cnt
from temp_01 t01
    cross join temp_02 t02
    cross join temp_03 t03
    cross join temp_04 t04
    cross join temp_05 t05
    cross join temp_06 t06
-- @formatter:on
;

-- @formatter:off
-- 채널별 action_type 전환 퍼널 세션 수 구하기
-- 채널별로 action_type 0 -> 1 -> 2 -> 3 -> 6 순으로의 전환 퍼널 세션 수를 구함.
with temp_act_0 as (
    select tfb.sess_id, tfb.hit_type, tfb.action_type, tfb.channel_grouping
    from ga.temp_funnel_base tfb
    where tfb.action_type = '0'
)
, temp_hit_02 as (
    select ta0.sess_id as home_sess_id
        , ta0.channel_grouping as home_cgrp
        , tfb1.sess_id as plist_sess_id
        , tfb1.channel_grouping as plist_cgrp
        , tfb2.sess_id as pdetail_sess_id
        , tfb2.channel_grouping as pdetail_cgrp
        , tfb3.sess_id as cart_sess_id
        , tfb3.channel_grouping as cart_cgrp
        , tfb4.sess_id as check_sess_id
        , tfb4.channel_grouping as check_cgrp
        , tfb5.sess_id as pur_sess_id
        , tfb5.channel_grouping as pur_cgrp
    from temp_act_0 ta0
    left outer join ga.temp_funnel_base tfb1 on (ta0.sess_id = tfb1.sess_id and tfb1.action_type = '1')
    left outer join ga.temp_funnel_base tfb2 on (tfb1.sess_id = tfb2.sess_id and tfb2.action_type = '2')
    left outer join ga.temp_funnel_base tfb3 on (tfb2.sess_id = tfb3.sess_id and tfb3.action_type = '3')
    left outer join ga.temp_funnel_base tfb4 on (tfb3.sess_id = tfb4.sess_id and tfb4.action_type = '5')
    left outer join ga.temp_funnel_base tfb5 on (tfb4.sess_id = tfb5.sess_id and tfb5.action_type = '6')
)
select th02.home_cgrp
     , count(th02.home_sess_id) as home_sess_cnt
     , count(th02.plist_sess_id) as plist_sess_cnt
     , count(th02.pdetail_sess_id) as pdetail_sess_cnt
     , count(th02.cart_sess_id) as cart_sess_cnt
     , count(th02.check_sess_id) as check_sess_cnt
     , count(th02.pur_sess_id) as purchase_sess_cnt
from temp_hit_02 th02
group by th02.home_cgrp
-- @formatter:on
;