------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	MAU를 신규 사용자, 기존 사용자(재 방문) 건수로 분리하여 추출(세션 건수도 함께 추출)
*********************************************************************************/

-- @formatter:off
-- MAU를 신규 사용자, 기존 사용자(재 방문) 건수로 분리하여 추출
with temp_01 as (
    select gs.sess_id
        , gs.user_id
        , gs.visit_stime
        , gu.create_time
        , case
            when gu.create_time >= (:current_date - interval '30 days') and gu.create_time < :current_date
            then 1
            else 0
        end as is_new_user
    from ga.ga_sess gs
        inner join ga.ga_users gu on gs.user_id = gu.user_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
 )
select
    count(distinct t01.user_id) as user_cnt
    , count(distinct case when t01.is_new_user = 1 then t01.user_id end) as new_user_cnt
    , count(distinct case when t01.is_new_user = 0 then t01.user_id end) as repeat_user_cnt
    , count(*) as sess_cnt
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	채널별로 MAU를 신규 사용자, 기존 사용자로 나누고, 채널별 비율까지 함께 계산
*********************************************************************************/

-- 채널 그룹별로 조회
select channel_grouping, count(distinct user_id)
from ga.ga_sess
group by channel_grouping
;

-- @formatter:off
-- 채널별로 MAU를 신규 사용자, 기존 사용자로 나누고, 채널별 비율까지 함께 계산
with temp_01 as (
    select gs.sess_id
        , gs.user_id
        , gs.visit_stime
        , gu.create_time
        , gs.channel_grouping
        , case
            when gu.create_time >= (:current_date - interval '30 days') and gu.create_time < :current_date
            then 1
            else 0
        end as is_new_user
    from ga.ga_sess gs
        inner join ga.ga_users gu on gs.user_id = gu.user_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
)
, temp_02 as (
    select t01.channel_grouping
        , count(distinct case when t01.is_new_user = 1 then t01.user_id end) as new_user_cnt
        , count(distinct case when t01.is_new_user = 0 then t01.user_id end) as repeat_user_cnt
        , count(distinct t01.user_id) as channel_user_cnt
        , count(t01.*) as sess_cnt
    from temp_01 t01
    group by t01.channel_grouping
)
select t02.channel_grouping
    , t02.new_user_cnt
    , t02.repeat_user_cnt
    , t02.channel_user_cnt
    , t02.sess_cnt
    , 100.0 * t02.new_user_cnt / sum(t02.new_user_cnt) over () as new_user_cnt_by_channel
    , 100.0 * t02.repeat_user_cnt / sum(t02.repeat_user_cnt) over () as repeat_user_cnt_by_channel
from temp_02 t02
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	채널별 고유 사용자 건수와 매출금액 및 비율, 주문 사용자 건수와 주문 매출 금액 및 비율
	채널별로 고유 사용자 건수와 매출 금액을 구하고 고유 사용자 건수 대비 매출 금액 비율을 추출
	또한 고유 사용자 중에서 주문을 수행한 사용자 건수를 추출 후
  	주문 사용자 건수 대비 매출 금액 비율을 추출
*********************************************************************************/

-- @formatter:off
-- 채널별 고유 사용자 건수와 매출금액 및 비율, 주문 사용자 건수와 주문 매출 금액 및 비율
with temp_01 as (
    select gs.sess_id
        , gs.user_id
        , gs.channel_grouping
        , o.order_id
        , o.order_time
        , oi.product_id
        , oi.prod_revenue
    from ga.ga_sess gs
        left outer join ga.orders o on gs.sess_id = o.sess_id
        left outer join ga.order_items oi on o.order_id = oi.order_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
)
select t01.channel_grouping
    , sum(t01.prod_revenue) as ch_amt -- 채널별 매출
    --, count(distinct t01.sess_id) as ch_sess_cnt -- 채널별 고유 세션 수
    , count(distinct t01.user_id) as ch_user_cnt -- 채널별 고유 사용자 수
    --, count(distinct case when t01.order_id is not null then t01.sess_id end) as ch_ord_sess_cnt -- 채널별 주문 고유 세션수
    , count(distinct case when t01.order_id is not null then t01.user_id end) as ch_ord_user_cnt -- 채널별 주문 고유 사용자수
    --, sum(t01.prod_revenue) / count(distinct t01.sess_id) as ch_amt_per_sess -- 접속 세션별 주문 매출 금액
    , sum(t01.prod_revenue) / count(distinct t01.user_id) as ch_amt_per_user -- 접속 고유 사용자별 주문 매출 금액
    -- 주문 세션별 매출 금액
    --, sum(t01.prod_revenue) / count(distinct case when t01.order_id is not null then t01.sess_id end) as ch_ord_amt_per_sess
    -- 주문 고유 사용자별 매출 금액
    , sum(t01.prod_revenue) / count(distinct case when t01.order_id is not null then t01.user_id end) as ch_ord_amt_per_user
from temp_01 t01
group by t01.channel_grouping
order by ch_user_cnt desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	device 별 접속 건수 , 전체 건수대비 device별 접속 건수
	일/주별 device별 접속건수
*********************************************************************************/

-- device 별 접속 건수
select gs.device_category, count(*) as device_cnt
from ga.ga_sess gs
group by gs.device_category
;

-- 전체 건수 대비 device별 접속 건수
with temp_01 as (select count(gs.*) as total_cnt
                 from ga.ga_sess gs)
   , temp_02 as (select gs.device_category, count(*) as device_cnt
                 from ga.ga_sess gs
                 group by gs.device_category)
select t02.device_category, t02.device_cnt, 1.0 * t02.device_cnt / t01.total_cnt
from temp_01 t01,
     temp_02 t02
;

-- @formatter:off
-- mobile과 tablet을 함께 합쳐서 mobile_tablet으로 접속 건수 조사
select case
        when gs.device_category in ('mobile', 'tablet') then 'mobile_tablet'
        when gs.device_category = 'desktop' then 'desktop' end as device_category
    , count(*) as device_cnt
from ga.ga_sess gs
group by case
    when device_category in ('mobile', 'tablet') then 'mobile_tablet'
    when device_category = 'desktop' then 'desktop' end
-- @formatter:on
;

-- @formatter:off
-- 일별 접속자를 desktop, mobile, tablet 에 따라 접속자수 계산.
select date_trunc('day', gs.visit_stime)
     , sum(case when gs.device_category = 'desktop' then 1 else 0 end) as desktop_cnt
     , sum(case when gs.device_category = 'mobile' then 1 else 0 end) as mobile_cnt
     , sum(case when gs.device_category = 'tablet' then 1 else 0 end) as tablet_cnt
     , count(*)
from ga.ga_sess gs
group by date_trunc('day', gs.visit_stime)
-- @formatter:on
;

-- @formatter:off
-- 주별 접속자를 desktop, mobile, tablet 에 따라 접속자수 계산.
select date_trunc('week', gs.visit_stime)
     , sum(case when gs.device_category = 'desktop' then 1 else 0 end) as desktop_cnt
     , sum(case when gs.device_category = 'mobile' then 1 else 0 end) as mobile_cnt
     , sum(case when gs.device_category = 'tablet' then 1 else 0 end) as tablet_cnt
     , count(*)
from ga.ga_sess gs
group by date_trunc('week', gs.visit_stime)
-- @formatter:on
;

-- @formatter:off
-- 접속 device 별 매출과 device별 세션당 매출과 사용자별 매출액 추출.
with temp_01 as (
    select o.order_id
         , o.order_time
         , oi.product_id
         , oi.prod_revenue
         , gs.sess_id
         , gs.user_id
         , gs.device_category
    from ga.orders o
        inner join ga.order_items oi on o.order_id = oi.order_id
        inner join ga.ga_sess gs on o.sess_id = gs.sess_id
    where o.order_status = 'delivered'
)
select t01.device_category
     , sum(t01.prod_revenue) as device_sum_amount
     , count(distinct t01.sess_id) as sess_cnt
     , count(distinct t01.user_id) as user_cnt
     , sum(t01.prod_revenue) / count(distinct t01.sess_id) as sum_amount_per_sess
     , sum(t01.prod_revenue) / count(distinct t01.user_id) as sum_amount_per_user
from temp_01 t01
group by t01.device_category
-- @formatter:on
;