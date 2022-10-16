-- :current_date 값은 to_date('20161101', 'yyyymmdd')

/*********************************************************************************
	일별 세션건수, 일별 방문 사용자(유저), 사용자별 평균 세션 수
*********************************************************************************/

-- @formatter:off
-- 일별 세션건수, 일별 방문 사용자(유저), 사용자별 평균 세션 수
with temp_01 as (
    select
        to_char(date_trunc('day', gs.visit_stime), 'yyyy-mm-dd') as d_day
		-- ga_sess 테이블에는 sess_id로 unique하므로 count(sess_id)와 동일
        , count(distinct gs.sess_id) as daily_sess_cnt -- 일별 세션건수 (세션 아이디가 unique 하지 않는 경우)
        , count(gs.sess_id) as daily_sess_cnt_again -- 일별 세션건수
        , count(distinct gs.user_id) as daily_user_cnt -- 일별 방문 사용자
	from ga.ga_sess gs
	group by to_char(date_trunc('day', gs.visit_stime), 'yyyy-mm-dd')
)
select
    t01.*
    , 1.0 * t01.daily_sess_cnt / t01.daily_user_cnt as avg_user_sessions
	-- 아래와 같이 정수와 정수를 나눌 때 postgresql은 정수로 형변환 함  위 처럼 1.0을 곱해주거나 명시적으로 float type선언
	, t01.daily_sess_cnt / t01.daily_user_cnt as avg_user_sessions_floor
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	DAU, WAU, MAU 구하기
    DAU(Daily Active Users) - 하루 동안 방문한 순수 사용자 수
    WAU(Weekly Active Users) - 일주일 동안 방문한 순수 사용자 수
    MAU(Monthly Active Users) - 한달(30일) 동안 방문한 순수 사용자 수
*********************************************************************************/

/* 아래는 이미 많은 과거 데이터가 있을 경우를 가정하고 DAU, WAU, MAU를 추출함 */

-- 일별 방문한 고객 수(DAU)
select date_trunc('day', gs.visit_stime)::date as d_day, count(distinct gs.user_id) as user_cnt
from ga.ga_sess gs
-- where gs.visit_stime between to_date('2016-10-25', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('day', gs.visit_stime)::date
;

-- 주별 방문한 고객수(WAU)
select date_trunc('week', gs.visit_stime)::date as week_d기y, count(distinct gs.user_id) as user_cnt
from ga.ga_sess gs
-- where gs.visit_stime between to_date('2016-10-24', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('week', gs.visit_stime)::date
order by 1;

-- 월별 방문한 고객수(MAU)
select date_trunc('month', visit_stime)::date as month_day, count(distinct user_id) as user_cnt
from ga.ga_sess
-- where gs.visit_stime between to_date('2016-10-2', 'yyyy-mm-dd') and to_timestamp('2016-10-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
group by date_trunc('month', visit_stime)::date;

/* 아래는 하루 주기로 계속 DAU, WAU(이전 7일), MAU(이전 30일)를 계속 추출 */

-- interval로 전일 7일 구하기
select to_date('20161101', 'yyyymmdd') - interval '7 days';

-- 현재 일을 기준으로 전일의 DAU 구하기
select :current_date, count(distinct gs.user_id) as dau
from ga.ga_sess gs
where gs.visit_stime >= (:current_date - interval '1 days')
  and gs.visit_stime < :current_date
;

-- 현재 일을 기준으로 전 7일의 WAU 구하기
select :current_date, count(distinct gs.user_id) as wau
from ga.ga_sess gs
where gs.visit_stime >= (:current_date - interval '7 days')
  and gs.visit_stime < :current_date
;

-- 현재 일을 기준으로 전 30일의 mau 구하기
select :current_date, count(distinct gs.user_id) as mau
from ga.ga_sess gs
where gs.visit_stime >= (:current_date - interval '30 days')
  and gs.visit_stime < :current_date
;

-- 날짜별로 DAU, WAU, MAU 값을 가지는 테이블 생성
create table if not exists ga.daily_acquisitions
(
    curr_date date,
    dau       integer,
    wau       integer,
    mau       integer
);

-- @formatter:off
-- daily_acquisitions 테이블에 지정된 current_date별 DAU, WAU, MAU을 입력
insert into ga.daily_acquisitions
select
    :current_date
    -- scalar subquery는 select 절에 사용가능하면 단 한건, 한 컬럼만 추출되어야 함
    , (
        select count(distinct gs.user_id) as dau
        from ga.ga_sess gs
        where gs.visit_stime >= (:current_date - interval '1 days')
        and gs.visit_stime < :current_date
    )
    , (
        select count(distinct gs.user_id) as wau
        from ga.ga_sess gs
        where gs.visit_stime >= (:current_date - interval '7 days')
        and gs.visit_stime < :current_date
    )
    , (
        select count(distinct gs.user_id) as mau
        from ga.ga_sess gs
        where gs.visit_stime >= (:current_date - interval '30 days')
        and gs.visit_stime < :current_date
    )
-- @formatter:on
;

-- 데이터 입력 확인
select *
from ga.daily_acquisitions
;

-- @formatter:off
-- 과거 일자별로 DAU 생성
with temp_00 as (
    select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date
)
select t00.curr_date, count(distinct gs.user_id) as dau
from ga.ga_sess gs
    cross join temp_00 t00
where gs.visit_stime >= (t00.curr_date - interval '1 days')
  and gs.visit_stime < t00.curr_date
group by t00.curr_date
-- @formatter:on
;

-- @formatter:off
-- 과거 일자별로 지난 7일 WAU 생성
with temp_00 as (
    select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date
)
select t00.curr_date, count(distinct gs.user_id) as dau
from ga.ga_sess gs
    cross join temp_00 t00
where gs.visit_stime >= (t00.curr_date - interval '7 days')
  and gs.visit_stime < t00.curr_date
group by t00.curr_date
-- @formatter:on
;

-- @formatter:off
-- 과거 일자별로 지난 30일의 MAU 설정
with temp_00 as (
    select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date
)
select t00.curr_date, count(distinct gs.user_id) as dau
from ga.ga_sess gs
    cross join temp_00 t00
where gs.visit_stime >= (t00.curr_date - interval '30 days')
  and gs.visit_stime < t00.curr_date
group by t00.curr_date
-- @formatter:on
;

--데이터 확인 81587, 80693, 80082
select count(distinct gs.user_id) as mau
from ga.ga_sess gs
where gs.visit_stime >= (:current_date - interval '30 days')
  and gs.visit_stime < :current_date
;

-- 과거 일자별로 DAU 생성하는 임시 테이블 생성
drop table if exists ga.daily_dau
;

-- @formatter:off
create table ga.daily_dau
as
with temp_00 as (
    select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date
)
select t00.curr_date, count(distinct gs.user_id) as dau
from ga.ga_sess gs
    cross join temp_00 t00
where gs.visit_stime >= (t00.curr_date - interval '1 days')
  and gs.visit_stime < t00.curr_date
group by t00.curr_date
-- @formatter:on
;

-- 과거 일자별로 WAU 생성하는 임시 테이블 생성
drop table if exists ga.daily_wau
;

-- @formatter:off
create table ga.daily_wau
as
with temp_00 as (
    select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date
)
select t00.curr_date, count(distinct gs.user_id) as wau
from ga.ga_sess gs
    cross join temp_00 t00
where gs.visit_stime >= (t00.curr_date - interval '7 days')
  and gs.visit_stime < t00.curr_date
group by t00.curr_date
-- @formatter:on
;

-- 과거 일자별로 MAU 생성하는 임시 테이블 생성
drop table if exists ga.daily_mau;

-- @formatter:off
create table ga.daily_mau
as
with temp_00 as (
    select generate_series('2016-08-02'::date, '2016-11-01'::date, '1 day'::interval)::date as curr_date
)
select t00.curr_date, count(distinct gs.user_id) as mau
from ga.ga_sess gs
    cross join temp_00 t00
where gs.visit_stime >= (t00.curr_date - interval '30 days')
  and gs.visit_stime < t00.curr_date
group by t00.curr_date
-- @formatter:on
;

-- DAU, WAU, MAU 임시테이블을 일자별로 조인하여 daily_acquisitions 테이블 생성.
drop table if exists ga.daily_acquisitions;

-- @formatter:off
create table ga.daily_acquisitions
as
select dd.curr_date, dd.dau, dw.wau, dm.mau
from ga.daily_dau dd
    inner join ga.daily_wau dw on dd.curr_date = dw.curr_date
    inner join ga.daily_mau dm on dd.curr_date = dm.curr_date
-- @formatter:on
;

-- 데이터 확인
select da.*
from ga.daily_acquisitions da
;

drop table if exists ga.daily_acquisitions
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	DAU와 MAU의 비율. 고착도(stickiness)
  	월간 사용자들중 얼마나 많은 사용자가 주기적으로 방문하는가?
  	재방문 지표로 서비스의 활성화 지표 제공
*********************************************************************************/

-- @formatter:off
-- DAU와 MAU의 비율
with temp_dau as (
    select :current_date as curr_date, count(distinct gs.user_id) as dau
    from ga.ga_sess gs
    where gs.visit_stime >= (:current_date - interval '1 days') and gs.visit_stime < :current_date
)
, temp_mau as (
    select :current_date as curr_date, count(distinct gs.user_id) as mau
    from ga.ga_sess gs
    where gs.visit_stime >= (:current_date - interval '30 days') and gs.visit_stime < :current_date
)
select td.curr_date, td.dau, tm.mau, round(100.0 * td.dau / tm.mau, 2) as stickieness
from temp_dau td
    inner join temp_mau tm on td.curr_date = tm.curr_date
-- @formatter:on
;

-- @formatter:off
-- 일주일간 stickiess, 평균 stickness
select da.*
     , round(100.0 * da.dau / da.mau, 2) as stickieness
     , round(avg(100.0 * da.dau / da.mau) over (), 2) as avg_stickieness
from ga.daily_acquisitions da
where da.curr_date between to_date('2016-10-25', 'yyyy-mm-dd') and to_date('2016-10-31', 'yyyy-mm-dd')
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	사용자별 월별 세션 접속 횟수 구간별 분포 집계
	step 1: 사용자별 월별 접속 횟수, (월말 3일 이전 생성된 사용자 제외)
	step 2: 사용자별 월별 접속 횟수 구간별 분포 . 월별 + 접속 횟수 구간별로 Group by
	step 3: gubun 별로 pivot 하여 추출
*********************************************************************************/

-- user 생성일자가 해당 월의 마지막 일에서 3일전인 user 추출
-- 월의 마지막 일자 구하기
-- postgresql은 last_day()함수가 없음, 때문에 해당 일자가 속한 달의 첫번째 날짜 가령 10월 5일이면 10월 1일에 1달을 더하고 거기에 1일을 뺌
-- 즉 10월 5일 -> 10월 1일 -> 11월 1일 -> 10월 31일 순으로 계산함.
select user_id, create_time, (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date
from ga.ga_users
where create_time <= (date_trunc('month', create_time) + interval '1 month' - interval '1 day')::date - 2;

-- @formatter:off
-- 사용자별 월별 세션접속 횟수, 월말 3일 이전 생성된 사용자 제외
select gs.user_id
    , date_trunc('month', gs.visit_stime)::date as month
    -- 사용자별 접속 건수. 고유 접속 건수가 아니므로 count(distinct user_id)를 적용하지 않음
    , count(*) as monthly_user_cnt
from ga.ga_sess gs
    inner join ga.ga_users gu on gs.user_id = gu.user_id
where gu.create_time <= (date_trunc('month', gu.create_time) + interval '1 month' - interval '1 day')::date - 2
group by gs.user_id, date_trunc('month', visit_stime)::date
-- @formatter:on
;

-- @formatter:off
-- 사용자별 월별 세션 접속 횟수 구간별 집계, 월말 3일 이전 생성된 사용자 제외
with temp_01 as (
    select gs.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt
    from ga.ga_sess gs
        inner join ga.ga_users gu on gs.user_id = gu.user_id
    where gu.create_time <= (date_trunc('month', gu.create_time) + interval '1 month' - interval '1 day')::date - 2
    group by gs.user_id, date_trunc('month', visit_stime)::date
)
select t01.month
     , case
        when t01.monthly_user_cnt = 1 then '0_only_first_session'
        when t01.monthly_user_cnt between 2 and 3 then '2_between_3'
        when t01.monthly_user_cnt between 4 and 8 then '4_between_8'
        when t01.monthly_user_cnt between 9 and 14 then '9_between_14'
        when t01.monthly_user_cnt between 15 and 25 then '15_between_25'
        when t01.monthly_user_cnt >= 26 then 'over_26'
     end as gubun
     , count(*) as user_cnt
from temp_01 t01
group by t01.month
    , case
        when t01.monthly_user_cnt = 1 then '0_only_first_session'
        when t01.monthly_user_cnt between 2 and 3 then '2_between_3'
        when t01.monthly_user_cnt between 4 and 8 then '4_between_8'
        when t01.monthly_user_cnt between 9 and 14 then '9_between_14'
        when t01.monthly_user_cnt between 15 and 25 then '15_between_25'
        when t01.monthly_user_cnt >= 26 then 'over_26'
    end
order by 1, 2
-- @formatter:on
;

-- @formatter:off
-- gubun 별로 pivot 하여 추출
with temp_01 as (
    select gs.user_id, date_trunc('month', visit_stime)::date as month, count(*) as monthly_user_cnt
    from ga.ga_sess gs
        inner join ga.ga_users gu on gs.user_id = gu.user_id
    where gu.create_time <= (date_trunc('month', gu.create_time) + interval '1 month' - interval '1 day')::date - 2
    group by gs.user_id, date_trunc('month', visit_stime)::date
)
, temp_02 as (
    select t01.month
        , case
            when t01.monthly_user_cnt = 1 then '0_only_first_session'
            when t01.monthly_user_cnt between 2 and 3 then '2_between_3'
            when t01.monthly_user_cnt between 4 and 8 then '4_between_8'
            when t01.monthly_user_cnt between 9 and 14 then '9_between_14'
            when t01.monthly_user_cnt between 15 and 25 then '15_between_25'
            when t01.monthly_user_cnt >= 26 then 'over_26'
        end as gubun
        , count(*) as user_cnt
    from temp_01 t01
    group by t01.month
        , case
            when t01.monthly_user_cnt = 1 then '0_only_first_session'
            when t01.monthly_user_cnt between 2 and 3 then '2_between_3'
            when t01.monthly_user_cnt between 4 and 8 then '4_between_8'
            when t01.monthly_user_cnt between 9 and 14 then '9_between_14'
            when t01.monthly_user_cnt between 15 and 25 then '15_between_25'
            when t01.monthly_user_cnt >= 26 then 'over_26'
        end
)
select t02.month
     , sum(case when t02.gubun = '0_only_first_session' then t02.user_cnt else 0 end) as "0_only_first_session"
     , sum(case when t02.gubun = '2_between_3' then t02.user_cnt else 0 end) as "2_between_3"
     , sum(case when t02.gubun = '4_between_8' then t02.user_cnt else 0 end) as "4_between_8"
     , sum(case when t02.gubun = '9_between_14' then t02.user_cnt else 0 end) as "9_between_14"
     , sum(case when t02.gubun = '15_between_25' then t02.user_cnt else 0 end) as "15_between_25"
     , sum(case when t02.gubun = 'over_26' then t02.user_cnt else 0 end) as "over_26"
from temp_02 t02
group by t02.month
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	한달 기간중에 주간 방문 횟수별 사용자 건수
*********************************************************************************/

-- @formatter:off
-- 한달 기간중에 주간 방문 횟수별 사용자 건수
with temp_01 as (
    select gs.user_id
        , case
            when gs.visit_date between '20160801' and '20160807' then '1st'
            when gs.visit_date between '20160808' and '20160814' then '2nd'
            when gs.visit_date between '20160815' and '20160821' then '3rd'
            when gs.visit_date between '20160822' and '20160828' then '4th'
            when gs.visit_date between '20160829' and '20160904' then '5th'
        end as week_gubun
        , count(distinct visit_date) as daily_visit_cnt
    from ga.ga_sess gs
    where gs.visit_date between '20160801' and '20160831'
    group by gs.user_id
        , case
            when gs.visit_date between '20160801' and '20160807' then '1st'
            when gs.visit_date between '20160808' and '20160814' then '2nd'
            when gs.visit_date between '20160815' and '20160821' then '3rd'
            when gs.visit_date between '20160822' and '20160828' then '4th'
            when gs.visit_date between '20160829' and '20160904' then '5th'
        end
)
select t01.daily_visit_cnt
    , sum(case when t01.week_gubun = '1st' then 1 else 0 end) as week_1st_user_cnt
    , sum(case when t01.week_gubun = '2nd' then 1 else 0 end) as week_2nd_user_cnt
    , sum(case when t01.week_gubun = '3rd' then 1 else 0 end) as week_3rd_user_cnt
    , sum(case when t01.week_gubun = '4th' then 1 else 0 end) as week_4th_user_cnt
    , sum(case when t01.week_gubun = '5th' then 1 else 0 end) as week_5th_user_cnt
from temp_01 t01
group by t01.daily_visit_cnt
order by 1
-- @formatter:on
;

-- @formatter:off
-- 임시 테이블을 이용하여 동적으로 주간 기간 설정 - 한달 기간중에 주간 방문 횟수별 사용자 건수
with temp_00(week_gubun, start_date, end_date) as (
    values
        ('1st', '20160801', '20160807')
        , ('2nd', '20160808', '20160814')
        , ('3rd', '20160815', '20160821')
        , ('4th', '20160822', '20160828')
        , ('5th', '20160829', '20160904')
)
, temp_01 as (
    select gs.user_id
        , t00.week_gubun
	    , count(distinct gs.visit_date) as daily_visit_cnt
    from ga.ga_sess gs
	join temp_00 t00 on gs.visit_date between t00.start_date and end_date
    -- where gs.visit_date between (select min(t00.start_date) from temp_00) and (select max(t00.end_date) from temp_00 t00) -- 성능을 위해서
    group by gs.user_id, t00.week_gubun
)
select daily_visit_cnt
    , sum(case when week_gubun = '1st' then 1 else 0 end) as week_1st_user_cnt
    , sum(case when week_gubun = '2nd' then 1 else 0 end) as week_2nd_user_cnt
    , sum(case when week_gubun = '3rd' then 1 else 0 end) as week_3rd_user_cnt
    , sum(case when week_gubun = '4th' then 1 else 0 end) as week_4th_user_cnt
    , sum(case when week_gubun = '5th' then 1 else 0 end) as week_5th_user_cnt
from temp_01 group by daily_visit_cnt
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	사용자가 첫 세션 접속 후 두번째 세션 접속까지 걸리는
  	평균, 최대, 최소, 4분위 percentile 시간 추출
	step 1: 사용자 별로 접속 시간에 따라 session 별 순서 매김.
	step 2: session 별 순서가 첫번째와 두번째 인것 추출
	step 3: 사용자 별로 첫번째 세션의 접속 이후 두번째 세션의 접속 시간 차이를 가져 오기
	step 4: step 3의 데이터를 전체 평균, 최대, 최소, 4분위 percentile 시간 구하기
*********************************************************************************/

-- @formatter:off
-- 사용자 별로 접속 시간에 따라 session 별 순서 매김
select gs.user_id
    , row_number() over (partition by gs.user_id order by gs.visit_stime) as session_rnum
	, gs.visit_stime
	-- 추후에 1개 session만 있는 사용자는 제외하기 위해 사용.
	, count(*) over (partition by gs.user_id) as session_cnt
from ga.ga_sess gs
order by user_id, session_rnum
-- @formatter:on
;

-- @formatter:off
-- session 별 순서가 첫번째와 두번째 인것 추출하고 사용자 별로 첫번째 세션의 접속 이후 두번째 세션의 접속 시간 차이를 가져 오기
with temp_01 as (
    select gs.user_id
        , row_number() over (partition by gs.user_id order by gs.visit_stime) as session_rnum
        , gs.visit_stime
        -- 추후에 1개 session만 있는 사용자는 제외하기 위해 사용.
        , count(*) over (partition by gs.user_id) as session_cnt
    from ga.ga_sess gs
)
select t01.user_id
	-- 사용자별로 첫번째 세션, 두번째 세션만 있으므로 max(visit_stime)이 두번째 세션 접속 시간, min(visit_stime)이 첫번째 세션 접속 시간.
    , max(t01.visit_stime) - min(t01.visit_stime) as sess_time_diff
from temp_01 t01
where t01.session_rnum <= 2 and t01.session_cnt > 1 -- 첫번째 두번째 세션만 가져오되 첫번째 접속만 있는 사용자를 제외하기
group by t01.user_id
-- @formatter:on
;

-- @formatter:off
-- step 3의 데이터를 전체 평균, 최대값, 최소값, 4분위 percentile  구하기
with temp_01 as (
    select gs.user_id
        , row_number() over (partition by gs.user_id order by gs.visit_stime) as session_rnum
        , gs.visit_stime
        -- 추후에 1개 session만 있는 사용자는 제외하기 위해 사용
        , count(*) over (partition by gs.user_id) as session_cnt
    from ga.ga_sess gs
)
, temp_02 as (
    select t01.user_id
        -- 사용자별로 첫번째 세션, 두번째 세션만 있으므로 max(visit_stime)이 두번째 세션 접속 시간, min(visit_stime)이 첫번째 세션 접속 시간
        , max(t01.visit_stime) - min(t01.visit_stime) as sess_time_diff
    from temp_01 t01
    where t01.session_rnum <= 2 and t01.session_cnt > 1 -- 첫번째 두번째 세션만 가져오되 첫번째 접속만 있는 사용자를 제외하기
    group by t01.user_id
)
-- postgresql avg(time)은 interval이 제대로 고려되지 않음, justify_inteval()을 적용해야 함
select justify_interval(avg(t02.sess_time_diff)) as avg_time
    , max(t02.sess_time_diff) as max_time
    , min(t02.sess_time_diff) as min_time
    , percentile_disc(0.25) within group (order by t02.sess_time_diff) as percentile_1
    , percentile_disc(0.5) within group (order by t02.sess_time_diff) as percentile_2
    , percentile_disc(0.75) within group (order by t02.sess_time_diff) as percentile_3
    , percentile_disc(1.0) within group (order by t02.sess_time_diff) as percentile_4
from temp_02 t02
where t02.sess_time_diff::interval > interval '0 second'
-- @formatter:on
;
