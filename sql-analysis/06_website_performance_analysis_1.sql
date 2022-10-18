-- :current_date 값은 to_date('20161101', 'yyyymmdd')

/*********************************************************************************
	Hit수가 가장 많은 상위 5개 페이지(이벤트 포함)와
  	세션당 최대, 평균, 4분위 페이지/이벤트 Hit수
*********************************************************************************/

-- hit수가 가장 많은 상위 5개 페이지(이벤트 포함)
select gsh.page_path, count(gsh.*) as hits_by_page
from ga.ga_sess_hits gsh
group by gsh.page_path
order by hits_by_page desc
    fetch first 5 row only
;

-- @formatter:off
-- 세션당 최대, 평균, 4분위 페이지(이벤트 포함) Hit 수
with temp_01 as (
    select gsh.sess_id, count(gsh.*) as hits_by_sess
    from ga.ga_sess_hits gsh
    group by gsh.sess_id
)
select max(t01.hits_by_sess)
    , avg(t01.hits_by_sess)
    , percentile_disc(0.25) within group (order by t01.hits_by_sess) as percenttile_25
    , percentile_disc(0.5) within group (order by t01.hits_by_sess) as percenttile_50
    , percentile_disc(0.75) within group (order by t01.hits_by_sess) as percenttile_75
    , percentile_disc(0.8) within group (order by t01.hits_by_sess) as percenttile_80
    , percentile_disc(1) within group (order by t01.hits_by_sess) as percenttile_100
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	과거 30일간 일별 page hit 건수 및 30일 평균 일별 page hit
*********************************************************************************/

-- @formatter:off
-- 과거 30일간 일별 page hit 건수 및 30일 평균 일별 page hit
select date_trunc('day', gs.visit_stime)::date
    , count(gsh.*) as page_cnt
    -- group by가 적용된 결과 집합에 analytic avg()가 적용됨
    , round(avg(count(*)) over (), 2) as avg_page_cnt
from ga.ga_sess_hits gsh
    inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
where gs.visit_stime >= (:current_date - interval '30 days')
and gs.visit_stime < :current_date
and gsh.hit_type = 'PAGE'
group by date_trunc('day', gs.visit_stime)::date
-- @formatter:on
;

-- @formatter:off
-- 과거 30일간 일별 page hit 건수 및 30일 평균 일별 page hit, 다른 방법 (with + cross join)
with temp_01 as (
    select date_trunc('day', gs.visit_stime)::date, count(gsh.*) as page_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
    group by date_trunc('day', gs.visit_stime)::date
)
, temp_02 as (
    select avg(t01.page_cnt) from temp_01 t01
)
select t01.*, t02.*
from temp_01 t01
    cross join temp_02 t02
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	과거 한달간 페이지별 조회수와 순 페이지(세션 고유 페이지) 조회수
*********************************************************************************/

-- @formatter:off
-- 페이지별 조회수와 순페이지 조회수
with temp_01 as (
    select gsh.page_path, count(gsh.*) as page_cnt
    from ga.ga_sess_hits gsh
    where gsh.hit_type = 'PAGE'
    group by gsh.page_path
)
, temp_02 as (
    select t.page_path, count(t.*) as unique_page_cnt
    from (
        select distinct gsh.sess_id, gsh.page_path
        from ga.ga_sess_hits gsh
        where gsh.hit_type = 'PAGE'
    ) t
    group by t.page_path
)
select t01.page_path, t01.page_cnt, t02.unique_page_cnt
from temp_01 t01
    inner join temp_02 t02 on t01.page_path = t02.page_path
order by 2 desc
-- @formatter:on
;

/*
-- 아래와 같이 temp_02 를 구성해도 됨. 단 대용량 데이터의 경우 시간이 좀 더 걸릴 수 있음.
, temp_02 as (
    select t.page_path, count(*) as unique_page_cnt
    from (
        select gsh.sess_id
            , gsh.page_path
            , row_number() over (partition by gsh.sess_id, gsh.page_path order by gsh.page_path) as rnum
        from ga.ga_sess_hits gsh
        where gsh.hit_type = 'PAGE'
    ) t
    wheret t.rnum = 1
    group by t.page_path
)
*/

-- @formatter:off
-- 아래는 과거 한달간 페이지별 조회수와 순 페이지(세션 고유 페이지) 조회수
with temp_01 as (
    select gsh.page_path, count(gsh.*) as page_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
    group by gsh.page_path
)
, temp_02 as (
    select t.page_path, count(t.*) as unique_page_cnt
    from (
        select distinct gsh.sess_id, gsh.page_path
        from ga.ga_sess_hits gsh
            join ga.ga_sess gs on gsh.sess_id = gs.sess_id
        where gs.visit_stime >= (:current_date - interval '30 days')
        and gs.visit_stime < :current_date
        and hit_type = 'PAGE'
    ) t
    group by t.page_path
)
select t01.page_path, t01.page_cnt, t02.unique_page_cnt
from temp_01 t01
    inner join temp_02 t02 on t01.page_path = t02.page_path
order by 2 desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	과거 30일간 페이지별 평균 페이지 머문 시간
	세션별 마지막 페이지(탈출 페이지)는 평균 시간 계산에서 제외
	세션 시작 시 hit_seq=1이면(즉 입구 페이지) 무조건 hit_time이 0 임
*********************************************************************************/

-- 세션 시작 값이 아닌 값은 없음
select *
from ga.ga_sess_hits gsh
where gsh.hit_seq = 1
  and gsh.hit_time != 0
;

-- @formatter:off
-- 페이지별 평균 페이지 머문 시간
with temp_01 as (
    select gsh.sess_id
        , gsh.page_path
        , gsh.hit_seq
        , gsh.hit_time
        , lead(gsh.hit_time) over (partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
    from ga.ga_sess_hits gsh
    where gsh.hit_type = 'PAGE'
)
select t01.page_path
    , count(t01.*) as page_cnt
    , round(avg(t01.next_hit_time - t01.hit_time)/1000, 2) as avg_elapsed_sec
from temp_01 t01
group by t01.page_path
order by 2 desc
-- @formatter:on
;

-- @formatter:off
-- 페이지별 조회 건수와 순수 조회(세션별 unique 페이지), 평균 머문 시간(초)를 한꺼번에 구하기
-- 개별적인 집합을 각각 만든 뒤 이를 조인
with temp_01 as (
    select gsh.page_path, count(gsh.*) as page_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
	group by gsh.page_path
)
, temp_02 as (
	select t.page_path, count(t.*) as unique_page_cnt
	from (
		select distinct gsh.sess_id, gsh.page_path
        from ga.ga_sess_hits gsh
            inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
        where gs.visit_stime >= (:current_date - interval '30 days')
        and gs.visit_stime < :current_date
        and gsh.hit_type = 'PAGE'
	) t
	group by t.page_path
)
, temp_03 as (
	select
        gsh.sess_id
        , gsh.page_path
        , gsh.hit_seq
        , gsh.hit_time
        , lead(gsh.hit_time) over (partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
	from ga.ga_sess_hits gsh
		inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
	where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
	and gsh.hit_type = 'PAGE'
)
, temp_04 as (
    select t03.page_path
        , count(t03.*) as page_cnt
        , round(avg(t03.next_hit_time - t03.hit_time) / 1000.0, 2) as avg_elapsed_sec
    from temp_03 t03
    group by t03.page_path
)
select t01.page_path, t01.page_cnt, t02.unique_page_cnt, t03.avg_elapsed_sec
from temp_01 t01
	left join temp_02 t02 on t01.page_path = t02.page_path
	left join temp_04 t03 on t01.page_path = t03.page_path
order by 2 desc
-- @formatter:on
;

-- @formatter:off
-- 아래와 같이 공통 중간집합으로 보다 간단하게 추출할 수 있습니다.
with temp_01 as (
    select
        gsh.sess_id
        , gsh.page_path
        , gsh.hit_seq
        , gsh.hit_time
        , lead(gsh.hit_time) over (partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
        -- 세션내에서 동일한 page_path가 있을 경우 rnum은 2이상이 됨, 추후에 1값만 count를 적용
        , row_number() over (partition by gsh.sess_id, gsh.page_path order by gsh.hit_seq) as rnum
	from ga.ga_sess_hits gsh
		inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
	where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
	and gsh.hit_type = 'PAGE'
)
select t01.page_path
    , count(t01.*) as page_cnt
    , count(case when t01.rnum = 1 then '1' end) as unique_page_cnt
    , round(avg(t01.next_hit_time - hit_time) / 1000.0, 2) as avg_elapsed_sec
from temp_01 t01
group by t01.page_path
order by 2 desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	ga_sess_hits 테이블에서 개별 session 별로 진입 페이지(landing page)와
  	종료 페이지(exit page), 그리고 해당 page의 종료 페이지 여부 컬럼을 생성
	종료 페이지 여부는 반드시 hit_type이 PAGE일 때만 True임
*********************************************************************************/

-- @formatter:off
-- ga_sess_hits 테이블에서 진입 페이지(landing page)와 종료 페이지(exit page), 그리고 해당 page의 종료 페이지 여부 컬럼을 생성
with temp_01 as (
    select gsh.sess_id
        , gsh.hit_seq
        , gsh.hit_type
        , gsh.page_path

        , gsh.landing_screen_name
        -- 동일 sess_id 내에서 hit_seq가 가장 처음에 위치한 page_path가 landing page
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and current row) as landing_page

        , gsh.exit_screen_name
        -- 동일 sess_id 내에서 hit_seq가 가장 마지막에 위치한 page_path가 exit page.
        , last_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and unbounded following) as exit_page

        , gsh.is_exit
        , case
            when row_number() over (partition by gsh.sess_id, gsh.hit_type order by gsh.hit_seq desc) = 1 and gsh.hit_type = 'PAGE'
            then 'True'
            else ''
        end as is_exit_new
    from ga.ga_sess_hits gsh
)
select t01.*
from temp_01 t01
-- 검증 조건
-- where t01.is_exit_new != is_exit
-- where t01.is_exit = 'True' and t01.hit_type = 'EVENT'
-- where 'googlemerchandisestore.com' || t01.exit_page != regexp_replace(t01.exit_screen_name, 'shop.|www.', '')
-- @formatter:on
;

-- 소스 문자열을 조건에 따라 변경.
select regexp_replace('shop.googlemerchandisestore.com/google+redesign/shop+by+brand/google', 'shop.|www.', '');

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	landing page, exit page, landing page + exit page 별 page와 고유 session 건수
*********************************************************************************/

-- @formatter:off
-- landing page/exit별 page와 고유 session 건수
with temp_01 as (
    select gsh.sess_id
        , gsh.hit_seq
        , gsh.action_type
        , gsh.hit_type
        , gsh.page_path
        , gsh.landing_screen_name
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and current row) as landing_page
        , gsh.exit_screen_name
        -- hit_type이 PAGE일 때만 last_value()를 적용하고, EVENT일때는 NULL로 치환
        , case when gsh.hit_type = 'PAGE'
            then last_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and unbounded following)
        end as exit_page
        , gsh.is_exit
        -- hit_type이 PAGE이고 맨 마지막 hit_seq일때만 exit page임
        , case
            when row_number() over (partition by gsh.sess_id, gsh.hit_type order by gsh.hit_seq desc) = 1 and gsh.hit_type = 'PAGE'
            then 'True'
            else ''
        end as is_exit_new
    from ga.ga_sess_hits gsh
 )
, temp_02 as (
    select t01.sess_id
        , t01.hit_seq
        , t01.action_type
        , t01.hit_type
        , t01.page_path
        , t01.landing_screen_name
        , t01.exit_screen_name
        , t01.landing_page
        -- max() analtyic으로 null 값을 window 상단값 부터 복제함
        , max(t01.exit_page) over (partition by t01.sess_id) as exit_page
        , t01.is_exit
        , t01.is_exit_new
    from temp_01 t01
)
select
--     t02.landing_page
--     , count(t02.*) as page_cnt
--     , count(distinct t02.sess_id) as sess_cnt
    t02.exit_page
    , count(t02.*) as page_cnt
    , count(distinct t02.sess_id) as sess_cnt
from temp_02 t02
-- group by t02.landing_page
-- order by 2 desc
group by t02.exit_page
order by 2 desc
-- @formatter:on
;

-- @formatter:off
--  landing page + exit page 별 page와 고유 session 건수
with temp_01 as (
    select gsh.sess_id
        , gsh.hit_seq
        , gsh.action_type
        , gsh.hit_type
        , gsh.page_path
        , gsh.landing_screen_name
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and current row) as landing_page
        , gsh.exit_screen_name
        -- hit_type이 PAGE일 때만 last_value()를 적용하고, EVENT일때는 NULL로 치환
        , case when gsh.hit_type = 'PAGE'
            then last_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq rows between unbounded preceding and unbounded following)
        end as exit_page
        , gsh.is_exit
        -- hit_type이 PAGE이고 맨 마지막 hit_seq일때만 exit page임
        , case
            when row_number() over (partition by gsh.sess_id, gsh.hit_type order by gsh.hit_seq desc) = 1 and gsh.hit_type = 'PAGE'
            then 'True'
            else ''
        end as is_exit_new
    from ga.ga_sess_hits gsh
)
, temp_02 as (
    select t01.sess_id
        , t01.hit_seq
        , t01.action_type
        , t01.hit_type
        , t01.page_path
        , t01.landing_screen_name
        , t01.exit_screen_name
        , t01.landing_page
        -- max() analtyic으로 null 값을 window 상단값 부터 복제함
        , max(t01.exit_page) over (partition by t01.sess_id) as exit_page
        , t01.is_exit
        , t01.is_exit_new
    from temp_01 t01
)
select t02.landing_page
    , t02.exit_page
    , count(t02.*) as page_cnt
    , count(distinct t02.sess_id) as sess_cnt
from temp_02 t02
group by t02.landing_page, t02.exit_page
order by 3 desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	이탈율(Bounce Ratio) 추출
	최초 접속 후 다른 페이지로 이동하지 않고 바로 종료한 세션 비율 (Bounce Session)
	전체 페이지를 기준으로 이탈율을 구할 경우 bounce session 건수/고유 session 건수
*********************************************************************************/

-- bounce session 추출
select gsh.sess_id, count(gsh.*)
from ga.ga_sess_hits gsh
group by gsh.sess_id
having count(*) = 1
;

-- bounce session 대부분은 PAGE이지만 일부는 EVENT도 존재
select gsh.sess_id, count(gsh.*), max(gsh.hit_type), min(gsh.hit_type)
from ga.ga_sess_hits gsh
group by gsh.sess_id
having count(gsh.*) = 1
   and (max(gsh.hit_type) = 'EVENT' or min(gsh.hit_type) = 'EVENT');

-- @formatter:off
-- 전체 페이지에서 이탈율(bounce ratio) 구하기
with temp_01 as (
    select gsh.sess_id, count(gsh.*) as page_cnt
    from ga.ga_sess_hits gsh
    group by gsh.sess_id
)
select sum(case when t01.page_cnt = 1 then 1 else 0 end) as bounce_sess_cnt -- bounce session 건수
    , count(t01.*) as sess_cnt -- 고유 session 건수
    , round(100.0 * sum(case when t01.page_cnt = 1 then 1 else 0 end) / count(t01.*), 2) as bounce_sess_pct -- 이탈율
from temp_01 t01
-- @formatter:on
;

-- @formatter:off
-- 세션당 최대, 평균, 4분위 페이지(이벤트 포함) Hit 수
with temp_01 as (
    select gsh.sess_id, count(gsh.*) as hits_by_sess
    from ga.ga_sess_hits gsh
    group by gsh.sess_id
)
select max(t01.hits_by_sess)
     , avg(t01.hits_by_sess)
     , min(t01.hits_by_sess)
     , count(t01.*) as cnt
     , percentile_disc(0.25) within group (order by t01.hits_by_sess) as percentile_25
     , percentile_disc(0.50) within group (order by t01.hits_by_sess) as percentile_50
     , percentile_disc(0.75) within group (order by t01.hits_by_sess) as percentile_75
     , percentile_disc(0.80) within group (order by t01.hits_by_sess) as percentile_80
     , percentile_disc(1.0) within group (order by t01.hits_by_sess) as percentile_100
from temp_01 t01
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	과거 30일간 페이지별 이탈율(bounce rate)
	페이지별 이탈율을 계산할 경우 전체 세션이 아니라 현재 페이지가 세션별 첫페이지와 동일한 경우만 대상
	즉 bounce 세션 건수/현재 페이지가 세션별 첫페이지와 동일한 고유 세션 건수로 이탈율 계산
*********************************************************************************/

-- @formatter:off
-- 과거 30일간 페이지별 이탈율(bounce rate)
with temp_01 as (
    select gsh.page_path
        , gsh.sess_id
        , gsh.hit_seq
        , gsh.hit_type
        , gsh.action_type
        -- 세션별 페이지 건수를 구함
        , count(gsh.*) over (partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
        -- 세션별 첫페이지를 구해서 추후에 현재 페이지와 세션별 첫페이지가 같은지 비교하기 위한 용도
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
)
, temp_02 as (
    select t01.page_path
        , count(t01.*) as page_cnt
        -- 세션별 페이지 건수가 1일때만 bounce session이므로 페이지별 bounce session 건수를 구함
        , sum(case when t01.sess_cnt = 1 then 1 else 0 end) as bounce_cnt_per_page
        -- path_page와 세션별 첫번째 페이지가 동일한 경우에만 고유 세션 건수를 구함
        , count(distinct case when t01.first_page_path = t01.page_path then t01.sess_id end) as sess_cnt_per_page_01
        , count(distinct t01.sess_id) as sess_cnt_per_page_02
    from temp_01 t01
    group by t01.page_path
)
select t02.*
     -- 이탈율 계산, sess_cnt_01이 0 일 경우 0으로 나눌수 없으므로 Null값 처리, sess_cnt_01이 0이면 bounce session이 없으므로 이탈율은 0임
     , coalesce(round(100.0 * t02.bounce_cnt_per_page / (case when t02.sess_cnt_per_page_01 = 0 then null else t02.sess_cnt_per_page_01 end), 2), 0) as bounce_pct_01
     , round(100.0 * t02.bounce_cnt_per_page / t02.sess_cnt_per_page_02, 2) as bounce_pct_02
from temp_02 t02
order by t02.page_cnt desc
-- @formatter:on
;

-- @formatter:off
-- 데이터 검증, /basket.html 페이지에 대해서 /basket.html 으로 시작하는 session의 count와 그렇지 않은 count를 구하기
with temp_01 as (
    select gsh.page_path
        , gsh.sess_id
        , gsh.hit_seq
        , gsh.hit_type
        , gsh.action_type
        -- 세션별 페이지 건수를 구함.
        , count(gsh.*) over (partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
        -- 세션별 첫페이지를 구해서 추후에 현재 페이지와 세션별 첫페이지가 같은지 비교하기 위한 용도
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
)
, temp_02 as (
    select t01.*
        -- 해당 page가 first page 인지 여부를 1과 0으로 표시
        , case when t01.first_page_path = t01.page_path then 1 else 0 end as first_gubun
    from temp_01 t01
    where t01.page_path = '/basket.html'
)
select t02.first_gubun, count(distinct t02.sess_id)
from temp_02 t02
group by t02.first_gubun
-- @formatter:on
;

-- @formatter:off
-- 앞에서 구한 페이지별 페이지 조회수, 순 페이지 조회수, 평균 머문시간과 함께 이탈율 집계
with temp_01 as (
    select gsh.sess_id
        , gsh.page_path
        , gsh.hit_seq
        , gsh.hit_time
        , lead(gsh.hit_time) over (partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
        -- 세션내에서 동일한 page_path가 있을 경우 rnum은 2이상이 됨. 추후에 1값만 count를 적용
        , row_number() over (partition by gsh.sess_id, gsh.page_path order by gsh.hit_seq) as rnum
        -- 세션별 페이지 건수를 구함
        , count(gsh.*) over (partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
        -- 세션별 첫페이지를 구해서 추후에 현재 페이지와 세션별 첫페이지가 같은지 비교하기 위한 용도
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
)
, temp_02 as (
    select t01.page_path
        , count(t01.*) as page_cnt
        , count(case when t01.rnum = 1 then '1' end) as unique_page_cnt
        , round(avg(t01.next_hit_time - t01.hit_time) / 1000.0, 2) as avg_elapsed_sec
        -- 세션별 페이지 건수가 1일때만 bounce session이므로 페이지별 bounce session 건수를 구함
        , sum(case when t01.sess_cnt = 1 then 1 else 0 end) as bounce_cnt_per_page
        -- path_page와 세션별 첫번째 페이지가 동일한 경우에만 고유 세션 건수를 구함
        , count(distinct case when t01.first_page_path = t01.page_path then t01.sess_id end) as sess_cnt_per_page
    from temp_01 t01
    group by t01.page_path
)
select t02.page_path
    , t02.page_cnt
    , t02.unique_page_cnt
    , t02.avg_elapsed_sec
    -- 이탈율 집계
    , coalesce(round(100.0 * t02.bounce_cnt_per_page / (case when t02.sess_cnt_per_page = 0 then null else t02.sess_cnt_per_page end), 2), 0) as bounce_pct
from temp_02 t02
order by t02.page_cnt desc
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	과거 30일간 광고 채널(channel_grouping) 별 이탈율(bounce rate) 및
  	광고 채널(channel_grouping) + 페이지별 이탈율(bounce rate)
	광고 채널별 이탈율을 구할 경우에는 채널별 bounce 세션 건수 / 채널별 고유 세션 건수
	광고 채널 + 페이지별 이탈율을 구할 경우에는 채널+페이지별
  	bounce 세션 건수/ 채널+페이지별에서 현재 페이지가 세션별 첫페이지와 동일한 경우의 고유세션
*********************************************************************************/

-- 광고 채널(channel_grouping)별 세션 건수
select gs.channel_grouping, count(gs.*)
from ga.ga_sess gs
group by gs.channel_grouping
;

-- @formatter:off
-- 과거 30일간 광고 채널(channel_grouping) 별 이탈율(bounce rate)
with temp_01 as (
    select gsh.page_path
        , gsh.sess_id
        , gs.channel_grouping
        , gsh.hit_seq
        , gsh.hit_type
        , gsh.action_type
        -- 세션별 페이지 건수를 구함
        , count(gsh.*) over (partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
)
select t01.channel_grouping
     , count(t01.*) as page_cnt
     --세션별 페이지 건수가 1일때만 bounce session이므로 채널별 bounce session 건수를 구함
     , sum(case when t01.sess_cnt = 1 then 1 else 0 end) as bounce_sess_cnt
     -- 채널별로 고유 세션 건수를 구함
     , count(distinct t01.sess_id) as sess_cnt
     , round(100.0 * sum(case when t01.sess_cnt = 1 then 1 else 0 end) / count(distinct t01.sess_id), 2) as bounce_pct
from temp_01 t01
group by t01.channel_grouping
order by page_cnt desc
-- @formatter:on
;

-- @formatter:off
-- 광고 채널(channel_grouping) + 페이지별 이탈율(bounce rate)
with temp_01 as (
    select gsh.page_path
        , gsh.sess_id
        , gs.channel_grouping
        , gsh.hit_seq
        , gsh.hit_type
        , gsh.action_type
        -- 세션별 페이지 건수를 구함
        , count(gsh.*) over (partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
        -- 세션별 첫페이지를 구해서 추후에 현재 페이지와 세션별 첫페이지가 같은지 비교하기 위한 용도
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
--     and gsh.page_path = '/home'
)
, temp_02 as (
    select t01.channel_grouping
        , t01.page_path
        , count(t01.*) as page_cnt
        -- 세션별 페이지 건수가 1일때만 bounce session이므로 페이지별 bounce session 건수를 구함
        , sum(case when t01.sess_cnt = 1 then 1 else 0 end) as bounce_cnt_per
        -- path_page와 세션별 첫번째 페이지가 동일한 경우에만 고유 세션 건수를 구함
        , count(distinct case when t01.first_page_path = t01.page_path then t01.sess_id end) as sess_cnt_per
    from temp_01 t01
--     where t01.page_path='/home'
    group by t01.channel_grouping, t01.page_path
--     having t01.page_path = '/home'
)
select t02.*
     -- 이탈율 계산. sess_cnt_01이 0 일 경우 0으로 나눌수 없으므로 Null값 처리. sess_cnt_01이 0이면 bounce session이 없으므로 이탈율은 0임
     , coalesce(round(100.0 * t02.bounce_cnt_per / (case when t02.sess_cnt_per = 0 then null else t02.sess_cnt_per end), 2), 0) as bounce_pct
from temp_02 t02
order by t02.page_cnt desc, t02.page_path, t02.channel_grouping
-- @formatter:on
;

-- 신규방문자/재방문자 이탈율, Device별 이탈율도 함께 적용해 볼것

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	과거 30일간 페이지별 종료율(Exit ratio) 구하기
    세션들이 특정 페이지로 얼마나 많이 종료가 되었는지를 나타냄
*********************************************************************************/

-- @formatter:off
-- 과거 30일간 페이지별 종료율(Exit ratio) 구하기
with temp_01 as (
    select gsh.sess_id
        , gsh.page_path
        , gsh.hit_seq
        , gsh.hit_type
        , gsh.action_type
        , gsh.is_exit
        -- 종료 페이지 여부를 구함, 종료 페이지면 1 아니면 0
        , case when row_number() over (partition by gsh.sess_id order by gsh.hit_seq desc) = 1 then 1 else 0 end as is_exit_page
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
)
select t01.page_path
     , count(t01.*) as page_cnt
     -- 페이지별 고유 세션 건수를 구함
     , count(distinct t01.sess_id) as sess_cnt
     -- 해당 페이지가 종료 페이지일 경우에만 고유 세션 건수를 구함 (distinct 없어도 됨)
     , count(distinct case when t01.is_exit_page = 1 then t01.sess_id end) as exit_cnt
     -- 아래와 같이 is_exit_page가 1, 0 이고 개별 session에 exit page는 최대 1개 이므로 아래와 같이 사용해도 됨
     --, sum(t01.is_exit_page) as exit_cnt_01
     , round(100.0 * count(distinct case when t01.is_exit_page = 1 then t01.sess_id end) / count(distinct sess_id), 2) as exit_pct
from temp_01 t01
group by t01.page_path
order by page_cnt desc
-- @formatter:on
;

-- @formatter:off
-- 앞에서 구한 페이지별 페이지 조회수, 순 페이지 조회수, 평균 머문시간, 이탈율과 함께 종료율 집계
with temp_01 as (
    select gsh.sess_id
        , gsh.page_path
        , gsh.hit_seq
        , gsh.hit_time
        , lead(gsh.hit_time) over (partition by gsh.sess_id order by gsh.hit_seq) as next_hit_time
        -- 세션내에서 동일한 page_path가 있을 경우 rnum은 2이상이 됨. 추후에 1값만 count를 적용
        , row_number() over (partition by gsh.sess_id, gsh.page_path order by gsh.hit_seq) as rnum
        -- 세션별 페이지 건수를 구함
        , count(gsh.*) over (partition by gsh.sess_id rows between unbounded preceding and unbounded following) as sess_cnt
        -- 세션별 첫페이지를 구해서 추후에 현재 페이지와 세션별 첫페이지가 같은지 비교하기 위한 용도
        , first_value(gsh.page_path) over (partition by gsh.sess_id order by gsh.hit_seq) as first_page_path
        --- 종료 페이지 여부를 구함, 종료 페이지면 1 아니면 0
        , case
            when row_number() over (partition by gsh.sess_id order by gsh.hit_seq desc) = 1 then 1
            else 0
        end as is_exit_page
    from ga.ga_sess_hits gsh
        inner join ga.ga_sess gs on gsh.sess_id = gs.sess_id
    where gs.visit_stime >= (:current_date - interval '30 days')
    and gs.visit_stime < :current_date
    and gsh.hit_type = 'PAGE'
)
, temp_02 as (
    select t01.page_path
        , count(t01.*) as page_cnt
        , count(case when t01.rnum = 1 then '1' end) as unique_page_cnt
        , round(avg(t01.next_hit_time - t01.hit_time) / 1000.0, 2) as avg_elapsed_sec
        --세션별 페이지 건수가 1일때만 bounce session이므로 페이지별 bounce session 건수를 구함
        , sum(case when t01.sess_cnt = 1 then 1 else 0 end) as bounce_cnt_per_page
        -- path_page와 세션별 첫번째 페이지가 동일한 경우에만 고유 세션 건수를 구함
        , count(distinct case when t01.first_page_path = t01.page_path then t01.sess_id end) as sess_cnt_per_page
        , count(distinct t01.sess_id) as sess_cnt
        -- 해당 페이지가 종료 페이지일 경우에만 고유 세션 건수를 구함.
        , count(distinct case when t01.is_exit_page = 1 then t01.sess_id end) as exit_cnt
    from temp_01 t01
    group by t01.page_path
)
select t02.page_path
    , t02.page_cnt
    , t02.unique_page_cnt
    , t02.sess_cnt as visit_cnt
    , t02.avg_elapsed_sec
    -- 이탈율 집계
    , coalesce(round(100.0 * t02.bounce_cnt_per_page / (case when t02.sess_cnt_per_page = 0 then null else t02.sess_cnt_per_page end), 2), 0) as bounce_pct
    -- 종료율 집계
    , round(100.0 * t02.exit_cnt / t02.sess_cnt, 2) as exit_pct
from temp_02 t02
order by t02.page_cnt desc
-- @formatter:on
;


