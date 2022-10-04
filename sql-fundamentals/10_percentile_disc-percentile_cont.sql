/*********************************************************************************
	percentile_disc, percentile_cont 실습
*********************************************************************************/

-- 4분위별 sal 값을 반환.
select percentile_disc(0.25) within group (order by e.sal) as qt_1
	 , percentile_disc(0.5) within group (order by e.sal) as qt_2
	 , percentile_disc(0.75) within group (order by e.sal) as qt_3
	 , percentile_disc(1.0) within group (order by e.sal) as qt_4
from hr.emp e
;

-- @formatter:off
-- percentile_disc는 cume_dist의 inverse 값을 반환
-- percentile_disc는 0 ~ 1 사이의 분위수값을 입력하면 해당 분위수 값 이상인 것 중에서 최소 cume_dist 값을 가지는 값을 반환
with temp_01 as (
	select percentile_disc(0.25) within group (order by e.sal) as qt_1
		, percentile_disc(0.5) within group (order by e.sal) as qt_2
		, percentile_disc(0.75) within group (order by e.sal) as qt_3
		, percentile_disc(1.0) within group (order by e.sal) as qt_4
 	from hr.emp e
 )
select e.empno
	 , e.ename
	 , e.sal
	 , cume_dist() over (order by e.sal) as cume_dist
	 , t01.qt_1
	 , t01.qt_2
	 , t01.qt_3
	 , t01.qt_4
from hr.emp e
	cross join temp_01 t01
order by e.sal
-- @formatter:on
;

-- @formatter:off
-- products 테이블에서 category별 percentile_disc 구하기
with temp_01 as (
	select p.category_id
		  , max(c.category_name) as category_name
		  , percentile_disc(0.25) within group (order by p.unit_price) as qt_1
		  , percentile_disc(0.5) within group (order by p.unit_price) as qt_2
		  , percentile_disc(0.75) within group (order by p.unit_price) as qt_3
		  , percentile_disc(1.0) within group (order by p.unit_price) as qt_4
	from nw.products p
		inner join nw.categories c on c.category_id = p.category_id
	group by p.category_id
)
select *
from temp_01
-- @formatter:on
;

-- @formatter:off
-- percentile_disc와 cume_dist 비교하기
with temp_01 as (
	select p.category_id
		, max(c.category_name) as category_name
		, percentile_disc(0.25) within group (order by p.unit_price) as qt_1
		, percentile_disc(0.5) within group (order by p.unit_price) as qt_2
		, percentile_disc(0.75) within group (order by p.unit_price) as qt_3
		, percentile_disc(1.0) within group (order by p.unit_price) as qt_4
	from nw.products p
		inner join nw.categories c on c.category_id = p.category_id
	group by p.category_id
 )
select p.product_id
	 , p.product_name
	 , p.category_id
	 , t01.category_name
	 , p.unit_price
	 , cume_dist() over (partition by p.category_id order by p.unit_price) as cume_dist_by_cat
	 , t01.qt_1
	 , t01.qt_2
	 , t01.qt_3
	 , t01.qt_4
from nw.products p
    inner join temp_01 t01 on p.category_id = t01.category_id
-- @formatter:on
;


--입력 받은 분위수가 특정 로우를 정확하게 지정하지 못하고, 두 로우 사이일때
--percentile_cont는 보간법을 이용하여 보정하며, percentile_cont는 두 로우에서 작은 값을 반환
select 'cont' as gubun
	 , percentile_cont(0.25) within group (order by e.sal) as qt_1
	 , percentile_cont(0.5) within group (order by e.sal) as qt_2
	 , percentile_cont(0.75) within group (order by e.sal) as qt_3
	 , percentile_cont(1.0) within group (order by e.sal) as qt_4
from hr.emp e

union all

select 'disc' as gubun
	 , percentile_disc(0.25) within group (order by e.sal) as qt_1
	 , percentile_disc(0.5) within group (order by e.sal) as qt_2
	 , percentile_disc(0.75) within group (order by e.sal) as qt_3
	 , percentile_disc(1.0) within group (order by e.sal) as qt_4
from hr.emp e
;

-- @formatter:off
-- percentile_cont와 percentile_disc를 cume_dist와 비교
with temp_01 as (
	select 'cont' as gubun
		, percentile_cont(0.25) within group (order by e.sal) as qt_1
		, percentile_cont(0.5) within group (order by e.sal) as qt_2
		, percentile_cont(0.75) within group (order by e.sal) as qt_3
		, percentile_cont(1.0) within group (order by e.sal) as qt_4
	from hr.emp e

	union all

	select 'disc' as gubun
		, percentile_disc(0.25) within group (order by e.sal) as qt_1
		, percentile_disc(0.5) within group (order by e.sal) as qt_2
		, percentile_disc(0.75) within group (order by e.sal) as qt_3
		, percentile_disc(1.0) within group (order by e.sal) as qt_4
	from hr.emp e
)
select e.empno
	 , e.ename
	 , e.sal
	 , cume_dist() over (order by e.sal)
	 , t01.qt_1
	 , t01.qt_2
	 , t01.qt_3
	 , t01.qt_4
from hr.emp e
	cross join temp_01 t01
where t01.gubun = 'cont'
-- @formatter:on
;