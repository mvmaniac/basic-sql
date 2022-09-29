/*********************************************************************************
	first_value, last_value 실습
*********************************************************************************/

-- 부서별로 가장 hiredate가 오래된 사람의 sal 가져오기
select e.empno
	 , e.ename
	 , e.deptno
	 , e.hiredate
	 , e.sal
	 , first_value(e.sal) over (partition by e.deptno order by e.hiredate) as first_hiredate_sal
from hr.emp e
;

-- @formatter:off
-- 부서별로 가장 hiredate가 최근인 사람의 sal 가져오기, windows절이 rows between unbounded preceding and unbounded following이 되어야 함
select e.empno
	 , e.ename
	 , e.deptno
	 , e.hiredate
	 , e.sal
	 , last_value(e.sal) over (partition by e.deptno order by e.hiredate rows between unbounded preceding and unbounded following) as last_hiredate_sal_01
	 , last_value(e.sal) over (partition by e.deptno order by e.hiredate rows between unbounded preceding and current row) as last_hiredate_sal_02
from hr.emp e
-- @formatter:on
;

-- @formatter:off
-- last_value() over (order by asc) 대신 first_value() over (order by desc)를 적용 가능
select e.empno
	 , e.ename
	 , e.deptno
	 , e.hiredate
	 , e.sal
	 , last_value(e.sal) over (partition by e.deptno order by e.hiredate rows between unbounded preceding and unbounded following) as last_hiredate_sal
	 , first_value(e.sal) over (partition by e.deptno order by e.hiredate desc) as last_hiredate_sal
from hr.emp e
-- @formatter:on
;

-- first_value()와 min() 차이
select e.empno
	 , e.ename
	 , e.deptno
	 , e.hiredate
	 , e.sal
	 , first_value(e.sal) over (partition by e.deptno order by e.hiredate) as first_hiredate_sal
	 , min(e.sal) over (partition by e.deptno order by e.hiredate) as min_sal
from hr.emp e
;

-- @formatter:off
-- 연속된 데이터 흐름에서 값이 Null일 경우 바로 값이 있는 바로 위의 데이터를 가져 오기
with ref_days as (
	select generate_series('1996-07-04'::date, '1996-07-23'::date, '1 day'::interval)::date as ord_date
)
, temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(oi.amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
 )
, temp_02 as (
	select rd.ord_date, t01.daily_sum as daily_sum
	from ref_days rd
		left outer join temp_01 t01 on rd.ord_date = t01.ord_date
)
, temp_03 as (
	select t02.*
		, first_value(t02.daily_sum) over (order by t02.ord_date)
		, case
			when t02.daily_sum is null then 0
			else row_number() over ()
		end as rnum
	from temp_02 t02
)
, temp_04 as (
	select t03.*
		, max(lpad(t03.rnum::text, 6, '0') || t03.daily_sum) over (order by t03.ord_date rows between unbounded preceding and current row) as temp_str
	from temp_03 t03
	order by ord_date
)
select t04.*, substring(t04.temp_str, 7)::float as inherited_daily_sum
from temp_04 t04
-- @formatter:on
;
