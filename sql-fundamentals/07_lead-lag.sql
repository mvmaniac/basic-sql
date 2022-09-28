/*********************************************************************************
	lag, lead 실습
*********************************************************************************/

-- lag() 현재 행보다 이전 행의 데이터를 가져옴, 동일 부서에서 hiredate순으로 이전 ename을 가져옴
select e.empno
	 , e.deptno
	 , e.hiredate
	 , e.ename
	 , lag(e.ename) over (partition by e.deptno order by e.hiredate) as prev_ename
from hr.emp e
;

-- lead( ) 현재 행보다 다음 행의 데이터를 가져옴, 동일 부서에서 hiredate순으로 다음 ename을 가져옴
select e.empno
	 , e.deptno
	 , e.hiredate
	 , e.ename
	 , lead(e.ename) over (partition by e.deptno order by e.hiredate) as next_ename
from hr.emp e
;

-- lag() over (order by desc)는 lead() over (order by asc)와 동일하게 동작하므로 혼돈을 방지하기 위해 order by 는 asc로 통일하는것이 좋음.
select e.empno
	 , e.deptno
	 , e.hiredate
	 , e.ename
	 , lag(e.ename) over (partition by e.deptno order by e.hiredate desc) as lag_desc_ename
	 , lead(e.ename) over (partition by e.deptno order by e.hiredate) as lead_desc_ename
from hr.emp e
;

-- lag 적용 시 windows에서 가져올 값이 없을 경우 default 값을 설정할 수 있음, 이 경우 반드시 offset을 정해 줘야함
select e.empno
	 , e.deptno
	 , e.hiredate
	 , e.ename
	 , lag(e.ename, 1, 'No Previous') over (partition by e.deptno order by e.hiredate) as prev_ename
from hr.emp e
;

-- Null 처리를 아래와 같이 수행할 수도 있음
select e.empno
	 , e.deptno
	 , e.hiredate
	 , e.ename
	 , coalesce(lag(e.ename) over (partition by e.deptno order by e.hiredate), 'No Previous') as prev_ename
from hr.emp e
;

-- @formatter:off
-- 현재일과 1일전 매출데이터와 그 차이를 출력, 1일전 매출 데이터가 없을 경우 현재일 매출 데이터를 출력하고, 차이는 0
with temp_01 as (
	select date_trunc('day', o.order_date)::date as ord_date, sum(oi.amount) as daily_sum
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by date_trunc('day', o.order_date)::date
)
select t01.ord_date
	 , t01.daily_sum
	 , coalesce(lag(t01.daily_sum) over (order by t01.ord_date), t01.daily_sum) as prev_daily_sum
	 , t01.daily_sum - coalesce(lag(t01.daily_sum) over (order by t01.ord_date), t01.daily_sum) as diff_prev
from temp_01 t01
-- @formatter:on
;
