/*********************************************************************************
	rank, dense_rank, row_number 실습
*********************************************************************************/

-- rank, dense_rank, row_number 사용하기 - 1
select e.empno
	 , e.ename
	 , e.job
	 , e.sal
	 , rank() over (order by e.sal desc) as rank
	 , dense_rank() over (order by e.sal desc) as dense_rank
	 , row_number() over (order by e.sal desc) as row_number
from hr.emp e
;

-- rank, dense_rank, row_number 사용하기 - 2
select e.empno
	 , e.ename
	 , e.job
	 , e.deptno
	 , e.sal
	 , rank() over (partition by e.deptno order by e.sal desc) as rank
	 , dense_rank() over (partition by e.deptno order by e.sal desc) as dense_rank
	 , row_number() over (partition by e.deptno order by e.sal desc) as row_number
from hr.emp e
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	순위 함수 실습
*********************************************************************************/

-- 회사내 근무 기간 순위(hiredate), 공동 순위가 있을 경우 차순위는 밀려서 순위 정함
select e.*
	 , rank() over (order by e.hiredate) as hire_rank
from hr.emp e
;

-- 부서별로 가장 급여가 높은/낮은 순으로 순위, 공동 순위 시 차순위는 밀리지 않음
select e.*
	 , dense_rank() over (partition by e.deptno order by e.sal desc) as sal_rank_desc
	 , dense_rank() over (partition by e.deptno order by e.sal) as sal_rank_asc
from hr.emp e
;

-- @formatter:off
-- 부서별 가장 급여가 높은 직원 정보, 공동 순위는 없으며 반드시 고유 순위를 정함 (고유 순위는 바뀌지 않음...)
select t.*
from (
	select e.*
		, row_number() over (partition by e.deptno order by e.sal desc) as sal_rn
	from hr.emp e
) t
where t.sal_rn = 1
-- @formatter:on
;

-- @formatter:off
-- 부서별 급여 top 2 직원 정보, 공동 순위는 없으며 반드시 고유 순위를 정함
select t.*
from (
	select e.*
		 , row_number() over (partition by e.deptno order by e.sal desc) as sal_rn
	from hr.emp e
) t
where t.sal_rn = 2
-- @formatter:on
;

-- @formatter:off
-- 부서별 가장 급여가 높은 직원과 가장 급여가 낮은 직원 정보, 공동 순위는 없으며 반드시 고유 순위를 정함
select t.*
	, case
		when t.sal_rn_desc = 1 then 'top'
		when t.sal_rn_asc = 1 then 'bottom'
		else 'middle'
	end as gubun
from (
	select e.*
		, row_number() over (partition by e.deptno order by e.sal desc) as sal_rn_desc
		, row_number() over (partition by e.deptno order by e.sal) as sal_rn_asc
	from hr.emp e
) t
where t.sal_rn_desc = 1 or t.sal_rn_asc = 1
-- @formatter:on
;

-- @formatter:off
-- 부서별 가장 급여가 높은 직원과 가장 급여가 낮은 직원 정보 그리고 두 직원값의 급여차이도 함께 추출. 공동 순위는 없으며 반드시 고유 순위를 정함
with temp_01 as (
	select t.*
		, case
			when t.sal_rn_desc = 1 then 'top'
			when t.sal_rn_asc = 1 then 'bottom'
			else 'middle'
		end as gubun
	from (
		select e.*
			, row_number() over (partition by e.deptno order by e.sal desc) as sal_rn_desc
			, row_number() over (partition by e.deptno order by e.sal) as sal_rn_asc
		from hr.emp e
	) t
	where t.sal_rn_desc = 1 or t.sal_rn_asc = 1
 )
, temp_02 as (
	select t01.deptno
		, max(t01.sal) as max_sal
		, min(t01.sal) as min_sal
	from temp_01 t01
	group by t01.deptno
 )
select t01.*, t02.max_sal - t02.min_sal as diff_sal
from temp_01 t01
	inner join temp_02 t02 on t01.deptno = t02.deptno
order by t01.deptno, t01.sal desc
-- @formatter:on
;

-- 회사내 커미션 높은 순위, rank와 row_number 모두 추출
select e.*
	 , rank() over (order by e.comm desc) as comm_rank
	 , row_number() over (order by e.comm desc) as comm_rnum
from hr.emp e
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	순위 함수에서 null 처리 실습
*********************************************************************************/

-- null을 가장 선두 순위로 처리, nulls first 옵션이 기본 값
select e.*
	 , rank() over (order by e.comm desc nulls first) as comm_rank
	 , row_number() over (order by e.comm desc nulls first) as comm_rnum
from hr.emp e
;

-- null을 가장 마지막 순위로 처리
select e.*
	 , rank() over (order by e.comm desc nulls last) as comm_rank
	 , row_number() over (order by e.comm desc nulls last) as comm_rnum
from hr.emp e
;

-- null을 전처리하여 순위 정함
select e.*
	 , rank() over (order by coalesce(e.comm, 0) desc) as comm_rank
	 , row_number() over (order by coalesce(e.comm, 0) desc) as comm_rnum
from hr.emp e
;
