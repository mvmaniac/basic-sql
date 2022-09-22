/*********************************************************************************
	Group by 실습 - 1
*********************************************************************************/

-- emp 테이블에서 부서별 최대 급여, 최소 급여, 평균 급여를 구할것
select e.deptno, max(e.sal) as max_sal, min(e.sal) as min_sal, round(avg(e.sal), 2) as avg_sal
from hr.emp e
group by e.deptno
;

-- emp 테이블에서 부서별 최대 급여, 최소 급여, 평균 급여를 구하되 평균 급여가 2000 이상인 경우만 추출
select e.deptno, max(e.sal) as max_sal, min(e.sal) as min_sal, round(avg(e.sal), 2) as avg_sal
from hr.emp e
group by e.deptno
having avg(e.sal) >= 2000
;

-- @formatter:off
-- emp 테이블에서 부서별 최대 급여, 최소 급여, 평균 급여를 구하되 평균 급여가 2000 이상인 경우만 추출(with 절을 이용)
with temp_01 as (
	select e.deptno, max(e.sal) as max_sal, min(e.sal) as min_sal, round(avg(e.sal), 2) as avg_sal
	from hr.emp e
	group by e.deptno
)
select t01.*
from temp_01 t01
where t01.avg_sal >= 2000
-- @formatter:on
;

-- 부서명 SALES와 RESEARCH 소속 직원별로 과거부터 현재까지 모든 급여를 취합한 평균 급여
select e.empno, max(e.ename) as ename, avg(e.sal) as avg_sal --, count(*) as cnt
from hr.dept d
	inner join hr.emp e on d.deptno = e.deptno
	inner join hr.emp_salary_hist esh on e.empno = esh.empno
where d.dname in ('SALES', 'RESEARCH')
group by e.empno
order by 1
;

-- @formatter:off
-- 부서명 SALES와 RESEARCH 소속 직원별로 과거부터 현재까지 모든 급여를 취합한 평균 급여(with 절로 풀기)
with temp_01 as	(
	select d.dname, e.empno, e.ename, e.job, esh.fromdate, esh.todate, esh.sal
	from hr.dept d
		inner join hr.emp e on d.deptno = e.deptno
		inner join hr.emp_salary_hist esh on e.empno = esh.empno
	where d.dname in ('SALES', 'RESEARCH')
	order by d.dname, e.empno, esh.fromdate
)
select t01.empno, max(t01.ename) as ename, avg(t01.sal) as avg_sal
from temp_01 t01
group by t01.empno
-- @formatter:on
;

-- 부서명 SALES와 RESEARCH 부서별 평균 급여를 소속 직원들의 과거부터 현재까지 모든 급여를 취합하여 구할것
select d.deptno, max(d.dname) as dname, avg(esh.sal) as avg_sal, count(*) as cnt
from hr.dept d
	inner join hr.emp e on d.deptno = e.deptno
	inner join hr.emp_salary_hist esh on e.empno = esh.empno
where d.dname in ('SALES', 'RESEARCH')
group by d.deptno
order by 1
;

-- @formatter:off
-- 부서명 SALES와 RESEARCH 부서별 평균 급여를 소속 직원들의 과거부터 현재까지 모든 급여를 취합하여 구할것(with절로 풀기)
with temp_01 as (
	select d.deptno
		  , d.dname
		  , e.empno
		  , e.ename
		  , e.job
		  , esh.fromdate
		  , esh.todate
		  , esh.sal
	from hr.dept d
		inner join hr.emp e on d.deptno = e.deptno
		inner join hr.emp_salary_hist esh on e.empno = esh.empno
	where d.dname in ('SALES', 'RESEARCH')
	order by d.dname, e.empno, esh.fromdate
)
select t01.deptno, max(t01.dname) as dname, avg(t01.sal) as avg_sal
from temp_01 t01
group by t01.deptno
order by 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	Group by 실습 - 2 (집계함수와 count(distinct))
*********************************************************************************/

-- 추가적인 테스트 테이블 생성
drop table if exists hr.emp_test
;

create table hr.emp_test
as
	select *
	from hr.emp
;

insert into hr.emp_test
select 8000
	 , 'CHMIN'
	 , 'ANALYST'
	 , 7839
	 , TO_DATE('19810101', 'YYYYMMDD')
	 , 3000
	 , 1000
	 , 20
;

-- Aggregation은 Null값을 처리하지 않음
select et.deptno
	 , count(*) as cnt
	 , sum(et.comm)
	 , max(et.comm)
	 , min(et.comm)
	 , avg(et.comm)
from hr.emp_test et
group by et.deptno
;

select et.*
from hr.emp_test et
where et.deptno = 30
;

select e.mgr, count(*), sum(e.comm)
from hr.emp e
group by e.mgr
;

-- max, min 함수는 숫자열 뿐만 아니라, 문자열,날짜/시간 타입에도 적용가능
select e.deptno
	 , max(e.job)
	 , min(e.ename)
	 , max(e.hiredate)
	 , min(e.hiredate)
	 --, sum(e.ename)
	 --, avg(e.ename)
from hr.emp e
group by e.deptno
;

-- count(distinct 컬럼명)은 지정된 컬럼명으로 중복을 제거한 고유한 건수를 추출
select count(distinct et.job)
from hr.emp_test et
;

select et.deptno, count(*) as cnt, count(distinct et.job)
from hr.emp_test et
group by et.deptno
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	Group by 실습 - 3 (Group by절에 가공 컬럼 및 case when 적용)
*********************************************************************************/

-- emp 테이블에서 입사년도별 평균 급여 구하기
select to_char(e.hiredate, 'yyyy') as hire_year, avg(e.sal) as avg_sal, count(*) as cnt
from hr.emp e
group by to_char(e.hiredate, 'yyyy')
order by 1
;

-- 1000미만, 1000-1999, 2000-2999와 같이 1000단위 범위내에 sal이 있는 레벨로 group by 하고 해당 건수를 구함
select floor(e.sal / 1000) * 1000 as bin_range, count(*)
from hr.emp e
group by floor(e.sal / 1000) * 1000
order by 1
;

select *, floor(e.sal / 1000) * 1000 as bin_range, sal / 1000, floor(sal / 1000)
from hr.emp e
;

-- @formatter:off
-- job이 SALESMAN인 경우와 그렇지 않은 경우만 나누어서 평균/최소/최대 급여를 구하기
select
    case
        when job = 'SALESMAN' then 'SALESMAN'
        else 'OTHERS'
	end as job_gubun
	, avg(sal) as avg_sal
	, max(sal) as max_sal
	, min(sal) as min_sal
	, count(*) as cnt
from hr.emp
group by
	case
        when job = 'SALESMAN' then 'SALESMAN'
	    else 'OTHERS'
	end
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	Group by 실습 - 4 (Group by와 Aggregate 함수의 case when 을 이용한 pivoting)
**********************************************************************************/

select e.job, sum(e.sal) as sales_sum
from hr.emp e
group by e.job
;

select sum(case when e.job = 'SALESMAN' then e.sal end) as sales_sum
	 , sum(case when e.job = 'MANAGER' then e.sal end) as manager_sum
	 , sum(case when e.job = 'ANALYST' then e.sal end) as analyst_sum
	 , sum(case when e.job = 'CLERK' then e.sal end) as clerk_sum
	 , sum(case when e.job = 'PRESIDENT' then e.sal end) as president_sum
from hr.emp e
;

-- deptno + job 별로 group by
select e.deptno, e.job, sum(e.sal) as sal_sum
from hr.emp e
group by e.deptno, e.job
;

-- deptno로 group by하고 job으로 pivoting
select e.deptno
	 , sum(e.sal) as sal_sum
	 , sum(case when e.job = 'SALESMAN' then e.sal end) as sales_sum
	 , sum(case when e.job = 'MANAGER' then e.sal end) as manager_sum
	 , sum(case when e.job = 'ANALYST' then e.sal end) as analyst_sum
	 , sum(case when e.job = 'CLERK' then e.sal end) as clerk_sum
	 , sum(case when e.job = 'PRESIDENT' then e.sal end) as president_sum
from hr.emp e
group by e.deptno
;

-- group by Pivoting시 조건에 따른 건수 계산 유형(count case when then 1 else null end)
select e.deptno
	 , count(*) as cnt
	 , count(case when e.job = 'SALESMAN' then 1 end) as sales_cnt
	 , count(case when e.job = 'MANAGER' then 1 end) as manager_cnt
	 , count(case when e.job = 'ANALYST' then 1 end) as analyst_cnt
	 , count(case when e.job = 'CLERK' then 1 end) as clerk_cnt
	 , count(case when e.job = 'PRESIDENT' then 1 end) as president_cnt
from hr.emp e
group by e.deptno
;

-- group by Pivoting시 조건에 따른 건수 계산 시 잘못된 사례(count case when then 1 else null end)
select e.deptno
	 , count(*) as cnt
	 , count(case when e.job = 'SALESMAN' then 1 else 0 end) as sales_cnt
	 , count(case when e.job = 'MANAGER' then 1 else 0 end) as manager_cnt
	 , count(case when e.job = 'ANALYST' then 1 else 0 end) as analyst_cnt
	 , count(case when e.job = 'CLERK' then 1 else 0 end) as clerk_cnt
	 , count(case when e.job = 'PRESIDENT' then 1 else 0 end) as president_cnt
from hr.emp e
group by e.deptno
;

-- group by Pivoting시 조건에 따른 건수 계산 시 sum()을 이용
select e.deptno
	 , count(*) as cnt
	 , sum(case when e.job = 'SALESMAN' then 1 else 0 end) as sales_cnt
	 , sum(case when e.job = 'MANAGER' then 1 else 0 end) as manager_cnt
	 , sum(case when e.job = 'ANALYST' then 1 else 0 end) as analyst_cnt
	 , sum(case when e.job = 'CLERK' then 1 else 0 end) as clerk_cnt
	 , sum(case when e.job = 'PRESIDENT' then 1 else 0 end) as president_cnt
from hr.emp e
group by deptno
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	Group by rollup
**********************************************************************************/

-- deptno + job레벨 외에 dept내의 전체 job 레벨(결국 dept레벨), 전체 Aggregation 수행
select e.deptno, e.job, sum(e.sal)
from hr.emp e
group by rollup (e.deptno, e.job)
order by 1, 2
;


-- 상품 카테고리 + 상품별 매출합 구하기
select c.category_name, p.product_name, sum(oi.amount)
from nw.order_items oi
	join nw.products p on oi.product_id = p.product_id
	join nw.categories c on p.category_id = c.category_id
group by c.category_name, p.product_name
order by 1, 2
;

-- 상품 카테고리 + 상품별 매출합 구하되, 상품 카테고리 별 소계 매출합 및 전체 상품의 매출합을 함께 구하기
select c.category_name, p.product_name, sum(oi.amount)
from nw.order_items oi
	join nw.products p on oi.product_id = p.product_id
	join nw.categories c on p.category_id = c.category_id
group by rollup (c.category_name, p.product_name)
order by 1, 2
;

-- 년+월+일별 매출합 구하기
-- 월 또는 일을 01, 02와 같은 형태로 표시하려면 to_char()함수, 1, 2와 같은 숫자값으로 표시하려면 date_part()함수 사용
select to_char(o.order_date, 'yyyy') as year
	 , to_char(o.order_date, 'mm') as month
	 , to_char(o.order_date, 'dd') as day
	 , sum(oi.amount) as sum_amount
from nw.order_items oi
	join nw.orders o on oi.order_id = o.order_id
group by to_char(o.order_date, 'yyyy'), to_char(o.order_date, 'mm'), to_char(o.order_date, 'dd')
order by 1, 2, 3
;

-- 년+월+일별 매출합 구하되, 월별 소계 매출합, 년별 매출합, 전체 매출합을 함께 구하기
with temp_01 as (
	select to_char(o.order_date, 'yyyy') as year
		, to_char(o.order_date, 'mm') as month
		, to_char(o.order_date, 'dd') as day
		, sum(oi.amount) as sum_amount
	from nw.order_items oi
		inner join nw.orders o on oi.order_id = o.order_id
	group by rollup (to_char(o.order_date, 'yyyy'), to_char(o.order_date, 'mm'), to_char(o.order_date, 'dd'))
	-- order by 1, 2, 3
)
select
	case when t01.year is null then '총매출' else t01.year end as year
	, case
		when t01.year is null then null
		else case when t01.month is null then '년 총매출' else t01.month end
	end as month
	, case
		when t01.year is null or t01.month is null then null
		else case when t01.day is null then '월 총매출' else t01.day end
	end as day
	, sum_amount
from temp_01 t01
order by t01.year, t01.month, t01.day
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	Group by cube
**********************************************************************************/

-- deptno, job의 가능한 결합으로 Group by 수행
select e.deptno, e.job, sum(e.sal)
from hr.emp e
group by cube (e.deptno, e.job)
order by 1, 2
;

-- 상품 카테고리 + 상품별 + 주문처리직원별 매출
select c.category_name, p.product_name, e.last_name || e.first_name as emp_name, sum(oi.amount)
from nw.order_items oi
	join nw.products p on oi.product_id = p.product_id
	join nw.categories c on p.category_id = c.category_id
	join nw.orders o on oi.order_id = o.order_id
	join nw.employees e on o.employee_id = e.employee_id
group by c.category_name, p.product_name, e.last_name || e.first_name
order by 1, 2, 3
;

--상품 카테고리, 상품별, 주문처리직원별 가능한 결합으로 Group by 수행
select c.category_name, p.product_name, e.last_name || e.first_name as emp_name, sum(oi.amount)
from nw.order_items oi
	join nw.products p on oi.product_id = p.product_id
	join nw.categories c on p.category_id = c.category_id
	join nw.orders o on oi.order_id = o.order_id
	join nw.employees e on o.employee_id = e.employee_id
group by cube (c.category_name, p.product_name, e.last_name || e.first_name)
order by 1, 2, 3
;
