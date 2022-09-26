/*********************************************************************************
	sum, max, min, avg, count 실습
*********************************************************************************/

-- order_items 테이블에서 order_id 별 amount 총합까지 표시
select oi.order_id
	 , oi.line_prod_seq
	 , oi.product_id
	 , oi.amount
	 , sum(oi.amount) over (partition by oi.order_id) as total_sum_by_ord
from nw.order_items oi
;

-- order_items 테이블에서시 rder_id별 line_prod_seq순으로 누적 amount 합까지 표시
select oi.order_id
	 , oi.line_prod_seq
	 , oi.product_id
	 , oi.amount
	 , sum(oi.amount) over (partition by oi.order_id) as total_sum_by_ord
	 , sum(oi.amount) over (partition by oi.order_id order by oi.line_prod_seq) as cum_sum_by_ord
from nw.order_items oi
;

-- order_items 테이블에서 order_id별 line_prod_seq순으로 누적 amount 합
-- 집계 (aggregate) 계열 analytic 함수는 order by 절이 있을 경우 window 절은 기본적으로 range unbounded preceding and current row 임
-- 다만 rows between unbounded preceding and current row 생각해도 됨
-- 만약 order by 절이 없다면 window 는 해당 partition 의 모든 row 를 대상
-- 만약 partition 절도 없다면 window 는 전체 데이터의 row 를 대상
select oi.order_id
	 , oi.line_prod_seq
	 , oi.product_id as amount
	 , sum(oi.amount) over (partition by oi.order_id) as total_sum_by_ord
	 , sum(oi.amount) over (partition by oi.order_id order by oi.line_prod_seq) as cum_sum_by_ord_01
	 , sum(oi.amount)
	   over (partition by oi.order_id order by oi.line_prod_seq rows between unbounded preceding and current row) as cum_sum_by_ord_02
	 , sum(oi.amount) over ( ) as total_sum
from nw.order_items oi
where oi.order_id between 10248 and 10250
;

-- order_items 테이블에서 order_id 별 상품 최대 구매금액, order_id별 상품 누적 최대 구매금액
select oi.order_id
	 , oi.line_prod_seq
	 , oi.product_id
	 , oi.amount
	 , max(oi.amount) over (partition by oi.order_id) as total_max_by_ord
	 , max(oi.amount) over (partition by oi.order_id order by oi.line_prod_seq) as cum_max_by_ord
from nw.order_items oi
;

-- order_items 테이블에서 order_id 별 상품 최소 구매금액, order_id별 상품 누적 최소 구매금액
select oi.order_id
	 , oi.line_prod_seq
	 , oi.product_id
	 , oi.amount
	 , min(oi.amount) over (partition by oi.order_id) as total_min_by_ord
	 , min(oi.amount) over (partition by oi.order_id order by oi.line_prod_seq) as cum_min_by_ord
from nw.order_items oi
;

-- order_items 테이블에서 order_id 별 상품 평균 구매금액, order_id별 상품 누적 평균 구매금액
select oi.order_id
	 , oi.line_prod_seq
	 , oi.product_id
	 , oi.amount
	 , avg(oi.amount) over (partition by oi.order_id) as total_avg_by_ord
	 , avg(oi.amount) over (partition by oi.order_id order by oi.line_prod_seq) as cum_avg_by_ord
from nw.order_items oi
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	aggregation analytic 실습
*********************************************************************************/

-- 직원 정보 및 부서별로 직원 급여의 hiredate순으로 누적 급여합
select e.empno
	 , e.ename
	 , e.deptno
	 , e.sal
	 , e.hiredate
	 , sum(e.sal) over (partition by e.deptno order by e.hiredate) as cum_sal
from hr.emp e
;

--직원 정보 및 부서별 평균 급여와 개인 급여와의 차이 출력
select e.empno
	 , e.ename
	 , e.deptno
	 , e.sal
	 , avg(e.sal) over (partition by e.deptno) as dept_avg_sal
	 , e.sal - avg(e.sal) over (partition by e.deptno) as dept_avg_sal_diff
from hr.emp e
;

-- analytic을 사용하지 않고 위와 동일한 결과 출력
with temp_01 as (
	select e.deptno, avg(e.sal) as dept_avg_sal
	from hr.emp e
	group by e.deptno
 )
select e.empno
	 , e.ename
	 , e.deptno
	 , t01.dept_avg_sal
	 , e.sal - t01.dept_avg_sal as dept_avg_sal_diff
from hr.emp e
	inner join temp_01 t01 on e.deptno = t01.deptno
order by e.deptno
;

-- 직원 정보및 부서별 총 급여 대비 개인 급여의 비율 출력(소수점 2자리까지로 비율 출력)
select e.empno
	 , e.ename
	 , e.deptno
	 , e.sal
	 , sum(e.sal) over (partition by e.deptno) as dept_sum_sal
	 , round(e.sal / sum(e.sal) over (partition by e.deptno), 2) as dept_sum_sal_ratio
from hr.emp e
;

-- 직원 정보 및 부서에서 가장 높은 급여 대비 비율 출력(소수점 2자리까지로 비율 출력)
select e.empno
	 , e.ename
	 , e.deptno
	 , e.sal
	 , max(e.sal) over (partition by e.deptno) as dept_max_sal
	 , round(e.sal / max(e.sal) over (partition by e.deptno), 2) as dept_max_sal_ratio
from hr.emp e
;

-- @formatter:off
-- product_id 총 매출액을 구하고, 전체 매출 대비 개별 상품의 총 매출액 비율을 소수점2자리로 구한 뒤 매출액 비율 내림차순으로 정렬
with temp_01 as (
	select oi.product_id, sum(oi.amount) as sum_by_prod
	from nw.order_items oi
	group by oi.product_id
)
select t01.product_id
	 , t01.sum_by_prod
	 , sum(t01.sum_by_prod) over () as total_sum
	 , round(1.0 * t01.sum_by_prod / sum(t01.sum_by_prod) over (), 2) as sum_ratio
from temp_01 t01
order by 4 desc
-- @formatter:on
;

-- @formatter:off
-- 직원별 개별 상품 매출액, 직원별 가장 높은 상품 매출액을 구하고, 직원별로 가장 높은 매출을 올리는 상품의 매출 금액 대비 개별 상품 매출 비율 구하기
with temp_01 as (
	select o.employee_id, oi.product_id, sum(oi.amount) as sum_by_emp_prod
	from nw.order_items oi
		inner join nw.orders o on o.order_id = oi.order_id
	group by o.employee_id, oi.product_id
)
select t01.employee_id
	, t01.product_id
	, t01.sum_by_emp_prod
	, max(t01.sum_by_emp_prod) over (partition by t01.employee_id) as max_sum_emp_prod
	, round(1.0 * t01.sum_by_emp_prod / max(t01.sum_by_emp_prod) over (partition by t01.employee_id), 2) as max_sum_ratio
from temp_01 t01
order by 1, 5 desc
-- @formatter:on
;

-- 상품별 매출합을 구하되, 상품 카테고리별 매출합의 5% 이상이고, 동일 카테고리에서 상위 3개 매출의 상품 정보 추출
-- 1. 상품별 + 상품 카테고리별 총 매출 계산. (상품별 + 상품 카테고리별 총 매출은 결국 상품별 총 매출임)
-- 2. 상품 카테고리별 총 매출 계산 및 동일 카테고리에서 상품별 랭킹 구함
-- 3. 상품 카테고리 매출의 5% 이상인 상품 매출과 매출 기준 top 3 상품 추출
with temp_01 as (
	select oi.product_id, max(p.category_id) as category_id, sum(oi.amount) as sum_by_prod
	from nw.order_items oi
		inner join nw.products p on p.product_id = oi.product_id
	group by oi.product_id
)
, temp_02 as (
	select t01.product_id
		, t01.category_id
		, t01.sum_by_prod
		, sum(t01.sum_by_prod) over (partition by t01.category_id) as sum_by_cat
		, row_number() over (partition by t01.category_id order by t01.sum_by_prod desc) as top_prod_ranking
	from temp_01 t01
)
select t02.*
from temp_02 t02
where t02.sum_by_prod >= 0.05 * t02.sum_by_cat and top_prod_ranking <= 3
;
