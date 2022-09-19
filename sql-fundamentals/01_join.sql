/*********************************************************************************
	조인 실습 - 1
*********************************************************************************/

-- 직원 정보와 직원이 속한 부서명을 가져오기
select e.*, d.dname
from hr.emp e
	inner join hr.dept d on e.deptno = d.deptno
;

-- job이 SALESMAN인 직원정보와 직원이 속한 부서명을 가져오기
select e.*, d.dname
from hr.emp e
	inner join hr.dept d on e.deptno = d.deptno
where e.job = 'SALESMAN'
;

-- 부서명 SALES와 RESEARCH의 소속 직원들의 부서명, 직원번호, 직원명, JOB
-- 그리고 과거 급여 정보 추출
select d.dname, e.empno, e.ename, e.job, esh.fromdate, esh.todate, esh.sal
from hr.dept d
	inner join hr.emp e on d.deptno = e.deptno
	inner join hr.emp_salary_hist esh on e.empno = esh.empno
where d.dname in ('SALES', 'RESEARCH')
order by d.dname, e.empno, esh.fromdate
;

-- 부서명 SALES와 RESEARCH의 소속 직원들의 부서명, 직원번호, 직원명, JOB
-- 그리고 과거 급여 정보중 1983년 이전 데이터는 무시하고 데이터 추출
select d.dname, e.empno, e.ename, e.job, esh.fromdate, esh.todate, esh.sal
from hr.dept d
	inner join hr.emp e on d.deptno = e.deptno
	inner join hr.emp_salary_hist esh on e.empno = esh.empno
where d.dname in ('SALES', 'RESEARCH')
  and esh.fromdate >= to_date('19830101', 'yyyymmdd')
order by d.dname, e.empno, esh.fromdate
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	조인 실습 - 2
*********************************************************************************/

-- 고객명 Antonio Moreno이 1997년에 주문한 주문 정보를 주문 아이디, 주문일자, 배송일자, 배송 주소를 고객 주소와 함께 구할것
select c.contact_name, c.address, o.order_id, o.order_date, o.shipped_date, o.ship_address
from nw.customers c
	inner join nw.orders o on c.customer_id = o.customer_id
where c.contact_name = 'Antonio Moreno'
  and o.order_date between to_date('19970101', 'yyyymmdd') and to_date('19971231', 'yyyymmdd')
;

-- Berlin에 살고 있는 고객이 주문한 주문 정보를 구할것
-- 고객명, 주문id, 주문일자, 주문접수 직원명, 배송업체명을 구할것
select c.customer_id
	 , c.contact_name
	 , o.order_id
	 , o.order_date
	 , e.first_name || ' ' || e.last_name as employee_name
	 , s.company_name as shipper_name
from nw.customers c
	inner join nw.orders o on c.customer_id = o.customer_id
	inner join nw.employees e on o.employee_id = e.employee_id
	inner join nw.shippers s on o.ship_via = s.shipper_id
where c.city = 'Berlin'
;

-- Beverages 카테고리에 속하는 모든 상품아이디와 상품명, 그리고 이들 상품을 제공하는 supplier 회사명 정보 구할것
select c.category_id, c.category_name, p.product_id, p.product_name, s.supplier_id, s.company_name
from nw.categories c
	inner join nw.products p on c.category_id = p.category_id
	inner join nw.suppliers s on s.supplier_id = p.supplier_id
where c.category_name = 'Beverages'
;

-- 고객명 Antonio Moreno이 1997년에 주문한 주문 상품정보를 고객 주소, 주문 아이디, 주문일자, 배송일자, 배송 주소 및
-- 주문 상품아이디, 주문 상품명, 주문 상품별 금액, 주문 상품이 속한 카테고리명, supplier명을 구할 것
select c.contact_name
	 , c.address
	 , o.order_id
	 , oi.product_id
	 , o.order_date
	 , o.shipped_date
	 , o.ship_address
	 , p.product_name
	 , oi.amount
	 , ca.category_name
	 , s.contact_name as supplier_name
from nw.customers c
	inner join nw.orders o on c.customer_id = o.customer_id
	inner join nw.order_items oi on o.order_id = oi.order_id
	inner join nw.products p on p.product_id = oi.product_id
	inner join nw.categories ca on ca.category_id = p.category_id
	inner join nw.suppliers s on p.supplier_id = s.supplier_id
where c.contact_name = 'Antonio Moreno'
  and o.order_date between to_date('19970101', 'yyyymmdd') and to_date('19971231', 'yyyymmdd')
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	조인 실습 - Outer 조인
*********************************************************************************/

-- 주문이 단 한번도 없는 고객 정보 구하기
select c.customer_id, c.contact_name, o.order_id, o.customer_id
from nw.customers c
	left outer join nw.orders o on c.customer_id = o.customer_id
where o.order_id is null
;

-- 부서정보와 부서에 소속된 직원명 정보 구하기. 부서가 직원을 가지고 있지 않더라도 부서정보는 표시되어야 함
select d.deptno, d.dname, e.ename
from hr.dept d
	left outer join hr.emp e on d.deptno = e.deptno
;

-- Madrid에 살고 있는 고객이 주문한 주문 정보를 구할것
-- 고객명, 주문id, 주문일자, 주문접수 직원명, 배송업체명을 구하되,
-- 만일 고객이 주문을 한번도 하지 않은 경우라도 고객정보는 빠지면 안됨. 이경우 주문 정보가 없으면 주문id를 0으로 나머지는 Null로 구할것
select c.customer_id
	 , c.contact_name
	 , coalesce(o.order_id, 0) as order_id
	 , e.first_name || ' ' || e.last_name as employee_name
	 , s.company_name as shipper_name
from nw.customers c
	left outer join nw.orders o on c.customer_id = o.customer_id
	left outer join nw.employees e on o.employee_id = e.employee_id
	left outer join nw.shippers s on o.ship_via = s.shipper_id
where c.city = 'Madrid'
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	조인 실습 - Full Outer 조인
*********************************************************************************/

-- full outer join 테스트를 위해 소속 부서가 없는 테스트용 데이터 생성
drop table if exists hr.emp_test
;

create table hr.emp_test
as
	select *
	from hr.emp
;

-- 소속 부서를 Null로 update
update hr.emp_test
set deptno = null
where empno = 7934
;

-- dept를 기준으로 left outer 조인시에는 소속직원이 없는 부서는 추출 되지만. 소속 부서가 없는 직원은 추출할 수 없음
select d.deptno, d.dname, et.empno, et.ename
from hr.dept d
	left outer join hr.emp_test et on d.deptno = et.deptno
;

-- full outer join 하여 양쪽 모두의 집합이 누락되지 않도록 함
select d.deptno, d.dname, et.empno, et.ename
from hr.dept d
	full outer join hr.emp_test et on d.deptno = et.deptno
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	조인 실습 - Non Equi 조인과 Cross 조인
*********************************************************************************/

-- 직원정보와 급여등급 정보를 추출 (Non Equi)
select e.empno, e.ename, s.grade as salgrade, s.losal, s.hisal
from hr.emp e
	inner join hr.salgrade s on e.sal between s.losal and s.hisal
;

-- 직원 급여의 이력정보를 나타내며, 해당 급여를 가졌던 시작 시점에서의 부서번호도 함께 가져올것
select esh.empno, esh.fromdate, esh.sal, edh.deptno, d.dname, edh.fromdate, edh.todate
from hr.emp_salary_hist esh
	inner join hr.emp_dept_hist edh on esh.empno = edh.empno and esh.fromdate between edh.fromdate and edh.todate
	inner join hr.dept d on edh.deptno = d.deptno
;

-- @formatter:off
-- cross 조인
with temp_01 as (
	select 1 as rnum
	union all
	select 2 as rnum
)
select a.*, b.*
from hr.dept a
	cross join temp_01 b;
-- @formatter:on
;
