/*********************************************************************************
	서브쿼리 유형 기본
*********************************************************************************/

-- 평균 급여 이상의 급여를 받는 직원
select e.*
from hr.emp e
where e.sal >= (select avg(sq.sal) from hr.emp sq)
;

-- 가장 최근 급여 정보
select esh.*
from hr.emp_salary_hist esh
where esh.todate = (select max(sq.todate) from hr.emp_salary_hist sq where esh.empno = sq.empno)
;

-- 스칼라 서브쿼리
select e.ename
	 , e.deptno
	 , (select sq.dname from hr.dept sq where e.deptno = sq.deptno) as dname
from hr.emp e
;

-- @formatter:off
-- 인라인뷰 서브쿼리
select t.deptno, d.dname, t.sum_sal
from (
	select e.deptno, sum(e.sal) as sum_sal
	from hr.emp e
	group by e.deptno
) t
	inner join hr.dept d on t.deptno = d.deptno
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	where 절 서브쿼리 이해
*********************************************************************************/

-- ok
select d.*
from hr.dept d
where d.deptno in (select sq, deptno from hr.emp sq where sq.sal > 1000)
;

-- 수행 안됨
select d.*, sq.ename
from hr.dept d
where d.deptno in (select sq.deptno from hr.emp sq where sq.sal > 1000)
;

--ok
select d.*
from hr.dept d
where exists(select sq.deptno from hr.emp sq where sq.deptno = d.deptno and sq.sal > 1000)
;

-- 서브쿼리의 반환값은 무조건 중복이 제거된 unique한 값 - 비상관 서브쿼리
select o.*
from nw.orders o
where o.order_id in (select sq.order_id from nw.order_items sq where sq.amount > 100)
;

-- 서브쿼리의 반환값은 메이쿼리의 개별 레코드로 연결된 결과값에서 무조건 중복이 제거된 unique한 값 - 상관 서브쿼리
select o.*
from nw.orders o
where exists(select order_id from nw.order_items sq where o.order_id = sq.order_id and sq.amount > 100)
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	비상관(non-correlated) 서브쿼리
*********************************************************************************/

-- in 연산자는 괄호내에 한개 이상의 값을 상수값 또는 서브쿼리 결과 값으로 가질 수 있으며 개별값의 = 조건들의 or 연산을 수행
select e.*
from hr.emp e
where e.deptno in (20, 30)
;

-- 위 쿼리와 같음
select e.*
from hr.emp e
where e.deptno = 20 or e.deptno = 30
;

-- 여러개의 중복된 값을 괄호 내에 가질 경우 중복을 제거하고 unique한 값을 가짐
select d.*
from hr.dept d
where d.deptno in (select sq.deptno from hr.emp sq where sq.sal < 1300)
;

-- 단일 컬럼 뿐 아니라 여러컬럼을 가질 수 있음
select d.*
from hr.dept d
where (d.deptno, d.loc) in (select sq.deptno, 'DALLAS' from hr.emp sq where sq.sal < 1300)
;

-- @formatter:off
-- 고객이 가장 최근에 주문한 주문 정보 추출
select *
from nw.orders o
where (o.customer_id, o.order_date) in (
	select sq.customer_id, max(sq.order_date)
	from nw.orders sq
	group by sq.customer_id
)
-- @formatter:on
;

-- 메인쿼리-서브쿼리의 연결 연산자가 단순 비교 연산자일 경우 서브쿼리는 단 한개의 값을 반환해야 함
select e.*
from hr.emp e
where e.sal <= (select avg(sq.sal) from hr.emp sq)
;

-- 메인쿼리-서브쿼리의 연결 연산자가 = 인데 서브쿼리의 반환값이 여러개이므로 수행 안됨
select d.*
from hr.dept d
where d.deptno = (select sq.deptno from hr.emp sq where sq.sal < 1300)
;

-- @formatter:off
-- 단순 비교 연산자로 서브쿼리를 연결하여도 여러 컬럼 조건을 가질 수 있음
select o.*
from nw.orders o
where (o.customer_id, o.order_date) = (
	select sq.customer_id, max(sq.order_date)
	from nw.orders sq
	where sq.customer_id = 'VINET'
	group by sq.customer_id
)
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	상관(correlated) 서브쿼리
*********************************************************************************/

-- 상관 서브쿼리, 주문에서 상품 금액이 100보다 큰 주문을 한 주문 정보
select o.*
from nw.orders o
where exists(select sq.order_id from nw.order_items sq where o.order_id = sq.order_id and sq.amount > 100)
;

-- 비상관 서브쿼리, 상품 금액이 100보다 큰 주문을 한 주문 정보
select o.*
from nw.orders o
where o.order_id in (select sq.order_id from nw.order_items sq where sq.amount > 100)
;

-- @formatter:off
-- 2건 이상 주문을 한 고객 정보
select c.*
from nw.customers c
where exists(
    select 1
	from nw.orders sq
	where sq.customer_id = c.customer_id
	group by sq.customer_id
	having count(*) >= 2
)
-- @formatter:on
;

-- @formatter:off
-- 1997년 이후에 한건이라도 주문을 한 고객 정보
select c.*
from nw.customers c
where exists(
	select 1
	from nw.orders sq
	where sq.customer_id = c.customer_id
	and sq.order_date >= to_date('19970101', 'yyyymmdd')
)
-- @formatter:on
;

-- @formatter:off
--1997년 이후에 단 한건도 주문하지 않은 고객 정보
select c.*
from nw.customers c
where not exists(
	select 1
	from nw.orders sq
	where sq.customer_id = c.customer_id
	and sq.order_date >= to_date('19970101', 'yyyymmdd')
)
;
-- @formatter:on

-- @formatter:off
-- 조인으로 변환
select c.*
from nw.customers c
	left outer join (
		select o.customer_id
		from nw.orders o
		where o.order_date >= to_date('19970101', 'yyyymmdd')
		group by o.customer_id
	) t on c.customer_id = t.customer_id
where t.customer_id is null
-- @formatter:on
;

-- @formatter:off
-- 직원의 급여이력에서 가장 최근의 급여이력
select *
from hr.emp_salary_hist esh
where esh.todate = (
	select max(sq.todate)
	from hr.emp_salary_hist sq
	where sq.empno = esh.empno
)
-- @formatter:on
;

-- @formatter:off
-- 아래는 메인쿼리의 개별 레코드 별로 empno 연결조건으로 단 한건이 아닌 여러건을 반환하므로 수행 오류
select *
from hr.emp_salary_hist esh
where esh.todate = (
	select sq.todate
	from hr.emp_salary_hist sq
	where sq.empno = esh.empno
)
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	서브쿼리 실습 - 가장 높은 sal을 받는 직원 정보
*********************************************************************************/

-- 가장 높은 sal을 받는 직원정보
select e.*
from hr.emp e
where e.sal = (select max(sq.sal) from hr.emp sq)
;

-- 조인
select e.*
from hr.emp e
	inner join (select max(sq.sal) as sal from hr.emp sq) t on e.sal = t.sal
;

-- @formatter:off
-- Analytic SQL
select t.*
from (
	select sq.*
	     , row_number() over (order by sq.sal desc) as rnum
	from hr.emp sq
) t
where t.rnum = 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	서브쿼리 실습 - 직원의 가장 최근 부서 근무이력 조회
*********************************************************************************/

drop table if exists hr.emp_dept_hist_01
;

-- todate가 99991231가 아닌 경우를 한개 레코드로 생성하기 위해 임시 테이블 생성
create table hr.emp_dept_hist_01
as
	select edh.*
	from hr.emp_dept_hist edh
;

update hr.emp_dept_hist_01 edh01
set todate = to_date('1983-12-24', 'yyyy-mm-dd')
where edh01.empno = 7934 and edh01.todate = to_date('99991231', 'yyyymmdd')
;

select edh01.*
from hr.emp_dept_hist_01 edh01
;

-- @formatter:off
-- 직원의 가장 최근 부서 근무이력 조회. 비상관 서브쿼리
select *
from hr.emp_dept_hist_01 edh01
where (edh01.empno, edh01.todate) in (
	select sq.empno, max(sq.todate)
	from hr.emp_dept_hist_01 sq
	group by sq.empno
)
;
-- @formatter:on

-- 상관 서브쿼리로 구하기
select *
from hr.emp_dept_hist_01 edh01
where edh01.todate = (select max(sq.todate) from hr.emp_dept_hist_01 sq where sq.empno = edh01.empno)
;

-- @formatter:off
-- Analytic SQL로 구하기
select t.*
from (
	select sq.*
		, row_number() over (partition by sq.empno order by sq.todate desc) as rnum
	from hr.emp_dept_hist_01 sq
) t
where t.rnum = 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	서브쿼리 실습 - 고객의 첫번째 주문일의 주문정보와 고객 정보를 함께 추출
*********************************************************************************/

-- 고객의 첫번째 주문일의 order_id, order_date, shipped_date와 함께 고객명(contact_name), 고객거주도시(city) 정보를 함께 추출
select o.order_id, o.order_date, o.shipped_date, c.contact_name, c.city
from nw.orders o
	inner join nw.customers c on o.customer_id = c.customer_id
where o.order_date = (select min(sq.order_date) from nw.orders sq where sq.customer_id = o.customer_id)
;

-- @formatter:off
-- Analytic SQL로 구하기
select t.order_id, t.order_date, t.shipped_date, t.contact_name, t.city
from (
	select o.order_id
		, o.order_date
		, o.shipped_date
		, c.contact_name
		, c.city
		, row_number() over (partition by o.customer_id order by o.order_date) as rnum
	from nw.orders o
	inner join nw.customers c on o.customer_id = c.customer_id
) t
where t.rnum = 1
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	서브쿼리 실습
  	고객별 주문 상품 평균 금액보다 더 큰 금액의 주문 상품명, 주문번호, 주문 상품금액을
  	구하되 고객명과 고객도시명을 함께 추출
*********************************************************************************/

-- 고객별 주문상품 평균 금액
select o.customer_id, avg(oi.amount) as avg_amount
from nw.orders o
	inner join nw.order_items oi on o.order_id = oi.order_id
group by o.customer_id
;

-- @formatter:off
-- 상관 서브쿼리로 구하기
select c.customer_id, c.contact_name, c.city, o.order_id, oi.product_id, oi.amount, p.product_name
from nw.customers c
	inner join nw.orders o on c.customer_id = o.customer_id
	inner join nw.order_items oi on o.order_id = oi.order_id
	inner join nw.products p on oi.product_id = p.product_id
where oi.amount >= (
	select avg(sq_oi.amount) as avg_amount
	from nw.orders sq_o
		inner join nw.order_items sq_oi on sq_o.order_id = sq_oi.order_id
	where sq_o.customer_id = c.customer_id
	group by sq_o.customer_id
)
order by c.customer_id, oi.amount
-- @formatter:on
;

-- @formatter:off
-- Analytic SQL로 구하기
select t.customer_id, t.contact_name, t.city, t.order_id, t.product_id, t.amount, t.product_name
from (
	select c.customer_id
		, c.contact_name
		, c.city
		, o.order_id
		, oi.product_id
		, oi.amount
		, p.product_name
		, avg(oi.amount) over (partition by c.customer_id rows between unbounded preceding and unbounded following) as avg_amount
	from nw.customers c
		inner join nw.orders o on c.customer_id = o.customer_id
		inner join nw.order_items oi on o.order_id = oi.order_id
		inner join nw.products p on oi.product_id = p.product_id
) t
where t.amount >= t.avg_amount
order by t.customer_id, t.amount
-- @formatter:on
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	Null값이 있는 컬럼의 not in과 not exists 차이 실습
*********************************************************************************/

select e.*
from hr.emp e
where e.deptno in (20, 30, null)
;

select e.*
from hr.emp e
where e.deptno = 20 or e.deptno = 30 or e.deptno = null
;

-- 테스트를 위한 임의의 테이블 생성.
drop table if exists nw.region
;

create table nw.region
as
	select o.ship_region as region_name
	from nw.orders o
	group by o.ship_region
;

-- 새로운 XX값을 region테이블에 입력.
insert into nw.region
values ('XX')
;

commit
;

select *
from nw.region
;

-- null값이 포함된 컬럼을 서브쿼리로 연결할 시 in과 exists는 모두 동일.
select r.*
from nw.region r
where r.region_name in (select sq.ship_region from nw.orders sq)
;

select r.*
from nw.region r
where exists(select sq.ship_region from nw.orders sq where sq.ship_region = r.region_name)
;

-- null값이 포함된 컬럼을 서브쿼리로 연결 시 not in과 not exists의 결과는 서로 다름.
select r.*
from nw.region r
where r.region_name not in (select sq.ship_region from nw.orders sq)
;

select r.*
from nw.region r
where not exists(select sq.ship_region from nw.orders sq where sq.ship_region = r.region_name)
;

-- true
select 1 = 1
;

-- false
select 1 = 2
;

-- null
select null = null
;

-- null
select 1 = 1 and null
;

-- null
select 1 = 1 and (null = null)
;

-- true
select 1 = 1 or null
;

-- false
select not 1 = 1
;

-- null
select not null
;

-- not in을 사용할 경우 null인 값은 서브쿼리내에서 is not null로 미리 제거해야 함.
select r.*
from nw.region r
where r.region_name not in (select sq.ship_region from nw.orders sq where sq.ship_region is not null)
;

-- not exists의 경우 null 값을 제외하려면 서브쿼리가 아닌 메인쿼리 영역에서 제외
select r.*
from nw.region r
where not exists(select sq.ship_region from nw.orders sq where sq.ship_region = r.region_name)
--and r.region_name is not null -- null 값을 제외할 경우 (서브쿼리가 아닌 where절에 사용 해야 함)
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	스칼라 서브쿼리 이해
*********************************************************************************/

-- 직원의 부서명을 스칼라 서브쿼리로 추출
select e.*
	 , (select sq.dname from hr.dept sq where sq.deptno = e.deptno) as dname
from hr.emp e
;

-- 아래는 수행 오류 발생. 스칼라 서브쿼리는 단 한개의 결과 값만 반환해야 함
select d.*
	 , (select sq.ename from hr.emp sq where sq.deptno = d.deptno) as ename
from hr.dept d
;

-- 아래는 수행 오류 발생. 스칼라 서브쿼리는 단 한개의 열값만 반환해야 함
select e.*
	 , (select sq.dname, sq.deptno from hr.dept sq where sq.deptno = e.deptno) as dname
from hr.emp e
;

-- case when 절에서 스칼라 서브쿼리 사용
select e.*
	 , (case
			when e.deptno = 10 then (select sq.dname from hr.dept sq where sq.deptno = 20)
			else (select sq.dname from hr.dept sq where sq.deptno = e.deptno)
		end
	) as dname
from hr.emp e
;

-- 스칼라 서브쿼리는 일반 select와 동일하게 사용. group by 적용 무방
select e.*
	 , (select avg(sq.sal) from hr.emp sq where sq.deptno = e.deptno) as dept_avg_sal
from hr.emp e
;

-- 조인으로 변경
select e.*, t.avg_sal
from hr.emp e
	inner join (select sq.deptno, avg(sq.sal) as avg_sal from hr.emp sq group by sq.deptno) t on e.deptno = t.deptno
;

------------------------------------------------------------------------------------------------------------------------
/*********************************************************************************
	스칼라 서브쿼리 실습
*********************************************************************************/

-- 직원 정보와 해당 직원을 관리하는 매니저의 이름 추출
select e.*
	 , (select sq.ename from hr.emp sq where sq.empno = e.mgr) as mgr_name
from hr.emp e
;

select e.*, e2.ename as mgr_name
from hr.emp e
	left outer join hr.emp e2 on e.mgr = e2.empno
;

-- 주문정보와 ship_country가 France이면 주문 고객명을, 아니면 직원명을 new_name으로 출력
select o.order_id
	 , o.customer_id
	 , o.employee_id
	 , o.order_date
	 , o.ship_country
	 , (select sq.contact_name from nw.customers sq where sq.customer_id = o.customer_id) as customer_name
	 , (select sq.first_name || ' ' || sq.last_name
		from nw.employees sq
		where sq.employee_id = o.employee_id) as employee_name
	 , case
		   when o.ship_country = 'France' then
			   (select sq.contact_name from nw.customers sq where sq.customer_id = o.customer_id)
		   else (select sq.first_name || ' ' || sq.last_name from nw.employees sq where sq.employee_id = o.employee_id)
	   end as new_name
from nw.orders o
;

-- 조인으로 변경
select o.order_id
	 , o.customer_id
	 , o.employee_id
	 , o.order_date
	 , o.ship_country
	 , c.contact_name
	 , e.first_name || ' ' || e.last_name
	 , case
		   when o.ship_country = 'France' then c.contact_name
		   else e.first_name || ' ' || e.last_name
	   end as new_name
from nw.orders o
	left outer join nw.customers c on o.customer_id = c.customer_id
	left outer join nw.employees e on o.employee_id = e.employee_id
;

-- 고객정보와 고객이 처음 주문한 일자의 주문 일자 추출
select c.customer_id
	 , c.contact_name
	 , (select min(sq.order_date) from nw.orders sq where sq.customer_id = c.customer_id) as first_order_date
from nw.customers c
;

-- @formatter:off
-- 조인으로 변경
select c.customer_id, c.contact_name, t.first_order_date
from nw.customers c
	left outer join (
		select o.customer_id, min(o.order_date) as first_order_date
		from nw.orders o
		group by o.customer_id
	) t on c.customer_id = t.customer_id
;
-- @formatter:on

-- @formatter:off
-- 고객정보와 고객이 처음 주문한 일자의 주문 일자와 그때의 배송 주소, 배송 일자 추출
select c.customer_id
	, c.contact_name
	, (select min(sq.order_date) from nw.orders sq where sq.customer_id = c.customer_id) as first_order_date
	, (
		select o.ship_address
		from nw.orders o
		where o.customer_id = c.customer_id and o.order_date = (
			select min(sq.order_date)
			from nw.orders sq
			where sq.customer_id = o.customer_id
		)
	) as first_ship_address
	, (
		select o.shipped_date
		from nw.orders o
		where o.customer_id = c.customer_id and o.order_date = (
			select min(sq.order_date)
			from nw.orders sq
			where sq.customer_id = o.customer_id
		)
	) as first_shipped_date
from nw.customers c
order by c.customer_id
-- @formatter:on
;

-- @formatter:off
-- 조인으로 변경.
select c.customer_id
	 , c.contact_name
	 , o.order_date
	 , o.ship_address
	 , o.shipped_date
from nw.customers c
	left outer join nw.orders o on c.customer_id = o.customer_id
		and o.order_date = (
			select min(sq.order_date)
			from nw.orders sq
			where sq.customer_id = o.customer_id
		)
order by c.customer_id
-- @formatter:on
;

-- @formatter:off
-- 고객정보와 고객이 마지막 주문한 일자의 주문 일자와 그때의 배송 주소, 배송 일자 추출
-- 현재 데이터가 고객이 하루에 주문을 두번한 경우가 있음. max(order_date) 시 고객이 하루에 주문을 두번한 일자가 나오고 있음
-- 때문에 반드시 1개의 값만 스칼라 서브쿼리에서 반환하도록 limit 1 추가
select c.customer_id
	, c.contact_name
	, (select max(sq.order_date) from nw.orders sq where sq.customer_id = c.customer_id) as last_order_date
	, (
		select o.ship_address
		from nw.orders o
		where o.customer_id = c.customer_id and o.order_date = (
			select max(sq.order_date)
			from nw.orders sq
			where sq.customer_id = o.customer_id
		)
		limit 1
	) as last_ship_address
	, (
		select o.shipped_date
		from nw.orders o
		where o.customer_id = c.customer_id and o.order_date = (
			select max(sq.order_date)
			from nw.orders sq
			where sq.customer_id = o.customer_id
		)
		limit 1
	) as last_shipped_date
from nw.customers c
order by c.customer_id
-- @formatter:on
;

-- @formatter:off
-- 조인으로 변경
select c.customer_id
	 , c.contact_name
	 , o.order_date
	 , o.ship_address
	 , o.shipped_date
	 , row_number() over (partition by c.customer_id order by o.order_date desc) as rnum
from nw.customers c
	left outer join nw.orders o on c.customer_id = o.customer_id
	    and o.order_date = (
	    	select max(sq.order_date)
			from nw.orders sq
			where c.customer_id = sq.customer_id
		)
where c.customer_id = 'ALFKI'
-- limit 1
-- @formatter:on
;
