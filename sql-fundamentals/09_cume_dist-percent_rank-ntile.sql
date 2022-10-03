/*********************************************************************************
	cume_dist, percent_rank, ntile 실습
*********************************************************************************/

-- cume_dist는 percentile을 파티션내의 건수로 적용하고 0 ~ 1 사이 값으로 변환
-- 파티션내의 자신을 포함한 이전 로우수/ 파티션내의 로우 건수로 계산될 수 있음
select e.empno
	 , e.ename
	 , e.job
	 , e.sal
	 , rank() over (order by e.sal desc) as rank
	 , cume_dist() over (order by e.sal desc) as cume_dist
	 , cume_dist() over (order by e.sal desc) * 12.0 as xxtile
from hr.emp e
;

select oi.order_id
	 , rank() over (order by oi.amount desc) as rank
	 , cume_dist() over (order by oi.amount desc) as cume_dist
from nw.order_items oi
;

-- percent_rank는 rank를 0 ~ 1 사이 값으로 정규화 시킴
-- (파티션내의 rank() 값 - 1) / (파티션내의 로우 건수 - 1)
select e.empno
	 , e.ename
	 , e.job
	 , e.sal
	 , rank() over (order by e.sal desc) as rank
	 , percent_rank() over (order by e.sal desc) as percent_rank
	 , 1.0 * (rank() over (order by e.sal desc) - 1) / 11 as percent_rank_calc
from hr.emp e
;

-- ntile은 지정된 숫자만큼의 분위를 정하여 그룹핑하는데 사용
select e.empno
	 , e.ename
	 , e.job
	 , e.sal
	 , ntile(5) over (order by e.sal desc) as ntile
from hr.emp e
;

-- @formatter:off
-- 상품 매출 순위 상위 10%의 상품 및 매출액
with temp_01 as (
	select oi.product_id, sum(oi.amount) as sum_amount
	from nw.orders o
		inner join nw.order_items oi on o.order_id = oi.order_id
	group by oi.product_id
 )
select t.*
from (
	select t01.product_id
		, p.product_name
		, t01.sum_amount
		, cume_dist() over (order by t01.sum_amount) as percentile_norm
		, 1.0 * row_number() over (order by t01.sum_amount) / count(*) over () as rnum_norm
	from temp_01 t01
		inner join nw.products p on p.product_id = t01.product_id
) t
where t.percentile_norm >= 0.9
-- @formatter:on
;
