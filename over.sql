-- OVER 절
-- COUNT, SUM, MIN, MAX와 같은 분석 함수와 집계 함수에 동시에 존재하는 함수는 OVER 절에 있으면 분석함수 그렇지 않으면 집계함수 임
-- 분석함수의 분석 대상을 정하는 역할을 함
-- 대부분의 분석함수는 OVER절과 같이 사용
-- OVER()와 같이 괄호 안에 아무런 옵션을 주지 않으면 조회된 결과 전체가 분석 대상임

-- OVER절 이해하기
-- 조회된 주문 건수를 마지막 컬럼에 추가하는 SQL, 에러발생
SELECT
	O.ORD_SEQ
	, O.CUS_ID
	, O.ORD_DT
	, COUNT(*) AS ALL_CNT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170302', 'YYYYMMDD')
;

-- 조회된 주문 건수를 마지막 컬럼에 추가하는 SQL, 분석함수 사용
-- 첫 번째 로우의 분석 대상은 분석함수를 사용하기 전의 조회된 결과가 분석 대상임
-- 마지막 로우의 분석 대상 역시 첫 번쨰 로우 분석 대상과 같음
-- OVER의 괄호 안에 별다른 옵션을 주지 않으면 조회가 완료된 결과 전체가 분석 대상임
SELECT
	O.ORD_SEQ
	, O.CUS_ID
	, O.ORD_DT
	, COUNT(*) OVER() AS ALL_CNT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170302', 'YYYYMMDD')
;

-- 분석 대상
-- GROUP BY가 없는 SQL
SELECT
	O.ORD_SEQ
	, O.CUS_ID
	, COUNT(*) OVER() AS ALL_CNT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170101', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170201', 'YYYYMMDD')
ORDER BY O.ORD_SEQ
;

-- GROUP BY가 있는 SQL
-- GROUP BY까지 처리된 결과가 분석 대상임
SELECT
	O.CUS_ID
	, COUNT(*) OVER() AS ALL_CNT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170101', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170201', 'YYYYMMDD')
GROUP BY O.CUS_ID
;

-- 분석함수와 GROUP BY 동시 사용
-- 안쪽의 SUM(O.ORD_AMT)는 CUS_ID 별로 주문금액을 집계한 집계함수고 바깥쪽의 SUM() OVER()는 분석함수 임
-- 분석함수는 집계함수가 처리된 SUM(O.ORD_AMT)에 대해서 분석을 수행
-- 분석함수와 GROUP BY가 동시에 사용될 때는 GROUP BY에 명시된 컬럼이나 SUM(O.ORD_AMT) 처럼 집계함수를 사용한 결과만 분석함수로 분석 할 수 있음
SELECT
	O.CUS_ID
	, SUM(SUM(O.ORD_AMT)) OVER() AS ALL_CNT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170101', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170201', 'YYYYMMDD')
GROUP BY O.CUS_ID
;

-- 분석함수와 집계함수의 차이
-- COUNT(*) AS BY_CUS_ORD_CNT: 고객별 주문건수, GROUP BY의 CUS_ID별 집계를 수행하는 집계함수
-- COUNT(*) OVER() AS ALL_CUST_CNT: 조회된 고객 수, 분석 대상의 데이터 건수를 세는 분석함수
-- SUM(COUNT(*)) OVER() AS BY_CUS_ORD_CNT
-- : 고객별 주문건수에 대한 합
-- : 안쪽의 COUNT(*)는 CUS_ID별 집계를 수행하는 집계함수
-- : 바깥쪽의 SUM() OVER()는 CUS_ID별 COUNT(*)에 대한 분석 함수
SELECT
	O.CUS_ID
 	, COUNT(*) AS BY_CUS_ORD_CNT
	, COUNT(*) OVER() AS ALL_CUST_CNT
	, SUM(COUNT(*)) OVER() AS ALL_ORD_CNT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170101', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170201', 'YYYYMMDD')
GROUP BY O.CUS_ID
;

-- OVER PARTITION BY
-- CUS_ID별로 PARTITION BY를 사용
-- 각 로우 별로 자신의 CUS_ID와 같은 CUS_ID를 가진 로우가 분석 대상임
-- PARTITION BY에 정의된 컬럼 값에 따라 칸막이를 만들어서 분석함
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID) AS BY_CUST_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 다양하게 PARTITION BY를 사용
-- OVER(PARTITION BY O.CUS_ID): 자신의 로우와 CUS_ID가 같은 로우들을 분석대상으로 지정
-- OVER(PARTITION BY O.ORD_ST): 자신의 로우와 ORD_ST가 같은 로우들을 분석대상으로 지정
-- OVER(PARTITION BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM'): 자신의 로우와 CUS_ID, 주문년월이 같은 로우들을 분석대상으로 지정
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
    , O.ORD_ST
	, SUM(O.ORD_AMT) AS ORD_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID) AS BY_CUST_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.ORD_ST) AS BY_ORD_ST_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')) AS BY_CUST_YM_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM'), O.ORD_ST
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM'), O.ORD_ST
;

-- ROLLUP과 PARTITION BY를 비교
-- ROLLUP
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY ROLLUP(O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM'))
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- PARTITION BY
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID) AS BY_CUST_AMT
	, SUM(SUM(O.ORD_AMT)) OVER() AS ALL_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 고객별로 주문금액 비율 구하기, PARTITION BY를 사용
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, ROUND(SUM(O.ORD_AMT) / (SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID)) * 100.00, 2) AS ORD_AMT_RT_BY_CUST
	, ROUND(SUM(O.ORD_AMT) / (SUM(SUM(O.ORD_AMT)) OVER()) * 100.00, 2) AS ORD_AMT_RT_BY_ALL_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- OVER ORDER BY
-- 각 로우별로 ORDER BY에 따라 분석 대상이 다르게 정해짐
-- OVER 절 안에 ORDER BY가 있으면 ORDER BY 기준으로 자신보다 먼저 조회된 데이터가 분석대상이 됨
-- 특정 고객의 3월부터 8월까지의 6개월 간의 주문 조회, 월별 누적주문금액을 같이 표시
-- 누적주문금액
-- 3월 누적주문금액은 3월 주문금액과 동일
-- 4월 누적주문금액은 3월과 4월 주문금액 합계
-- 8월 누적주문금액은 3~8월의 주문금액 합계
SELECT
	TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')) AS ORD_YM_SUM
FROM T_ORD O
WHERE O.CUS_ID = 'CUS_0002'
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- ORDER BY, PARTITION BY와 동시 사용 가능
-- 분석대상을 PARTITION BY로 나눈 후 나누어진 단위별로 ORDER BY를 처리함
-- PARTITION BY가 ORDER BY보다 먼저 와야 함
-- PARTITION BY에 대상 컬럼을 콤마로 구분해서 적은 후 파티션의 마지막 컬럼과 ORDER BY 사이에 콤마를 사용하면 안됨
-- OVER(PARTITION BY O.CUS_ID, O.ORD_ST, O.PAY_TP ORDER BY O.ORD_AMT)
SELECT
    O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
 	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID) AS BY_CUST_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')) AS BY_CUS_ORD_YM_SUM
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- RANK, DENSE_RANK 분석함수
-- 순위를 구할 수 있는 분석함수
-- OVER 절 안에 ORDER BY를 필수적으로 사용
SELECT
	O.CUS_ID
	, SUM(O.ORD_AMT) AS ORD_AMT
	, RANK() OVER(ORDER BY SUM(O.ORD_AMT) DESC) AS RNK
FROM T_ORD O
GROUP BY O.CUS_ID
;

-- RANK와 DENSE_RANK의 비교
-- 동률의 순위를 가진 데이터를 처리하는 방법이 다름
-- RANK는 AMT가 같은 B와 C를 모두 2위로 순위를 부여한 후에 다음 순위는 4위로 처리, 동률이 두명이므로 다음 순위는 하나를 건너뜀
-- DENSE_RANK는 동률이 있어도 다음 순위를 연속해서 부여함
SELECT
	T.ID
	, T.AMT
	, RANK() OVER(ORDER BY T.AMT DESC) AS RANK_RES
 	, DENSE_RANK() OVER(ORDER BY T.AMT DESC) AS DENSE_RANK_RES
FROM (
	SELECT 'A' AS ID, 300 AS AMT FROM DUAL
    UNION ALL
	SELECT 'B' AS ID, 150 AS AMT FROM DUAL
	UNION ALL
	SELECT 'C' AS ID, 150 AS AMT FROM DUAL
	UNION ALL
	SELECT 'D' AS ID, 100 AS AMT FROM DUAL
) T
;

-- ROW_NUMBER()를 이용한 순위 구하기
-- 조회결과에 줄 번호를 부여하는 분석함수
-- RANK와 DENSE_RANK와 유사하나 중복된 순위를 내보내지 않음
-- ROWNUM을 대체하는 기능은 아님, 대체로 ROWNUM이 ROW_NUMBER보다 성능 면에서 유리
SELECT
	T.ID
	, T.AMT
	, RANK() OVER(ORDER BY T.AMT DESC) AS RANK_RES
	, ROW_NUMBER() OVER(ORDER BY T.AMT DESC) AS ROW_NUM_RES
FROM (
	SELECT 'A' AS ID, 300 AS AMT FROM DUAL
	UNION ALL
	SELECT 'B' AS ID, 150 AS AMT FROM DUAL
	UNION ALL
	SELECT 'C' AS ID, 150 AS AMT FROM DUAL
	UNION ALL
	SELECT 'D' AS ID, 100 AS AMT FROM DUAL
) T
;

-- 3월, 4월 주문데 대해 월별로 주문금액 Top-3 고객 구하기
-- 품복별 Top-N이나 판매점별 Top-N을 구하기 위해 사용할 수 있는 패턴
SELECT
	T.ORD_YM
	, T.CUS_ID
	, T.ORD_AMT
	, T.BY_YM_RANK
FROM (
    SELECT
		TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
		, O.CUS_ID
		, SUM(O.ORD_AMT) AS ORD_AMT
 		, ROW_NUMBER() OVER(PARTITION BY TO_CHAR(O.ORD_DT, 'YYYYMM') ORDER BY SUM(O.ORD_AMT) DESC) AS BY_YM_RANK
    FROM T_ORD O
    WHERE O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
	AND O.ORD_DT < TO_DATE('20170501', 'YYYYMMDD')
    GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM'), O.CUS_ID
) T
WHERE T.BY_YM_RANK <= 3
ORDER BY T.ORD_YM, T.BY_YM_RANK
;

-- 고객별로 마지막 주문만 조회
-- 일회성으로 데이터를 추출해야 할 때 사용할만한 패턴
SELECT T.*
FROM (
	SELECT
		O.*
		, ROW_NUMBER() OVER(PARTITION BY O.CUS_ID ORDER BY O.ORD_DT DESC, O.ORD_SEQ) AS ORD_RANK
    FROM T_ORD O
) T
WHERE T.ORD_RANK = 1
;

-- LAG, LEAD
-- LAG는 자신의 이전 값
-- : LAG(컬럼명, offset) OVER([PARTITION BY ~] ORDER BY ~)
-- LEAD는 자신의 이후 값을 가져오는 분석함수
-- : LEAD(컬럼명, offset) OVER([PARTITION BY ~] ORDER BY ~)
-- offset: 현재 로우에서 몇 로우 이전 또는 로우 이후를 뜻함
-- OVER절에 ORDER BY를 사용해야 함
SELECT
	O.CUS_ID
	, SUM(O.ORD_AMT) AS ORD_AMT
	, ROW_NUMBER() OVER(ORDER BY SUM(O.ORD_AMT) DESC) AS RNK
	, LAG(O.CUS_ID, 1) OVER(ORDER BY SUM(O.ORD_AMT) DESC) AS LAG_1
 	, LEAD(O.CUS_ID, 1) OVER(ORDER BY SUM(O.ORD_AMT) DESC) AS LEAD_1
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0020', 'CUS_0021', 'CUS_0022', 'CUS_0023')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
GROUP BY O.CUS_ID
;

-- 주문년월 별 주문금액에, 전월 주문금액을 같이 표시, LAG를 활용
SELECT
	TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, LAG(SUM(O.ORD_AMT), 1) OVER(ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')) AS BF_YM_ORD_AMY
FROM T_ORD O
WHERE O.ORD_ST = 'COMP'
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 븐선함수 대신하기
-- 특정 고객의 주문년월별 주문금액, 특정 고객의 총 주문금액을 같이 표시
SELECT
	TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS YM_ORD_AMT
	, SUM(SUM(O.ORD_AMT)) OVER() AS TTL_ORD_AMT
FROM T_ORD O
WHERE O.CUS_ID = 'CUS_0002'
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 서브쿼리로 대신하기
SELECT
	TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS YM_ORD_AMT
	, (
		SELECT SUM(ORD_AMT)
	    FROM T_ORD
	    WHERE CUS_ID = 'CUS_0002'
	) AS TTL_ORD_AMT
FROM T_ORD O
WHERE O.CUS_ID = 'CUS_0002'
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 인라인-뷰로 대신하기
-- SUM이 아닌 MAX를 사용해야 함
-- GROUP BY 하기 전에 조인이 먼저 처리되므로, SUM을 사용하면 TTL_ORD_AMT가 몇 배로 부풀려짐
SELECT
	TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS YM_ORD_AMT
	, MAX(T.TTL_ORD_AMT) AS TTL_ORD_AMT
FROM T_ORD O
	, (
		SELECT SUM(ORD_AMT) AS TTL_ORD_AMT
		FROM T_ORD
		WHERE CUS_ID = 'CUS_0002'
	) T
WHERE O.CUS_ID = 'CUS_0002'
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- PARTITION BY를 대신하기
-- 고객별 총 주문금액, PARTITION BY 사용
-- PARTITION BY의 경우는 대체하는 SQL이 작성 양도 많고 복잡함, 가능하면 분석함수를 사용하는 것이 좋음
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, SUM(SUM(O.ORD_AMT)) OVER(PARTITION BY O.CUS_ID) AS BY_CUST_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 서브쿼리로 대신하기
-- 상관 서브쿼리로 성능에 있어서는 좋은 방법은 아님
SELECT
	O.CUS_ID
	, TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, (
		SELECT SUM(OS.ORD_AMT)
	    FROM T_ORD OS
	    WHERE OS.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
		AND OS.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
	    AND OS.CUS_ID = O.CUS_ID
	) AS BY_CUS_AMT
FROM T_ORD O
WHERE O.CUS_ID IN ('CUS_0002', 'CUS_0003')
AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
GROUP BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY O.CUS_ID, TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 인라인-뷰로 대신하기
SELECT
	T1.CUS_ID
	, T1.ORD_YM
	, T1.ORD_AMT
	, T2.BY_CUS_AMT
FROM
(
	SELECT
		T01.CUS_ID
		, TO_CHAR(T01.ORD_DT, 'YYYYMM') AS ORD_YM
		, SUM(T01.ORD_AMT) AS ORD_AMT
	FROM T_ORD T01
	WHERE T01.CUS_ID IN ('CUS_0002', 'CUS_0003')
	AND T01.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
	AND T01.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
	GROUP BY T01.CUS_ID, TO_CHAR(T01.ORD_DT, 'YYYYMM')
) T1,
(
	SELECT
		T02.CUS_ID
		, SUM(T02.ORD_AMT) AS BY_CUS_AMT
	FROM T_ORD T02
	WHERE T02.CUS_ID IN ('CUS_0002', 'CUS_0003')
	AND T02.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
	AND T02.ORD_DT < TO_DATE('20170601', 'YYYYMMDD')
	GROUP BY T02.CUS_ID
) T2
WHERE T1.CUS_ID = T2.CUS_ID
ORDER BY T1.CUS_ID, T1.ORD_YM
;

-- ROW_NUMBER를 대신하기
-- 주문년월별 주문금액 순위 구하기, ROW_NUMBER 사용
SELECT
	TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
	, SUM(O.ORD_AMT) AS ORD_AMT
	, ROW_NUMBER() OVER(ORDER BY SUM(O.ORD_AMT) DESC) AS ORD_AMT_RANK
FROM T_ORD O
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- ROWNUM으로 대신하기
-- 인라인-뷰 안에 ORDER BY를 꼭 명시해야 함
SELECT
	T.ORD_YM
	, T.ORD_AMT
	, ROWNUM AS ORD_AMT_RANK
FROM (
	SELECT
		TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
		, SUM(O.ORD_AMT) AS ORD_AMT
	FROM T_ORD O
	GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
	ORDER BY SUM(O.ORD_AMT) DESC
) T
ORDER BY T.ORD_YM
;

-- 서브쿼리로 대신하기
SELECT
	T.ORD_YM
	, T.ORD_AMT
	, (
		SELECT COUNT(*)
	    FROM (
	    	SELECT
				TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
				, SUM(O.ORD_AMT) AS ORD_AMT
	        FROM T_ORD O
	        GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
		) S
	    WHERE S.ORD_AMT >= T.ORD_AMT
	) AS ORD_AMT_RANK
FROM (
	SELECT
		TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
		, SUM(O.ORD_AMT) AS ORD_AMT
	FROM T_ORD O
	GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
	ORDER BY SUM(O.ORD_AMT) DESC
) T
ORDER BY T.ORD_YM
;

-- 인라인-뷰와 셀프-조인으로 대신하기
SELECT
	T0.ORD_YM
	, MAX(T0.ORD_AMT) AS ORD_AMT
	, COUNT(*) AS ORD_AMT_RANK
FROM
(
	SELECT
		TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
		, SUM(O.ORD_AMT) AS ORD_AMT
	FROM T_ORD O
	GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
) T0,
(
	SELECT
		TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
		, SUM(O.ORD_AMT) AS ORD_AMT
	FROM T_ORD O
	GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
) T1
WHERE T1.ORD_AMT >= T0.ORD_AMT
GROUP BY T0.ORD_YM
ORDER BY T0.ORD_YM
;