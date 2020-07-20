-- WHERE 절 가이드
-- 1. WHERE 절의 컬럼은 변형하지 않음
-- : 원래 값을 변형하면 인덱스를 효율적으로 사용할 수 없음

-- 17년 3월달의 고객ID별 주문 건수 구하기
-- WHERE 절의 컬럼을 변형
-- INDEX FAST FULL SCAN은 인덱스 리프 블록을 모두 읽어서 필요한 데이터를 찾아내는 방식
/*
-------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation             | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
-------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT      |               |      1 |        |     60 |00:00:05.72 |     123K|    123K|       |       |          |
|   1 |  HASH GROUP BY        |               |      1 |     90 |     60 |00:00:05.72 |     123K|    123K|  1394K|  1394K| 1395K (0)|
|*  2 |   INDEX FAST FULL SCAN| X_T_ORD_BIG_3 |      1 |    304K|   1850K|00:00:01.56 |     123K|    123K|       |       |          |
-------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.CUS_ID
	, COUNT(*) AS ORD_CNT
FROM T_ORD_BIG TOB
WHERE SUBSTR(TOB.ORD_YMD, 1, 6) = '201703'
GROUP BY TOB.CUS_ID
;

-- LIKE 조건을 사용
/*
------------------------------------------------------------------------------------------------------------------------
| Id  | Operation         | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT  |               |      1 |        |     60 |00:00:00.37 |    7494 |       |       |          |
|   1 |  HASH GROUP BY    |               |      1 |     90 |     60 |00:00:00.37 |    7494 |  1394K|  1394K| 1394K (0)|
|*  2 |   INDEX RANGE SCAN| X_T_ORD_BIG_3 |      1 |   1707K|   1850K|00:00:00.30 |    7494 |       |       |          |
------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.CUS_ID
	, COUNT(*) AS ORD_CNT
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD LIKE '201703%'
GROUP BY TOB.CUS_ID
;

-- WHERE 조건절에서 사용해면 안되는 패턴 두가지
-- 컬럼을 결합해 조건처리
SELECT *
FROM T_ORD_BIG TOB
WHERE TOB.ORD_ST || TOB.PAY_TP = 'COMP' || 'BANK'
;

-- 컬럼을 소문자로 변경해서 조건 처리
SELECT *
FROM T_ORD_BIG TOB
WHERE LOWER(TOB.CUS_ID) = 'cus_0022'
;

-- 2. 날짜 조건 처리하기
-- T_ORD_BIG 에는 주문일시(ORD_DT)와 주문일자(ORD_YMD) 컬럼이 동시에 존재, 주문일시는 DATE 자료형, 주문일자는 8자리 문자 자료형

-- 2-1. 바른 사용법, 문자열 자료형 컬럼 vs. 문자열 자료형 조건 값
-- ORD_YMD가 20170313D인 데이터 조회하기
-- WHERE 조건절의 ORD_YMD는 문자열 자료형이고 이에 맞게 조건 값도 문자열을 사용
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.PAY_TP
	, COUNT(*) AS CNT
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD = '20170313'
GROUP BY TOB.PAY_TP
;

-- 2-2. 잘못된 사용법, 문자열 자료형 컬럼 vs. DATE 자료형 조건 값
-- ORD_YMD가 20170313D인 데이터 조회하기, 날짜형 변수 사용
-- 실행계획을 보면 FULL SCAN하고 있으며, Predicate Information에서는 ORD_YMD 컬럼에 대해 INTERNAL_FUNCTION 처리하고 있음
-- 테이블의 ORD_YMD를 DATE 자료형으로 모두 자동 변환 한 것
-- 오라클은 문자열 자로형과 DATE 자로형 간에 비교가 발생하면 문자열 자료형을 DATE으로 자동 변환 함
-- 자동 형 변환할 경우도 마찬가지로 인덱스를 제대로 사용할 수 없음
/*
------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |           |      1 |        |      2 |00:00:21.09 |     258K|    258K|       |       |          |
|   1 |  HASH GROUP BY     |           |      1 |      2 |      2 |00:00:21.09 |     258K|    258K|  1520K|  1520K|  525K (0)|
|*  2 |   TABLE ACCESS FULL| T_ORD_BIG |      1 |  87307 |  60000 |00:01:58.04 |     258K|    258K|       |       |          |
------------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   2 - filter(INTERNAL_FUNCTION(""TOB"".""ORD_YMD"")=TO_DATE(' 2017-03-13 00:00:00', 'syyyy-mm-dd hh24:mi:ss'))"
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.PAY_TP
 	, COUNT(*) AS CNT
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD = TO_DATE('20170313', 'YYYYMMDD')
GROUP BY TOB.PAY_TP
;

-- 2-3. 바른사용법, DATE 자료형 컬럼 vs. 문자열 자로형 조건 값
-- ORD_DT에 대한 인덱스 생성
CREATE INDEX X_T_ORD_BIG_ORD_DT ON T_ORD_BIG(ORD_DT)
;

-- ORD_DT가 20170313인 데이터 조회하기
-- 조건 값이 문자열 자로형 이므로 조건 값이 DATE자로형으로 변경 됨
-- 테이블의 컬림인 ORD_DT가 변형된 것이 아니므로 ORD_DT에 대한 인덱스를 사용할 수 있음
/*
------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name               | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |                    |      1 |        |      2 |00:00:00.04 |   20995 |       |       |          |
|   1 |  HASH GROUP BY                       |                    |      1 |      2 |      2 |00:00:00.04 |   20995 |  1520K|  1520K|  524K (0)|
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG          |      1 |  87307 |  60000 |00:00:00.04 |   20995 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_ORD_DT |      1 |  87307 |  60000 |00:00:00.01 |     162 |       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.PAY_TP
	, COUNT(*) AS CNT
FROM T_ORD_BIG TOB
WHERE TOB.ORD_DT = '20170313'
GROUP BY TOB.PAY_TP
;

-- 2-4. 잘못된 사용법, DATE 자료형 컬럼을 문자열로 변환 vs. 문자열 자로형 조건 값
-- ORD_DT가 20170313인 데이터 조회하기, ORD_DT 컬럼을 변형
-- 테이블의 컬럼을 변형했으므로 ORD_DT에 대한 인덱스를 사용할 수 없음
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.PAY_TP
	 , COUNT(*) AS CNT
FROM T_ORD_BIG TOB
WHERE TO_CHAR(TOB.ORD_DT, 'YYYYMMDD') = '20170313'
GROUP BY TOB.PAY_TP
;

-- 올바른 사용법
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.PAY_TP
	, COUNT(*) AS CNT
FROM T_ORD_BIG TOB
WHERE TOB.ORD_DT = TO_DATE('20170313', 'YYYYMMDD')
GROUP BY TOB.PAY_TP
;

-- 2-5. DATE 자로형 컬럼에 범위 조건 처리
-- DATE 자료형에 시분초가 입력되어 있는 경우 같다(=) 조건이 아닌 범위 조건을 사용해야 함
SELECT *
FROM (
	SELECT 1 AS ORD_NO, TO_DATE('20170313 00:00:00', 'YYYYMMDD HH24:MI:SS') AS ORD_DT FROM DUAL
    UNION ALL
	SELECT 2 AS ORD_NO, TO_DATE('20170313 02:00:00', 'YYYYMMDD HH24:MI:SS') AS ORD_DT FROM DUAL
	UNION ALL
	SELECT 3 AS ORD_NO, TO_DATE('20170313 23:59:59', 'YYYYMMDD HH24:MI:SS') AS ORD_DT FROM DUAL
	UNION ALL
	SELECT 4 AS ORD_NO, TO_DATE('20170314 02:00:00', 'YYYYMMDD HH24:MI:SS') AS ORD_DT FROM DUAL
) T
WHERE T.ORD_DT >= TO_DATE('20170313', 'YYYYMMDD')
AND T.ORD_DT < TO_DATE('20170313', 'YYYYMMDD') + 1
;

-- 3. 조건값은 컬럼과 같은 자로형을 사용함
-- ORD_YMD 컬럼에 숫자형 변수를 사용
-- Predicate Information을 보면 ORD_YMD 컬럼을 TO_NUMBER로 변환하고 있음
-- 오라클은 문자와 숫자를 비교하면 숫자로 변형, 문자와 DATE를 비교하면 문자를 DATE로 변환함
-- 그러므로 조건 값 쪽을 무조건 문자형으로만 처리하면 테이블의 커럼이 자동 형 변환되는 경우는 없음
-- 하지만 가능하면 테이블의 원래 컬럼과 같은 자료형을 사용하는 습관을 갖는 것이 좋음
/*
------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |           |      1 |        |      2 |00:00:05.57 |     258K|    258K|       |       |          |
|   1 |  HASH GROUP BY     |           |      1 |      2 |      2 |00:00:05.57 |     258K|    258K|  1520K|  1520K|  522K (0)|
|*  2 |   TABLE ACCESS FULL| T_ORD_BIG |      1 |  71876 |  60000 |00:00:30.78 |     258K|    258K|       |       |          |
------------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   2 - filter(TO_NUMBER(""TOB"".""ORD_YMD"")=20170313)"
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.PAY_TP
	, COUNT(*) AS CNT
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD = 20170313
GROUP BY TOB.PAY_TP
;

-- 4. NOT IN 보다는 IN을 사용함(긍정형 조건을 사용함)
-- NOT IN 뿐만 아니라 같지않다(!=)와 같은 부정형 조건은 피하는 것이 좋음
-- IN 조건으로 SQL를 개발해 놓는 것이 좋음, 옵티마지어가 INDEX RANGE SCAN을 사용할 수 있는 가능성을 만둘어 주기 때문
-- INDEX RANGE SCAN이 효율적인지는 옵티마이져가 스스포 판단할 것임
-- 부정형 조건을 긍정형 조건으로 변경한다고 무조건 인덱스를 효율적으로 사용하는 것은 아님
-- 인덱스를 사용한다고 무조건 성능이 좋은 것도 아님
-- 하지만 긍졍형 조건은 인덱스를 효츌적으로 활용할 가능성을 열어 줌, 되도록 긍정형 조건으로 SQL을 작성하는 것이 좋음

-- 5. 불필요한 LIKE는 제거
-- 오라클의 옵티마이져는 LIKE 조건과 같다(=) 조건을 다르게 생각함
-- 같다(=) 조건이라면 LIKE 조건보다 인덱스를 사용할 가능성이 더 큼
-- 꼭 필요한 경우가 아니면 LIKE 보다는 같다(=) 조건을 사용하는 것이 좋음

-- 버퍼 캐시 지우기
ALTER SYSTEM FLUSH BUFFER_CACHE
;

-- 실제 실행계획 조회하기
SELECT
	V$S.SQL_ID
	, V$S.CHILD_NUMBER
	, V$S.SQL_TEXT
FROM V$SQL V$S
WHERE V$S.SQL_TEXT LIKE '%GATHER_PLAN_STATISTICS%'
ORDER BY V$S.LAST_ACTIVE_TIME DESC
;

SELECT *
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('4vpt8fkhh2y4z', 0, 'ALLSTATS LAST'))
;

