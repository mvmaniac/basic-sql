-- 복합 인덱스
-- 하나의 복합 인덱스로 여러 개의 인덱스를 대신 할 수 있음
-- 인덱스가 많아질수록 입력, 수정, 삭제에서는 성능 감소가 발생함
-- 데이터 변경 발생할 때 마다 인덱스 역시 변경을 해주어야 하기 때문

-- CUS_ID 에 대한 단일 인덱스 제거
-- DROP INDEX X_T_ORD_BIG_3;

-- 2개의 조건이 사용된 SQL, ORD_YMD 인덱스를 사용
/*
----------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      1 |00:00:06.60 |     349K|  39226 |       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |      2 |      1 |00:00:06.60 |     349K|  39226 |  1186K|  1186K|  524K (0)|
|*  2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |   2081 |  30000 |00:00:02.57 |     349K|  39226 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_1 |      1 |    187K|   1850K|00:00:00.42 |    5156 |   5156 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS INDEX(TOB X_T_ORD_BIG_1) */
	TOB.ORD_ST, SUM(TOB.ORD_AMT)
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD LIKE '201703%'
AND TOB.CUS_ID = 'CUS_0075'
GROUP BY TOB.ORD_ST
;

-- ORD_YMD, CUS_ID 순으로 복합 인덱스를 생성
CREATE INDEX X_T_ORD_BIG_3 ON T_ORD_BIG(ORD_YMD, CUS_ID)
;

-- ORD_YMD, CUS_ID 복합 인덱스를 사용하도록 SQL을 수행
/*
----------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      1 |00:00:01.25 |   37494 |  12313 |       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |      2 |      1 |00:00:01.25 |   37494 |  12313 |  1186K|  1186K|  524K (0)|
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |   2081 |  30000 |00:00:00.10 |   37494 |  12313 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_3 |      1 |   2081 |  30000 |00:00:00.06 |    7494 |   7493 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS INDEX(TOB X_T_ORD_BIG_3) */
	TOB.ORD_ST, SUM(TOB.ORD_AMT)
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD LIKE '201703%'
AND TOB.CUS_ID = 'CUS_0075'
GROUP BY TOB.ORD_ST
;

-- 단일 인덱스를 사용한 경우, ORD_YND 조건은 인덱스를 이용해 해결 되었지만, CUS_ID 조건은 테이블에 방문해야만 해결 가능
-- 복합 인덱스의 경우에는 ORD_YMD와 CUS_ID에 대한 조건을 모두 인덱스 안에서 해결 함
-- 인덱스를 설계 할 때 중요하게 고려할 부분이 바로 테이블 접근(TABLE ACCESS BY INDEX ROWID)을 줄이는 것
-- WHERE 조건절의 모든 컬럼을 복합 인덱스로 구성하면 테이블 접근을 최소화 할 수 있지만
-- 복합 인덱스에 너무 많은 컬럼을 사용하면 데이터의 입력, 수정, 삭제에서 성능 저하가 나타남
-- 그러므로 적절한 컬럼 수로 복합 인덱스를 구성 해야 함

-- 복합 인덱스, 컬럼선정과 순서
-- A, B, C 컬럼 순서의 복합 인덱스와 C, B, A 컬럼의 복합 인덱스는 완전히 다른 인덱스
-- 같다(=) 조건이 사용된 컬럼을 복합 인덱스의 앞 부분에 두는 것이 기본 원칙
-- 두 번째 컬럼의 조건이 범위 조건이여도 상관 없음
-- 다만 무조건 맞는 공식이 아니며, 범위 조건 컬럼이 앞 부분에 두어야 상책인 경우도 있음
-- 인덱스에 따라 IO가 어떻게 변경되는지 추척해보면서 최적의 인덱스를 찾는 연습을 해야함

-- CUS_ID, ORD_YMD로 구성된 인덱스
CREATE INDEX X_T_ORD_BIG_4 ON T_ORD_BIG(CUS_ID, ORD_YMD)
;

-- CUS_ID, ORD_YMD 인덱스를 사용하는 SQL
/*
 ----------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      1 |00:00:01.72 |   30125 |   8706 |       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |      2 |      1 |00:00:01.72 |   30125 |   8706 |  1186K|  1186K|  485K (0)|
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |   2081 |  30000 |00:00:00.08 |   30125 |   8706 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_4 |      1 |   2081 |  30000 |00:00:00.01 |     125 |    124 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS INDEX(TOB X_T_ORD_BIG_4) */
	TOB.ORD_ST, SUM(TOB.ORD_AMT)
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD LIKE '201703%'
AND TOB.CUS_ID = 'CUS_0075'
GROUP BY TOB.ORD_ST
;

-- 많은 조건이 걸리는 SQL
SELECT COUNT(*)
FROM T_ORD_BIG TOB
WHERE TOB.ORD_AMT = 2400
AND TOB.PAY_TP = 'CARD'
AND TOB.ORD_YMD = '20170406'
AND TOB.ORD_ST = 'COMP'
AND TOB.CUS_ID = 'CUS_0036'
;

-- 각 조건 별로 카운트 해보기
-- ORD_YMD 조건이 90,000 건의 데이터를 찾아내고, CUS_ID 조건은 330,000건의 데이터를 찾아냄
-- ORD_YMD, CUS_ID로 복합 인덱스를 구헝하면 충분한 성능이 나옴
SELECT 'ORD_AMT' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.ORD_AMT = 2400
UNION ALL
SELECT 'PAY_TP' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.PAY_TP = 'CARD'
UNION ALL
SELECT 'ORD_YMD' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.ORD_YMD = '20170406'
UNION ALL
SELECT 'ORD_ST' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.ORD_ST = 'COMP'
UNION ALL
SELECT 'CUS_ID' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.CUS_ID = 'CUS_0036'
;

-- ORD_YMD, CUS_ID 인덱스를 사용하는 SQL
/*
----------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers |
----------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      1 |00:00:00.01 |   10045 |
|   1 |  SORT AGGREGATE                      |               |      1 |      1 |      1 |00:00:00.01 |   10045 |
|*  2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |      4 |  10000 |00:00:00.02 |   10045 |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_3 |      1 |   1449 |  10000 |00:00:00.01 |      45 |
----------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS INDEX(TOB X_T_ORD_BIG_3) */
	COUNT(*)
FROM T_ORD_BIG TOB
WHERE TOB.ORD_AMT = 2400
AND TOB.PAY_TP = 'CARD'
AND TOB.ORD_YMD = '20170406'
AND TOB.ORD_ST = 'COMP'
AND TOB.CUS_ID = 'CUS_0036'
;

-- Predicate Information, access
-- CUS_0075의 201703 주문을 조회하는 SQL
-- Predicate Information 의 access 부분을 보면 CUS_ID 조건만 사용됨
-- 인덱스를 제대로 탔다면 ORD_YMD에 대한 조건도 access에 표시 되어야 함
/*
 ----------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      1 |00:00:04.47 |   31381 |  12896 |       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |      2 |      1 |00:00:04.47 |   31381 |  12896 |  1520K|  1520K|  513K (0)|
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |   3400 |  30000 |00:00:13.59 |   31381 |  12896 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_4 |      1 |   3400 |  30000 |00:00:00.05 |    1381 |   1290 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   3 - access(""TOB"".""CUS_ID""='CUS_0075')"
"       filter(SUBSTR(""TOB"".""ORD_YMD"",1,6)='201703')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.ORD_ST, COUNT(*)
FROM T_ORD_BIG TOB
WHERE SUBSTR(TOB.ORD_YMD, 1, 6) = '201703'
AND TOB.CUS_ID = 'CUS_0075'
GROUP BY TOB.ORD_ST
;

-- CUS_0075의 201703 주문을 조회하는 SQL, LIKE로 처리
-- CUS_ID와 ORD_YMD를 동시에 access 하고 있음
/*
-------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
-------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      1 |00:00:00.04 |   30125 |       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |      2 |      1 |00:00:00.04 |   30125 |  1520K|  1520K|  524K (0)|
|   2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |  19059 |  30000 |00:00:00.04 |   30125 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_4 |      1 |  19059 |  30000 |00:00:00.01 |     125 |       |       |          |
-------------------------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   3 - access(""TOB"".""CUS_ID""='CUS_0075' AND ""TOB"".""ORD_YMD"" LIKE '201703%')"
"       filter(""TOB"".""ORD_YMD"" LIKE '201703%')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	TOB.ORD_ST, COUNT(*)
FROM T_ORD_BIG TOB
WHERE TOB.ORD_YMD LIKE '201703%'
AND TOB.CUS_ID = 'CUS_0075'
GROUP BY TOB.ORD_ST
;

-- 테이블 및 인덱스 크기 확인
SELECT
	DS.SEGMENT_NAME
	, DS.SEGMENT_TYPE
	, DS.BYTES / 1024 / 1024 AS SIZE_MB
	, DS.BYTES / T.CNT AS BYTE_PER_ROW
FROM DBA_SEGMENTS DS
, (SELECT COUNT(*) AS CNT FROM ORA_TEST_USER.T_ORD_BIG) T
WHERE DS.SEGMENT_NAME LIKE '%ORD_BIG%'
ORDER BY DS.SEGMENT_NAME
;

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
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('fvz80rc36p7tm', 0, 'ALLSTATS LAST'))
;