-- HASH JOIN (해시 조인)
-- 다른 방식보다 더 많은 CPU와 메모리 자원을 사용
-- 대용량 데이터를 조인할 때 적합, 머지 조인이 활용되는 경우는 많지 않음
-- 해시 조인 역시 선행 집합이 선택이 매우 중요
-- 선행 집합은 빌드 입력(Build-Input)으로 처리하며, 후행 집합은 검증 입력(Probe-Input)으로 처리됨
-- 빌드 입력은 조인할 대상에 해시 함수를 적용해 조인을 준비를 하는 과정
-- 검증 입력은 후행 집합에 해시 함수를 적용해 빌드 입력과 비교해 조인을 처리하는 과정
-- 빌드 입력의 데이터가 적으면 적을수록 성능에 유리, 빌드 입력이 메모리 영역인 해시 영역에 모두 위치해야만 최고의 성능을 낼 수 있음
-- 빌들 입력의 데이터가 너무 많아 해시 영역에 올릴 수 없으면, 임시 공간을 사용하게 되면 이로 인해 성능 저하가 발생함

-- 해시 조인 SQL
-- 1. 조인하려는 두 개의 테이블 중 고객 테이블을 선택해 읽어 들임
-- 2. 고객을 읽어 들이면서 조인 조건으로 사용된 컬럼(CUS_ID) 값에 해시 함수를 적용
-- 3. 해시 함수의 결괏값에 따라 데이터를 분류해 해시 영역(HASH AREA)에 올려 놓음
-- 4. 주문 테이블을 읽음
-- 5. 이때도 주문 테이블의 CUS_ID 값에 같은 해시 함수 처리를 함
-- 6. 해시 함수의 결괏값에 따라 해시 영역에 있는 3번의 결과와 조인을 수행
-- 7. 4 ~ 6번 과정을 반복 수행하면서 조인 결과를 만들어 내보냄
/*

--------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name  | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
--------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |       |      1 |        |    501 |00:00:00.01 |      56 |      7 |       |       |          |
|*  1 |  HASH JOIN         |       |      1 |   9141 |    501 |00:00:00.01 |      56 |      7 |  1376K|  1376K| 1570K (0)|
|   2 |   TABLE ACCESS FULL| M_CUS |      1 |     90 |     90 |00:00:00.01 |       7 |      4 |       |       |          |
|   3 |   TABLE ACCESS FULL| T_ORD |      1 |   3047 |    501 |00:00:00.01 |      11 |      1 |       |       |          |
--------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_HASH(O) */
	MC.RGN_ID
	 , MC.CUS_ID
	 , MC.CUS_NM
	 , O.ORD_DT
	 , O.ORD_ST
	 , O.ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD O ON O.CUS_ID = MC.CUS_ID
;

-- 대량의 데이터 처리
-- T_ORD_BIG 전체를 조인, 머지 조인으로 처리
/*
-------------------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  | Writes |  OMem |  1Mem | Used-Mem | Used-Tmp|
-------------------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |           |      1 |        |     90 |00:00:35.26 |     258K|    404K|    146K|       |       |          |         |
|   1 |  WINDOW BUFFER                 |           |      1 |     90 |     90 |00:00:35.26 |     258K|    404K|    146K|  6144 |  6144 | 6144  (0)|         |
|   2 |   SORT GROUP BY NOSORT         |           |      1 |     90 |     90 |00:00:34.74 |     258K|    404K|    146K|       |       |          |         |
|   3 |    MERGE JOIN                  |           |      1 |     91M|     30M|00:00:28.14 |     258K|    404K|    146K|       |       |          |         |
|   4 |     TABLE ACCESS BY INDEX ROWID| M_CUS     |      1 |     90 |     90 |00:00:00.01 |       3 |      0 |      0 |       |       |          |         |
|   5 |      INDEX FULL SCAN           | PK_M_CUS  |      1 |     90 |     90 |00:00:00.01 |       1 |      0 |      0 |       |       |          |         |
|*  6 |     SORT JOIN                  |           |     90 |     30M|     30M|00:00:24.92 |     258K|    404K|    146K|   643M|  8326K|   99M (1)|     575K|
|   7 |      TABLE ACCESS FULL         | T_ORD_BIG |      1 |     30M|     30M|00:00:04.45 |     258K|    258K|      0 |       |       |          |         |
-------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_MERGE(TOB) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
	 , SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
GROUP BY MC.CUS_ID
;

-- T_ORD_BIG 전체를 조인, 해시 조인으로 처리
/*
--------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation            | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
--------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT     |           |      1 |        |     90 |00:00:16.04 |     258K|    258K|       |       |          |
|   1 |  WINDOW BUFFER       |           |      1 |     90 |     90 |00:00:16.04 |     258K|    258K|  6144 |  6144 | 6144  (0)|
|   2 |   HASH GROUP BY      |           |      1 |     90 |     90 |00:00:16.03 |     258K|    258K|   779K|   779K| 2548K (0)|
|*  3 |    HASH JOIN         |           |      1 |     91M|     30M|00:00:04.38 |     258K|    258K|  1376K|  1376K| 1635K (0)|
|   4 |     TABLE ACCESS FULL| M_CUS     |      1 |     90 |     90 |00:00:00.01 |       7 |      0 |       |       |          |
|   5 |     TABLE ACCESS FULL| T_ORD_BIG |      1 |     30M|     30M|00:00:02.19 |     258K|    258K|       |       |          |
--------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_HASH(TOB) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
	 , SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
GROUP BY MC.CUS_ID
;

-- 빌드 입력의 중요성
-- T_ORD_BIG 전체를 조인, T_ORD_BIG을 선행 집합으로 처리
/*
---------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation            | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  | Writes |  OMem |  1Mem | Used-Mem | Used-Tmp|
---------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT     |           |      1 |        |     90 |00:00:25.48 |     258K|    336K|  78833 |       |       |          |         |
|   1 |  WINDOW BUFFER       |           |      1 |     90 |     90 |00:00:25.48 |     258K|    336K|  78833 |  6144 |  6144 | 6144  (0)|         |
|   2 |   HASH GROUP BY      |           |      1 |     90 |     90 |00:00:25.48 |     258K|    336K|  78833 |   779K|   779K| 2548K (0)|         |
|*  3 |    HASH JOIN         |           |      1 |     91M|     30M|00:00:11.07 |     258K|    336K|  78833 |  1618M|    36M|   44M (1)|     636K|
|   4 |     TABLE ACCESS FULL| T_ORD_BIG |      1 |     30M|     30M|00:00:01.61 |     258K|    258K|      0 |       |       |          |         |
|   5 |     TABLE ACCESS FULL| M_CUS     |      1 |     90 |     90 |00:00:00.01 |       7 |      0 |      0 |       |       |          |         |
---------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(TOB) USE_HASH(MC) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
	 , SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
GROUP BY MC.CUS_ID
;

-- 대량의 데이터에서만 사용할 것인가?
-- 3개의 테이블 조인, M_ITM과 T_ORD_JOIN을 먼저 처리
/*
-----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |      7 |00:00:00.04 |     372 |      5 |       |       |          |
|   1 |  HASH GROUP BY                 |                |      1 |    100 |      7 |00:00:00.04 |     372 |      5 |  1149K|  1149K|  897K (0)|
|*  2 |   HASH JOIN                    |                |      1 |  19304 |  10000 |00:00:00.02 |     372 |      5 |  1856K|  1856K| 1514K (0)|
|*  3 |    TABLE ACCESS FULL           | M_CUS          |      1 |     30 |     30 |00:00:00.01 |       7 |      0 |       |       |          |
|   4 |    NESTED LOOPS                |                |      1 |  19631 |  26000 |00:00:00.04 |     365 |      5 |       |       |          |
|   5 |     NESTED LOOPS               |                |      1 |  24540 |  26000 |00:00:00.02 |     127 |      5 |       |       |          |
|*  6 |      TABLE ACCESS FULL         | M_ITM          |      1 |     10 |     10 |00:00:00.01 |       7 |      5 |       |       |          |
|*  7 |      INDEX RANGE SCAN          | X_T_ORD_JOIN_4 |     10 |   2454 |  26000 |00:00:00.01 |     120 |      0 |       |       |          |
|   8 |     TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |  26000 |   1963 |  26000 |00:00:00.01 |     238 |      0 |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	MI.ITM_ID
	, MI.ITM_NM
	, TOJ.ORD_ST
	, COUNT(*) AS ORD_QTY
FROM M_ITM MI
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.ITM_ID = MI.ITM_ID
	INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
WHERE MI.ITM_TP = 'ELEC'
AND MC.CUS_GD = 'B'
AND TOJ.ORD_YMD LIKE '201702%'
GROUP BY MI.ITM_ID, MI.ITM_NM, TOJ.ORD_ST
;

-- 3개의 테이블 조인, NL조인으로 처리
-- 해시 조인과 비교하여 Buffers 값이 차이가 있음
/*
 -----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                               | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                        |                |      1 |        |      7 |00:00:00.06 |   26369 |       |       |          |
|   1 |  HASH GROUP BY                          |                |      1 |    100 |      7 |00:00:00.06 |   26369 |  1149K|  1149K|  897K (0)|
|   2 |   NESTED LOOPS                          |                |      1 |  19304 |  10000 |00:00:00.02 |   26369 |       |       |          |
|   3 |    NESTED LOOPS                         |                |      1 |  19631 |  26000 |00:00:00.05 |     369 |       |       |          |
|   4 |     NESTED LOOPS                        |                |      1 |  19631 |  26000 |00:00:00.02 |     365 |       |       |          |
|*  5 |      TABLE ACCESS FULL                  | M_ITM          |      1 |     10 |     10 |00:00:00.01 |       7 |       |       |          |
|   6 |      TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |     10 |   1963 |  26000 |00:00:00.02 |     358 |       |       |          |
|*  7 |       INDEX RANGE SCAN                  | X_T_ORD_JOIN_4 |     10 |   2454 |  26000 |00:00:00.01 |     120 |       |       |          |
|*  8 |     INDEX UNIQUE SCAN                   | PK_M_CUS       |  26000 |      1 |  26000 |00:00:00.01 |       4 |       |       |          |
|*  9 |    TABLE ACCESS BY INDEX ROWID          | M_CUS          |  26000 |      1 |  10000 |00:00:00.02 |   26000 |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MI TOJ MC) USE_NL(TOJ MC) */
	MI.ITM_ID
	, MI.ITM_NM
	, TOJ.ORD_ST
	, COUNT(*) AS ORD_QTY
FROM M_ITM MI
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.ITM_ID = MI.ITM_ID
	INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
WHERE MI.ITM_TP = 'ELEC'
AND MC.CUS_GD = 'B'
AND TOJ.ORD_YMD LIKE '201702%'
GROUP BY MI.ITM_ID, MI.ITM_NM, TOJ.ORD_ST
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
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('fmnyvubqng05u', 0, 'ALLSTATS LAST'))
;