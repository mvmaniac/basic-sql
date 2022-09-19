-- MERGE JOIN (머지 조인)
-- 두 데이터 집합을 연결 조건 값으로 정렬한 후 조인을 처리하는 방식
-- 정렬된 데이터를 차례대로 읽어가면서 조인을 수행
-- 연결 조건 기준으로 정렬되어 있어야만 조인이 가능하므로 소트 머지 조인(Sort Merge Join) 또는 소트 조인이라고 부름
-- 소트작업을 얼마나 어떻게 줄이느냐가 성능 향상의 주요 포인트

-- 머지 조인 SQL
-- 고객과 주문을 각각 정렬 후, 양쪽을 순차적으로 읽으면서 조인 처리
-- 조인 컬럼인 CUS_ID로 정렬 됨
/*
---------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                    | Name     | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
---------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT             |          |      1 |        |    501 |00:00:00.01 |      33 |     22 |       |       |          |
|   1 |  MERGE JOIN                  |          |      1 |   9141 |    501 |00:00:00.01 |      33 |     22 |       |       |          |
|   2 |   TABLE ACCESS BY INDEX ROWID| M_CUS    |      1 |     90 |     15 |00:00:00.01 |      10 |      2 |       |       |          |
|   3 |    INDEX FULL SCAN           | PK_M_CUS |      1 |     90 |     15 |00:00:00.01 |       5 |      1 |       |       |          |
|*  4 |   SORT JOIN                  |          |     15 |   3047 |    501 |00:00:00.01 |      23 |     20 |   178K|   178K|  158K (0)|
|   5 |    TABLE ACCESS FULL         | T_ORD    |      1 |   3047 |   3047 |00:00:00.01 |      23 |     20 |       |       |          |
---------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_MERGE(O) */
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
-- 고객별 2월 전체 주문금액 조회, T_ORD_BIG, NL 조인 사용
/*
------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |           |      1 |        |     72 |00:00:08.61 |    2238K|    258K|       |       |          |
|   1 |  WINDOW BUFFER                 |           |      1 |     90 |     72 |00:00:08.61 |    2238K|    258K|  6144 |  6144 | 6144  (0)|
|   2 |   HASH GROUP BY                |           |      1 |     90 |     72 |00:00:08.61 |    2238K|    258K|   779K|   779K| 2547K (0)|
|   3 |    NESTED LOOPS                |           |      1 |   2129K|   1980K|00:00:13.68 |    2238K|    258K|       |       |          |
|   4 |     NESTED LOOPS               |           |      1 |   2129K|   1980K|00:00:12.02 |     258K|    258K|       |       |          |
|*  5 |      TABLE ACCESS FULL         | T_ORD_BIG |      1 |   2129K|   1980K|00:00:09.97 |     258K|    258K|       |       |          |
|*  6 |      INDEX UNIQUE SCAN         | PK_M_CUS  |   1980K|      1 |   1980K|00:00:00.97 |       4 |      0 |       |       |          |
|   7 |     TABLE ACCESS BY INDEX ROWID| M_CUS     |   1980K|      1 |   1980K|00:00:01.07 |    1980K|      0 |       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(TOB) USE_NL(MC) FULL(TOB) */
	MC.CUS_ID
	, MAX(MC.CUS_NM) AS CUS_NM
	, MAX(MC.CUS_GD) AS CUS_GD
	, SUM(TOB.ORD_AMT) AS ORD_AMT
	, SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
WHERE TOB.ORD_YMD LIKE '201702%'
GROUP BY MC.CUS_ID
;

-- 고객별 2월 전체 주문금액 조회, T_ORD_BIG, 머지 조인 사용
-- 1. PK_M_CUS 인덱스를 INDEX FULL SCAN (CUS_ID 순서의 리프 블록을 차례대로 읽음)
-- 2. 1번에서 찾은 ROWID를 이용해 M_CUS에 접근 (TABLE ACCESS BY INDEX ROWID)
-- 3. T_ORD_BIG을 TABLE ACCESS FULL, ORD_YMD가 201702%인 데이터를 검색
-- 4. 3번의 결과를 CUS_ID 순서로 정렬
-- 5. 2번을 처리하면서 4번의 결과 머지 조인 처리
-- : NL 조인 처럼 후행 테이블을 반복해서 다시 읽지 않음
/*
------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |           |      1 |        |     72 |00:00:06.30 |     258K|    258K|       |       |          |
|   1 |  WINDOW BUFFER                 |           |      1 |     90 |     72 |00:00:06.30 |     258K|    258K|  6144 |  6144 | 6144  (0)|
|   2 |   SORT GROUP BY NOSORT         |           |      1 |     90 |     72 |00:00:06.39 |     258K|    258K|       |       |          |
|   3 |    MERGE JOIN                  |           |      1 |   6389K|   1980K|00:00:06.17 |     258K|    258K|       |       |          |
|   4 |     TABLE ACCESS BY INDEX ROWID| M_CUS     |      1 |     90 |     90 |00:00:00.01 |       3 |      0 |       |       |          |
|   5 |      INDEX FULL SCAN           | PK_M_CUS  |      1 |     90 |     90 |00:00:00.01 |       1 |      0 |       |       |          |
|*  6 |     SORT JOIN                  |           |     90 |   2129K|   1980K|00:00:05.95 |     258K|    258K|    66M|  2819K|   58M (0)|
|*  7 |      TABLE ACCESS FULL         | T_ORD_BIG |      1 |   2129K|   1980K|00:00:09.30 |     258K|    258K|       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_MERGE(TOB) FULL(TOB) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
	 , SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
WHERE TOB.ORD_YMD LIKE '201702%'
GROUP BY MC.CUS_ID
;

-- 머지 조인 인덱스
-- 조인에 참여하는 테이블별로 대상을 줄일 수 있는 조건에 인덱스를 만들면 됨
-- 여기서는 T_ORD_BIG의 ORD_YMD 컬럼에 인덱스를 고려 할 수 있음
/*
------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |           |      1 |        |     72 |00:00:04.12 |     258K|    258K|       |       |          |
|   1 |  WINDOW BUFFER                 |           |      1 |     90 |     72 |00:00:04.12 |     258K|    258K|  6144 |  6144 | 6144  (0)|
|   2 |   SORT GROUP BY NOSORT         |           |      1 |     90 |     72 |00:00:04.09 |     258K|    258K|       |       |          |
|   3 |    MERGE JOIN                  |           |      1 |   1865K|    720K|00:00:04.08 |     258K|    258K|       |       |          |
|   4 |     TABLE ACCESS BY INDEX ROWID| M_CUS     |      1 |     90 |     90 |00:00:00.01 |       3 |      0 |       |       |          |
|   5 |      INDEX FULL SCAN           | PK_M_CUS  |      1 |     90 |     90 |00:00:00.01 |       1 |      0 |       |       |          |
|*  6 |     SORT JOIN                  |           |     90 |    621K|    720K|00:00:04.00 |     258K|    258K|    23M|  1785K|   21M (0)|
|*  7 |      TABLE ACCESS FULL         | T_ORD_BIG |      1 |    621K|    720K|00:00:26.76 |     258K|    258K|       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_MERGE(TOB) FULL(TOB) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
	 , SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
WHERE TOB.ORD_YMD BETWEEN '20170201' AND '20170210'
GROUP BY MC.CUS_ID
;

-- ORD_YMD가 포함된 인덱스를 수행한 결과 X_T_ORD_BIG_1 인덱스를 사용한 경우 좋았음
-- X_T_ORD_BIG_1: ORD_YMD
-- X_T_ORD_BIG_3: ORD_YMD, CUS_ID
-- X_T_ORD_BIG_4: CUS_ID, ORD_YMD, ORD_ST
-- 아래 결과로  머지 조인은 단일 인덱스를 사용해야 한다는 결론을 내리면 곤란, 항상 테스트해보고 결론을 도출해야 함
/*
-----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                       | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |               |      1 |        |     72 |00:00:02.66 |   87465 |  14364 |       |       |          |
|   1 |  WINDOW BUFFER                  |               |      1 |     90 |     72 |00:00:02.66 |   87465 |  14364 |  6144 |  6144 | 6144  (0)|
|   2 |   SORT GROUP BY NOSORT          |               |      1 |     90 |     72 |00:00:02.62 |   87465 |  14364 |       |       |          |
|   3 |    MERGE JOIN                   |               |      1 |   1865K|    720K|00:00:02.55 |   87465 |  14364 |       |       |          |
|   4 |     TABLE ACCESS BY INDEX ROWID | M_CUS         |      1 |     90 |     90 |00:00:00.01 |       3 |      0 |       |       |          |
|   5 |      INDEX FULL SCAN            | PK_M_CUS      |      1 |     90 |     90 |00:00:00.01 |       1 |      0 |       |       |          |
|*  6 |     SORT JOIN                   |               |     90 |    621K|    720K|00:00:02.46 |   87462 |  14364 |    23M|  1785K|   21M (0)|
|   7 |      TABLE ACCESS BY INDEX ROWID| T_ORD_BIG     |      1 |    621K|    720K|00:00:02.37 |   87462 |  14364 |       |       |          |
|*  8 |       INDEX RANGE SCAN          | X_T_ORD_BIG_1 |      1 |    621K|    720K|00:00:00.18 |    2009 |   2009 |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_MERGE(TOB) INDEX(TOB X_T_ORD_BIG_1) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
	 , SUM(SUM(TOB.ORD_AMT)) OVER() TTL_ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
WHERE TOB.ORD_YMD BETWEEN '20170201' AND '20170210'
GROUP BY MC.CUS_ID
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
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('8jky1mvagcswm', 0, 'ALLSTATS LAST'))
;