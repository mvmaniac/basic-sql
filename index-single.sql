-- 단일 인덱스

-- 인덱스가 필요한 SQL
/*
------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation          | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |           |      1 |        |      2 |00:00:02.63 |     258K|    258K|       |       |          |
|   1 |  HASH GROUP BY     |           |      1 |     15 |      2 |00:00:02.63 |     258K|    258K|  1452K|  1452K|  646K (0)|
|*  2 |   TABLE ACCESS FULL| T_ORD_BIG |      1 |     15 |      2 |00:00:00.02 |     258K|    258K|       |       |          |
------------------------------------------------------------------------------------------------------------------------------
 */
SELECT /*+ GATHER_PLAN_STATISTICS */
	TO_CHAR(TOB.ORD_DT, 'YYYYMM'), COUNT(*)
FROM T_ORD_BIG TOB
WHERE TOB.CUS_ID = 'CUS_0064'
AND TOB.PAY_TP = 'BANK'
AND TOB.RNO = 2
GROUP BY TO_CHAR(TOB.ORD_DT, 'YYYYMM')
;

-- 효율적인 인덱스 찾기
-- 선택성이 좋은 컬럼을 사용 해야 함
-- 주어지 조건에 해당하는 데이터가 적을수록 선택성이 좋고, 조건에 해하는 데이터가 많을수록 선택성이 나쁨
-- 아래 쿼리를 예를 들면 RNO = 2 조건이 결과가 적으므로, 단일 인덱스 하나를 만들어야 한다면 RNO 컬럼으로 만드는게 좋음
SELECT 'CUS_ID' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.CUS_ID = 'CUS_0064'
UNION ALL
SELECT 'PAY_TP' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.PAY_TP = 'BANK'
UNION ALL
SELECT 'RNO' AS COL, COUNT(*) AS CNT FROM T_ORD_BIG TOB WHERE TOB.RNO = 2
;

-- RNO에 대한 단일 인덱스 생성
CREATE INDEX X_T_ORD_BIG_2 ON T_ORD_BIG(RNO)
;

-- RNO에 대한 단일 인덱스 생성 후 SQL 수행
/*
----------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      2 |00:00:00.01 |      35 |     30 |       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |     15 |      2 |00:00:00.01 |      35 |     30 |  1452K|  1452K|  648K (0)|
|*  2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |     15 |      2 |00:00:00.01 |      35 |     30 |       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_2 |      1 |   3047 |   3047 |00:00:00.01 |       9 |      8 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------------
 */
SELECT /*+ GATHER_PLAN_STATISTICS INDEX(TOB X_T_ORD_BIG_2) */
	TO_CHAR(TOB.ORD_DT, 'YYYYMM'), COUNT(*)
FROM T_ORD_BIG TOB
WHERE TOB.CUS_ID = 'CUS_0064'
AND TOB.PAY_TP = 'BANK'
AND TOB.RNO = 2
GROUP BY TO_CHAR(TOB.ORD_DT, 'YYYYMM')
;

-- CUS_ID에 대한 단일 인덱스 생성
CREATE INDEX X_T_ORD_BIG_3 ON T_ORD_BIG(CUS_ID)
;

-- CUS_ID에 대한 단일 인덱스 생성 후 SQL 수행
/*
----------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                            | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                     |               |      1 |        |      2 |00:00:02.66 |     250K|    234K|       |       |          |
|   1 |  HASH GROUP BY                       |               |      1 |     15 |      2 |00:00:02.66 |     250K|    234K|  1452K|  1452K|  648K (0)|
|*  2 |   TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_BIG     |      1 |     15 |      2 |00:00:00.01 |     250K|    234K|       |       |          |
|*  3 |    INDEX RANGE SCAN                  | X_T_ORD_BIG_3 |      1 |    338K|    340K|00:00:00.07 |     950 |    949 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS INDEX(TOB X_T_ORD_BIG_3) */
	TO_CHAR(TOB.ORD_DT, 'YYYYMM'), COUNT(*)
FROM T_ORD_BIG TOB
WHERE TOB.CUS_ID = 'CUS_0064'
AND TOB.PAY_TP = 'BANK'
AND TOB.RNO = 2
GROUP BY TO_CHAR(TOB.ORD_DT, 'YYYYMM')

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
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('dgbtwfttb9h3z', 0, 'ALLSTATS LAST'))
;