-- paging
-- WAS 페이징: 모든 데이터를 가져와 WAS에서 페이징 처리를 하는 방법
-- DB 페이징: 데이터베이스에서 페이징에 필요한 만큼의 데이터만 조회하는 방법
-- : 성능까지 고려한 방법은 아님, 조회에 필요한 데이터를 모두 읽은 다음에 페이지에 필요한 만큼만 데이터를 잘라서 WAS로 보내는 방법
-- DB-INDEX 페이징: 인덱스를 이용해 페이징에 필요한 데이터만 정확히 읽어내는 방법
-- : 엔덱스를 이용해 필요한 데이터만 정확히 읽어내는 방법, 인덱스와 ROWNUM을 활용해 구현
-- : 페이징 건수가 30건이라면 정확히 30건의 데이터만 접근
-- : 필요한 데이터만 읽기 때문에 성능에 가장 좋지만, 상황에 따라 불가능할 때도 있음
-- : 부분 범위 처리 페이징이라고 부를 수도 있고 NO-SORT 페이징이라고 부를 수 있음

--DB 페이징
-- 주문 리스트 조회
SELECT
	TOJ.ORD_SEQ
	, TOJ.ORD_YMD
	, TOJ.CUS_ID
	, MC.CUS_NM
	, MR.RGN_NM
	, TOJ.ORD_ST
	, TOJ.ITM_ID
FROM T_ORD_JOIN TOJ
	INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
	INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
WHERE TOJ.ORD_YMD LIKE '201703%'
ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
;

-- 주문 리스트 조회, 첫 페이지 조회
-- 원래의 SQL은 인라인-뷰로 처리하고 인라인-뷰 바깥에서 ROWNUM을 사용해 30건만 조회
-- ORDER BY는 인라인-뷰 안쪽에, ROWNUM은 인라인-뷰 바깥쪽에 위치
-- ROWNUM을 ORDER BY와 같은 블록에 있다면 ROWNUM이 ORDER BY 이전에 처리되므로 정렬순서가 뒤죽박죽 됨
-- 실행계획에서 'COUNT STOPKEY'는 ROWNUM 조건을 처리하는 단계
-- 실행계획에서 'SORT ORDER BY STOPKEY'는 정렬를 처리하는 단계?
/*
-----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                               | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                        |                |      1 |        |     30 |00:00:00.19 |    2689 |       |       |          |
|*  1 |  COUNT STOPKEY                          |                |      1 |        |     30 |00:00:00.19 |    2689 |       |       |          |
|   2 |   VIEW                                  |                |      1 |    545K|     30 |00:00:00.19 |    2689 |       |       |          |
|*  3 |    SORT ORDER BY STOPKEY                |                |      1 |    545K|     30 |00:00:00.19 |    2689 | 36864 | 36864 |32768  (0)|
|*  4 |     HASH JOIN                           |                |      1 |    545K|    192K|00:00:00.21 |    2689 |  1355K|  1355K| 1640K (0)|
|   5 |      MERGE JOIN                         |                |      1 |     90 |     90 |00:00:00.01 |       8 |       |       |          |
|   6 |       TABLE ACCESS BY INDEX ROWID       | M_RGN          |      1 |      5 |      5 |00:00:00.01 |       2 |       |       |          |
|   7 |        INDEX FULL SCAN                  | PK_M_RGN       |      1 |      5 |      5 |00:00:00.01 |       1 |       |       |          |
|*  8 |       SORT JOIN                         |                |      5 |     90 |     90 |00:00:00.01 |       6 | 11264 | 11264 |10240  (0)|
|   9 |        TABLE ACCESS FULL                | M_CUS          |      1 |     90 |     90 |00:00:00.01 |       6 |       |       |          |
|  10 |      TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |      1 |    181K|    192K|00:00:00.15 |    2681 |       |       |          |
|* 11 |       INDEX SKIP SCAN                   | X_T_ORD_JOIN_4 |      1 |    181K|    192K|00:00:00.10 |     933 |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		TOJ.ORD_SEQ
		, TOJ.ORD_YMD
		, TOJ.CUS_ID
		, MC.CUS_NM
		, MR.RGN_NM
		, TOJ.ORD_ST
		, TOJ.ITM_ID
	FROM T_ORD_JOIN TOJ
		INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
		INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
	WHERE TOJ.ORD_YMD LIKE '201703%'
	ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
) T
WHERE ROWNUM <= 30
;

-- 주문 리스트 조회, 두번째 페이지 조회, 잘못된 방법
-- 실행하면 조회되는 데이터가 없음
-- ROWNUM은 조회되는 데이터에 1부터 차례대로 번호를 매김
-- 그러므로 1을 거치지 않고서 2나 3이 나올 수 없음
SELECT * FROM T_ORD_JOIN WHERE ROWNUM = 1; -- 조회 가능
SELECT * FROM T_ORD_JOIN WHERE ROWNUM = 2; -- 조회 불가능
SELECT * FROM T_ORD_JOIN WHERE ROWNUM <= 2; -- 조회 가능
SELECT * FROM T_ORD_JOIN WHERE ROWNUM >= 2; -- 조회 불가능
SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		TOJ.ORD_SEQ
		, TOJ.ORD_YMD
		, TOJ.CUS_ID
		, MC.CUS_NM
		, MR.RGN_NM
		, TOJ.ORD_ST
		, TOJ.ITM_ID
	FROM T_ORD_JOIN TOJ
		INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
		INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
	WHERE TOJ.ORD_YMD LIKE '201703%'
	ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
) T
WHERE ROWNUM >= 31
AND ROWNUM <= 60
;


-- 주문 리스트 조회, 두번째 페이지 조회, 정상적인 방법
-- 두번째 페이지의 마지막까지 조회되도록 ROWNUM <= 60 조건을 사용
-- 이 결과를 다시 인라인-뷰로 처리하고 두 번째 페이지의 시작 데이터부터 조회과 되도록 RNO >= 31 조건을 사용
-- 페이징 처리에서 ROW_NUMBER는 ROWNUM보다 성능이 좋지 못할 가능성이 크기 때문에 페이징 처리에서는 ROWNUM을 사용
/*
------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                                | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                         |                |      1 |        |     30 |00:00:00.16 |    2689 |       |       |          |
|*  1 |  VIEW                                    |                |      1 |     60 |     30 |00:00:00.16 |    2689 |       |       |          |
|*  2 |   COUNT STOPKEY                          |                |      1 |        |     60 |00:00:00.16 |    2689 |       |       |          |
|   3 |    VIEW                                  |                |      1 |    545K|     60 |00:00:00.16 |    2689 |       |       |          |
|*  4 |     SORT ORDER BY STOPKEY                |                |      1 |    545K|     60 |00:00:00.16 |    2689 | 38912 | 38912 |34816  (0)|
|*  5 |      HASH JOIN                           |                |      1 |    545K|    192K|00:00:00.17 |    2689 |  1355K|  1355K| 1618K (0)|
|   6 |       MERGE JOIN                         |                |      1 |     90 |     90 |00:00:00.01 |       8 |       |       |          |
|   7 |        TABLE ACCESS BY INDEX ROWID       | M_RGN          |      1 |      5 |      5 |00:00:00.01 |       2 |       |       |          |
|   8 |         INDEX FULL SCAN                  | PK_M_RGN       |      1 |      5 |      5 |00:00:00.01 |       1 |       |       |          |
|*  9 |        SORT JOIN                         |                |      5 |     90 |     90 |00:00:00.01 |       6 | 11264 | 11264 |10240  (0)|
|  10 |         TABLE ACCESS FULL                | M_CUS          |      1 |     90 |     90 |00:00:00.01 |       6 |       |       |          |
|  11 |       TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |      1 |    181K|    192K|00:00:00.11 |    2681 |       |       |          |
|* 12 |        INDEX SKIP SCAN                   | X_T_ORD_JOIN_4 |      1 |    181K|    192K|00:00:00.08 |     933 |       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		ROWNUM AS RNO
		, T1.*
	FROM (
		SELECT
			TOJ.ORD_SEQ
			, TOJ.ORD_YMD
			, TOJ.CUS_ID
			, MC.CUS_NM
			, MR.RGN_NM
			, TOJ.ORD_ST
			, TOJ.ITM_ID
		FROM T_ORD_JOIN TOJ
			INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
			INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
		WHERE TOJ.ORD_YMD LIKE '201703%'
		ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
	) T1
	WHERE ROWNUM <= 60
) T2
WHERE T2.RNO >= 31
;

-- DB-INDEX 페이징
-- SQL 조건절에만 인덱스를 만드는 것이 아니라 페이징에 사용되는 ORDER BY 컬럼까지 고려해 인덱스를 설계
-- 1. WHERE 절에 조건으로 사용된 컬럼을 복합 인덱스의 선두컬럼으로 사용
-- : 조건이 여러개라면 같다(=) 조건의 컬럼을 앞쪽에, 범위조건을 뒤쪽에 놓음
-- 2. ORDER BY에 사용된 컬럼을 1번에서 정의한 컬럼 뒤에 차례대로 위치 시킴
-- 아무리 인덱스를 구성해도 WHERE 절의 조건과 ORDER BY에 따라 작동하지 않을 수 있으므로 반드시 실행계획을 확인하고 원하는대로 처리 되었는지 확인 해야 함

-- 주문 리스트를 조회, DB-INDEX 페이징
-- INDEX RANGE SCAN DESCENDING 방식으로 접근해 필요한 60건만 가져옴, 이렇게 할 수 있는 이유는 조회하려는 데이터 순서와 인덱스의 리프블록의 데이터순서가 같기 떄문
-- 실제 DB-INDEX 페이징이 동작했는지 확인하려면 아래 항목들을 참고
-- 1. INDEX RANGE SCAN DESCENDING(또는 ASCENDING) 오퍼레이션이 있어야 함
-- : 상황에 따라서는 INDEX FULL SCAN이 나올 수 있음
-- 2. 1번 항목에서, 페이징 건수만큼만 또는 약간 초과해서 A-Rows가 나와야 함
-- : ORDER BY나 조건절, 인덱스 구성에 따라 A-Rows가 페이징 건수보다 높을 수 있음
-- : A-Rows가 최대한 페이징 건수에 가깝거나, 큰 비효율이 없어야 함
-- 3. 1번 항목 이후에 COUNT STOPKEY가 있어야 함
/*
--------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                          | Name             | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
--------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                   |                  |      1 |        |     30 |00:00:00.01 |     134 |      6 |
|*  1 |  VIEW                              |                  |      1 |     21 |     30 |00:00:00.01 |     134 |      6 |
|*  2 |   COUNT STOPKEY                    |                  |      1 |        |     60 |00:00:00.01 |     134 |      6 |
|   3 |    VIEW                            |                  |      1 |     21 |     60 |00:00:00.01 |     134 |      6 |
|   4 |     NESTED LOOPS                   |                  |      1 |     21 |     60 |00:00:00.01 |     134 |      6 |
|   5 |      NESTED LOOPS                  |                  |      1 |     21 |     60 |00:00:00.01 |      74 |      6 |
|   6 |       NESTED LOOPS                 |                  |      1 |     21 |     60 |00:00:00.01 |      70 |      6 |
|   7 |        TABLE ACCESS BY INDEX ROWID | T_ORD_JOIN       |      1 |    181K|     60 |00:00:00.01 |       6 |      5 |
|*  8 |         INDEX RANGE SCAN DESCENDING| X_T_ORD_JOIN_PG1 |      1 |     21 |     60 |00:00:00.01 |       4 |      5 |
|   9 |        TABLE ACCESS BY INDEX ROWID | M_CUS            |     60 |      1 |     60 |00:00:00.01 |      64 |      1 |
|* 10 |         INDEX UNIQUE SCAN          | PK_M_CUS         |     60 |      1 |     60 |00:00:00.01 |       4 |      1 |
|* 11 |       INDEX UNIQUE SCAN            | PK_M_RGN         |     60 |      1 |     60 |00:00:00.01 |       4 |      0 |
|  12 |      TABLE ACCESS BY INDEX ROWID   | M_RGN            |     60 |      1 |     60 |00:00:00.01 |      60 |      0 |
--------------------------------------------------------------------------------------------------------------------------
*/
-- 페이징처리를 위한 인덱스를 추가
CREATE INDEX X_T_ORD_JOIN_PG1 ON T_ORD_JOIN(ORD_YMD, ORD_SEQ)
;

SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		ROWNUM AS RNO
		, T1.*
	FROM (
		SELECT
			TOJ.ORD_SEQ
			, TOJ.ORD_YMD
			, TOJ.CUS_ID
			, MC.CUS_NM
			, MR.RGN_NM
			, TOJ.ORD_ST
			, TOJ.ITM_ID
		FROM T_ORD_JOIN TOJ
			INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
			INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
		WHERE TOJ.ORD_YMD LIKE '201703%'
		ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
	) T1
	WHERE ROWNUM <= 60
) T2
WHERE T2.RNO >= 31
;

-- 100번째 페이지 조회, DB-INDEX 페이징
-- 원래는 DB 페이징 방식으로 나와야 하나 DB-INDEX 페이징 방식으로 나옴 (확인필요?)
-- 옵티마이저가 판단하여 DB 페이징 방식으로 바꿈
-- DB-INDEX 페이징은 뒤쪽의 피이지를 조회할수록 성능이 저하됨
-- 100번째 페이지를 보려면 1페이지부터 시작ㅎ새 100페이지까지 해당하는 데이터를 차례대로 모두 접근해야 하기 때문
-- 보통 사용자들은 첫 번째 페이지나 두 번째 페이지만 보고나서 조건을 변경해서 조회하는 경향이 있으므로 뒤쪽의 페이지를 읽을 가능성은 크지 않음
-- 일단 시스템 성능에 큰 문제가 없다면 DB 페이징 방식을 주로 사용하고, 많이 사용되는 화면에만 DB-INDEX 페이징 기술을 사용
/*
--------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                          | Name             | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
--------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                   |                  |      1 |        |     30 |00:00:00.01 |    6048 |      8 |
|*  1 |  VIEW                              |                  |      1 |   1001 |     30 |00:00:00.01 |    6048 |      8 |
|*  2 |   COUNT STOPKEY                    |                  |      1 |        |   3000 |00:00:00.01 |    6048 |      8 |
|   3 |    VIEW                            |                  |      1 |   1001 |   3000 |00:00:00.01 |    6048 |      8 |
|   4 |     NESTED LOOPS                   |                  |      1 |   1001 |   3000 |00:00:00.01 |    6048 |      8 |
|   5 |      NESTED LOOPS                  |                  |      1 |   1001 |   3000 |00:00:00.01 |    3048 |      8 |
|   6 |       NESTED LOOPS                 |                  |      1 |   1001 |   3000 |00:00:00.01 |    3044 |      8 |
|   7 |        TABLE ACCESS BY INDEX ROWID | T_ORD_JOIN       |      1 |    181K|   3000 |00:00:00.01 |      40 |      8 |
|*  8 |         INDEX RANGE SCAN DESCENDING| X_T_ORD_JOIN_PG1 |      1 |   1001 |   3000 |00:00:00.01 |      14 |      8 |
|   9 |        TABLE ACCESS BY INDEX ROWID | M_CUS            |   3000 |      1 |   3000 |00:00:00.01 |    3004 |      0 |
|* 10 |         INDEX UNIQUE SCAN          | PK_M_CUS         |   3000 |      1 |   3000 |00:00:00.01 |       4 |      0 |
|* 11 |       INDEX UNIQUE SCAN            | PK_M_RGN         |   3000 |      1 |   3000 |00:00:00.01 |       4 |      0 |
|  12 |      TABLE ACCESS BY INDEX ROWID   | M_RGN            |   3000 |      1 |   3000 |00:00:00.01 |    3000 |      0 |
--------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		ROWNUM AS RNO
		, T1.*
	FROM (
		SELECT
			TOJ.ORD_SEQ
			, TOJ.ORD_YMD
			, TOJ.CUS_ID
			, MC.CUS_NM
			, MR.RGN_NM
			, TOJ.ORD_ST
			, TOJ.ITM_ID
		FROM T_ORD_JOIN TOJ
			INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
			INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
		WHERE TOJ.ORD_YMD LIKE '201703%'
		ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
	) T1
	WHERE ROWNUM <= 100 * 30 -- 페이지번호 * 페이지당 로우수
) T2
WHERE T2.RNO >= (100 * 30) - (30 - 1) -- (페이지번호 * 페이지당 로우수) - (페이지당 로우수 - 1)
;

-- 페이징을 위한 카운트 처리
-- 오라클의 옵티마이져가 ORDER BY와 M_RGN에 대한 조인처리가 사라짐, 카운트 결과에 영향을 주지 않는 요소를 옵티마이져가 자동으로 제거
/*
---------------------------------------------------------------------------------------------------------
| Id  | Operation           | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
---------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT    |                |      1 |        |      1 |00:00:00.12 |     967 |   1072 |
|   1 |  SORT AGGREGATE     |                |      1 |      1 |      1 |00:00:00.12 |     967 |   1072 |
|   2 |   NESTED LOOPS      |                |      1 |    545K|    192K|00:00:00.20 |     967 |   1072 |
|*  3 |    TABLE ACCESS FULL| M_CUS          |      1 |     90 |     90 |00:00:00.01 |       7 |      0 |
|*  4 |    INDEX RANGE SCAN | X_T_ORD_JOIN_2 |     90 |   6057 |    192K|00:00:00.19 |     960 |   1072 |
---------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	COUNT(*)
FROM  (
	SELECT
		TOJ.ORD_SEQ
		, TOJ.ORD_YMD
		, TOJ.CUS_ID
		, MC.CUS_NM
		, MR.RGN_NM
		, TOJ.ORD_ST
		, TOJ.ITM_ID
	FROM T_ORD_JOIN TOJ
		INNER JOIN M_CUS MC ON MC.CUS_ID = TOJ.CUS_ID
		INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
	WHERE TOJ.ORD_YMD LIKE '201703%'
	ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
) T1
;

-- 페이징을 위한 카운트 최적화
-- 카운트 SQL의 성능을 개선하려면
-- 첫 번째는 옵티마이져가 한 것 처럼 카운트에 영향을 주지 않는 부분을 제거하는 것
-- 두 번째는 페이지 표시에 필요한 만큼만 카운트 하는 것
-- 예를 들어 한 페이지에 30건씩 10개 블록으로 페이지를 표시하려면 301건의 데이터만 읽으면 됨
-- 300건이 있으면 1부터 10까지 페이지 표시가 가능하고 Next 버튼이 필요한지 판단하기 위해 추가로 한 건만 읽으면 됨
-- 만약에 11 ~ 20페이지가 있는지 표시하려면 ROWNUM 조건을 조정해 601건만 카운트하면 됨
-- 인라인-뷰 안에 ORDER BY를 제거하면 안됨
/*
--------------------------------------------------------------------------------------------------------------
| Id  | Operation                       | Name             | Starts | E-Rows | A-Rows |   A-Time   | Buffers |
--------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |                  |      1 |        |      1 |00:00:00.01 |       4 |
|   1 |  SORT AGGREGATE                 |                  |      1 |      1 |      1 |00:00:00.01 |       4 |
|   2 |   VIEW                          |                  |      1 |    301 |    301 |00:00:00.01 |       4 |
|*  3 |    COUNT STOPKEY                |                  |      1 |        |    301 |00:00:00.01 |       4 |
|   4 |     VIEW                        |                  |      1 |    301 |    301 |00:00:00.01 |       4 |
|*  5 |      INDEX RANGE SCAN DESCENDING| X_T_ORD_JOIN_PG1 |      1 |    181K|    301 |00:00:00.01 |       4 |
--------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	COUNT(*)
FROM (
	SELECT *
	FROM (
		SELECT
			TOJ.ORD_SEQ
			, TOJ.ORD_YMD
		FROM T_ORD_JOIN TOJ
		WHERE TOJ.ORD_YMD LIKE '201703%'
		ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
	) T1
	WHERE ROWNUM <= (30 * 10) + 1
)
;

-- DB-INDEX 페이징 성능 개선
-- 100번째 페이지 조회 성능 개선
-- 페이징 정렬 기준 컬럼은 T_ORD_JOIN에 있으므로 T_ORD_JOIN에서 30건을 잘라낸 후 나머지 테이블과 조인을 해도 결과는 같음
-- 인라인-뷰안에 T_ORD_JOIN만 조회해서 30건을 잘라낸 후에, 마지막에 M_CUS와 M_RGN을 조인 처리
-- 인라인-뷰DML RNO 값으로 정렬을 다시 함, 페이징 된 후에 조인이 처리되므로 조인을 하면서 페이징의 정렬 순서가 틀어질 수 있기 때문
/*
-------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                         | Name             | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
-------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                  |                  |      1 |        |     30 |00:00:00.01 |      49 |       |       |          |
|   1 |  SORT ORDER BY                    |                  |      1 |   9000 |     30 |00:00:00.01 |      49 |  4096 |  4096 | 4096  (0)|
|*  2 |   HASH JOIN                       |                  |      1 |   9000 |     30 |00:00:00.01 |      49 |  1355K|  1355K| 1615K (0)|
|   3 |    MERGE JOIN                     |                  |      1 |     90 |     90 |00:00:00.01 |       9 |       |       |          |
|   4 |     TABLE ACCESS BY INDEX ROWID   | M_RGN            |      1 |      5 |      5 |00:00:00.01 |       2 |       |       |          |
|   5 |      INDEX FULL SCAN              | PK_M_RGN         |      1 |      5 |      5 |00:00:00.01 |       1 |       |       |          |
|*  6 |     SORT JOIN                     |                  |      5 |     90 |     90 |00:00:00.01 |       7 | 11264 | 11264 |10240  (0)|
|   7 |      TABLE ACCESS FULL            | M_CUS            |      1 |     90 |     90 |00:00:00.01 |       7 |       |       |          |
|*  8 |    VIEW                           |                  |      1 |   3000 |     30 |00:00:00.01 |      40 |       |       |          |
|*  9 |     COUNT STOPKEY                 |                  |      1 |        |   3000 |00:00:00.01 |      40 |       |       |          |
|  10 |      VIEW                         |                  |      1 |   3000 |   3000 |00:00:00.01 |      40 |       |       |          |
|  11 |       TABLE ACCESS BY INDEX ROWID | T_ORD_JOIN       |      1 |    181K|   3000 |00:00:00.01 |      40 |       |       |          |
|* 12 |        INDEX RANGE SCAN DESCENDING| X_T_ORD_JOIN_PG1 |      1 |   3000 |   3000 |00:00:00.01 |      14 |       |       |          |
-------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	T2.RNO
	, T2.ORD_SEQ
	, T2.ORD_YMD
	, T2.CUS_ID
	, MC.CUS_NM
	, MR.RGN_NM
	, T2.ORD_ST
	, T2.ITM_ID
FROM
	(
		SELECT
			ROWNUM AS RNO
			 , T1.*
		FROM (
			SELECT
				TOJ.ORD_SEQ
				, TOJ.ORD_YMD
				, TOJ.CUS_ID
				, TOJ.ORD_ST
				, TOJ.ITM_ID
			FROM T_ORD_JOIN TOJ
			WHERE TOJ.ORD_YMD LIKE '201703%'
			ORDER BY TOJ.ORD_YMD DESC, TOJ.ORD_SEQ DESC
		) T1
		WHERE ROWNUM <= 100 * 30 -- 페이지번호 * 페이지당 로우수
	) T2
	INNER JOIN M_CUS MC ON MC.CUS_ID = T2.CUS_ID
	INNER JOIN M_RGN MR ON MR.RGN_ID = MC.RGN_ID
WHERE T2.RNO >= (100 * 30) - (30 - 1) -- (페이지번호 * 페이지당 로우수) - (페이지당 로우수 - 1)
ORDER BY T2.RNO
;

-- DB-INDEX 페이징으로 유도하기
-- DB-INDEX 페이징이 되지 않는 SQL
/*
------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                                | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                         |                |      1 |        |     30 |00:00:00.01 |     185 |       |       |          |
|*  1 |  VIEW                                    |                |      1 |     30 |     30 |00:00:00.01 |     185 |       |       |          |
|*  2 |   COUNT STOPKEY                          |                |      1 |        |     30 |00:00:00.01 |     185 |       |       |          |
|   3 |    VIEW                                  |                |      1 |     31 |     30 |00:00:00.01 |     185 |       |       |          |
|*  4 |     SORT GROUP BY STOPKEY                |                |      1 |     31 |     30 |00:00:00.01 |     185 |  6144 |  6144 | 6144  (0)|
|   5 |      NESTED LOOPS OUTER                  |                |      1 |     31 |     90 |00:00:00.01 |     185 |       |       |          |
|   6 |       TABLE ACCESS BY INDEX ROWID BATCHED| M_CUS          |      1 |     90 |     90 |00:00:00.01 |       3 |       |       |          |
|   7 |        INDEX FULL SCAN                   | PK_M_CUS       |      1 |      1 |     90 |00:00:00.01 |       1 |       |       |          |
|   8 |       TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |     90 |     31 |      0 |00:00:00.01 |     182 |       |       |          |
|*  9 |        INDEX RANGE SCAN                  | X_T_ORD_JOIN_2 |     90 |     10 |      0 |00:00:00.01 |     182 |       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		ROWNUM AS RNO
		, T1. *
    FROM (
		SELECT
			MC.CUS_ID
			, MAX(MC.CUS_NM) AS CUM_NM
			, SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
        FROM M_CUS MC
        	LEFT OUTER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID AND TOJ.ORD_YMD = '201703%'
        GROUP BY MC.CUS_ID
        ORDER BY MC.CUS_ID
	) T1
    WHERE ROWNUM <= 30
) T2
WHERE T2.RNO >= 1
;

-- DB-INDEX 페이징이 되지 않는 SQL, M_CUS만 사용해서 DB-INDEX 페이징을 구현
-- M_CUS만 조회하면 PK_M_CUS 인덱스를 사용해 DB-INDEX 페이징이 작동
-- 불필요한 GROUP BY와 MAX(MC.CUS_NM)은 제거
/*
-----------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name     | Starts | E-Rows | A-Rows |   A-Time   | Buffers |
-----------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |          |      1 |        |     30 |00:00:00.01 |       2 |
|*  1 |  VIEW                          |          |      1 |     30 |     30 |00:00:00.01 |       2 |
|*  2 |   COUNT STOPKEY                |          |      1 |        |     30 |00:00:00.01 |       2 |
|   3 |    VIEW                        |          |      1 |     30 |     30 |00:00:00.01 |       2 |
|   4 |     TABLE ACCESS BY INDEX ROWID| M_CUS    |      1 |     90 |     30 |00:00:00.01 |       2 |
|   5 |      INDEX FULL SCAN           | PK_M_CUS |      1 |     30 |     30 |00:00:00.01 |       1 |
-----------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */ *
FROM (
	SELECT
		ROWNUM AS RNO
		 , T1. *
	FROM (
		SELECT
			MC.CUS_ID
			, MC.CUS_NM
		FROM M_CUS MC
		ORDER BY MC.CUS_ID
	) T1
	WHERE ROWNUM <= 30
) T2
WHERE T2.RNO >= 1
;

-- 페이징 후 T_ORD_JOIN을 서브쿼리로 처리
-- PK_M_CUS를 INDEX FULL SCAN으로 페이징에 필요한 30건만 읽음, DB-INDEX 페이징 되고 있다고 볼 수 있음
/*
----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                              | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                       |                |      1 |        |     30 |00:00:00.04 |     898 |       |       |          |
|   1 |  NESTED LOOPS OUTER                    |                |      1 |     90 |     30 |00:00:00.04 |     898 |       |       |          |
|*  2 |   VIEW                                 |                |      1 |     30 |     30 |00:00:00.01 |       2 |       |       |          |
|*  3 |    COUNT STOPKEY                       |                |      1 |        |     30 |00:00:00.01 |       2 |       |       |          |
|   4 |     VIEW                               |                |      1 |     30 |     30 |00:00:00.01 |       2 |       |       |          |
|   5 |      TABLE ACCESS BY INDEX ROWID       | M_CUS          |      1 |     90 |     30 |00:00:00.01 |       2 |       |       |          |
|   6 |       INDEX FULL SCAN                  | PK_M_CUS       |      1 |     30 |     30 |00:00:00.01 |       1 |       |       |          |
|   7 |   VIEW PUSHED PREDICATE                | VW_SSQ_1       |     30 |      1 |     21 |00:00:00.03 |     896 |       |       |          |
|   8 |    SORT GROUP BY                       |                |     30 |      1 |     21 |00:00:00.03 |     896 |  2048 |  2048 | 2048  (0)|
|   9 |     TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |     30 |   2019 |  63000 |00:00:00.02 |     896 |       |       |          |
|* 10 |      INDEX RANGE SCAN                  | X_T_ORD_JOIN_2 |     30 |   2019 |  63000 |00:00:00.01 |     319 |       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
    T2.*
	, (
		SELECT SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
	    FROM T_ORD_JOIN TOJ
	    WHERE TOJ.CUS_ID = T2.CUS_ID
	    AND TOJ.ORD_YMD LIKE '201703%'
	) AS ORD_AMT
FROM (
	SELECT
		ROWNUM AS RNO
		, T1. *
	FROM (
		SELECT
			MC.CUS_ID
			, MC.CUS_NM
		FROM M_CUS MC
		ORDER BY MC.CUS_ID
	) T1
	WHERE ROWNUM <= 30
) T2
WHERE T2.RNO >= 1
;

-- 페이징 후 T_ORD_JOIN을 아우터-조인으로 처리
/*
---------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                             | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |  OMem |  1Mem | Used-Mem |
---------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                      |                |      1 |        |     30 |00:00:00.04 |     898 |       |       |          |
|   1 |  SORT GROUP BY                        |                |      1 |     30 |     30 |00:00:00.04 |     898 |  6144 |  6144 | 6144  (0)|
|   2 |   NESTED LOOPS OUTER                  |                |      1 |    181K|  63009 |00:00:00.03 |     898 |       |       |          |
|*  3 |    VIEW                               |                |      1 |     30 |     30 |00:00:00.01 |       2 |       |       |          |
|*  4 |     COUNT STOPKEY                     |                |      1 |        |     30 |00:00:00.01 |       2 |       |       |          |
|   5 |      VIEW                             |                |      1 |     30 |     30 |00:00:00.01 |       2 |       |       |          |
|   6 |       TABLE ACCESS BY INDEX ROWID     | M_CUS          |      1 |     90 |     30 |00:00:00.01 |       2 |       |       |          |
|   7 |        INDEX FULL SCAN                | PK_M_CUS       |      1 |     30 |     30 |00:00:00.01 |       1 |       |       |          |
|   8 |    TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |     30 |   6057 |  63000 |00:00:00.02 |     896 |       |       |          |
|*  9 |     INDEX RANGE SCAN                  | X_T_ORD_JOIN_2 |     30 |   2019 |  63000 |00:00:00.01 |     319 |       |       |          |
---------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	T3.RNO
	, T3.CUS_ID
	, MAX(T3.CUS_NM)
	, SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM (
	SELECT T2.*
    FROM (
		SELECT
			ROWNUM AS RNO
			, T1. *
		FROM (
			SELECT
				MC.CUS_ID
				, MC.CUS_NM
			FROM M_CUS MC
			ORDER BY MC.CUS_ID
		) T1
		WHERE ROWNUM <= 30
	) T2
	WHERE T2.RNO >= 1
) T3
LEFT OUTER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = T3.CUS_ID AND TOJ.ORD_YMD LIKE '201703%'
GROUP BY T3.RNO, T3.CUS_ID
ORDER BY T3.RNO, T3.CUS_ID
;

-- DB-INDEX의 한계
-- 사용자 요구 사항이 변경되면 인덱스를 재구성해야 할 수 있음
-- ORDER BY에 집계함수가 사용되는 경우

-- 버퍼 캐시 지우기
ALTER SYSTEM FLUSH BUFFER_CACHE
;

-- 실제 실행계획을 만든 SQL의 SQL ID 찾아내기
SELECT
	V$S.SQL_ID
	, V$S.CHILD_NUMBER
	, V$S.SQL_TEXT
FROM V$SQL V$S
WHERE V$S.SQL_TEXT LIKE '%GATHER_PLAN_STATISTICS%'
ORDER BY V$S.LAST_ACTIVE_TIME DESC
;

SELECT *
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('9b66qqx0r7n21', 0, 'ALLSTATS LAST'))
;
