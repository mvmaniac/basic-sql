-- NESTED LOOPS JOIN (NL 조인)
-- 중첩된 반복문 형태로 데이터를 연결하는 방식
-- 선행집합(선행테이블)과 후행집합(후행테이블)의 정의가 중요
-- 선행집합은 바깥쪽 루프가 되고 후행집합은 안쪽 루프가 됨
-- 많은 양의 데이터를 조인하기에는 한계가 있음
-- OLTP 환경의 로그인 처리, 계좌이체, 주문처리 같은 자주 실행되는 SQL은 NL 조인만으로 처리하는게 DB 성능에 도움이 됨,
-- 단, NL 조인의 성능이 확보되도록 적절한 인덱스가 구성되어 있어야 함

-- 고려사항
-- 휴행 집합의 조인 조건 컬럼에는 인덱스 필수
-- 후행 집합에 사용 조인 조건과 WHERE 조건 컬럼에 복합 인덱스를 고려
-- 후행 집합의 접근 횟수를 줄이려면 선행 집합의 건수가 작아야 함
-- 조인에 참여하는 두 테이블을 각가 분리해서 카운트해보고 적은 결과가 나오는 쪽을 선행 집합으로 함
-- 또한 WHERE 조건도 포함하여 카운트해야 하고, 선행 집합을 바꾸자 Buffers가 줄어들어 성능이 개선되었는지 실행게획을 확인해야 함

-- NL 조인 SQL
-- 고객건수 만큼 주문을 반복 접근
-- 1. M_CUS의 첫 번쨰 로우를 읽음
-- 2. 1번 단게의 CUS_ID와 같은 CUS_ID를 가진 데이터를 T_ORD에서 검색 (T_ORD 전체 읽음)
-- 3. M_CUS의 두 번쨰 로우를 읽음
-- 4. 3번 단게의 CUS_ID와 같은 CUS_ID를 가진 데이터를 T_ORD에서 검색 (T_ORD 전체 읽음)
-- 5. M_CUS의 세 번쨰 로우를 읽음
-- 6. 5번 단게의 CUS_ID와 같은 CUS_ID를 가진 데이터를 T_ORD에서 검색 (T_ORD 전체 읽음)
/*
-----------------------------------------------------------------------------------------------
| Id  | Operation          | Name  | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
-----------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT   |       |      1 |        |    501 |00:00:00.01 |     354 |     27 |
|   1 |  NESTED LOOPS      |       |      1 |   9141 |    501 |00:00:00.01 |     354 |     27 |
|   2 |   TABLE ACCESS FULL| M_CUS |      1 |     90 |     15 |00:00:00.01 |      11 |      6 |
|*  3 |   TABLE ACCESS FULL| T_ORD |     15 |    102 |    501 |00:00:00.01 |     343 |     21 |
-----------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_NL(O) */
	MC.RGN_ID
	 , MC.CUS_ID
	 , MC.CUS_NM
	 , O.ORD_DT
	 , O.ORD_ST
	 , O.ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD O ON O.CUS_ID = MC.CUS_ID
;

-- 후행 집합에 필요한 인덱스
-- 특정 고객의 특정일자 주문
-- NL 조인은 후행 테이블 쪽에 조인 조건 컬럼에 인덱스가 필수
-- 1. PK_M_CUS 인덱스를 이용해 CUS_ID=CUS_0009 조건에 맞는 데이틀 찾음
-- 2. 인덱스 리프 블록의 ROWID를 이용해 M_CUS의 실제 데이터에 접근
-- 3. M_CUS의 CUS_ID 값을 이용해 NL 조인을 처리함
-- 4. 3번에서 받은 CUS_ID와 같은 CUS_ID 값을 가진 데이터를 T_ORD_JOIN에서 찾음
-- : 이때 T_ORD_JOIN에는 CUS_ID에 대한 인덱스가 없음 그러므로 FULL_SCAN으로 처리
-- 5. 3번에서 받은 CUS_ID와 같고, ORD_YMD 조건이 만족하면 결과로 내보냄
/*
---------------------------------------------------------------------------------------------------------------
| Id  | Operation                     | Name       | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
---------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT              |            |      1 |        |      1 |00:00:00.11 |   26472 |  26454 |
|   1 |  SORT GROUP BY NOSORT         |            |      1 |    105 |      1 |00:00:00.11 |   26472 |  26454 |
|   2 |   NESTED LOOPS                |            |      1 |    105 |   2000 |00:00:00.02 |   26472 |  26454 |
|   3 |    TABLE ACCESS BY INDEX ROWID| M_CUS      |      1 |      1 |      1 |00:00:00.01 |       2 |      0 |
|*  4 |     INDEX UNIQUE SCAN         | PK_M_CUS   |      1 |      1 |      1 |00:00:00.01 |       1 |      0 |
|*  5 |    TABLE ACCESS FULL          | T_ORD_JOIN |      1 |    105 |   2000 |00:00:00.01 |   26470 |  26454 |
---------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS */
	MC.CUS_ID
	, MAX(MC.CUS_NM) AS CUS_NM
	, MAX(MC.CUS_GD) AS CUS_GD
	, COUNT(*) AS ORD_CNT
	, SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE MC.CUS_ID = 'CUS_0009'
AND TOJ.ORD_YMD = '20170218'
GROUP BY MC.CUS_ID
;

-- 후행 집합에 필요한 인덱스
-- 특정 고객의 특정일자 주문, T_ORD_JOIN(CUS_ID) 인덱스 사용
CREATE INDEX X_T_ORD_JOIN_1 ON T_ORD_JOIN(CUS_ID)
;
-- 1. PK_M_CUS 인덱스를 이용해 CUS_ID=CUS_0009 조건에 맞는 데이틀 찾음
-- 2. 인덱스 리프 블록의 ROWID를 이용해 M_CUS의 실제 데이터에 접근
-- 3. M_CUS의 CUS_ID 값을 이용해 NL 조인을 처리함
-- 4. 3번에서 받은 CUS_ID와 같은 CUS_ID 값을 가진 데이터를 T_ORD_JOIN에서 찾음
-- : 이때 X_T_ORD_JOIN_1을 이용해 INDEX RANGE SCAN으로 검색
-- 5. X_T_ORD_JOIN_1 리프 블록의 ROWID를 이용해 T_ORD_JOIN의 실제 데이터 접근
-- 6. ROWID로 접근한 데이터 블록에서 ORD_YMD의 WHERE 조건을 확인
-- : 조건이 맞으면 결과에 내보내고, 조건에 맞지 않으면 결과에서 버려짐
--
-- 하지만 여전히 비효울적인 부분이 있음
-- ORD_YMD는 인덱스에 없으므로 테이블에서 확인 필요함
-- 5번 단계와 6번 단계의 A-Rows 부분을 보면 6번단계에서 55,000건을 찾았지만, 5번단계에서는 최종 2,000건만 조인결과로 사용됨
-- 결과적으로 53,000건의 데이터가 버려짐, 이 비효율을 제거하려면 복합인덱스를 만들어야 함
/*
-------------------------------------------------------------------------------------------------------------------
| Id  | Operation                     | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
-------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT              |                |      1 |        |      1 |00:00:00.01 |     646 |    398 |
|   1 |  SORT GROUP BY NOSORT         |                |      1 |    138 |      1 |00:00:00.01 |     646 |    398 |
|   2 |   NESTED LOOPS                |                |      1 |    138 |   2000 |00:00:00.01 |     646 |    398 |
|   3 |    TABLE ACCESS BY INDEX ROWID| M_CUS          |      1 |      1 |      1 |00:00:00.01 |       2 |      1 |
|*  4 |     INDEX UNIQUE SCAN         | PK_M_CUS       |      1 |      1 |      1 |00:00:00.01 |       1 |      1 |
|*  5 |    TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |      1 |    138 |   2000 |00:00:00.01 |     644 |    397 |
|*  6 |     INDEX RANGE SCAN          | X_T_ORD_JOIN_1 |      1 |  55000 |  55000 |00:00:00.01 |     156 |    155 |
-------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   4 - access(""MC"".""CUS_ID""='CUS_0009')"
"   5 - filter(""TOJ"".""ORD_YMD""='20170218')"
"   6 - access(""TOJ"".""CUS_ID""='CUS_0009')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_NL(TOJ) INDEX(TOJ X_T_ORD_JOIN_1) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE MC.CUS_ID = 'CUS_0009'
AND TOJ.ORD_YMD = '20170218'
GROUP BY MC.CUS_ID
;

-- 후행 집합에 필요한 인덱스
-- 특정 고객의 특정일자 주문, T_ORD_JOIN(CUS_ID, ORD_YMD) 인덱스 사용
CREATE INDEX X_T_ORD_JOIN_2 ON T_ORD_JOIN(CUS_ID, ORD_YMD)
;
-- 위 쿼리를 비효율을 복합인덱스로 제거
/*
-------------------------------------------------------------------------------------------------------------------
| Id  | Operation                     | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
-------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT              |                |      1 |        |      1 |00:00:00.01 |      29 |     10 |
|   1 |  SORT GROUP BY NOSORT         |                |      1 |    138 |      1 |00:00:00.01 |      29 |     10 |
|   2 |   NESTED LOOPS                |                |      1 |    138 |   2000 |00:00:00.01 |      29 |     10 |
|   3 |    TABLE ACCESS BY INDEX ROWID| M_CUS          |      1 |      1 |      1 |00:00:00.01 |       2 |      0 |
|*  4 |     INDEX UNIQUE SCAN         | PK_M_CUS       |      1 |      1 |      1 |00:00:00.01 |       1 |      0 |
|   5 |    TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |      1 |    138 |   2000 |00:00:00.01 |      27 |     10 |
|*  6 |     INDEX RANGE SCAN          | X_T_ORD_JOIN_2 |      1 |    138 |   2000 |00:00:00.01 |      11 |     10 |
-------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   4 - access(""MC"".""CUS_ID""='CUS_0009')"
"   6 - access(""TOJ"".""CUS_ID""='CUS_0009' AND ""TOJ"".""ORD_YMD""='20170218')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_NL(TOJ) INDEX(TOJ X_T_ORD_JOIN_2) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE MC.CUS_ID = 'CUS_0009'
AND TOJ.ORD_YMD = '20170218'
GROUP BY MC.CUS_ID
;

-- 선행 집합 변경에 따른 쿼리 변형
-- 특정 고객의 특정일자 주문, T_ORD_JOIN을 선행 집합으로 사용
-- Predicate Information 에서 5번의 access 조건으로 CUS_ID를 사용하고 있다는 점인데 T_ORD_JOIN에 CUS_ID 조건을 준 적이 없음
-- X_T_ORD_JOIN_2 는 CUS_ID, ORD_YMD 순서로 구성된 인덱스로 CUS_ID 조건이 같다(=) 조건으로 사용되어야만 ORD_YMD 조건도 호율적임
-- 오라클 옵티마이져가 CUS_ID 조건으로 T_ORD_JOIN 쪽에도 자동으로 추가해 준 것임
/*
-----------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |
-----------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |      1 |00:00:00.01 |    2031 |
|   1 |  SORT GROUP BY NOSORT          |                |      1 |    138 |      1 |00:00:00.01 |    2031 |
|   2 |   NESTED LOOPS                 |                |      1 |    138 |   2000 |00:00:00.01 |    2031 |
|   3 |    NESTED LOOPS                |                |      1 |    138 |   2000 |00:00:00.01 |      31 |
|   4 |     TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |      1 |    138 |   2000 |00:00:00.01 |      27 |
|*  5 |      INDEX RANGE SCAN          | X_T_ORD_JOIN_2 |      1 |    138 |   2000 |00:00:00.01 |      11 |
|*  6 |     INDEX UNIQUE SCAN          | PK_M_CUS       |   2000 |      1 |   2000 |00:00:00.01 |       4 |
|   7 |    TABLE ACCESS BY INDEX ROWID | M_CUS          |   2000 |      1 |   2000 |00:00:00.01 |    2000 |
-----------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   5 - access(""TOJ"".""CUS_ID""='CUS_0009' AND ""TOJ"".""ORD_YMD""='20170218')"
"   6 - access(""MC"".""CUS_ID""='CUS_0009')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(TOJ) USE_NL(MC) INDEX(TOJ X_T_ORD_JOIN_2) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE MC.CUS_ID = 'CUS_0009'
AND TOJ.ORD_YMD = '20170218'
GROUP BY MC.CUS_ID
;

-- 조인 횟수를 줄이자 #1
-- CUS_GD가 A, ORD_YMD가 20170218인 주문 조회, T_ORD_JOIN이 선행 집합
-- NL 조인의 선행 집합 건수를 줄이면 후행 집합의 접근 횟수가 저절로 줄어들어 성능에 좋아짐
-- NL 조인이 2, 3번 단계에 총 두 번 나타나는데 오라클의 버전이 올라가면 NL 조인 성능을 높이려는 방법 정도로 이해하면 됨
-- : 일반적으로 두개의 테이블 조인하면 한 번의 조인 과정만 나옴
-- 5번 단계를 보면 INDEX SKIP SCAN 이 있는데 인덱스를 이용해 데이터를 검색하는 방법 중 하나
-- X_T_ORD_JOIN은 CUS_ID, ORD_YMD 순서로 구성되어 있는데 SQL에 CUS_ID에 대한 조건이 없고 ORD_YMD에 대한 조건만 존재하므로 INDEX SKIP SCAN를 활용하게 된 것
-- ORD_YMD가 선두인 인덱스를 만들어 INDEX RANGE SCAN이 나아도록 하는 것 이 좋음
-- 6번 단계의 Starts를 보면 12,000 번의 후행 집합에 대한 접근이 발생하는데 그 말은 M_CUS에는 INDEX_RANGE_SCAN이 12,000번 발생하고 있음
-- 성능 개선을 위해서라면 12,000번의 접근 횟수를 줄일 필요가 있음
/*
--------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |
--------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |      6 |00:00:00.03 |   12384 |    276 |
|   1 |  SORT GROUP BY NOSORT          |                |      1 |     60 |      6 |00:00:00.03 |   12384 |    276 |
|   2 |   NESTED LOOPS                 |                |      1 |   8076 |   9000 |00:00:00.03 |   12384 |    276 |
|   3 |    NESTED LOOPS                |                |      1 |   8076 |  12000 |00:00:00.03 |     384 |    275 |
|   4 |     TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |      1 |   8076 |  12000 |00:00:00.02 |     380 |    275 |
|*  5 |      INDEX SKIP SCAN           | X_T_ORD_JOIN_2 |      1 |   8076 |  12000 |00:00:00.01 |     282 |    271 |
|*  6 |     INDEX UNIQUE SCAN          | PK_M_CUS       |  12000 |      1 |  12000 |00:00:00.01 |       4 |      0 |
|*  7 |    TABLE ACCESS BY INDEX ROWID | M_CUS          |  12000 |      1 |   9000 |00:00:00.01 |   12000 |      1 |
--------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   5 - access(""TOJ"".""ORD_YMD""='20170218')"
"       filter(""TOJ"".""ORD_YMD""='20170218')"
"   6 - access(""TOJ"".""CUS_ID""=""MC"".""CUS_ID"")"
"   7 - filter(""MC"".""CUS_GD""='A')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(TOJ) USE_NL(MC) INDEX(TOJ X_T_ORD_JOIN_2) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE TOJ.ORD_YMD = '20170218'
AND MC.CUS_GD = 'A'
GROUP BY MC.CUS_ID
;

-- CUS_GD가 A, ORD_YMD가 20170218인 주문 조회, M_CUS를 선행 집합으로 처리
/*
-----------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |
-----------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |      6 |00:00:00.01 |     235 |
|   1 |  SORT GROUP BY NOSORT          |                |      1 |     60 |      6 |00:00:00.01 |     235 |
|   2 |   NESTED LOOPS                 |                |      1 |  16152 |   9000 |00:00:00.01 |     235 |
|   3 |    NESTED LOOPS                |                |      1 |  16152 |   9000 |00:00:00.01 |     161 |
|*  4 |     TABLE ACCESS BY INDEX ROWID| M_CUS          |      1 |     60 |     60 |00:00:00.01 |       3 |
|   5 |      INDEX FULL SCAN           | PK_M_CUS       |      1 |     90 |     90 |00:00:00.01 |       1 |
|*  6 |     INDEX RANGE SCAN           | X_T_ORD_JOIN_2 |     60 |     90 |   9000 |00:00:00.01 |     158 |
|   7 |    TABLE ACCESS BY INDEX ROWID | T_ORD_JOIN     |   9000 |    269 |   9000 |00:00:00.01 |      74 |
-----------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   4 - filter(""MC"".""CUS_GD""='A')"
"   6 - access(""TOJ"".""CUS_ID""=""MC"".""CUS_ID"" AND ""TOJ"".""ORD_YMD""='20170218')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_NL(TOJ) INDEX(TOJ X_T_ORD_JOIN_2) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE TOJ.ORD_YMD = '20170218'
AND MC.CUS_GD = 'A'
GROUP BY MC.CUS_ID
;

-- 조인 횟수를 줄이자 #2
-- T_ORD_JOIN에 범위조건(LIKE) 사용
-- 6번 단계의 Starts 가 209K(209,000)로, 후행집합을 209,000번 반복 접근을 하고 있음
/*
-------------------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                              | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
-------------------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                       |                |      1 |        |     72 |00:00:00.60 |     211K|   2134 |       |       |          |
|   1 |  HASH GROUP BY                         |                |      1 |     90 |     72 |00:00:00.60 |     211K|   2134 |   775K|   775K| 2496K (0)|
|   2 |   NESTED LOOPS                         |                |      1 |    205K|    209K|00:00:00.51 |     211K|   2134 |       |       |          |
|   3 |    NESTED LOOPS                        |                |      1 |    205K|    209K|00:00:00.33 |    2289 |   2134 |       |       |          |
|   4 |     TABLE ACCESS BY INDEX ROWID BATCHED| T_ORD_JOIN     |      1 |    205K|    209K|00:00:00.15 |    2285 |   2134 |       |       |          |
|*  5 |      INDEX RANGE SCAN                  | X_T_ORD_JOIN_3 |      1 |    205K|    209K|00:00:00.05 |     585 |    550 |       |       |          |
|*  6 |     INDEX UNIQUE SCAN                  | PK_M_CUS       |    209K|      1 |    209K|00:00:00.11 |       4 |      0 |       |       |          |
|   7 |    TABLE ACCESS BY INDEX ROWID         | M_CUS          |    209K|      1 |    209K|00:00:00.14 |     209K|      0 |       |       |          |
-------------------------------------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   5 - access(""TOJ"".""ORD_YMD"" LIKE '201702%')"
"       filter(""TOJ"".""ORD_YMD"" LIKE '201702%')"
"   6 - access(""TOJ"".""CUS_ID""=""MC"".""CUS_ID"")"
*/
CREATE INDEX X_T_ORD_JOIN_3 ON T_ORD_JOIN(ORD_YMD)
;
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(TOJ) USE_NL(MC) INDEX(TOJ X_T_ORD_JOIN_3) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE TOJ.ORD_YMD LIKE '201702%'
GROUP BY MC.CUS_ID
;

-- 각각의 테이블 카운트
SELECT COUNT(*) FROM M_CUS; -- 90
SELECT COUNT(*) FROM T_ORD_JOIN WHERE ORD_YMD LIKE '201702%'; -- 209000

-- T_ORD_JOIN에 범위조건(LIKE) 사용, M_CUS를 선행 집합으로 사용
/*
-----------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers |
-----------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |     72 |00:00:00.20 |    2889 |
|   1 |  SORT GROUP BY NOSORT          |                |      1 |     90 |     72 |00:00:00.20 |    2889 |
|   2 |   NESTED LOOPS                 |                |      1 |    616K|    209K|00:00:00.19 |    2889 |
|   3 |    NESTED LOOPS                |                |      1 |    616K|    209K|00:00:00.06 |    1027 |
|   4 |     TABLE ACCESS BY INDEX ROWID| M_CUS          |      1 |     90 |     90 |00:00:00.01 |       3 |
|   5 |      INDEX FULL SCAN           | PK_M_CUS       |      1 |     90 |     90 |00:00:00.01 |       1 |
|*  6 |     INDEX RANGE SCAN           | X_T_ORD_JOIN_2 |     90 |   2283 |    209K|00:00:00.04 |    1024 |
|   7 |    TABLE ACCESS BY INDEX ROWID | T_ORD_JOIN     |    209K|   6849 |    209K|00:00:00.07 |    1862 |
-----------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

"   6 - access(""TOJ"".""CUS_ID""=""MC"".""CUS_ID"" AND ""TOJ"".""ORD_YMD"" LIKE '201702%')"
"       filter(""TOJ"".""ORD_YMD"" LIKE '201702%')"
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_NL(TOJ) INDEX(TOJ X_T_ORD_JOIN_2) */
	MC.CUS_ID
	 , MAX(MC.CUS_NM) AS CUS_NM
	 , MAX(MC.CUS_GD) AS CUS_GD
	 , COUNT(*) AS ORD_CNT
	 , SUM(TOJ.ORD_QTY * TOJ.UNT_PRC) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE TOJ.ORD_YMD LIKE '201702%'
GROUP BY MC.CUS_ID
;

-- 여러 테이블 조인
-- 3개 테이블 조인
/*
-----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |      7 |00:00:00.09 |     986 |      9 |       |       |          |
|   1 |  HASH GROUP BY                 |                |      1 |    100 |      7 |00:00:00.09 |     986 |      9 |  1149K|  1149K|  896K (0)|
|*  2 |   HASH JOIN                    |                |      1 |  25256 |  10000 |00:00:00.06 |     986 |      9 |  1538K|  1538K| 1342K (0)|
|*  3 |    TABLE ACCESS FULL           | M_ITM          |      1 |     10 |     10 |00:00:00.01 |       7 |      6 |       |       |          |
|   4 |    NESTED LOOPS                |                |      1 |    202K|  70000 |00:00:00.09 |     979 |      3 |       |       |          |
|   5 |     NESTED LOOPS               |                |      1 |    202K|  70000 |00:00:00.03 |     352 |      3 |       |       |          |
|*  6 |      TABLE ACCESS FULL         | M_CUS          |      1 |     30 |     30 |00:00:00.03 |       7 |      3 |       |       |          |
|*  7 |      INDEX RANGE SCAN          | X_T_ORD_JOIN_2 |     30 |   2283 |  70000 |00:00:00.02 |     345 |      0 |       |       |          |
|   8 |     TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |  70000 |   6849 |  70000 |00:00:00.03 |     627 |      0 |       |       |          |
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

-- 각각의 테이블 카운트
SELECT COUNT(*) FROM M_CUS MC WHERE MC.CUS_GD = 'B'; -- 30
SELECT COUNT(*) FROM M_ITM MI WHERE MI.ITM_TP = 'ELEC'; -- 10

-- 각 조인 상황별로 카운트
-- 70,000
SELECT COUNT(*) AS CNT
FROM M_CUS MC
    INNER JOIN T_ORD_JOIN TOJ ON TOJ.CUS_ID = MC.CUS_ID
WHERE MC.CUS_GD = 'B'
AND TOJ.ORD_YMD LIKE '201702%'
;

-- 26,000
SELECT COUNT(*) AS CNT
FROM M_ITM MI
	INNER JOIN T_ORD_JOIN TOJ ON TOJ.ITM_ID = MI.ITM_ID
WHERE MI.ITM_TP = 'ELEC'
AND TOJ.ORD_YMD LIKE '201702%'
;

-- 3개 테이블 조인, M_ITM와 T_ORD_JOIN을 먼저 처리
CREATE INDEX X_T_ORD_JOIN_4 ON T_ORD_JOIN(ITM_ID, ORD_YMD)
;
/*
-----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name           | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
-----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |                |      1 |        |      7 |00:00:00.02 |     372 |    114 |       |       |          |
|   1 |  HASH GROUP BY                 |                |      1 |    100 |      7 |00:00:00.02 |     372 |    114 |  1149K|  1149K|  896K (0)|
|*  2 |   HASH JOIN                    |                |      1 |  19304 |  10000 |00:00:00.02 |     372 |    114 |  1922K|  1922K| 1590K (0)|
|*  3 |    TABLE ACCESS FULL           | M_CUS          |      1 |     30 |     30 |00:00:00.01 |       7 |      0 |       |       |          |
|   4 |    NESTED LOOPS                |                |      1 |  19631 |  26000 |00:00:00.03 |     365 |    114 |       |       |          |
|   5 |     NESTED LOOPS               |                |      1 |  24540 |  26000 |00:00:00.01 |     127 |    114 |       |       |          |
|*  6 |      TABLE ACCESS FULL         | M_ITM          |      1 |     10 |     10 |00:00:00.01 |       7 |      0 |       |       |          |
|*  7 |      INDEX RANGE SCAN          | X_T_ORD_JOIN_4 |     10 |   2454 |  26000 |00:00:00.01 |     120 |    114 |       |       |          |
|   8 |     TABLE ACCESS BY INDEX ROWID| T_ORD_JOIN     |  26000 |   1963 |  26000 |00:00:00.01 |     238 |      0 |       |       |          |
-----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS USE_NL(TOJ) INDEX(TOJ X_T_ORD_JOIN_4) */
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

-- 선행 집합은 항상 작은 쪽이어야 하는가?
-- NL 조인 성능 테스트, M_CUS를 선행으로 NL 조인
-- 데이터가 적은 M_CUS를 선행 집합으로 NL 치리
-- T_ORD_BIG에서 한 달간의 데이터를 인덱스로 읽기에는 무리가 있음, 인덱스로 읽어야 할 데이터가 너무 많기 때문
/*
----------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name          | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
----------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |               |      1 |        |     81 |00:00:18.34 |    2440K|    442K|       |       |          |
|   1 |  SORT ORDER BY                 |               |      1 |   5728 |     81 |00:00:18.34 |    2440K|    442K|  9216 |  9216 | 8192  (0)|
|   2 |   HASH GROUP BY                |               |      1 |   5728 |     81 |00:00:18.34 |    2440K|    442K|  1041K|  1041K| 1480K (0)|
|   3 |    NESTED LOOPS                |               |      1 |   7042K|   2430K|00:00:19.63 |    2440K|    442K|       |       |          |
|   4 |     NESTED LOOPS               |               |      1 |   7042K|   2430K|00:00:02.05 |   10111 |  10027 |       |       |          |
|   5 |      TABLE ACCESS FULL         | M_CUS         |      1 |     90 |     90 |00:00:00.01 |       7 |      0 |       |       |          |
|*  6 |      INDEX RANGE SCAN          | X_T_ORD_BIG_4 |     90 |  26084 |   2430K|00:00:00.89 |   10104 |  10027 |       |       |          |
|   7 |     TABLE ACCESS BY INDEX ROWID| T_ORD_BIG     |   2430K|  78253 |   2430K|00:00:16.28 |    2430K|    432K|       |       |          |
----------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(MC) USE_NL(TOB) INDEX(TOB X_T_ORD_BIG_4) */
	MC.CUS_ID
	, MC.CUS_NM
	, SUM(TOB.ORD_AMT) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
WHERE TOB.ORD_YMD LIKE '201701%'
GROUP BY MC.CUS_ID, MC.CUS_NM
ORDER BY SUM(TOB.ORD_AMT) DESC
;

-- NL 조인 성능 테스트, T_ORD_BIG을 선행으로 NL 조인
/*
------------------------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                      | Name      | Starts | E-Rows | A-Rows |   A-Time   | Buffers | Reads  |  OMem |  1Mem | Used-Mem |
------------------------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT               |           |      1 |        |     81 |00:00:09.03 |    2688K|    258K|       |       |          |
|   1 |  SORT ORDER BY                 |           |      1 |   5728 |     81 |00:00:09.03 |    2688K|    258K|  9216 |  9216 | 8192  (0)|
|   2 |   HASH GROUP BY                |           |      1 |   5728 |     81 |00:00:09.03 |    2688K|    258K|  1041K|  1041K| 1480K (0)|
|   3 |    NESTED LOOPS                |           |      1 |   2347K|   2430K|00:00:10.57 |    2688K|    258K|       |       |          |
|   4 |     NESTED LOOPS               |           |      1 |   2347K|   2430K|00:00:08.60 |     258K|    258K|       |       |          |
|*  5 |      TABLE ACCESS FULL         | T_ORD_BIG |      1 |   2347K|   2430K|00:00:06.57 |     258K|    258K|       |       |          |
|*  6 |      INDEX UNIQUE SCAN         | PK_M_CUS  |   2430K|      1 |   2430K|00:00:01.22 |       4 |      0 |       |       |          |
|   7 |     TABLE ACCESS BY INDEX ROWID| M_CUS     |   2430K|      1 |   2430K|00:00:01.30 |    2430K|      0 |       |       |          |
------------------------------------------------------------------------------------------------------------------------------------------
*/
SELECT /*+ GATHER_PLAN_STATISTICS LEADING(TOB) USE_NL(MC) FULL(TOB) */
	MC.CUS_ID
	 , MC.CUS_NM
	 , SUM(TOB.ORD_AMT) AS ORD_AMT
FROM M_CUS MC
	INNER JOIN T_ORD_BIG TOB ON TOB.CUS_ID = MC.CUS_ID
WHERE TOB.ORD_YMD LIKE '201701%'
GROUP BY MC.CUS_ID, MC.CUS_NM
ORDER BY SUM(TOB.ORD_AMT) DESC
;

-- 테스트용 테이블인 T_ORD_JOIN 테이블 추가
CREATE TABLE T_ORD_JOIN AS
	SELECT
		ROW_NUMBER() OVER (ORDER BY O.ORD_SEQ, TOD.ORD_DET_NO, T.RNO) AS ORD_SEQ
		 , O.CUS_ID
		 , O.ORD_DT
		 , O.ORD_ST
		 , O.PAY_TP
		 , TOD.ITM_ID
		 , TOD.ORD_QTY
		 , TOD.UNT_PRC
		 , TO_CHAR(O.ORD_DT, 'YYYYMMDD') AS ORD_YMD
	FROM T_ORD O
	   , T_ORD_DET TOD
	   , (
		SELECT ROWNUM AS RNO
		FROM DUAL CONNECT BY ROWNUM <= 1000
	) T
	WHERE O.ORD_SEQ = TOD.ORD_SEQ
;

ALTER TABLE T_ORD_JOIN
	ADD CONSTRAINT PK_T_ORD_JOIN PRIMARY KEY (ORD_SEQ) USING INDEX
;

CALL DBMS_STATS.GATHER_TABLE_STATS('ORA_TEST_USER', 'T_ORD_JOIN')
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
FROM TABLE (DBMS_XPLAN.DISPLAY_CURSOR('1bs308vk85nw8', 0, 'ALLSTATS LAST'))
;
