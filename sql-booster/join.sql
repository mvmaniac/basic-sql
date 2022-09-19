-- 이너-조인의 특징
-- 1. 조인 조건을 만족하는 데이터만 결합되어 결과에 나옴
-- (이때, 조인 조건은 같다(=) 뿐만 아니라, 다른 조건식도 사용할 수 있음. 예를들어 같지않다(!=) T1.COL != T2.COL)
-- 2. 한 건과 M(Many)건이 조인되면 M건의 결과가 나옴
-- (T1의 B 한 건이 T2의 B 두 건과 결합해 두건의 결과가 나옴)

-- 여러 테이블을 조인하는 경우
-- 1. 한 순간에는 두 개의 데이터 집합에 대해서만 조인 발생
-- 2. 조인이 이루어진 두 개의 데이터 집합은 하나의 새로운 하나의 데이터 집합
-- 3. 테이블 간의 관계를 이해하고 조인을 작성

-- 특정 고객의 17년 3월의 아이템평가 (T_ITM_EVL) 기록과 3월 주문(T_ORD)에 대해
-- 고객ID, 고객명, 아이템평가건수, 주문건수를 출력
-- M:1:M 조인 결과는 제대로 나오나 잘못된 이너조인의 예
SELECT
    C.CUS_ID
    , C.CUS_NM
    , COUNT(DISTINCT TIE.ITM_ID || '-' || TO_CHAR(TIE.EVL_LST_NO)) AS EVAL_CNT
    , COUNT(DISTINCT O.ORD_SEQ) AS ORD_CNT
FROM M_CUS C
    INNER JOIN T_ITM_EVL TIE ON C.CUS_ID = TIE.CUS_ID
        AND TIE.EVL_DT >= TO_DATE('20170301', 'YYYYMMDD')
        AND TIE.EVL_DT < TO_DATE('20170401', 'YYYYMMDD')

    INNER JOIN T_ORD O ON C.CUS_ID = O.CUS_ID
        AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
        AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
WHERE C.CUS_ID = 'CUS_0023'
GROUP BY C.CUS_ID, C.CUS_NM
;

-- M:1:M 조인 해결, UNION ALL 사용 #1
SELECT
    T.CUS_ID
    , MAX(T.CUS_NM) AS CUS_NM
    , SUM(T.EVL_CNT) AS EVL_CNT
    , SUM(T.ORD_CNT) AS ORD_CNT
FROM (
    SELECT C.CUS_ID, MAX(C.CUS_NM) AS CUS_NM, COUNT(*) AS EVL_CNT, NULL AS ORD_CNT
    FROM M_CUS C
         INNER JOIN T_ITM_EVL TIE ON C.CUS_ID = TIE.CUS_ID
            AND TIE.EVL_DT >= TO_DATE('20170301', 'YYYYMMDD')
            AND TIE.EVL_DT < TO_DATE('20170401', 'YYYYMMDD')
    WHERE C.CUS_ID = 'CUS_0023'
    GROUP BY C.CUS_ID, C.CUS_NM

    UNION ALL

    SELECT C.CUS_ID, MAX(C.CUS_NM) AS CUS_NM, NULL AS EVL_CNT, COUNT(*) AS ORD_CNT
    FROM M_CUS C
        INNER JOIN T_ORD O ON C.CUS_ID = O.CUS_ID
           AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
           AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
    WHERE C.CUS_ID = 'CUS_0023'
    GROUP BY C.CUS_ID, C.CUS_NM
) T
GROUP BY T.CUS_ID
;

-- M:1:M 조인 해결, UNION ALL 사용 #2
SELECT
    C.CUS_ID
     , MAX(C.CUS_NM) AS CUS_NM
     , SUM(T.EVL_CNT) AS EVL_CNT
     , SUM(T.ORD_CNT) AS ORD_CNT
FROM M_CUS C
    INNER JOIN (
        SELECT TIE.CUS_ID, COUNT(*) AS EVL_CNT, NULL AS ORD_CNT
        FROM T_ITM_EVL TIE
        WHERE TIE.CUS_ID = 'CUS_0023'
		AND TIE.EVL_DT >= TO_DATE('20170301', 'YYYYMMDD')
		AND TIE.EVL_DT < TO_DATE('20170401', 'YYYYMMDD')
        GROUP BY TIE.CUS_ID

        UNION ALL

        SELECT O.CUS_ID, NULL AS EVL_CNT, COUNT(*) AS ORD_CNT
        FROM T_ORD O
        WHERE O.CUS_ID = 'CUS_0023'
		AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
		AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
        GROUP BY O.CUS_ID
     ) T ON C.CUS_ID = T.CUS_ID
GROUP BY C.CUS_ID
;

-- M:1:M 조인 해결, 1:1:1로 조인
SELECT
    C.CUS_ID
    , C.CUS_NM
    , TIE.EVL_CNT
    , O.ORD_CNT
FROM M_CUS C
    INNER JOIN (
        SELECT TIE.CUS_ID, COUNT(*) AS EVL_CNT
        FROM T_ITM_EVL TIE
        WHERE TIE.CUS_ID = 'CUS_0023'
	  	AND TIE.EVL_DT >= TO_DATE('20170301', 'YYYYMMDD')
		AND TIE.EVL_DT < TO_DATE('20170401', 'YYYYMMDD')
        GROUP BY TIE.CUS_ID
    ) TIE ON C.CUS_ID = TIE.CUS_ID

    INNER JOIN (
        SELECT O.CUS_ID, COUNT(*) AS ORD_CNT
        FROM T_ORD O
        WHERE O.CUS_ID = 'CUS_0023'
		AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
		AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
        GROUP BY O.CUS_ID
    ) O ON C.CUS_ID = O.CUS_ID
WHERE C.CUS_ID = 'CUS_0023'
;

-- 아우터-조인
-- 1. 기준 데이터 집합: 아우터-조인의 기준이 되는 집합
-- 2. 참조 데이터 집합: 아우터-조인의 참조가 되는 집합

-- OUTER JOIN 테스트
SELECT
    C.CUS_ID
    , C.CUS_NM
    , TIE.CUS_ID
    , TIE.ITM_ID
    , TIE.EVL_LST_NO
    , TIE.EVL_DT
FROM M_CUS C
    , T_ITM_EVL TIE
WHERE C.CUS_ID IN ('CUS_0073')
AND C.CUS_ID = TIE.CUS_ID(+)
AND TIE.EVL_DT(+) >= TO_DATE('20170201', 'YYYYMMDD')
AND TIE.EVL_DT(+) < TO_DATE('20170301', 'YYYYMMDD')
;

-- OUTER JOIN 테스트, ANSI 쿼리로 변경
SELECT
    C.CUS_ID
     , C.CUS_NM
     , TIE.CUS_ID
     , TIE.ITM_ID
     , TIE.EVL_LST_NO
     , TIE.EVL_DT
FROM M_CUS C
    LEFT OUTER JOIN T_ITM_EVL TIE ON C.CUS_ID = TIE.CUS_ID
        AND TIE.EVL_DT >= TO_DATE('20170201', 'YYYYMMDD')
        AND TIE.EVL_DT < TO_DATE('20170301', 'YYYYMMDD')
WHERE C.CUS_ID IN ('CUS_0073')
;

-- 아이템 ID별 주문수량
-- 'PC', 'ELEC' 아이템 유형의 아이템별 주문수량 조회 (주문이 없어도 0으로 나와야 한다)
SELECT
    MI.ITM_ID
    , MI.ITM_NM
    , NVL(T.ORD_QTY, 0)
FROM M_ITM MI
    LEFT OUTER JOIN (
        SELECT TOD.ITM_ID, SUM(TOD.ORD_QTY) AS ORD_QTY
        FROM T_ORD O
            INNER JOIN T_ORD_DET TOD ON O.ORD_SEQ = TOD.ORD_SEQ
                AND O.ORD_ST = 'COMP'
                AND O.ORD_DT >= TO_DATE('20170101', 'YYYYMMDD')
                AND O.ORD_DT < TO_DATE('20170201', 'YYYYMMDD')
        GROUP BY TOD.ITM_ID
    ) T ON MI.ITM_ID = T.ITM_ID
WHERE MI.ITM_TP IN ('PC', 'ELEC')
ORDER BY MI.ITM_TP, MI.ITM_ID
;