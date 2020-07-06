-- 17년 8월 총 주문금액 구하기, SELECT 절 단독 서브쿼리
-- SELECT 절의 서브쿼리가 단독 실해이 가능해서 단독 서브쿼리
SELECT TO_CHAR(O.ORD_DT, 'YYYYMMDD') AS ORD_YMD
    , SUM(O.ORD_AMT) AS ORD_AMT
    , (
        SELECT SUM(OS.ORD_AMT)
        FROM T_ORD OS
        WHERE OS.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
        AND OS.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
    ) AS TOTAL_ORD_AMT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMMDD')
ORDER BY ORD_YMD
;

-- 17년 8월 총 주문금액, 주문일자의 주문금액비율 구하기, SELECT 절 단독 서브쿼리
-- 주문금액비율 = 주문일자별 주문금액(ORD_AMT) / 17년 8월 주문 총 금액 (TOTAL_ORD_AMT) * 100.00
SELECT TO_CHAR(O.ORD_DT, 'YYYYMMDD') AS ORD_YMD
     , SUM(O.ORD_AMT) AS ORD_AMT
     , (
         SELECT SUM(OS.ORD_AMT)
         FROM T_ORD OS
         WHERE OS.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
         AND OS.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
     ) AS TOTAL_ORD_AMT
     , ROUND(
         SUM(O.ORD_AMT) / (
            SELECT SUM(OS.ORD_AMT)
            FROM T_ORD OS
            WHERE OS.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
            AND OS.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
         ) * 100, 2
     ) AS ORD_AMT_RT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMMDD')
ORDER BY ORD_YMD
;

-- 17년 8월 총 주문금액, 주문일자의 주문금액비율 구하기, 인라인 뷰로 변경
SELECT T.ORD_YMD
    , T.ORD_AMT
    , T.TOTAL_ORD_AMT
    , ROUND(T.ORD_AMT / T.TOTAL_ORD_AMT * 100, 2) AS ORD_AMT_RT
FROM (
    SELECT TO_CHAR(O.ORD_DT, 'YYYYMMDD') AS ORD_YMD
         , SUM(O.ORD_AMT) AS ORD_AMT
         , (
             SELECT SUM(OS.ORD_AMT)
             FROM T_ORD OS
             WHERE OS.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
             AND OS.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
         ) AS TOTAL_ORD_AMT
    FROM T_ORD O
    WHERE O.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
    AND O.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
    GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMMDD')
) T
ORDER BY T.ORD_YMD
;

-- 코드값을 가져오는 SELECT 절 상관 서브쿼리
-- 서브쿼리 캐싱이라고 해서 서브쿼리 입력값과 결괏값을 캐시에 저장해 놓고 재사용하는 것을 뜻함
-- 캐시는 무제한이 아니므로 코드와 같이 값의 종류가 작을 때 서브쿼리 캐싱 효과를 극대화 할 수 있음
SELECT MI.ITM_TP
    , (
        SELECT CBC.BAS_CD_NM
        FROM C_BAS_CD CBC
        WHERE CBC.BAS_CD_DV = 'ITM_TP'
        AND CBC.BAS_CD = MI.ITM_TP
        AND CBC.LNG_CD = 'KO'
    ) AS ITM_TP_NM
    , MI.ITM_ID
    , MI.ITM_NM
FROM M_ITM MI
;

-- 고객정보를 가져오는 반복되는 SELECT 절 상관 서브쿼리
SELECT O.CUS_ID
    , TO_CHAR(O.ORD_DT, 'YYYYMMDD') AS ORD_YMD
    , (SELECT MC.CUS_NM FROM M_CUS MC WHERE MC.CUS_ID = O.CUS_ID) AS CUS_NM
    , (SELECT MC.CUS_GD FROM M_CUS MC WHERE MC.CUS_ID = O.CUS_ID) AS CUS_GD
    , O.ORD_AMT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
;

-- SELECT 절의 서브쿼리는 가장 바깥의 SELECT 절에만 사용하도록 함
-- 기본적으로 상관 서브쿼리 메인 SQL의 결과 건수 만큼 반복 수행 됨
-- 상관 서브쿼리 사용 가이드
-- 1. 상관 서브쿼리에서 사용되는 WHERE 절의 컬럼은 적절한 인덱스가 필수
-- * 상관 서브쿼리의 WHERE 절 컬럼에 인덱스가 있어야만 성능을 보장
-- * 인덱스가 있어도 성능이 필요한 만큼 나오지 않으면 SQL 자체를 변경 (상관 서브쿼리 제거)

-- 2. 메인 SQL에서 조회하는 결과 건수가 작을 때만 상관 서브쿼리를 사용
-- * 메인 SQL의 결과가 많을 수록 성능이 나빠질 가능성 큼

-- 3. 코드처럼 값의 종류가 작을 때는 상관 서브쿼리를 사용하면 성능이 좋아 질 수 있음

-- 4. 가능하면 상관 서브쿼리를 사용하지 않는 습관을 들임
-- * 상관 서브쿼리보다 조인을 사용하는 것이 SQL 실력에 도움이 됨

-- SELECT 절 서브쿼리, 단일 값
-- 고객 이름과 등급을 합쳐서 하나의 컬럼으로 처리
-- 단가(UNT_PRC)와 주문수량(ORD_QTY)를 곱해서 주문금액으로 처리
SELECT O.ORD_DT
    , O.CUS_ID
    , (SELECT MC.CUS_NM ||'(' || MC.CUS_GD || ')' FROM M_CUS MC WHERE MC.CUS_ID = O.CUS_ID) AS CUS_NM_GD
    , (SELECT SUM(TOD.UNT_PRC * TOD.ORD_QTY) FROM T_ORD_DET TOD WHERE TOD.ORD_SEQ = O.ORD_SEQ) AS ORD_AMT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170801', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170901', 'YYYYMMDD')
;

-- 고객별 마지막 주문금액
-- 고객의 모든 주문을 읽어야 하므로 성능이 좋지 않을 수 있음
SELECT MC.CUS_ID
    , MC.CUS_NM
    , (
        SELECT TO_NUMBER(
            SUBSTR(
                MAX(LPAD(TO_CHAR(O.ORD_SEQ), 8, '0') || TO_CHAR(O.ORD_AMT))
                , 9
            )
        )
        FROM T_ORD O
        WHERE O.CUS_ID = MC.CUS_ID
    ) AS LAST_ORD_AMT
FROM M_CUS MC
ORDER BY MC.CUS_ID
;

-- 위 쿼리를 중첩된 서브쿼리로 개선
-- 조회되는 건수가 작을 때만 이와 같은 방법을 사용
SELECT MC.CUS_ID
     , MC.CUS_NM
     , (
         SELECT O.ORD_AMT
         FROM T_ORD O
         WHERE O.ORD_SEQ = (
            SELECT MAX(O2.ORD_SEQ)
            FROM T_ORD O2
            WHERE O2.CUS_ID = MC.CUS_ID
         )
     ) AS LAST_ORD_AMT
FROM M_CUS MC
ORDER BY MC.CUS_ID
;

-- WHERE절 단독 서브쿼리
-- 마지막 주문 한 건 조회, ORD_SEQ가 가장 큰 데이터가 마지막 주문
-- 아래 쿼리는 T_ORD 테이블를 2번 사용함
SELECT *
FROM T_ORD O
WHERE O.ORD_SEQ = (SELECT MAX(O2.ORD_SEQ) FROM T_ORD O2)
;

-- 마지막 주문 한 건 조회, ORDER BY와 ROWNUM을 사용
-- ORD_SEQ에 대한 인덱스가 필수
SELECT *
FROM (
    SELECT *
    FROM T_ORD O
    ORDER BY O.ORD_SEQ DESC
) T
WHERE ROWNUM <= 1
;

-- IN 조건으로 사용하기
-- 3월 주문건수가 4건 이상인 고객의 3월달 주문 리스트
SELECT *
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
AND O.CUS_ID IN (
    SELECT O2.CUS_ID
    FROM T_ORD O2
    WHERE O2.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
    AND O2.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
    GROUP BY O2.CUS_ID
    HAVING COUNT(*) >= 4
)
ORDER BY O.CUS_ID
;

-- 3월 주문건수가 4건 이상인 고객의 3월달 주문 리스트, 조인으로 처리
SELECT O.*
FROM T_ORD O
    INNER JOIN (
        SELECT O2.CUS_ID
        FROM T_ORD O2
        WHERE O2.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
        AND O2.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
        GROUP BY O2.CUS_ID
        HAVING COUNT(*) >= 4
    ) T ON T.CUS_ID = O.CUS_ID
WHERE O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
ORDER BY O.CUS_ID
;

-- WHERE절 상관 서브쿼리
-- 3월에 주문이 존재하는 고객들을 조회
SELECT *
FROM M_CUS MC
WHERE EXISTS(
    SELECT *
    FROM T_ORD O
    WHERE O.CUS_ID = MC.CUS_ID
    AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
    AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
)
;

-- 3월에 ELEC 아이템 유형의 주문이 존재하는 고객들을 조회
SELECT *
FROM M_CUS MC
WHERE EXISTS(
    SELECT *
    FROM T_ORD O
        INNER JOIN T_ORD_DET TOD ON O.ORD_SEQ = TOD.ORD_SEQ
        INNER JOIN M_ITM MI ON MI.ITM_ID = TOD.ITM_ID
           AND MI.ITM_TP = 'ELEC'
    WHERE O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
    AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
)
;

-- 전체 고객을 조회, 3월에 주문이 존재하는지 여부를 같이 보여줌
SELECT MC.CUS_ID
    , MC.CUS_NM
    , (
        CASE
            WHEN EXISTS(
                SELECT *
                FROM T_ORD O
                WHERE O.CUS_ID = MC.CUS_ID
                AND O.ORD_DT >= TO_DATE('20170301', 'YYYYMMDD')
                AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
            ) THEN 'Y'
            ELSE 'N'
        END
    ) AS ORD_YN_03
FROM M_CUS MC
;