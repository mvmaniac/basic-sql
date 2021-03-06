-- 주문일시, 지불유형별 주문금액
SELECT O.ORD_DT
     , O.PAY_TP
     , SUM(O.ORD_AMT) AS ORD_AMT
FROM T_ORD O
WHERE O.ORD_ST = 'COMP'
GROUP BY O.ORD_DT, O.PAY_TP
ORDER BY O.ORD_DT, O.PAY_TP
;

-- CASE 문을 이용한 가격유형 별 주문 건수 카운트
SELECT O.ORD_ST,
       CASE
           WHEN O.ORD_AMT >= 5000 THEN 'High Order'
           WHEN O.ORD_AMT >= 3000 THEN 'Middle Order'
           ELSE 'Low Order'
       END ORD_AMT_TP,
       COUNT(*) AS ORD_CNT
FROM T_ORD O
GROUP BY O.ORD_ST,
         CASE
             WHEN O.ORD_AMT >= 5000 THEN 'High Order'
             WHEN O.ORD_AMT >= 3000 THEN 'Middle Order'
             ELSE 'Low Order'
         END
ORDER BY 1, 2
;

-- TO_CHAR 변형을 이용한 주문년월, 지불유형별 주문건수
SELECT TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
     , O.PAY_TP
     , COUNT(*) AS ORD_CNT
FROM T_ORD O
WHERE O.ORD_ST = 'COMP'
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM'), O.PAY_TP
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM'), O.PAY_TP
;

-- 주문년월별 계좌이체 건수와 카드결제 건수
SELECT TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
     , SUM(CASE WHEN O.PAY_TP = 'BANK' THEN 1 END) AS BANK_PAY_CNT
     , SUM(CASE WHEN O.PAY_TP = 'CARD' THEN 1 END) AS CARD_PAY_CNT
FROM T_ORD O
WHERE O.ORD_ST = 'COMP'
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;

-- 지불유형별 주문건수
SELECT O.PAY_TP
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201701' THEN 'X' END) AS ORD_CNT_1701
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201702' THEN 'X' END) AS ORD_CNT_1702
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201703' THEN 'X' END) AS ORD_CNT_1703
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201704' THEN 'X' END) AS ORD_CNT_1704
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201705' THEN 'X' END) AS ORD_CNT_1705
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201706' THEN 'X' END) AS ORD_CNT_1706
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201707' THEN 'X' END) AS ORD_CNT_1707
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201708' THEN 'X' END) AS ORD_CNT_1708
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201709' THEN 'X' END) AS ORD_CNT_1709
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201710' THEN 'X' END) AS ORD_CNT_1710
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201711' THEN 'X' END) AS ORD_CNT_1711
    , COUNT(CASE WHEN TO_CHAR(O.ORD_DT, 'YYYYMM') = '201712' THEN 'X' END) AS ORD_CNT_1712
FROM T_ORD O
WHERE O.ORD_ST = 'COMP'
GROUP BY O.PAY_TP
ORDER BY O.PAY_TP
;

-- 지불유형별 주문건수, 인라인 뷰 사용
SELECT T1.PAY_TP
     , MAX(CASE WHEN T1.ORD_YM = '201701' THEN T1.ORD_CNT END) AS ORD_CNT_1701
     , MAX(CASE WHEN T1.ORD_YM = '201702' THEN T1.ORD_CNT END) AS ORD_CNT_1702
     , MAX(CASE WHEN T1.ORD_YM = '201703' THEN T1.ORD_CNT END) AS ORD_CNT_1703
     , MAX(CASE WHEN T1.ORD_YM = '201704' THEN T1.ORD_CNT END) AS ORD_CNT_1704
     , MAX(CASE WHEN T1.ORD_YM = '201705' THEN T1.ORD_CNT END) AS ORD_CNT_1705
     , MAX(CASE WHEN T1.ORD_YM = '201706' THEN T1.ORD_CNT END) AS ORD_CNT_1706
     , MAX(CASE WHEN T1.ORD_YM = '201707' THEN T1.ORD_CNT END) AS ORD_CNT_1707
     , MAX(CASE WHEN T1.ORD_YM = '201708' THEN T1.ORD_CNT END) AS ORD_CNT_1708
     , MAX(CASE WHEN T1.ORD_YM = '201709' THEN T1.ORD_CNT END) AS ORD_CNT_1709
     , MAX(CASE WHEN T1.ORD_YM = '201710' THEN T1.ORD_CNT END) AS ORD_CNT_1710
     , MAX(CASE WHEN T1.ORD_YM = '201712' THEN T1.ORD_CNT END) AS ORD_CNT_1712
     , MAX(CASE WHEN T1.ORD_YM = '201711' THEN T1.ORD_CNT END) AS ORD_CNT_1711
FROM (
    SELECT O.PAY_TP
         , TO_CHAR(O.ORD_DT, 'YYYYMM') AS ORD_YM
         , COUNT(*) AS ORD_CNT
    FROM T_ORD O
    WHERE O.ORD_ST = 'COMP'
    GROUP BY O.PAY_TP, TO_CHAR(O.ORD_DT, 'YYYYMM')
) T1
GROUP BY T1.PAY_TP
;

-- 주문년월별 주문고객 수(중복을 제거해서 카운트)
SELECT TO_CHAR(O.ORD_DT, 'YYYYMM')
     , COUNT(DISTINCT O.CUS_ID) AS CUS_CNT
     , COUNT(*) AS ORD_CNT
FROM T_ORD O
WHERE O.ORD_DT >= TO_DATE('20170101', 'YYYYMMDD')
AND O.ORD_DT < TO_DATE('20170401', 'YYYYMMDD')
GROUP BY TO_CHAR(O.ORD_DT, 'YYYYMM')
ORDER BY TO_CHAR(O.ORD_DT, 'YYYYMM')
;