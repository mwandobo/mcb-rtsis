CREATE PROCEDURE LOAN_UPD_BTC_PARMS_CUST_V2 (IN cprog_id VARCHAR (5))
LANGUAGE SQL P1 :
BEGIN
   DECLARE L_CUST_COUNT INT DEFAULT 0;
DECLARE L_COUNT INT DEFAULT 0;
DECLARE L_TOT_CUSTS INT DEFAULT 0;
DECLARE CNT INT DEFAULT 0;
DECLARE SN CHAR (10) DEFAULT 0;
DECLARE L_CUST_ID INT;
DECLARE L_CUST_FROM INT DEFAULT 0;
DECLARE B_ID CHAR (5);
DECLARE CUSTOMER_NUMBER CURSOR
    FOR
SELECT
   COUNT (*)
FROM    CUSTOMER C
WHERE    EXISTS (
   SELECT
       L.CUST_ID
   FROM        LOAN_ACCOUNT L
                      ,
       LOAN_ACCOUNT_INFO F
                      ,
       LNS_CUST_RECLASS lcr
   WHERE        L.LOAN_STATUS < 3
       AND L.ACC_STATUS < 3
       AND L.LAST_NRM_TRX_CNT <> 0
       AND F.ACC_TRANSITION_FLG <> 1
       AND L.ACC_SN = F.FK_LOAN_ACCOUNTACC
       AND L.ACC_TYPE = F.FK0LOAN_ACCOUNTACC
       AND L.FK_UNITCODE = F.FK_LOAN_ACCOUNTFK
       AND L.CUST_ID = C.CUST_ID
       AND l.cust_id = lcr.CUST_ID
   GROUP BY
       L.CUST_ID);
DECLARE C_DIVIDE_CUSTOMER CURSOR
    FOR
SELECT
   L.CUST_ID
FROM    LOAN_ACCOUNT L
        ,
   LOAN_ACCOUNT_INFO F
        ,
   LNS_CUST_RECLASS lcr
WHERE    L.LOAN_STATUS < 3
   AND L.ACC_STATUS < 3
   AND L.LAST_NRM_TRX_CNT <> 0
   AND F.ACC_TRANSITION_FLG <> 1
   AND L.ACC_SN = F.FK_LOAN_ACCOUNTACC
   AND L.ACC_TYPE = F.FK0LOAN_ACCOUNTACC
   AND L.FK_UNITCODE = F.FK_LOAN_ACCOUNTFK
   AND l.cust_id = lcr.CUST_ID
   AND L.CUST_ID > L_CUST_FROM GROUP BY
   L.CUST_ID
ORDER BY
   L.CUST_ID ASC;
DECLARE BATCH_PROG CURSOR
    FOR
SELECT
   T.PROGRAM_ID
FROM    BATCH_CONTROL_INIT T
WHERE    T.PROGRAM_ID LIKE cprog_id || '%'
   AND T.SERIAL_NUMBER = SN;
UPDATE
   batch_control_init
SET    serial_number = CASE
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'B') THEN 2
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'C') THEN 3
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'D') THEN 4
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'E') THEN 5
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'F') THEN 6
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'G') THEN 7
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'H') THEN 8
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'I') THEN 9
       WHEN (substr (program_id,
       LENGTH (program_id),
       1) = 'J') THEN 10
       ELSE 1
   END
WHERE    program_id LIKE cprog_id || '%';
UPDATE
   batch_parameters b
SET    B.CUSTOMER_FROM = 0
        ,
   B.CUSTOMER_TO = 9999999
WHERE    B.PROGRAM_ID LIKE cprog_id || '%';
COMMIT;
OPEN CUSTOMER_NUMBER;
FETCH CUSTOMER_NUMBER
INTO    L_CUST_COUNT;
CLOSE CUSTOMER_NUMBER;
SET L_COUNT = TRUNC (L_CUST_COUNT / 10) + 1;
OPEN C_DIVIDE_CUSTOMER;
SET L_CUST_FROM = 0;
OUR_LOOP:
    LOOP
        FETCH C_DIVIDE_CUSTOMER
INTO    L_CUST_ID;
IF L_TOT_CUSTS > L_CUST_COUNT THEN
           LEAVE OUR_LOOP;
END IF;
SET CNT = CNT + 1;
SET L_TOT_CUSTS = L_TOT_CUSTS + 1;
IF CNT + 1 = L_COUNT THEN
            SET SN = SN + 1;
SET CNT = 0;
OPEN BATCH_PROG;
FETCH BATCH_PROG
INTO    B_ID;
CLOSE BATCH_PROG;
UPDATE
   BATCH_PARAMETERS B
SET    B.CUSTOMER_FROM = L_CUST_FROM
            ,
   B.CUSTOMER_TO = L_CUST_ID
WHERE    B.PROGRAM_ID = B_ID;
SET L_CUST_FROM = L_CUST_ID + 1;
END IF;
END LOOP OUR_LOOP;
CLOSE C_DIVIDE_CUSTOMER;
IF SN < 10 THEN
	    SET SN = SN + 1;
OPEN BATCH_PROG;
FETCH BATCH_PROG
INTO    B_ID;
CLOSE BATCH_PROG;
UPDATE
   BATCH_PARAMETERS B
SET    B.CUSTOMER_FROM = L_CUST_FROM
	        ,
   B.CUSTOMER_TO = 9999999
WHERE    B.PROGRAM_ID = B_ID;
ELSE
UPDATE
   BATCH_PARAMETERS B
SET    B.CUSTOMER_TO = 9999999
WHERE    B.PROGRAM_ID = B_ID;
END IF;
UPDATE
   batch_parameters b
SET    B.CUSTOMER_FROM = 0
        ,
   B.CUSTOMER_TO = 0
WHERE    B.PROGRAM_ID LIKE cprog_id || '%'
   AND B.CUSTOMER_FROM = 0
   AND B.CUSTOMER_TO = 9999999
   AND B.PROGRAM_ID <> (
   SELECT
       min (program_id)
   FROM        batch_parameters
   WHERE        program_id LIKE cprog_id || '%');
UPDATE
   batch_control_init
SET    serial_number = '1'
WHERE    program_id LIKE cprog_id || '%';
COMMIT;
END;

