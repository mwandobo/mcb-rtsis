create table W_EOM_COLLATERAL
(
    EOM_DATE                 DATE,
    COMBO_KEY                CHAR(20),
    PRODUCT_CODE             INTEGER,
    UNIT_CODE                INTEGER,
    COLLATERAL_SN            DECIMAL(10),
    COL_EST_VALUE_AMN        DECIMAL(15, 2),
    COLLATERAL_STATUS        CHAR(1),
    COLLATERAL_STATUS_IND    VARCHAR(12),
    COLLATERAL_MECHANISM_IND VARCHAR(15),
    PRODUCT_DESCRIPTION      VARCHAR(40),
    CURRENCY_CODE            VARCHAR(3),
    PRODUCT_DYNAMIC_DESCR    VARCHAR(40),
    YIELD_PERC               DECIMAL(8, 4),
    YIELD_LIMIT_AMN          DECIMAL(15, 2),
    YIELD_UTILISED_AMN       DECIMAL(12, 2)
);

create unique index PK_W_EOM_COLLATERAL
    on W_EOM_COLLATERAL (EOM_DATE, COMBO_KEY);

CREATE PROCEDURE W_EOM_COLLATERAL ( )
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE W_EOM_COLLATERAL
 WHERE EOM_DATE = (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS);
MERGE INTO W_EOM_COLLATERAL A
USING (SELECT (SELECT SCHEDULED_DATE FROM BANK_PARAMETERS) AS EOM_DATE,
              CAST (FK_COLLATERAL_TFK || '|' || FK_UNITCODE || '|' || COLLATERAL_SN AS CHAR (20)) AS COMBO_KEY,
              FK_COLLATERAL_TFK AS PRODUCT_CODE,
              FK_UNITCODE AS UNIT_CODE,
              COLLATERAL_SN,
              COL_EST_VALUE_AMN,
              D.COLLATERAL_STATUS,
              (CASE D.COLLATERAL_STATUS
                      WHEN '0' THEN 'Deleted'
                      WHEN '1' THEN 'Approved'
                      WHEN '2' THEN 'Not Approved'
                      ELSE 'n/a'
              END) AS COLLATERAL_STATUS_IND,
              P.DESCRIPTION AS PRODUCT_DESCRIPTION,
              (CASE E.COL_MECHANISM
                      WHEN  '1'  THEN 'Prenotation'
                      WHEN  '2'  THEN 'Mortgage'
                      WHEN  '3'  THEN 'Deposit Pledges'
                      WHEN  '4'  THEN 'Stock Mortgages'
                      WHEN  '5'  THEN 'Stock Margin'
                      WHEN  '6'  THEN 'Ship Mortgage'
                      WHEN  '7'  THEN 'Bond Mortgage'
                      WHEN  '8'  THEN 'Automobiles'
                      WHEN  '9'  THEN 'Cheques'
                      WHEN  '10' THEN 'Invoices'
                      WHEN  '11' THEN 'Dynamic'
                      WHEN  '99' THEN 'Misc.'
                      ELSE  'n/a'
              END)  AS COLLATERAL_MECHANISM_IND,
              (CASE E.COL_MECHANISM
                      WHEN  '11' THEN FK_DYNAMIC_FIELD
                      ELSE  P.DESCRIPTION END)  AS PRODUCT_DYNAMIC_DESCR,
              D.YIELD_PERC,
              D.YIELD_LIMIT_AMN,
              D.YIELD_UTILISED_AMN
         FROM R_COLLATERAL D
              LEFT JOIN PRODUCT P ON (P.ID_PRODUCT = D.FK_COLLATERAL_TFK)
              LEFT JOIN COLLATERAL_TYPE E
                ON (E.FK_PRODUCTID_PRODU = D.FK_COLLATERAL_TFK)) B
   ON  (A.EOM_DATE = B.EOM_DATE AND A.COMBO_KEY = B.COMBO_KEY)
WHEN NOT MATCHED THEN
   INSERT (EOM_DATE,              COMBO_KEY,         PRODUCT_CODE,          UNIT_CODE,           COLLATERAL_SN,
           COL_EST_VALUE_AMN,     COLLATERAL_STATUS, COLLATERAL_STATUS_IND, PRODUCT_DESCRIPTION, COLLATERAL_MECHANISM_IND,
           PRODUCT_DYNAMIC_DESCR,  YIELD_PERC,        YIELD_LIMIT_AMN,       YIELD_UTILISED_AMN)
   VALUES (B.EOM_DATE,              B.COMBO_KEY,         B.PRODUCT_CODE,          B.UNIT_CODE,           B.COLLATERAL_SN,
           B.COL_EST_VALUE_AMN,     B.COLLATERAL_STATUS, B.COLLATERAL_STATUS_IND, B.PRODUCT_DESCRIPTION, B.COLLATERAL_MECHANISM_IND,
           B.PRODUCT_DYNAMIC_DESCR, B.YIELD_PERC,        B.YIELD_LIMIT_AMN,       B.YIELD_UTILISED_AMN);
END;

