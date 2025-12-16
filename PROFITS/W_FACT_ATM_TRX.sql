CREATE VIEW profits.W_FACT_ATM_TRX
(
   BANK_CODE,
   TERMINAL_NUMBER,
   SOURCE_ACCOUNT_NUMBER,
   SOURCE_ACCOUNT_CD,
   DESTINATION_ACCOUNT_NUMBER,
   DESTINATION_ACCOUNT_CD,
   TUN_UNIT_CODE,
   TUN_UNIT_NAME,
   TUN_USER_CODE,
   TUN_DATE,
   TUN_USER_SN,
   TRX_DATE,
   TRANSACTION_TIME,
   PROCESSING_CODE,
   TRANSACTION_DESCRIPTION,
   REFERENCE_NUMBER,
   TRANSACTION_AMOUNT,
   CARD_NUMBER,
   REVERSED_FLAG,
   REVERSAL_FLAG,
   STAND_IN_FLAG,
   CHANNEL_IND,
   RECONCILED_FLAG,
   CARD_BIN,
   ACTIVITY_TYPE_IND,
   POS_ACTIVITY_TYPE_IND,
   AUDIT_NUMBER,
   PHONE_NUMBER,
   COMPANY,
   PULL_PUSH_MSG,
   INTER_ID_TRANSACT,
   EXTRAIT_COMMENTS
)
AS
  SELECT A.BANK_CODE,
          A.TERMINAL TERMINAL_NUMBER,
          CASE
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_SRC)) = 12
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_SRC), 1, 11)
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_SRC)) = 11
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_SRC), 1, 10)
             ELSE
                TRIM(A.CUST_ACCOUNT_SRC)
          END
             SOURCE_ACCOUNT_NUMBER,
              CASE
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_SRC)) = 12
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_SRC), 12, 1)
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_SRC)) = 11
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_SRC), 11, 1)
             ELSE
                0
          END
--          NVL(SUBSTR(TRIM(A.CUST_ACCOUNT_SRC), -1, 1), ' ')
             SOURCE_ACCOUNT_CD,
   CASE
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_DST)) = 12
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_DST), 1, 11)
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_DST)) = 11
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_DST), 1, 10)
             ELSE
                TRIM(A.CUST_ACCOUNT_DST)
          END
            DESTINATION_ACCOUNT_NUMBER,
              CASE
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_DST)) = 12
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_DST), 12, 1)
             WHEN LENGTH(TRIM(A.CUST_ACCOUNT_DST)) = 11
             THEN
                SUBSTR(TRIM(A.CUST_ACCOUNT_DST), 11, 1)
             ELSE
                0
          END
             DESTINATION_ACCOUNT_CD,
          A.TUN_UNIT TUN_UNIT_CODE,
          U.UNIT_NAME TUN_UNIT_NAME,
          A.TUN_USR TUN_USER_CODE,
          A.TUN_DATE TUN_DATE,
          A.TUN_USER_SN TUN_USER_SN,           ----> new addition for CBP-4717
          A.TUN_DATE TRX_DATE,
          A.TRANSACTION_TIME TRANSACTION_TIME,
          A.PROCESSING_CODE PROCESSING_CODE,
        --  just.DESCRIPTION    TRANSACTION_DESCRIPTION,
          proce.DESCRIPTION    TRANSACTION_DESCRIPTION,
          A.REFERENCE_NUMBER REFERENCE_NUMBER,
          A.TRANSACTION_AMOUNT TRANSACTION_AMOUNT,
          A.CARD_NUMBER CARD_NUMBER,
          DECODE (A.REVERSED, '1', 'Reversed', 'Posted') REVERSED_FLAG,
          DECODE (A.REVERSAL, '1', 'Reversal', 'Original') REVERSAL_FLAG,
          DECODE (A.STAND_IN_FLAG, '1', 'Stand-in', 'Not Stand-in')
             STAND_IN_FLAG,
          CASE
             WHEN A.TUN_USR = 'POSUSER '
             THEN
                'POS'
             WHEN A.TERMINAL IN (SELECT DESCRIPTION
                                   FROM GENERIC_DETAIL
                                  WHERE FK_GENERIC_HEADPAR = 'MOB') --tun_usr = 'ELMOBILE'
             THEN
                'Mobile'
             ELSE
                'ATM'
          END
             CHANNEL_IND,
          DECODE (A.RECONCILED_FLG, '1', 'Reconciled', 'Unreconciled')
             RECONCILED_FLAG,
          SUBSTR (TRIM (A.CARD_NUMBER), 1, 6) CARD_BIN,
          CASE
             WHEN A.TERMINAL IN (SELECT DESCRIPTION
                                   FROM GENERIC_DETAIL
                                  WHERE FK_GENERIC_HEADPAR = 'MOB') --a.terminal = 'ELMOBILE'
             THEN
                'Mobile'
             WHEN    A.TUN_USR LIKE 'POS%'
                  OR A.TERMINAL IN (SELECT DESCRIPTION
                                      FROM GENERIC_DETAIL
                                     WHERE FK_GENERIC_HEADPAR = 'POS')
             THEN
                'POS'
             WHEN A.MTI_CODE IS NOT NULL
             THEN
                'Other Card On Us'
             WHEN ATM_BIN.CARDLESS_FLAG = '1'
             THEN
                'Cardless'
             WHEN     NVL (ATM_BIN.CARDLESS_FLAG, '0') = '0'
                  AND A.TUN_USR NOT LIKE 'P%'
                  AND USR.CODE IS NOT NULL
             THEN
                'Our Card on Us'
             WHEN     A.TUN_USR LIKE 'ATMUS%'
                  AND NVL (ATM_BIN.CARDLESS_FLAG, '0') = '0'
             THEN
                'Our Card On Other Banks'
             ELSE
                'n/a'
          END
             ACTIVITY_TYPE_IND,
          CASE
             WHEN A.TUN_USR LIKE 'POS%'
             THEN
                CASE
                   WHEN     A.TERMINAL IN (SELECT DESCRIPTION
                                             FROM GENERIC_DETAIL
                                            WHERE FK_GENERIC_HEADPAR = 'POS')
                        AND A.MTI_CODE IS NOT NULL
                   THEN
                      'Other Cards Our POS'
                   WHEN     NVL (ATM_BIN.CARDLESS_FLAG, '0') = '0'
                        AND A.TERMINAL IN (SELECT DESCRIPTION
                                             FROM GENERIC_DETAIL
                                            WHERE FK_GENERIC_HEADPAR = 'POS')
                   THEN
                      'Our Cards Other POS'
                   WHEN     NVL (ATM_BIN.CARDLESS_FLAG, '0') = '0'
                        AND USR.CODE IS NOT NULL
                   THEN
                      'Our Cards Our POS'
                   ELSE
                      'n/a'
                END
             ELSE
                'n/a'
          END
             POS_ACTIVITY_TYPE_IND,
          A.AUDIT_NUMBER,
          A.PHONE_NUMBER,
          A.COMPANY,
          A.PULL_PUSH_MSG,
          A.INTER_ID_TRANSACT ,
          FST_DEMAND_EXTRAIT.COMMENTS1 || FST_DEMAND_EXTRAIT.COMMENTS2
             EXTRAIT_COMMENTS
     FROM profits.ATM_TRX_RECORDING A
          INNER JOIN profits.UNIT U ON ( A.TUN_UNIT = U.CODE )
          LEFT JOIN profits.USR ON (A.TERMINAL = USR.CODE )                    --OUR BANK
          LEFT JOIN profits.ATM_BIN ON (SUBSTR (TRIM (A.CARD_NUMBER), 1, 6) = TRIM (ATM_BIN.BIN_CODE)) --(because bin_code is char(9))
          LEFT JOIN profits.FST_DEMAND_EXTRAIT --(instead of inner join since Cardless transactions do not populate extrait)
             ON (    A.TUN_DATE = FST_DEMAND_EXTRAIT.TRX_DATE
                 AND A.TUN_USER_SN = FST_DEMAND_EXTRAIT.TRX_SN
                 AND A.TUN_UNIT = FST_DEMAND_EXTRAIT.TRX_UNIT
                 AND A.TUN_USR = FST_DEMAND_EXTRAIT.TRX_USR
                 AND FST_DEMAND_EXTRAIT.ENTRY_SER_NUM = 1)
          left join profits.justific just on (just.ID_JUSTIFIC = FST_DEMAND_EXTRAIT.ID_JUSTIFIC )
          left join profits.atm_process_code proce on (proce.ISO_CODE = A.PROCESSING_CODE and proce.REVERSAL_FLAG = A.REVERSAL);

