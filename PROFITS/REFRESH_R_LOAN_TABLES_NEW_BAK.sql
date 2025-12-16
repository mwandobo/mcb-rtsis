CREATE PROCEDURE REFRESH_R_LOAN_TABLES_NEW_BAK
   LANGUAGE SQL
BEGIN
   DECLARE L_TMSTAMP TIMESTAMP ( 6 ) ;
   DECLARE L_SCHED_DATE          DATE;
   DECLARE L_UNITCODE            DECIMAL ( 5 ) ;
   DECLARE L_ACC_TYPE            DECIMAL ( 2 ) ;
   DECLARE L_ACC_SN              DECIMAL ( 6 ) ;
   DECLARE L_LAST_NRM_TRX_CNT    DECIMAL ( 3 ) ;
   DECLARE L_MAX_REQUEST_SN      DECIMAL ( 4 ) ;
   DECLARE L_RL_PNL_INT_AMN      DECIMAL ( 15 , 2 ) ;
   DECLARE L_URL_PNL_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE L_ACR_PNL_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE L_THRDPRT_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE L_TOT_EXP_IN_CC_AMN   DECIMAL ( 15 , 2 ) ;
   DECLARE L_TOT_COM_IN_CC_AMN   DECIMAL ( 15 , 2 ) ;
   DECLARE L_TOT_SUBS_INT_AMN    DECIMAL ( 15 , 2 ) ;
   DECLARE L_TOT_EXPENSE_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE L_TOT_CONFIRM_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE L_POSITIVE_AMN        DECIMAL ( 15 , 2 ) ;
   DECLARE L_UNCLEAR_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE L_BLOCKED_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE L_DORMANT_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE L_LAST_YEAR           DECIMAL ( 4 ) ;
   DECLARE L_MONTH               CHAR ( 2 ) ;
   DECLARE l_INSTALL_ROUND_AMN   DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_PUB_COMM_AMN    DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_INT_SPRD_AMN    DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_THRDPRT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_MP_START_CAP_AMN    DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_DRAWDOWN_AMN    DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_PNL_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_NRM_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_CAP_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE l_TOT_COMMISSION_AMN  DECIMAL ( 15 , 2 ) ;
   DECLARE L_LST_INT_DB_AMN      DECIMAL ( 15 , 2 ) ;
   DECLARE l_CAPITAL_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE l_CAPITAL_AMN_1       DECIMAL ( 15 , 2 ) ;
   DECLARE l_EXPENSE_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE l_EXPENSE_AMN_1       DECIMAL ( 15 , 2 ) ;
   DECLARE l_COMMISSION_AMN      DECIMAL ( 15 , 2 ) ;
   DECLARE l_COMMISSION_AMN_1    DECIMAL ( 15 , 2 ) ;
   DECLARE l_RL_NRM_INT_AMN      DECIMAL ( 15 , 2 ) ;
   DECLARE l_RL_NRM_INT_AMN_1    DECIMAL ( 15 , 2 ) ;
   DECLARE l_URL_NRM_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_URL_NRM_INT_AMN_1   DECIMAL ( 15 , 2 ) ;
   DECLARE l_ACR_NRM_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_ACR_NRM_INT_AMN_1   DECIMAL ( 15 , 2 ) ;
   DECLARE l_URL_PUB_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_URL_PUB_INT_AMN_1   DECIMAL ( 15 , 2 ) ;
   DECLARE l_ACR_PUB_INT_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_ACR_PUB_INT_AMN_1   DECIMAL ( 15 , 2 ) ;
   DECLARE l_SUBSIDY_AMN         DECIMAL ( 15 , 2 ) ;
   DECLARE l_SUBSIDY_AMN_1       DECIMAL ( 15 , 2 ) ;
   DECLARE L_MAX_REQ_LOAN_STATUS CHAR ( 1 ) ;
   DECLARE L_LOAN_STATUS         CHAR ( 1 ) ;
   DECLARE LCOUNTER DECFLOAT;
   --req
   DECLARE l_req_sts_flg       CHAR ( 1 ) ;
   DECLARE l_request_type      CHAR ( 1 ) ;
   DECLARE l_request_loan_sts  CHAR ( 1 ) ;
   DECLARE l_request_sn        DECIMAL ( 4 ) ;
   DECLARE r_l_SUBSIDY_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_COMMISSION_AMN  DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_EXPENSE_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_ACR_PUB_INT_AMN DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_ACR_PNL_INT_AMN DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_ACR_NRM_INT_AMN DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_URL_PUB_INT_AMN DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_URL_PNL_INT_AMN DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_URL_NRM_INT_AMN DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_RL_PNL_INT_AMN  DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_RL_NRM_INT_AMN  DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_RQ_CAPITAL_BAL  DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_PROVISION_AMNT  DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_OV_ACCRUAL_AMN  DECIMAL ( 15 , 2 ) ;
   DECLARE r_l_HOLD_OV_RL_ACCR DECIMAL ( 15 , 2 ) ;
   DECLARE l_unitcode_2        DECIMAL ( 5 ) ;
   --final loan_account_info
   DECLARE l_NRM_ACCRUAL_AMN    DECIMAL ( 15 , 2 ) ;
   DECLARE l_OV_ACCRUAL_AMN     DECIMAL ( 15 , 2 ) ;
   DECLARE l_HOLD_NRM_RL_ACCR   DECIMAL ( 15 , 2 ) ;
   DECLARE l_HOLD_OV_RL_ACCR    DECIMAL ( 15 , 2 ) ;
   DECLARE l_ACC_TRANSITION_FLG CHAR ( 1 ) ;
   DECLARE L_NRM_RL_URL_FLG     DECIMAL ( 1 ) ;
   DECLARE L_OV_RL_URL_FLG      DECIMAL ( 1 ) ;
   DECLARE l_provision_amount   DECIMAL ( 15 , 2 ) ;
   DECLARE L_RECOVERIES_DT      DATE;
   
   DECLARE L_LOAN_CLASS CHAR ( 1 ) ;
   DECLARE L_SUB_CLASS  CHAR ( 1 ) ;
   
   DECLARE AT_END INT DEFAULT 0;
   
   -- GET THE SCHEDULED_DATE , THE MONTH AND THE YEAR
   DECLARE C_BANK_PARAMETERS CURSOR FOR
   SELECT
      SCHEDULED_DATE                                               ,
      SUBSTR ( TO_CHAR ( SCHEDULED_DATE , 'DD-MM-YYYY' ) , 4 , 2 ) ,
      SUBSTR ( TO_CHAR ( SCHEDULED_DATE , 'DD-MM-YYYY' ) , 7 , 4 )
   FROM
      BANK_PARAMETERS;
   
   DECLARE GET_TMSTAMP CURSOR FOR
   SELECT LST_TMSTAMP FROM R_LOAN_LST_TMSTAMP;
   
   DECLARE C_TMP_REFRESH_LNS CURSOR FOR
   SELECT
      ACC_UNIT            ,
      ACC_TYPE            ,
      ACC_SN              ,
      RL_PNL_INT_AMN      ,
      URL_PNL_INT_AMN     ,
      ACR_PNL_INT_AMN     ,
      TOT_EXP_IN_CC_AMN   ,
      TOT_COM_IN_CC_AMN   ,
      THRDPRT_AMN         ,
      TOT_SUBS_INT_AMN    ,
      TOT_EXPENSE_AMN     ,
      TOT_CONFIRM_AMN     ,
      POSITIVE_AMN        ,
      UNCLEAR_AMN         ,
      BLOCKED_AMN         ,
      DORMANT_AMN         ,
      LAST_NRM_TRX_CNT    ,
      REQUEST_SN          ,
      INSTALL_ROUND_AMN   ,
      TOT_PUB_COMM_AMN    ,
      TOT_INT_SPRD_AMN    ,
      TOT_THRDPRT_AMN     ,
      MP_START_CAP_AMN    ,
      TOT_DRAWDOWN_AMN    ,
      TOT_PNL_INT_AMN     ,
      TOT_NRM_INT_AMN     ,
      TOT_CAP_AMN         ,
      TOT_COMMISSION_AMN  ,
      LST_INT_DB_AMN      ,
      CAPITAL_AMN         ,
      EXPENSE_AMN         ,
      COMMISSION_AMN      ,
      RL_NRM_INT_AMN      ,
      URL_NRM_INT_AMN     ,
      ACR_NRM_INT_AMN     ,
      URL_PUB_INT_AMN     ,
      ACR_PUB_INT_AMN     ,
      SUBSIDY_AMN         ,
      O_CAPITAL_AMN       ,
      O_EXPENSE_AMN       ,
      O_COMMISSION_AMN    ,
      O_RL_NRM_INT_AMN    ,
      O_URL_NRM_INT_AMN   ,
      O_ACR_NRM_INT_AMN   ,
      O_URL_PUB_INT_AMN   ,
      O_ACR_PUB_INT_AMN   ,
      O_SUBSIDY_AMN       ,
      MAX_REQ_LOAN_STATUS ,
      COUNTER
   FROM
      TMP_REFRESH_LNS;
   
   
   
   --old definitions start
   DECLARE c_loan_acc_info CURSOR FOR
   SELECT
      FK_LOAN_ACCOUNTFK  ,
      FK0LOAN_ACCOUNTACC ,
      FK_LOAN_ACCOUNTACC ,
      NRM_ACCRUAL_AMN    ,
      OV_ACCRUAL_AMN     ,
      HOLD_NRM_RL_ACCR   ,
      HOLD_OV_RL_ACCR    ,
      ACC_TRANSITION_FLG ,
      UNCLEAR_AMN        ,
      NRM_RL_URL_FLG     ,
      OV_RL_URL_FLG      ,
      provision_amount   ,
      LOAN_CLASS         ,
      LOAN_SUB_CLASS     ,
      RECOVERIES_DT
   FROM
      loan_account_info
      /*where FK_LOAN_ACCOUNTFK = l_unitcode
      and FK0LOAN_ACCOUNTACC = l_acc_type
      and FK_LOAN_ACCOUNTACC = l_acc_sn*/
   ;
   
   --old definitions end
   
   DECLARE CONTINUE HANDLER FOR NOT FOUND SET AT_END = 1;
   
   
   -- BEGIN
   
   OPEN C_BANK_PARAMETERS;
   FETCH C_BANK_PARAMETERS INTO L_SCHED_DATE , L_MONTH , L_LAST_YEAR;
   
   CLOSE C_BANK_PARAMETERS;
   
   OPEN GET_TMSTAMP;
   FETCH GET_TMSTAMP INTO L_TMSTAMP;
   
   CLOSE GET_TMSTAMP;
   
   
   DELETE TMP_REFRESH_LNS_INST_1;
   
   COMMIT;
   
   INSERT INTO TMP_REFRESH_LNS_INST_1
      (
         ACC_UNIT          ,
         ACC_TYPE          ,
         ACC_SN            ,
         RL_PNL_INT_AMN    ,
         URL_PNL_INT_AMN   ,
         ACR_PNL_INT_AMN   ,
         TOT_EXP_IN_CC_AMN ,
         TOT_COM_IN_CC_AMN ,
         THRDPRT_AMN       ,
         TOT_SUBS_INT_AMN  ,
         TOT_EXPENSE_AMN   ,
         TOT_CONFIRM_AMN   ,
         POSITIVE_AMN      ,
         UNCLEAR_AMN       ,
         BLOCKED_AMN       ,
         DORMANT_AMN       ,
         LAST_NRM_TRX_CNT  ,
         REQUEST_SN        ,
         INSTALL_ROUND_AMN ,
         COUNTER           ,
         MAX_REQ_LOAN_STATUS
      )
   SELECT
      a.ACC_UNIT                              ,
      a.ACC_TYPE                              ,
      a.ACC_SN                                ,
      NVL ( SUM ( a.RL_PNL_INT_AMN ) , 0 )    ,
      NVL ( SUM ( a.URL_PNL_INT_AMN ) , 0 )   ,
      NVL ( SUM ( a.ACR_PNL_INT_AMN ) , 0 )   ,
      NVL ( SUM ( a.TOT_EXP_IN_CC_AMN ) , 0 ) ,
      NVL ( SUM ( a.TOT_COM_IN_CC_AMN ) , 0 ) ,
      NVL ( SUM ( a.THRDPRT_AMN ) , 0 )       ,
      NVL ( SUM ( a.TOT_SUBS_INT_AMN ) , 0 )  ,
      NVL ( SUM ( a.TOT_EXPENSE_AMN ) , 0 )   ,
      NVL ( SUM ( a.TOT_CONFIRM_AMN ) , 0 )   ,
      NVL ( SUM ( a.POSITIVE_AMN ) , 0 )      ,
      NVL ( SUM ( a.UNCLEAR_AMN ) , 0 )       ,
      NVL ( SUM ( a.BLOCKED_AMN ) , 0 )       ,
      NVL ( SUM ( a.DORMANT_AMN ) , 0 )       ,
      NVL ( MAX ( a.LAST_NRM_TRX_CNT ) , 0 )  ,
      NVL ( MAX ( a.REQUEST_SN ) , 0 )        ,
      (
         SELECT
            SUM ( c.INSTALL_ROUND_AMN )
         FROM
            loan_trx_recording c
         WHERE
            c.trx_date = L_SCHED_DATE
            AND
            c.tmstamp > l_tmstamp
            AND
            c.TRX_INTERNAL_SN = 1
            AND
            c.trx_code IN ( 74131 ,
                           74141  ,
                           74151 )
            AND
            c.acc_unit = A.acc_unit
            AND
            c.acc_type = A.acc_type
            AND
            c.acc_sn = A.acc_sn
      )
      ,
      (
         SELECT
            COUNT ( * )
         FROM
            loan_trx_recording
         WHERE
            trx_date = l_sched_date
            AND
            acc_unit = A.acc_unit
            AND
            acc_type = A.acc_type
            AND
            acc_sn = A.acc_sn
            AND
            tmstamp > l_tmstamp
            AND
            (
               (
                  trx_code = 74181
                  AND
                  i_justification = '74182'
               )
               OR
               (
                  trx_code = 74191
                  AND
                  i_justification = '74192'
               )
               OR
               (
                  trx_code = 74201
                  AND
                  i_justification = '74202'
               )
               OR
               (
                  trx_code = 74211
                  AND
                  i_justification = '74212'
               )
            )
      )
      ,
      (
         SELECT
            MAX ( request_loan_sts )
         FROM
            loan_trx_recording
         WHERE
            trx_date = l_sched_date
            AND
            acc_unit = A.acc_unit
            AND
            acc_type = A.acc_type
            AND
            acc_sn = A.acc_sn
            AND
            tmstamp > l_tmstamp
            AND
            (
               (
                  trx_code = 74181
                  AND
                  i_justification = '74182'
               )
               OR
               (
                  trx_code = 74191
                  AND
                  i_justification = '74192'
               )
               OR
               (
                  trx_code = 74201
                  AND
                  i_justification = '74202'
               )
               OR
               (
                  trx_code = 74211
                  AND
                  i_justification = '74212'
               )
            )
      )
   FROM
      LOAN_TRX_RECORDING a
   WHERE
      a.TRX_DATE = L_SCHED_DATE
      AND
      a.TMSTAMP > L_TMSTAMP
   GROUP BY
      a.ACC_UNIT ,
      a.ACC_TYPE ,
      a.ACC_SN;
   
   COMMIT;
   
   
   DELETE TMP_REFRESH_LNS_INST;
   
   COMMIT;
   
   INSERT INTO TMP_REFRESH_LNS_INST
      (
         ACC_UNIT           ,
         ACC_TYPE           ,
         ACC_SN             ,
         TOT_PUB_COMM_AMN   ,
         TOT_INT_SPRD_AMN   ,
         TOT_THRDPRT_AMN    ,
         MP_START_CAP_AMN   ,
         TOT_DRAWDOWN_AMN   ,
         TOT_PNL_INT_AMN    ,
         TOT_NRM_INT_AMN    ,
         TOT_CAP_AMN        ,
         TOT_COMMISSION_AMN ,
         LST_INT_DB_AMN
      )
   SELECT
      a.ACC_UNIT                               ,
      a.ACC_TYPE                               ,
      a.ACC_SN                                 ,
      NVL ( SUM ( a.TOT_PUB_COMM_AMN ) , 0 )   ,
      NVL ( SUM ( a.TOT_INT_SPRD_AMN ) , 0 )   ,
      NVL ( SUM ( a.TOT_THRDPRT_AMN ) , 0 )    ,
      NVL ( SUM ( a.MP_START_CAP_AMN ) , 0 )   ,
      NVL ( SUM ( a.TOT_DRAWDOWN_AMN ) , 0 )   ,
      NVL ( SUM ( a.TOT_PNL_INT_AMN ) , 0 )    ,
      NVL ( SUM ( a.TOT_NRM_INT_AMN ) , 0 )    ,
      NVL ( SUM ( a.TOT_CAP_AMN ) , 0 )        ,
      NVL ( SUM ( a.TOT_COMMISSION_AMN ) , 0 ) ,
      NVL ( SUM ( a.LST_INT_DB_AMN ) , 0 )
   FROM
      LOAN_TRX_RECORDING a
   WHERE
      a.TRX_DATE = L_SCHED_DATE
      AND
      a.TMSTAMP > L_TMSTAMP
      AND
      a.TRX_INTERNAL_SN = 1
   GROUP BY
      a.ACC_UNIT ,
      a.ACC_TYPE ,
      a.ACC_SN;
   
   COMMIT;
   
   
   DELETE TMP_REFRESH_LNS_NRM;
   
   COMMIT;
   
   INSERT INTO TMP_REFRESH_LNS_NRM
      (
         ACC_UNIT        ,
         ACC_TYPE        ,
         ACC_SN          ,
         CAPITAL_AMN     ,
         EXPENSE_AMN     ,
         COMMISSION_AMN  ,
         RL_NRM_INT_AMN  ,
         URL_NRM_INT_AMN ,
         ACR_NRM_INT_AMN ,
         URL_PUB_INT_AMN ,
         ACR_PUB_INT_AMN ,
         SUBSIDY_AMN
      )
   SELECT
      a.ACC_UNIT                            ,
      a.ACC_TYPE                            ,
      a.ACC_SN                              ,
      NVL ( SUM ( a.CAPITAL_AMN ) , 0 )     ,
      NVL ( SUM ( a.EXPENSE_AMN ) , 0 )     ,
      NVL ( SUM ( a.COMMISSION_AMN ) , 0 )  ,
      NVL ( SUM ( a.RL_NRM_INT_AMN ) , 0 )  ,
      NVL ( SUM ( a.URL_NRM_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.ACR_NRM_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.URL_PUB_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.ACR_PUB_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.SUBSIDY_AMN ) , 0 )
   FROM
      LOAN_TRX_RECORDING a
   WHERE
      a.TRX_DATE = L_SCHED_DATE
      AND
      a.TMSTAMP > L_TMSTAMP
      AND
      a.REQUEST_LOAN_STS = '1'
   GROUP BY
      a.ACC_UNIT ,
      a.ACC_TYPE ,
      a.ACC_SN;
   
   COMMIT;
   
   DELETE TMP_REFRESH_LNS_OV;
   
   COMMIT;
   
   INSERT INTO TMP_REFRESH_LNS_OV
      (
         ACC_UNIT        ,
         ACC_TYPE        ,
         ACC_SN          ,
         CAPITAL_AMN     ,
         EXPENSE_AMN     ,
         COMMISSION_AMN  ,
         RL_NRM_INT_AMN  ,
         URL_NRM_INT_AMN ,
         ACR_NRM_INT_AMN ,
         URL_PUB_INT_AMN ,
         ACR_PUB_INT_AMN ,
         SUBSIDY_AMN
      )
   SELECT
      a.ACC_UNIT                            ,
      a.ACC_TYPE                            ,
      a.ACC_SN                              ,
      NVL ( SUM ( a.CAPITAL_AMN ) , 0 )     ,
      NVL ( SUM ( a.EXPENSE_AMN ) , 0 )     ,
      NVL ( SUM ( a.COMMISSION_AMN ) , 0 )  ,
      NVL ( SUM ( a.RL_NRM_INT_AMN ) , 0 )  ,
      NVL ( SUM ( a.URL_NRM_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.ACR_NRM_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.URL_PUB_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.ACR_PUB_INT_AMN ) , 0 ) ,
      NVL ( SUM ( a.SUBSIDY_AMN ) , 0 )
   FROM
      LOAN_TRX_RECORDING a
   WHERE
      a.TRX_DATE = L_SCHED_DATE
      AND
      a.TMSTAMP > L_TMSTAMP
      AND
      a.REQUEST_LOAN_STS > '1'
   GROUP BY
      a.ACC_UNIT ,
      a.ACC_TYPE ,
      a.ACC_SN;
   
   COMMIT;
   
   DELETE TMP_REFRESH_LNS;
   
   COMMIT;
   
   INSERT INTO TMP_REFRESH_LNS
      (
         ACC_UNIT           ,
         ACC_TYPE           ,
         ACC_SN             ,
         RL_PNL_INT_AMN     ,
         URL_PNL_INT_AMN    ,
         ACR_PNL_INT_AMN    ,
         TOT_EXP_IN_CC_AMN  ,
         TOT_COM_IN_CC_AMN  ,
         THRDPRT_AMN        ,
         TOT_SUBS_INT_AMN   ,
         TOT_EXPENSE_AMN    ,
         TOT_CONFIRM_AMN    ,
         POSITIVE_AMN       ,
         UNCLEAR_AMN        ,
         BLOCKED_AMN        ,
         DORMANT_AMN        ,
         LAST_NRM_TRX_CNT   ,
         REQUEST_SN         ,
         INSTALL_ROUND_AMN  ,
         TOT_PUB_COMM_AMN   ,
         TOT_INT_SPRD_AMN   ,
         TOT_THRDPRT_AMN    ,
         MP_START_CAP_AMN   ,
         TOT_DRAWDOWN_AMN   ,
         TOT_PNL_INT_AMN    ,
         TOT_NRM_INT_AMN    ,
         TOT_CAP_AMN        ,
         TOT_COMMISSION_AMN ,
         LST_INT_DB_AMN     ,
         CAPITAL_AMN        ,
         EXPENSE_AMN        ,
         COMMISSION_AMN     ,
         RL_NRM_INT_AMN     ,
         URL_NRM_INT_AMN    ,
         ACR_NRM_INT_AMN    ,
         URL_PUB_INT_AMN    ,
         ACR_PUB_INT_AMN    ,
         SUBSIDY_AMN        ,
         O_CAPITAL_AMN      ,
         O_EXPENSE_AMN      ,
         O_COMMISSION_AMN   ,
         O_RL_NRM_INT_AMN   ,
         O_URL_NRM_INT_AMN  ,
         O_ACR_NRM_INT_AMN  ,
         O_URL_PUB_INT_AMN  ,
         O_ACR_PUB_INT_AMN  ,
         O_SUBSIDY_AMN      ,
         COUNTER            ,
         MAX_REQ_LOAN_STATUS
      )
   SELECT
      A.ACC_UNIT                       ,
      A.ACC_TYPE                       ,
      A.ACC_SN                         ,
      NVL ( A.RL_PNL_INT_AMN , 0 )     ,
      NVL ( A.URL_PNL_INT_AMN , 0 )    ,
      NVL ( A.ACR_PNL_INT_AMN , 0 )    ,
      NVL ( A.TOT_EXP_IN_CC_AMN , 0 )  ,
      NVL ( A.TOT_COM_IN_CC_AMN , 0 )  ,
      NVL ( A.THRDPRT_AMN , 0 )        ,
      NVL ( A.TOT_SUBS_INT_AMN , 0 )   ,
      NVL ( A.TOT_EXPENSE_AMN , 0 )    ,
      NVL ( A.TOT_CONFIRM_AMN , 0 )    ,
      NVL ( A.POSITIVE_AMN , 0 )       ,
      NVL ( A.UNCLEAR_AMN , 0 )        ,
      NVL ( A.BLOCKED_AMN , 0 )        ,
      NVL ( A.DORMANT_AMN , 0 )        ,
      NVL ( A.LAST_NRM_TRX_CNT , 0 )   ,
      NVL ( A.REQUEST_SN , 0 )         ,
      NVL ( A.INSTALL_ROUND_AMN , 0 )  ,
      NVL ( B.TOT_PUB_COMM_AMN , 0 )   ,
      NVL ( B.TOT_INT_SPRD_AMN , 0 )   ,
      NVL ( B.TOT_THRDPRT_AMN , 0 )    ,
      NVL ( B.MP_START_CAP_AMN , 0 )   ,
      NVL ( B.TOT_DRAWDOWN_AMN , 0 )   ,
      NVL ( B.TOT_PNL_INT_AMN , 0 )    ,
      NVL ( B.TOT_NRM_INT_AMN , 0 )    ,
      NVL ( B.TOT_CAP_AMN , 0 )        ,
      NVL ( B.TOT_COMMISSION_AMN , 0 ) ,
      NVL ( B.LST_INT_DB_AMN , 0 )     ,
      NVL ( C.CAPITAL_AMN , 0 )        ,
      NVL ( C.EXPENSE_AMN , 0 )        ,
      NVL ( C.COMMISSION_AMN , 0 )     ,
      NVL ( C.RL_NRM_INT_AMN , 0 )     ,
      NVL ( C.URL_NRM_INT_AMN , 0 )    ,
      NVL ( C.ACR_NRM_INT_AMN , 0 )    ,
      NVL ( C.URL_PUB_INT_AMN , 0 )    ,
      NVL ( C.ACR_PUB_INT_AMN , 0 )    ,
      NVL ( C.SUBSIDY_AMN , 0 )        ,
      NVL ( D.CAPITAL_AMN , 0 )        ,
      NVL ( D.EXPENSE_AMN , 0 )        ,
      NVL ( D.COMMISSION_AMN , 0 )     ,
      NVL ( D.RL_NRM_INT_AMN , 0 )     ,
      NVL ( D.URL_NRM_INT_AMN , 0 )    ,
      NVL ( D.ACR_NRM_INT_AMN , 0 )    ,
      NVL ( D.URL_PUB_INT_AMN , 0 )    ,
      NVL ( D.ACR_PUB_INT_AMN , 0 )    ,
      NVL ( D.SUBSIDY_AMN , 0 )        ,
      A.COUNTER                        ,
      A.MAX_REQ_LOAN_STATUS
   FROM
      TMP_REFRESH_LNS_INST_1 a
-- ,
--      TMP_REFRESH_LNS_INST   b ,
--      TMP_REFRESH_LNS_NRM    c ,
--      TMP_REFRESH_LNS_OV d
      --**   WHERE
      --**      a.ACC_UNIT = B.ACC_UNIT (+)
      --**      AND
      --**      A.ACC_TYPE = B.ACC_TYPE (+)
      --**     AND
      --**      A.ACC_SN = B.ACC_SN (+)
        RIGHT JOIN TMP_REFRESH_LNS_INST b
        ON
        a.ACC_UNIT = B.ACC_UNIT
        AND
        A.ACC_TYPE = B.ACC_TYPE
        AND
        A.ACC_SN = B.ACC_SN

        RIGHT JOIN TMP_REFRESH_LNS_NRM c
      --**      AND
      --**      A.ACC_UNIT = C.ACC_UNIT (+)
      --**      AND
      --**      A.ACC_TYPE = C.ACC_TYPE (+)
      --**      AND
      --**      A.ACC_SN = C.ACC_SN (+)
        ON
        A.ACC_UNIT = C.ACC_UNIT
        AND
        A.ACC_TYPE = C.ACC_TYPE
        AND
        A.ACC_SN = C.ACC_SN
        RIGHT JOIN TMP_REFRESH_LNS_OV d
      --**      AND
      --**      a.ACC_UNIT = D.ACC_UNIT (+)
      --**      AND
      --**      A.ACC_TYPE = D.ACC_TYPE (+)
      --**      AND
      --**      A.ACC_SN = D.ACC_SN (+);
        ON
        a.ACC_UNIT = D.ACC_UNIT
        AND
        A.ACC_TYPE = D.ACC_TYPE
        AND
        A.ACC_SN = D.ACC_SN
   ;
   
   COMMIT;
   
   -- INITIALIZATION
   SET L_RL_PNL_INT_AMN      = 0;
   SET L_URL_PNL_INT_AMN     = 0;
   SET L_ACR_PNL_INT_AMN     = 0;
   SET L_TOT_EXP_IN_CC_AMN   = 0;
   SET L_TOT_COM_IN_CC_AMN   = 0;
   SET L_THRDPRT_AMN         = 0;
   SET L_TOT_SUBS_INT_AMN    = 0;
   SET L_TOT_EXPENSE_AMN     = 0;
   SET L_TOT_CONFIRM_AMN     = 0;
   SET L_POSITIVE_AMN        = 0;
   SET L_UNCLEAR_AMN         = 0;
   SET L_BLOCKED_AMN         = 0;
   SET L_DORMANT_AMN         = 0;
   SET L_LAST_NRM_TRX_CNT    = 0;
   SET L_MAX_REQUEST_SN      = 0;
   SET l_INSTALL_ROUND_AMN   = 0;
   SET l_TOT_PUB_COMM_AMN    = 0;
   SET l_TOT_INT_SPRD_AMN    = 0;
   SET l_TOT_THRDPRT_AMN     = 0;
   SET l_MP_START_CAP_AMN    = 0;
   SET l_TOT_DRAWDOWN_AMN    = 0;
   SET l_TOT_PNL_INT_AMN     = 0;
   SET l_TOT_NRM_INT_AMN     = 0;
   SET l_TOT_CAP_AMN         = 0;
   SET l_TOT_COMMISSION_AMN  = 0;
   SET L_LST_INT_DB_AMN      = 0;
   SET l_CAPITAL_AMN         = 0;
   SET l_CAPITAL_AMN_1       = 0;
   SET l_EXPENSE_AMN         = 0;
   SET l_EXPENSE_AMN_1       = 0;
   SET l_COMMISSION_AMN      = 0;
   SET l_COMMISSION_AMN_1    = 0;
   SET l_RL_NRM_INT_AMN      = 0;
   SET l_RL_NRM_INT_AMN_1    = 0;
   SET l_URL_NRM_INT_AMN     = 0;
   SET l_URL_NRM_INT_AMN_1   = 0;
   SET l_ACR_NRM_INT_AMN     = 0;
   SET l_ACR_NRM_INT_AMN_1   = 0;
   SET l_URL_PUB_INT_AMN     = 0;
   SET l_URL_PUB_INT_AMN_1   = 0;
   SET l_ACR_PUB_INT_AMN     = 0;
   SET l_ACR_PUB_INT_AMN_1   = 0;
   SET l_SUBSIDY_AMN         = 0;
   SET l_SUBSIDY_AMN_1       = 0;
   SET L_MAX_REQ_LOAN_STATUS = '0';
   SET LCOUNTER              = 0;
   
   
   OPEN C_TMP_REFRESH_LNS;
   
   LOOP
      
      FETCH C_TMP_REFRESH_LNS
      INTO l_unitcode          ,
         l_acc_type            ,
         l_acc_sn              ,
         l_RL_PNL_INT_AMN      ,
         l_URL_PNL_INT_AMN     ,
         l_ACR_PNL_INT_AMN     ,
         l_TOT_EXP_IN_CC_AMN   ,
         l_TOT_COM_IN_CC_AMN   ,
         l_THRDPRT_AMN         ,
         l_TOT_SUBS_INT_AMN    ,
         l_TOT_EXPENSE_AMN     ,
         l_TOT_CONFIRM_AMN     ,
         l_POSITIVE_AMN        ,
         l_UNCLEAR_AMN         ,
         l_BLOCKED_AMN         ,
         l_DORMANT_AMN         ,
         l_last_nrm_trx_cnt    ,
         l_max_request_sn      ,
         l_INSTALL_ROUND_AMN   ,
         l_TOT_PUB_COMM_AMN    ,
         l_TOT_INT_SPRD_AMN    ,
         l_TOT_THRDPRT_AMN     ,
         l_MP_START_CAP_AMN    ,
         l_TOT_DRAWDOWN_AMN    ,
         l_TOT_PNL_INT_AMN     ,
         l_TOT_NRM_INT_AMN     ,
         l_TOT_CAP_AMN         ,
         l_TOT_COMMISSION_AMN  ,
         L_LST_INT_DB_AMN      ,
         l_CAPITAL_AMN         ,
         l_EXPENSE_AMN         ,
         l_COMMISSION_AMN      ,
         l_RL_NRM_INT_AMN      ,
         l_URL_NRM_INT_AMN     ,
         l_ACR_NRM_INT_AMN     ,
         l_URL_PUB_INT_AMN     ,
         l_ACR_PUB_INT_AMN     ,
         l_SUBSIDY_AMN         ,
         l_CAPITAL_AMN_1       ,
         l_EXPENSE_AMN_1       ,
         l_COMMISSION_AMN_1    ,
         l_RL_NRM_INT_AMN_1    ,
         l_URL_NRM_INT_AMN_1   ,
         l_ACR_NRM_INT_AMN_1   ,
         l_URL_PUB_INT_AMN_1   ,
         l_ACR_PUB_INT_AMN_1   ,
         l_SUBSIDY_AMN_1       ,
         L_MAX_REQ_LOAN_STATUS ,
         LCOUNTER;
      
      
      --      IF C_TMP_REFRESH_LNS%NOTFOUND THEN
      --         EXIT;
      --      END IF;
      IF AT_END = 1 THEN
         GOTO L1;
      END IF;
      
      UPDATE
         R_LOAN_ACCOUNT
      SET OV_RL_PNL_INT_BAL = OV_RL_PNL_INT_BAL  + L_RL_PNL_INT_AMN                         ,
         OV_URL_PNL_INT_BAL = OV_URL_PNL_INT_BAL + L_URL_PNL_INT_AMN                        ,
         OV_ACR_PNL_INT_BAL = OV_ACR_PNL_INT_BAL + L_ACR_PNL_INT_AMN                        ,
         TOT_EXP_IN_CC_AMN  = TOT_EXP_IN_CC_AMN  + L_TOT_EXP_IN_CC_AMN                      ,
         TOT_COM_IN_CC_AMN  = TOT_COM_IN_CC_AMN  + L_TOT_COM_IN_CC_AMN                      ,
         TOT_SUBS_INT_AMN   = TOT_SUBS_INT_AMN   + L_TOT_SUBS_INT_AMN                       ,
         TOT_EXPENSE_AMN    = TOT_EXPENSE_AMN    + L_TOT_EXPENSE_AMN                        ,
         TOT_CONFIRM_AMN    = TOT_CONFIRM_AMN    + L_TOT_CONFIRM_AMN                        ,
         LST_TRX_DT         = l_sched_date                                                  ,
         LAST_NRM_TRX_CNT   = L_LAST_NRM_TRX_CNT                                            ,
         REQ_INSTALL_SN     = L_MAX_REQUEST_SN                                              ,
         LOAN_STATUS        = DECODE ( LCOUNTER , 0 , LOAN_STATUS , L_MAX_REQ_LOAN_STATUS ) ,
         TOT_PUB_COMM_AMN   = TOT_PUB_COMM_AMN   + l_TOT_PUB_COMM_AMN                         ,
         TOT_INT_SPRD_AMN   = TOT_INT_SPRD_AMN   + l_TOT_INT_SPRD_AMN                         ,
         TOT_THRDPRT_AMN    = TOT_THRDPRT_AMN    + l_TOT_THRDPRT_AMN                          ,
         MP_START_CAP_AMN   = MP_START_CAP_AMN   + l_MP_START_CAP_AMN                         ,
         TOT_DRAWDOWN_AMN   = TOT_DRAWDOWN_AMN   + l_TOT_DRAWDOWN_AMN                         ,
         TOT_PNL_INT_AMN    = TOT_PNL_INT_AMN    + l_TOT_PNL_INT_AMN                          ,
         TOT_NRM_INT_AMN    = TOT_NRM_INT_AMN    + l_TOT_NRM_INT_AMN                          ,
         TOT_CAP_AMN        = TOT_CAP_AMN        + l_TOT_CAP_AMN                              ,
         TOT_COMMISSION_AMN = TOT_COMMISSION_AMN + l_TOT_COMMISSION_AMN                       ,
         NRM_CAP_BAL        = NRM_CAP_BAL        + l_CAPITAL_AMN                              ,
         OV_CAP_BAL         = OV_CAP_BAL         + l_CAPITAL_AMN_1                            ,
         NRM_EXP_BAL        = NRM_EXP_BAL        + l_EXPENSE_AMN                              ,
         OV_EXP_BAL         = OV_EXP_BAL         + l_EXPENSE_AMN_1                            ,
         NRM_COM_BAL        = NRM_COM_BAL        + l_COMMISSION_AMN                           ,
         OV_COM_BAL         = OV_COM_BAL         + l_COMMISSION_AMN_1                         ,
         NRM_RL_INT_BAL     = NRM_RL_INT_BAL     + l_RL_NRM_INT_AMN                           ,
         OV_RL_NRM_INT_BAL  = OV_RL_NRM_INT_BAL  + l_RL_NRM_INT_AMN_1                         ,
         NRM_URL_INT_BAL    = NRM_URL_INT_BAL    + l_URL_NRM_INT_AMN                          ,
         OV_URL_NRM_INT_BAL = OV_URL_NRM_INT_BAL + l_URL_NRM_INT_AMN_1                        ,
         NRM_ACR_INT_BAL    = NRM_ACR_INT_BAL    + l_ACR_NRM_INT_AMN                          ,
         OV_ACR_NRM_INT_BAL = OV_ACR_NRM_INT_BAL + l_ACR_NRM_INT_AMN_1                        ,
         NR_URL_PUB_INT_AMN = NR_URL_PUB_INT_AMN + l_URL_PUB_INT_AMN                          ,
         OV_URL_PUB_INT_AMN = OV_URL_PUB_INT_AMN + l_URL_PUB_INT_AMN_1                        ,
         NR_ACR_PUB_INT_AMN = NR_ACR_PUB_INT_AMN + l_ACR_PUB_INT_AMN                          ,
         OV_ACR_PUB_INT_AMN = OV_ACR_PUB_INT_AMN + l_ACR_PUB_INT_AMN_1                        ,
         NRM_SUBSIDY_BAL    = NRM_SUBSIDY_BAL    + l_SUBSIDY_AMN                              ,
         OV_SUBSIDY_BAL     = OV_SUBSIDY_BAL     + l_SUBSIDY_AMN_1
      WHERE
         FK_UNITCODE = L_UNITCODE
         AND
         ACC_TYPE = L_ACC_TYPE
         AND
         ACC_SN = L_ACC_SN;
      
      
      UPDATE
         R_LOAN_ACCOUNT_INF
      SET POSITIVE_AMN     = POSITIVE_AMN      + l_POSITIVE_AMN ,
         UNCLEAR_AMN       = UNCLEAR_AMN       + l_UNCLEAR_AMN  ,
         BLOCKED_AMN       = BLOCKED_AMN       + l_BLOCKED_AMN  ,
         DORMANT_AMN       = DORMANT_AMN       + l_DORMANT_AMN  ,
         INSTALL_ROUND_AMN = INSTALL_ROUND_AMN + l_INSTALL_ROUND_AMN--,
         --NRM_ACCRUAL_AMN      = l_NRM_ACCRUAL_AMN,
         --OV_ACCRUAL_AMN       = l_OV_ACCRUAL_AMN,
         --HOLD_NRM_RL_ACCR     = l_HOLD_NRM_RL_ACCR,
         --HOLD_OV_RL_ACCR      = l_HOLD_OV_RL_ACCR,
         --ACC_TRANSITION_FLG   = l_ACC_TRANSITION_FLG,
         --PROVISION_AMOUNT     = L_PROVISION_AMOUNT,
         --LOAN_CLASS           = L_LOAN_CLASS,
         --LOAN_SUB_CLASS       = L_SUB_CLASS
      WHERE
         FK_LOAN_ACCOUNTFK = L_UNITCODE
         AND
         FK0LOAN_ACCOUNTACC = L_ACC_TYPE
         AND
         FK_LOAN_ACCOUNTACC = L_ACC_SN;
   
   END LOOP;
   
   CLOSE C_TMP_REFRESH_LNS;
   COMMIT;
   
   OPEN c_loan_acc_info;
   LOOP
      SET l_NRM_ACCRUAL_AMN    = 0;
      SET l_OV_ACCRUAL_AMN     = 0;
      SET l_HOLD_NRM_RL_ACCR   = 0;
      SET l_HOLD_OV_RL_ACCR    = 0;
      SET L_RECOVERIES_DT      = To_Date ( '01/01/0001' , 'DD/MM/YYYY' ) ;
      SET l_ACC_TRANSITION_FLG = 0;
      SET L_UNCLEAR_AMN        = 0;
      SET L_PROVISION_AMOUNT   = 0;
      
      FETCH c_loan_acc_info
      INTO l_unitcode         ,
         l_acc_type           ,
         l_acc_sn             ,
         l_NRM_ACCRUAL_AMN    ,
         l_OV_ACCRUAL_AMN     ,
         l_HOLD_NRM_RL_ACCR   ,
         l_HOLD_OV_RL_ACCR    ,
         l_ACC_TRANSITION_FLG ,
         L_UNCLEAR_AMN        ,
         L_NRM_RL_URL_FLG     ,
         L_OV_RL_URL_FLG      ,
         l_provision_amount   ,
         L_LOAN_CLASS         ,
         L_SUB_CLASS          ,
         L_RECOVERIES_DT ;
      
      --      IF c_loan_acc_info%notfound THEN
      --         EXIT;
      --      END IF;
      
      IF AT_END = 1 THEN
         GOTO L1;
      END IF;
      
      UPDATE
         R_LOAN_ACCOUNT_INF
      SET NRM_ACCRUAL_AMN = l_NRM_ACCRUAL_AMN  ,
         OV_ACCRUAL_AMN   = l_OV_ACCRUAL_AMN   ,
         HOLD_NRM_RL_ACCR = l_HOLD_NRM_RL_ACCR ,
         HOLD_OV_RL_ACCR  = l_HOLD_OV_RL_ACCR  ,
         UNCLEAR_AMN      = L_UNCLEAR_AMN      ,
         NRM_RL_URL_FLG   = L_NRM_RL_URL_FLG   ,
         OV_RL_URL_FLG    = L_OV_RL_URL_FLG    ,
         PROVISION_AMOUNT = L_PROVISION_AMOUNT ,
         LOAN_CLASS       = L_LOAN_CLASS       ,
         LOAN_SUB_CLASS   = L_SUB_CLASS        ,
         RECOVERIES_DT    = L_RECOVERIES_DT
      WHERE
         FK_LOAN_ACCOUNTFK = l_unitcode
         AND
         FK0LOAN_ACCOUNTACC = l_acc_type
         AND
         FK_LOAN_ACCOUNTACC = l_acc_sn;
   
   END LOOP;
   CLOSE c_loan_acc_info;
   COMMIT;
   
   L1: SET AT_END = 0;
   
   -- EXCEPTION
   -- WHEN OTHERS THEN
   -- RAISE_APPLICATION_ERROR ( -20001 , 'AN ERROR WAS ENCOUNTERED - '
   -- ||
   -- SQLCODE
   -- ||
   -- ' -ERROR- '
   -- ||
   -- SQLERRM ) ;
END;

