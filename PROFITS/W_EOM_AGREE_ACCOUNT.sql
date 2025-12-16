create table W_EOM_AGREE_ACCOUNT
(
    EOM_DATE           DATE,
    ACCT_KEY           DECIMAL(11),
    ACCOUNT_NUMBER     CHAR(40),
    PRFT_SYSTEM        SMALLINT,
    ACCOUNT_CD         SMALLINT,
    AGR_UNIT           INTEGER,
    AGR_YEAR           SMALLINT,
    AGR_SN             INTEGER,
    AGR_MEMBERSHIP_SN  INTEGER,
    AGR_STATUS         VARCHAR(1),
    AGR_LC_INDICATOR   VARCHAR(1),
    AGR_EURO_INDICATOR VARCHAR(1),
    AGR_FC_INDICATOR   VARCHAR(1),
    AGR_ISSUE_DT       DATE,
    AGR_SIGNING_DT     DATE,
    AGR_LIMIT          DECIMAL(15, 2),
    AGR_UTILISED_LIMIT DECIMAL(15, 2),
    AGR_BLOCKED_LIMIT  DECIMAL(15, 2),
    AGR_BLOCKED_CNT    SMALLINT,
    AGR_AMENDMENT_CNT  SMALLINT,
    AGR_EXPIRY_DT      DATE,
    OFF_ASGN_DT        DATE,
    PRV_OFFICER        VARCHAR(8),
    PRV_OFFIC_ASN_DT   DATE,
    DEL_OFF_ASN_DT     DATE,
    DEL_PRV_OFFICER    VARCHAR(8),
    DEL_PRV_OFF_ASN_DT DATE,
    ACC_DOM_CNT        SMALLINT,
    ACC_EURO_ZONE_CNT  SMALLINT,
    ACC_FC_CNT         SMALLINT,
    ACC_ACTIVE_CNT     SMALLINT,
    ACC_OPEN_EXP_DT    DATE,
    ACC_KIND           VARCHAR(1),
    AGR_COMMENTS       VARCHAR(40),
    TMSTAMP            DATE,
    ONE_ACCOUNT_FLG    VARCHAR(1),
    FK_CUST_ADDRESSFK  INTEGER,
    FK_CUST_ADDRESSSER SMALLINT,
    FK_AGREEMENT_TYFK  INTEGER,
    CURRENCY_ID        INTEGER,
    FK_GENERIC_DETAFK  VARCHAR(5),
    FK_GENERIC_DETASER INTEGER,
    AGR_LIMIT_IND      VARCHAR(1),
    FK_BANKEMPLOYEEID  VARCHAR(8),
    FK0BANKEMPLOYEEID  VARCHAR(8),
    USER_CODE          CHAR(8),
    HISTORY_CNT        INTEGER,
    HISTORY_CURR_SN    INTEGER,
    AGR_ANNEX_DT       DATE,
    ADDITIONAL_CNT     INTEGER,
    PEND_FINAL_DT      DATE,
    AMN_PEND_STS       VARCHAR(1),
    AGR_AMN_PENDING    DECIMAL(15, 2),
    ACC_DOM_NEW_CNT    INTEGER,
    ACC_EURZON_NEW_CNT INTEGER,
    ACC_ACTIVE_NEW_CNT INTEGER,
    ACC_FC_NEW_CNT     INTEGER,
    AGR_EXTRA_COMMENTS VARCHAR(2046)
);

create unique index IDX_W_EOM_AGREE_ACCOUNT
    on W_EOM_AGREE_ACCOUNT (EOM_DATE, FK0BANKEMPLOYEEID);

create unique index PK_W_EOM_AGREE_ACCOUNT
    on W_EOM_AGREE_ACCOUNT (EOM_DATE, ACCOUNT_NUMBER);

CREATE PROCEDURE W_EOM_AGREE_ACCOUNT ( )
  SPECIFIC SQL160620112634261
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_agree_account
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_eom_agree_account (
               acct_key
              ,eom_date
              ,account_number
              ,prft_system
              ,account_cd
              ,agr_unit
              ,agr_year
              ,agr_sn
              ,agr_membership_sn
              ,user_code
              ,agr_status
              ,agr_lc_indicator
              ,agr_euro_indicator
              ,agr_fc_indicator
              ,agr_issue_dt
              ,agr_signing_dt
              ,agr_limit
              ,agr_utilised_limit
              ,agr_blocked_limit
              ,agr_blocked_cnt
              ,agr_amendment_cnt
              ,agr_expiry_dt
              ,off_asgn_dt
              ,prv_officer
              ,prv_offic_asn_dt
              ,acc_dom_cnt
              ,acc_euro_zone_cnt
              ,acc_fc_cnt
              ,acc_active_cnt
              ,acc_open_exp_dt
              ,acc_kind
              ,agr_comments
              ,tmstamp
              ,one_account_flg
              ,fk_cust_addressfk
              ,fk_cust_addressser
              ,fk_agreement_tyfk
              ,currency_id
              ,fk_generic_detafk
              ,fk_generic_detaser
              ,agr_limit_ind
              ,fk_bankemployeeid
              ,fk0bankemployeeid
              ,history_cnt
              ,history_curr_sn
              ,agr_annex_dt
              ,additional_cnt
             ,pend_final_dt
              ,amn_pend_sts
              ,agr_amn_pending
              ,acc_dom_new_cnt
              ,acc_eurzon_new_cnt
              ,acc_active_new_cnt
              ,acc_fc_new_cnt
              ,agr_extra_comments)
   SELECT account_ser_num acct_key
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
         ,b.account_number
         ,b.prft_system
         ,b.account_cd
         ,c.fk_unitcode agr_unit
         ,b.agr_year
         ,b.agr_sn
         ,b.agr_membership_sn
         ,c.user_code
         ,c.agr_status
         ,c.agr_lc_indicator
         ,c.agr_euro_indicator
         ,c.agr_fc_indicator
         ,c.agr_issue_dt
         ,c.agr_signing_dt
         ,c.agr_limit
         ,c.agr_utilised_limit
         ,c.agr_blocked_limit
         ,c.agr_blocked_cnt
         ,c.agr_amendment_cnt
         ,c.agr_expiry_dt
         ,c.off_asgn_dt
         ,c.prv_officer
         ,c.prv_offic_asn_dt
         ,c.acc_dom_cnt
         ,c.acc_euro_zone_cnt
         ,c.acc_fc_cnt
         ,c.acc_active_cnt
         ,c.acc_open_exp_dt
         ,c.acc_kind
         ,c.agr_comments
         ,c.tmstamp
         ,c.one_account_flg
         ,c.fk_cust_addressfk
         ,c.fk_cust_addressser
         ,c.fk_agreement_tyfk
         ,c.fk_currencyid_curr AS currency_id
         ,c.fk_generic_detafk
         ,c.fk_generic_detaser
         ,c.agr_limit_ind
         ,c.fk_bankemployeeid
         ,c.fk0bankemployeeid
         ,c.history_cnt
         ,c.history_curr_sn
         ,c.agr_annex_dt
         ,c.additional_cnt
         ,c.pend_final_dt
         ,c.amn_pend_sts
         ,c.agr_amn_pending
         ,c.acc_dom_new_cnt
         ,c.acc_eurzon_new_cnt
         ,c.acc_active_new_cnt
         ,c.acc_fc_new_cnt
         ,c.agr_extra_comments
   FROM   r_agreement c
          JOIN profits_account b
             ON (    b.agr_unit = c.fk_unitcode
                 AND b.agr_year = c.agr_year
                 AND b.agr_sn = c.agr_sn
                 AND b.agr_membership_sn = c.agr_membership_sn)
   WHERE  b.prft_system = 19;
END;

