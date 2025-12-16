create table EOM_AGREEMENT
(
    EOM_DATE           DATE     not null,
    CUST_ID            INTEGER  not null,
    ACCOUNT_NUMBER     CHAR(40) not null,
    ACCOUNT_CD         SMALLINT,
    ACC_UNIT           INTEGER,
    PRFT_SYSTEM        SMALLINT,
    AGR_YEAR           SMALLINT,
    AGR_SN             INTEGER,
    AGR_MEMBERSHIP_SN  INTEGER,
    OLD_NUMBER         DECIMAL(12),
    AGR_LIMIT          DECIMAL(15, 2),
    AGR_UTILISED_LIMIT DECIMAL(15, 2),
    AGR_BLOCKED_LIMIT  DECIMAL(15, 2),
    AGR_BLOCKED_CNT    SMALLINT,
    AGR_AMENDMENT_CNT  SMALLINT,
    ACC_DOM_CNT        SMALLINT,
    ACC_EURO_ZONE_CNT  SMALLINT,
    ACC_FC_CNT         SMALLINT,
    ACC_ACTIVE_CNT     SMALLINT,
    FK_CUST_ADDRESSFK  INTEGER,
    FK_CUST_ADDRESSSER SMALLINT,
    FK_AGREEMENT_TYFK  INTEGER,
    ID_CURRENCY        INTEGER,
    FK_GENERIC_DETASER INTEGER,
    HISTORY_CNT        INTEGER,
    HISTORY_CURR_SN    INTEGER,
    ADDITIONAL_CNT     INTEGER,
    AGR_AMN_PENDING    DECIMAL(15, 2),
    ACC_DOM_NEW_CNT    INTEGER,
    ACC_EURZON_NEW_CNT INTEGER,
    ACC_ACTIVE_NEW_CNT INTEGER,
    ACC_FC_NEW_CNT     INTEGER,
    AGR_EXPIRY_DT      DATE,
    OFF_ASGN_DT        DATE,
    PRV_OFFIC_ASN_DT   DATE,
    DEL_OFF_ASN_DT     DATE,
    DEL_PRV_OFF_ASN_DT DATE,
    AGR_ANNEX_DT       DATE,
    PEND_FINAL_DT      DATE,
    ACC_OPEN_EXP_DT    DATE,
    TMSTAMP            DATE,
    AGR_ISSUE_DT       DATE,
    AGR_SIGNING_DT     DATE,
    MAIN_BENEF_FLG     CHAR(1),
    AGR_STATUS         CHAR(1),
    AGR_LC_INDICATOR   CHAR(1),
    AGR_EURO_INDICATOR CHAR(1),
    AGR_FC_INDICATOR   CHAR(1),
    ACC_KIND           CHAR(1),
    AMN_PEND_STS       CHAR(1),
    ONE_ACCOUNT_FLG    CHAR(1),
    AGR_LIMIT_IND      CHAR(1),
    FK_GENERIC_DETAFK  CHAR(5),
    PRV_OFFICER        CHAR(8),
    FK_BANKEMPLOYEEID  CHAR(8),
    FK0BANKEMPLOYEEID  CHAR(8),
    DEL_PRV_OFFICER    CHAR(8),
    USER_CODE          CHAR(8),
    AGR_COMMENTS       CHAR(40),
    AGR_EXTRA_COMMENTS VARCHAR(2046),
    ACCT_KEY           DECIMAL(11),
    constraint IXU_EOM_002
        primary key (EOM_DATE, CUST_ID, ACCOUNT_NUMBER)
);

create unique index IDX_EOM_AGREEMENT_ACCTKEY
    on EOM_AGREEMENT (EOM_DATE, ACCT_KEY);

CREATE PROCEDURE EOM_AGREEMENT ( )
  SPECIFIC SQL160620112634262
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE eom_agreement
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO eom_agreement (
               account_number
              ,account_cd
              ,acc_unit
              ,prft_system
              ,agr_year
              ,agr_sn
              ,agr_membership_sn
              ,user_code
              ,old_number
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
              ,id_currency
              ,fk_generic_detafk
              ,fk_generic_detaser
              ,cust_id
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
              ,agr_extra_comments
              ,main_benef_flg
              ,eom_date)
   SELECT b.account_number
         ,b.account_cd
         ,c.fk_unitcode AS acc_unit
         ,b.prft_system
         ,c.agr_year
         ,c.agr_sn
         ,c.agr_membership_sn
         ,user_code
         ,old_number
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
         ,c.tmstamp
         ,one_account_flg
         ,fk_cust_addressfk
         ,fk_cust_addressser
         ,fk_agreement_tyfk
         ,fk_currencyid_curr AS id_currency
         ,fk_generic_detafk
         ,fk_generic_detaser
         ,d.fk_customercust_id AS cust_id
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
         ,agr_extra_comments
         ,d.main_benef_flg
         , (SELECT scheduled_date FROM bank_parameters) AS eom_date
   FROM   profits_account b, r_agreement c, agreement_benef d
   WHERE      b.agr_unit = c.fk_unitcode
          AND b.agr_year = c.agr_year
          AND b.agr_sn = c.agr_sn
          AND b.agr_membership_sn = c.agr_membership_sn
          AND b.prft_system = 19
          AND c.fk_unitcode = d.fk_agreementfk_uni
          AND c.agr_year = d.fk_agreementagr_ye
          AND c.agr_sn = d.fk_agreementagr_sn
          AND c.agr_membership_sn = d.fk_agreementagr_me
          AND d.benef_status = '1'
          AND c.agr_status IN ('2', '3');
END;

