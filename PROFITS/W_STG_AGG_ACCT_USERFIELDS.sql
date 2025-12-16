create table W_STG_AGG_ACCT_USERFIELDS
(
    ACCOUNT_NUMBER           CHAR(40),
    PRFT_SYSTEM              SMALLINT,
    PROVISION_AMT            DECIMAL(15, 2),
    INTEREST_IN_SUSPENSE_AMT DECIMAL(15, 2),
    DISCOUNTED_VALUE_AMT     DECIMAL(15, 2),
    FINAL_CLASS_IND          VARCHAR(100),
    FINAL_SUB_CLASS_IND      CHAR(1),
    ADJUSTED_SUB_CLASS_IND   VARCHAR(100),
    ADJUSTED_CLASS_IND       VARCHAR(100),
    ACTUAL_SUB_CLASS_IND     VARCHAR(100),
    COLLATERAL_OM_VALUE_AMT  DECIMAL(15, 2),
    FINAL_SUB_CLASS_NAME     CHAR(11),
    FINAL_CLASS_NAME         VARCHAR(14),
    DATE_CLASS_CHANGED       DATE
);

create unique index PK_W_STG_AGG_ACCT_USERFIELDS
    on W_STG_AGG_ACCT_USERFIELDS (ACCOUNT_NUMBER);

CREATE PROCEDURE W_STG_AGG_ACCT_USERFIELDS ( )
  SPECIFIC SQL160620112633247
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_stg_agg_acct_userfields;
INSERT INTO w_stg_agg_acct_userfields (
               account_number
              ,prft_system
              ,provision_amt
              ,interest_in_suspense_amt
              ,discounted_value_amt
              ,final_class_ind
              ,final_sub_class_ind
              ,adjusted_sub_class_ind
              ,adjusted_class_ind
              ,actual_sub_class_ind
              ,collateral_om_value_amt
              ,date_class_changed
              ,final_sub_class_name
              ,final_class_name)
   WITH t
        AS (SELECT   ud_account_number account_number
                    ,ud_prft_system prft_system
                    ,MAX (
                        UTILPKG.numtext (
                           CASE
                              WHEN     pfg_tag = 'PROVISION2'
                                   AND pfg_set_sn = '2'
                                   AND pfg_tag_set_code = 'PROVISION'
                              THEN
                                 field_value
                           END))
                        provision_amt
                    ,UTILPKG.numtext (
                        MAX (
                           CASE
                              WHEN     pfg_tag = 'PROVISION3'
                                   AND pfg_set_sn = '3'
                                   AND pfg_tag_set_code = 'PROVISION'
                              THEN
                                 field_value
                           END))
                        interest_in_suspense_amt
                    ,UTILPKG.numtext (
                        MAX (
                           CASE
                              WHEN     pfg_tag = 'PROVISION4'
                                   AND pfg_set_sn = '4'
                                   AND pfg_tag_set_code = 'PROVISION'
                              THEN
                                 field_value
                           END))
                        discounted_value_amt
                    ,MAX (
                        CASE
                           WHEN     pfg_tag = 'CLASSIFY13'
                                AND pfg_set_sn = '13'
                                AND pfg_tag_set_code = 'CLASSIFICATION'
                           THEN
                              field_value
                        END)
                        final_class_ind
                    ,MAX (
                        CASE
                           WHEN     pfg_tag = 'CLASSIFY14'
                                AND pfg_set_sn = '14'
                                AND pfg_tag_set_code = 'CLASSIFICATION'
                                AND LENGTH (TRIM (field_value)) IS NOT NULL
                           THEN
                              TRIM (field_value)
                        END)
                        final_sub_class_ind
                    ,MAX (
                        CASE
                           WHEN     pfg_tag = 'CLASSIFY22'
                                AND pfg_set_sn = '22'
                                AND pfg_tag_set_code = 'CLASSIFICATION'
                           THEN
                              field_value
                        END)
                        adjusted_sub_class_ind
                    ,MAX (
                        CASE
                           WHEN     pfg_tag = 'CLASSIFY21'
                                AND pfg_set_sn = '21'
                                AND pfg_tag_set_code = 'CLASSIFICATION'
                           THEN
                              field_value
                        END)
                        adjusted_class_ind
                    ,MAX (
                        CASE
                           WHEN     pfg_tag = 'CLASSIFY07'
                                AND pfg_set_sn = '7'
                                AND pfg_tag_set_code = 'CLASSIFICATION'
                           THEN
                              field_value
                        END)
                        actual_sub_class_ind
                    ,UTILPKG.numtext (
                        MAX (
                           CASE
                              WHEN     pfg_tag = 'CLASSIFY15'
                                   AND pfg_set_sn = '15'
                                   AND pfg_tag_set_code = 'CLASSIFICATION'
                              THEN
                                 field_value
                           END))
                        collateral_om_value_amt
                    ,MAX (
                        CASE
                           WHEN     pfg_tag = 'CLASSIFY02'
                                AND pfg_set_sn = '2'
                                AND pfg_tag_set_code = 'CLASSIFICATION'
                           THEN
                              field_value
                        END)
                        date_class_changed
            FROM     user_defined_fields
            WHERE        LENGTH (TRIM (ud_account_number)) IS NOT NULL
                     AND ud_prft_system = 4
            GROUP BY ud_account_number, ud_prft_system)
   SELECT account_number
         ,prft_system
         ,provision_amt
         ,interest_in_suspense_amt
         ,discounted_value_amt
         ,TRIM (final_class_ind) final_class_ind
         ,NVL (final_sub_class_ind, 0) final_sub_class_ind
         ,TRIM (adjusted_sub_class_ind) adjusted_sub_class_ind
         ,TRIM (adjusted_class_ind) adjusted_class_ind
         ,TRIM (actual_sub_class_ind) actual_sub_class_ind
         ,collateral_om_value_amt
         ,UTILPKG.datetext (date_class_changed, 'dd-mm-yyyy')
             date_class_changed
         ,CASE final_sub_class_ind
             WHEN '1' THEN 'Normal'
             WHEN '2' THEN 'Watch'
             WHEN '3' THEN 'Substandard'
             WHEN '4' THEN 'Doubtful'
             WHEN '5' THEN 'Loss'
             ELSE 'n/a'
          END
             final_sub_class_name
         ,CASE final_class_ind
             WHEN '1' THEN 'Non-performing'
             ELSE 'Performing'
          END
             final_class_name
   FROM   t;
END;

