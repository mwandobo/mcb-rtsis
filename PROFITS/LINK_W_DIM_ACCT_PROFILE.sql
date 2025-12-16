CREATE PROCEDURE LINK_W_DIM_ACCT_PROFILE ( )
  SPECIFIC SQL160620112633755
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_eom_account a
USING      (SELECT wdap.profile_key
                  ,auf.account_number
                  ,auf.prft_system
                  ,wdap.final_sub_class_ind
                  ,wdap.final_sub_class_name
            FROM   w_stg_agg_acct_userfields auf
                   INNER JOIN w_dim_acct_profile wdap
                      ON     auf.final_sub_class_ind =
                                wdap.final_sub_class_ind
                         AND auf.final_sub_class_name =
                                wdap.final_sub_class_name) b
ON         (    a.account_number = b.account_number
            AND a.prft_system = b.prft_system)
WHEN MATCHED
THEN
   UPDATE SET a.profile_key = b.profile_key;
END;

