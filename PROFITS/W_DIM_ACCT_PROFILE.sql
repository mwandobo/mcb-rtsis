create table W_DIM_ACCT_PROFILE
(
    PROFILE_KEY          SMALLINT,
    FINAL_SUB_CLASS_IND  CHAR(1),
    FINAL_SUB_CLASS_NAME CHAR(11)
);

create unique index IXU_W_DIM_ACCT_PROFILE
    on W_DIM_ACCT_PROFILE (FINAL_SUB_CLASS_IND, FINAL_SUB_CLASS_NAME);

create unique index PK_W_DIM_ACCT_PROFILE
    on W_DIM_ACCT_PROFILE (PROFILE_KEY);

CREATE PROCEDURE W_DIM_ACCT_PROFILE ( )
  SPECIFIC SQL160620112633450
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_dim_acct_profile a
USING      (SELECT     (SELECT NVL (MAX (profile_key), 0)
                        FROM   w_dim_acct_profile)
                     + ROW_NUMBER () OVER (ORDER BY final_sub_class_name)
                        profile_key
                    ,final_sub_class_ind
                    ,final_sub_class_name
            FROM     w_stg_agg_acct_userfields
            GROUP BY final_sub_class_ind, final_sub_class_name) b
ON         (    a.final_sub_class_ind = b.final_sub_class_ind
            AND a.final_sub_class_name = b.final_sub_class_name)
WHEN NOT MATCHED
THEN
   INSERT     (profile_key, final_sub_class_ind, final_sub_class_name)
   VALUES     (b.profile_key, b.final_sub_class_ind, b.final_sub_class_name);
END;

