create table W_EOM_SET_ACCT_HIERARCHY
(
    EOM_DATE         DATE        not null,
    ACCT_KEY         DECIMAL(11) not null,
    WEIGHTING_FACTOR DECIMAL(10, 6),
    RELATED_ACCT_KEY DECIMAL(11) not null,
    UPPER_ACCT_KEY   DECIMAL(11),
    LOWER_ACCT_KEY   DECIMAL(11),
    SELF_FLAG        VARCHAR(3),
    constraint PK_W_EOM_SET_ACCT_HIERARCHY
        primary key (EOM_DATE, ACCT_KEY, RELATED_ACCT_KEY)
);

CREATE PROCEDURE W_EOM_SET_ACCT_HIERARCHY ( )
  SPECIFIC SQL160620112708989
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_set_acct_hierarchy
WHERE  eom_date IN (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_eom_set_acct_hierarchy (
               eom_date
              ,acct_key
              ,weighting_factor
              ,related_acct_key
              ,upper_acct_key
              ,lower_acct_key
              ,self_flag)
   SELECT eom_date
         ,acct_key
         ,1 / COUNT (*) OVER (PARTITION BY eom_date, acct_key)
             weighting_factor
         ,related_acct_key
         ,upper_acct_key
         ,lower_acct_key
         ,CASE
             WHEN     upper_acct_key = lower_acct_key
                  AND upper_acct_key = acct_key
                  AND acct_key = related_acct_key
             THEN
                'Yes'
             ELSE
                'No'
          END
             self_flag
   FROM   (SELECT eom_account.eom_date
                 ,eom_account.acct_key
                 ,lower_acct_key related_acct_key
                 ,upper_acct_key
                 ,lower_acct_key
           FROM   w_dh_acct_agree dh_acct_agree_lookdown
                  LEFT JOIN w_eom_account eom_account
                     ON     dh_acct_agree_lookdown.upper_acct_key =
                               eom_account.acct_key
                        AND eom_account.eom_date >=
                               dh_acct_agree_lookdown.eff_from_date
                        AND eom_account.eom_date <=
                               dh_acct_agree_lookdown.eff_to_date
           UNION
           SELECT eom_account.eom_date
                 ,eom_account.acct_key
                 ,upper_acct_key related_acct_key
                 ,upper_acct_key
                 ,lower_acct_key
           FROM   w_dh_acct_agree dh_acct_agree_lookup
                  JOIN w_eom_account eom_account
                     ON     dh_acct_agree_lookup.lower_acct_key =
                               eom_account.acct_key
                        AND eom_account.eom_date >=
                               dh_acct_agree_lookup.eff_from_date
                        AND eom_account.eom_date <=
                               dh_acct_agree_lookup.eff_to_date)
   WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
END;

