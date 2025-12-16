create table W_FACT_PROVISIONS_RELEASE
(
    DATE_RELEASED           DATE,
    ACCT_KEY                DECIMAL(11),
    RELEASED_PROVISIONS_AMT DECIMAL(15, 2)
);

create unique index PK_W_FACT_PROVISIONS_RELEASE
    on W_FACT_PROVISIONS_RELEASE (DATE_RELEASED, ACCT_KEY);

CREATE PROCEDURE W_FACT_PROVISIONS_RELEASE ( )
  SPECIFIC SQL160620112637081
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_fact_provisions_release
WHERE  date_released = (SELECT scheduled_date FROM bank_parameters);
INSERT INTO w_fact_provisions_release (
               date_released
              ,acct_key
              ,released_provisions_amt)
   WITH pre
        AS (SELECT MAX (production_date) pre_date
            FROM   w_dim_date
            WHERE  production_date <
                      (SELECT scheduled_date FROM bank_parameters))
       ,t
        AS (SELECT *
            FROM   (SELECT   acct_key
                            ,final_class_name
                            ,LAG (
                                final_class_name)
                             OVER (
                                PARTITION BY acct_key
                                ORDER BY acct_key, eom_date)
                                AS prior_final_class
                    FROM     w_eom_loan_account, pre
                    WHERE    eom_date IN ( (SELECT scheduled_date
                                            FROM   bank_parameters)
                                         ,pre.pre_date)
                    GROUP BY acct_key, eom_date, final_class_name)
            WHERE      prior_final_class = 'Non-performing'
                   AND final_class_name = 'Performing')
   SELECT (SELECT scheduled_date FROM bank_parameters) date_released
         ,l.acct_key
         ,NVL (l.interest_in_suspense, 0) + NVL (l.provision_amount, 0)
             released_provisions_amt
   FROM   w_eom_loan_account l
          INNER JOIN t ON t.acct_key = l.acct_key
          INNER JOIN pre ON pre.pre_date = l.eom_date;
END;

