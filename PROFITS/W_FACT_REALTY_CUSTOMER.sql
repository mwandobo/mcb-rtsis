create table W_FACT_REALTY_CUSTOMER
(
    REALTY_KEY           DECIMAL(10) not null,
    CUST_ID              DECIMAL(7)  not null,
    INTERNAL_SN          DECIMAL(10) not null,
    ROW_EFFECTIVE_DATE   DATE        not null,
    ROW_EXPIRATION_DATE  DATE,
    ROW_CURRENT_FLAG     DECIMAL(1),
    STATUS_IND_NAME      VARCHAR(7),
    OWNERSHIP_PERCENTAGE DECIMAL(5),
    CUSTOMER_NAME        VARCHAR(200),
    CUST_CD              DECIMAL(1),
    constraint PK_W_FACT_REALTY_CUSTOMER
        primary key (REALTY_KEY, CUST_ID, INTERNAL_SN, ROW_EFFECTIVE_DATE)
);

CREATE PROCEDURE W_FACT_REALTY_CUSTOMER ( )
  SPECIFIC SQL160620112636978
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_fact_realty_customer
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (realty_key, cust_id, internal_sn) IN (SELECT t.realty_key
                                                        ,t.cust_id
                                                        ,t.internal_sn
                                                  FROM   w_fact_realty_customer t
                                                         INNER JOIN
                                                         (SELECT realty_key
                                                                ,cust_id
                                                                ,internal_sn
                                                                ,status_ind_name
                                                                ,ownership_percentage
                                                                ,customer_name
                                                                ,cust_cd
                                                          FROM   w_fact_realty_customer
                                                          WHERE  row_current_flag =
                                                                    1
                                                          MINUS
                                                          SELECT   fk_real_estateid
                                                                      realty_key
                                                                  ,fk_customercust_id
                                                                      cust_id
                                                                  ,internal_sn
                                                                  ,MAX (
                                                                      DECODE (
                                                                         r.entry_status
                                                                        ,'1', 'Active'
                                                                        ,'Deleted'))
                                                                      status_ind_name
                                                                  ,SUM (
                                                                      ownersh_perc)
                                                                      ownership_percentage
                                                                  ,DECODE (
                                                                      cust.cust_type
                                                                     ,1,    TRIM (
                                                                               first_name)
                                                                         || '-'
                                                                         || surname
                                                                     ,surname)
                                                                      customer_name
                                                                  ,cust.c_digit
                                                                      cust_cd
                                                          FROM     real_estate_cust r
                                                                   LEFT JOIN
                                                                   customer cust
                                                                      ON cust.cust_id =
                                                                            r.fk_customercust_id
                                                          GROUP BY fk_real_estateid
                                                                  ,fk_customercust_id
                                                                  ,internal_sn
                                                                  ,DECODE (
                                                                      cust.cust_type
                                                                     ,1,    TRIM (
                                                                               first_name)
                                                                         || '-'
                                                                         || surname
                                                                     ,surname)
                                                                  ,cust.c_digit)
                                                         s
                                                            ON (    t.realty_key =
                                                                       s.realty_key
                                                                AND t.cust_id =
                                                                       s.cust_id
                                                                AND t.internal_sn =
                                                                       s.internal_sn));
INSERT INTO w_fact_realty_customer (
               realty_key
              ,cust_id
              ,internal_sn
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag
              ,status_ind_name
              ,ownership_percentage
              ,customer_name
              ,cust_cd)
   SELECT realty_key
         ,cust_id
         ,internal_sn
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
         ,status_ind_name
         ,ownership_percentage
         ,customer_name
         ,cust_cd
   FROM   (SELECT   fk_real_estateid realty_key
                   ,fk_customercust_id cust_id
                   ,internal_sn
                   ,MAX (DECODE (r.entry_status, '1', 'Active', 'Deleted'))
                       status_ind_name
                   ,SUM (ownersh_perc) ownership_percentage
                   ,DECODE (
                       cust.cust_type
                      ,1, TRIM (first_name) || '-' || surname
                      ,surname)
                       customer_name
                   ,cust.c_digit cust_cd
           FROM     real_estate_cust r
                    LEFT JOIN customer cust
                       ON cust.cust_id = r.fk_customercust_id
           GROUP BY fk_real_estateid
                   ,fk_customercust_id
                   ,internal_sn
                   ,DECODE (
                       cust.cust_type
                      ,1, TRIM (first_name) || '-' || surname
                      ,surname)
                   ,cust.c_digit
           MINUS
           SELECT realty_key
                 ,cust_id
                 ,internal_sn
                 ,status_ind_name
                 ,ownership_percentage
                 ,customer_name
                 ,cust_cd
           FROM   w_fact_realty_customer
           WHERE  row_current_flag = 1);
END;

