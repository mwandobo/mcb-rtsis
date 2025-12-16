create table W_DH_CUST_CATEGORY
(
    CUSTOMER_KEY        DECIMAL(11) not null,
    CATEGORY_KEY        DECIMAL(9)  not null,
    ROW_EFFECTIVE_DATE  DATE        not null,
    ROW_EXPIRATION_DATE DATE,
    ROW_CURRENT_FLAG    DECIMAL(1),
    constraint PK_W_DH_CUST_CATEGORY
        primary key (CUSTOMER_KEY, CATEGORY_KEY, ROW_EFFECTIVE_DATE)
);

CREATE PROCEDURE W_DH_CUST_CATEGORY ( )
  SPECIFIC SQL160620112633653
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_dh_cust_category
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (customer_key, category_key) IN (SELECT t.customer_key
                                                  ,t.category_key
                                            FROM   w_dh_cust_category t
                                                   INNER JOIN
                                                   (SELECT customer_key
                                                          ,category_key
                                                    FROM   w_dh_cust_category
                                                    WHERE  row_current_flag =
                                                              1
                                                    MINUS
                                                    SELECT w_dim_customer.customer_key
                                                          ,w_dim_cust_category.category_key
                                                    FROM   customer_category
                                                           INNER JOIN
                                                           w_dim_customer
                                                              ON     w_dim_customer.cust_id =
                                                                        fk_customercust_id
                                                                 AND w_dim_customer.row_current_flag =
                                                                        1
                                                           INNER JOIN
                                                           generic_detail gd
                                                              ON (    customer_category.fk_generic_detafk =
                                                                         gd.fk_generic_headpar
                                                                  AND customer_category.fk_generic_detaser =
                                                                         gd.serial_num)
                                                           INNER JOIN
                                                           w_dim_cust_category
                                                              ON (    w_dim_cust_category.category_code =
                                                                         customer_category.fk_categorycategor
                                                                  AND w_dim_cust_category.category_value =
                                                                         gd.description))
                                                   s
                                                      ON (    t.customer_key =
                                                                 s.customer_key
                                                          AND t.category_key =
                                                                 s.category_key));
INSERT INTO w_dh_cust_category (
               customer_key
              ,category_key
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag)
   SELECT customer_key
         ,category_key
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
   FROM   (SELECT w_dim_customer.customer_key
                 ,w_dim_cust_category.category_key
           FROM   customer_category
                  INNER JOIN w_dim_customer
                     ON     w_dim_customer.cust_id = fk_customercust_id
                        AND w_dim_customer.row_current_flag = 1
                  INNER JOIN generic_detail gd
                     ON (    customer_category.fk_generic_detafk =
                                gd.fk_generic_headpar
                         AND customer_category.fk_generic_detaser =
                                gd.serial_num)
                  INNER JOIN w_dim_cust_category
                     ON (    w_dim_cust_category.category_code =
                                customer_category.fk_categorycategor
                         AND w_dim_cust_category.category_value =
                                gd.description)
           MINUS
           SELECT customer_key, category_key
           FROM   w_dh_cust_category
           WHERE  row_current_flag = 1);
END;

