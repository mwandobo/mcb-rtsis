create table W_DIM_CUST_CATEGORY
(
    CATEGORY_KEY   INTEGER,
    CATEGORY_CODE  VARCHAR(8),
    CATEGORY_NAME  VARCHAR(20),
    CATEGORY_VALUE VARCHAR(40)
);

create unique index IXU_W_DIM_CUST_CATEGORY
    on W_DIM_CUST_CATEGORY (CATEGORY_CODE, CATEGORY_VALUE);

create unique index PK_W_DIM_CUST_CATEGORY
    on W_DIM_CUST_CATEGORY (CATEGORY_KEY);

CREATE PROCEDURE W_DIM_CUST_CATEGORY ( )
  SPECIFIC SQL160620112633551
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_dim_cust_category a
USING      (SELECT   (SELECT NVL (MAX (category_key), 0)
                      FROM   w_dim_cust_category)
                   + ROW_NUMBER ()
                     OVER (
                        ORDER BY category_code, category_name, category_value)
                      category_key
                  ,category_code
                  ,category_name
                  ,category_value
            FROM   (SELECT DISTINCT
                           cc.fk_categorycategor category_code
                          ,c.description category_name
                          ,gd.description category_value
                    FROM   customer_category cc
                           INNER JOIN generic_detail gd
                              ON (    cc.fk_generic_detafk =
                                         gd.fk_generic_headpar
                                  AND cc.fk_generic_detaser = gd.serial_num)
                           INNER JOIN category c
                              ON c.category_code = cc.fk_categorycategor)) b
ON         (    a.category_code = b.category_code
            AND a.category_value = b.category_value)
WHEN NOT MATCHED
THEN
   INSERT     (
                 category_key
                ,category_code
                ,category_name
                ,category_value)
   VALUES     (
                 b.category_key
                ,b.category_code
                ,b.category_name
                ,b.category_value)
WHEN MATCHED
THEN
   UPDATE SET a.category_name = b.category_name;
END;

