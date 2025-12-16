create table W_DIM_PRODUCT
(
    PRODUCT_CODE INTEGER,
    DESCRIPTION  VARCHAR(40),
    MEMO_FLAG    VARCHAR(8),
    TREE_LEVEL_1 VARCHAR(40),
    TREE_LEVEL_2 VARCHAR(40)
);

create unique index PK_W_DIM_PRODUCT
    on W_DIM_PRODUCT (PRODUCT_CODE);

CREATE PROCEDURE W_DIM_PRODUCT ( )
  SPECIFIC SQL160620112632944
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
MERGE INTO w_dim_product a
USING      (SELECT   product.id_product product_code
                    ,product.description description
                    ,NVL2 (MIN (generic_detail.serial_num), 'Memo', 'Non-memo')
                        memo_flag
                    ,MAX (code_desc) tree_level_1
                    ,NVL (MAX (tree_level_2), MAX (code_desc)) tree_level_2
            FROM     product
                     LEFT JOIN w_code
                        ON     w_code.code_set_id = 5
                           AND product.product_type = w_code.code_value
                     LEFT JOIN
                     (SELECT UPPER (generic_detail.description) AS tree_level_2
                            ,product.id_product
                      FROM   deposit
                             LEFT JOIN generic_detail
                                ON (    generic_detail.fk_generic_headpar =
                                           deposit.fk_generic_detafk
                                    AND generic_detail.serial_num =
                                           deposit.fk_generic_detaser)
                             LEFT JOIN product
                                ON (product.id_product =
                                       deposit.fk_productid_produ)
                      WHERE  generic_detail.fk_generic_headpar = 'LACTP'
                      UNION
                      SELECT UPPER (generic_detail.description) AS tree_level_2
                            ,product.id_product
                      FROM   loan
                             LEFT JOIN generic_detail
                                ON (    generic_detail.fk_generic_headpar =
                                           loan.fkgh_has_account_t
                                    AND generic_detail.serial_num =
                                           loan.fkgd_has_account_t)
                             LEFT JOIN product
                                ON (product.id_product =
                                       loan.fk_productid_produ)
                      WHERE  generic_detail.fk_generic_headpar = 'LACTP'
                      UNION
                      SELECT NULL AS tree_level_2, fk_productid_produ
                      FROM   agreement_type
                             LEFT JOIN product
                                ON (product.id_product =
                                       agreement_type.fk_productid_produ)
                      UNION
                      SELECT UPPER (generic_detail.description) AS tree_level_2
                            ,product.id_product
                      FROM   lg
                             LEFT JOIN generic_detail
                                ON     generic_detail.fk_generic_headpar =
                                          lg.fkgh_account_type
                                   AND generic_detail.serial_num =
                                          lg.fkgd_account_type
                                   AND generic_detail.fk_generic_headpar =
                                          'LGACT'
                             LEFT JOIN product
                                ON (product.id_product = lg.fk_productid_produ)
                      UNION
                      SELECT UPPER (generic_detail.description) AS tree_level_2
                            ,product.id_product
                      FROM   lc_account
                             LEFT JOIN generic_detail
                                ON     generic_detail.fk_generic_headpar =
                                          lc_account.fk_generic_detafk
                                   AND generic_detail.serial_num =
                                          lc_account.fk_generic_detaser
                                   AND generic_detail.fk_generic_headpar =
                                          'INVCE'
                             LEFT JOIN product
                                ON (product.id_product =
                                       lc_account.fk_tradefk_product)
                      UNION
                      SELECT UPPER (generic_detail.description) AS tree_level_2
                            ,product.id_product
                      FROM   trade_finance
                             LEFT JOIN generic_detail
                                ON     generic_detail.fk_generic_headpar =
                                          trade_finance.fk_generic_detafk
                                   AND generic_detail.serial_num =
                                          trade_finance.fk_generic_detaser
                                   AND generic_detail.fk_generic_headpar =
                                          'CBCOD'
                             LEFT JOIN product
                                ON (product.id_product =
                                       trade_finance.fk_tradefk_prod)) t
                        ON t.id_product = product.id_product
                     LEFT JOIN generic_detail
                        ON     generic_detail.serial_num = t.id_product
                           AND generic_detail.fk_generic_headpar = 'EOM09'
                           AND generic_detail.serial_num > 0
            GROUP BY product.id_product, product.description) b
ON         (a.product_code = b.product_code)
WHEN NOT MATCHED
THEN
   INSERT     (
                 product_code
                ,description
                ,memo_flag
                ,tree_level_1
                ,tree_level_2)
   VALUES     (
                 b.product_code
                ,b.description
                ,b.memo_flag
                ,b.tree_level_1
                ,b.tree_level_2)
WHEN MATCHED
THEN
   UPDATE SET a.description = b.description
             ,a.memo_flag = b.memo_flag
             ,a.tree_level_1 = b.tree_level_1
             ,a.tree_level_2 = b.tree_level_2;
END;

