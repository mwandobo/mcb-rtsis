create table W_FACT_COLLAT_ITEM_ALLOC
(
    EFF_FROM_DATE        DATE,
    EFF_TO_DATE          DATE,
    COLLATERAL_COMBO_KEY CHAR(20),
    ITEM_INTERNAL_SN     DECIMAL(10),
    REALTY_KEY           DECIMAL(10),
    REMOVAL_DATE         DATE,
    REMOVAL_IND          CHAR(9),
    ROW_CURRENT_FLAG     SMALLINT default 0
);

create unique index PK_W_FACT_COLLAT_ITEM_ALLOC
    on W_FACT_COLLAT_ITEM_ALLOC (EFF_FROM_DATE, EFF_TO_DATE, COLLATERAL_COMBO_KEY, ITEM_INTERNAL_SN);

CREATE PROCEDURE W_FACT_COLLAT_ITEM_ALLOC ( )
  SPECIFIC SQL160620112636775
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_fact_collat_item_alloc
SET    eff_to_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (collateral_combo_key
           ,item_internal_sn
           ,realty_key
           ,removal_date
           ,removal_ind) IN (SELECT collateral_combo_key
                                   ,item_internal_sn
                                   ,realty_key
                                   ,removal_date
                                   ,removal_ind
                             FROM   w_fact_collat_item_alloc
                             WHERE  row_current_flag = 1
                             MINUS
                             SELECT CAST (
                                          TRIM (b.record_type)
                                       || '|'
                                       || b.ctbl_internal_sn AS CHAR (20))
                                       collateral_combo_key
                                   ,internal_sn item_internal_sn
                                   ,real_estate_id realty_key
                                   ,removal_date
                                   ,CAST (
                                       DECODE (
                                          removal_ind
                                         ,'1', 'Removed'
                                         ,'0', 'Active'
                                         ,'n/a') AS CHAR (9))
                                       removal_ind
                             FROM   collateral_detail b);
INSERT INTO w_fact_collat_item_alloc (
               eff_from_date
              ,eff_to_date
              ,collateral_combo_key
              ,item_internal_sn
              ,realty_key
              ,removal_date
              ,removal_ind
              ,row_current_flag)
   SELECT (SELECT scheduled_date FROM bank_parameters) eff_from_date
         ,DATE '9999-12-31' eff_to_date
         ,collateral_combo_key
         ,item_internal_sn
         ,realty_key
         ,removal_date
         ,removal_ind
         ,1 row_current_flag
   FROM   (SELECT CAST (
                     TRIM (b.record_type) || '|' || b.ctbl_internal_sn AS CHAR (20))
                     collateral_combo_key
                 ,internal_sn item_internal_sn
                 ,real_estate_id realty_key
                 ,removal_date
                 ,CAST (
                     DECODE (
                        removal_ind
                       ,'1', 'Removed'
                       ,'0', 'Active'
                       ,'n/a') AS CHAR (9))
                     removal_ind
           FROM   collateral_detail b
           MINUS
           SELECT collateral_combo_key
                 ,item_internal_sn
                 ,realty_key
                 ,removal_date
                 ,removal_ind
           FROM   w_fact_collat_item_alloc
           WHERE  row_current_flag = 1);
END;

