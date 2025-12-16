CREATE PROCEDURE LINK_EOM_DH_COLLAT_COLLAT_ITEM ( )
  SPECIFIC SQL160620112633856
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DELETE w_eom_dh_collat_collat_item
WHERE  eom_date = (SELECT scheduled_date FROM bank_parameters);
MERGE INTO w_eom_dh_collat_collat_item a
USING      (SELECT (SELECT scheduled_date FROM bank_parameters) eom_date
                  ,CAST (
                         fk_collateral_tfk
                      || '|'
                      || fk_unitcode
                      || '|'
                      || collateral_sn AS CHAR (20))
                      collat_combo_key
                  ,CAST (
                      TRIM (record_type) || '|' || internal_sn AS CHAR (12))
                      collat_item_combo_key
            FROM   r_collateral pledge
                   JOIN collateral_table ct
                      ON     fk_collateral_tfk = used_collat_type
                         AND fk_unitcode = used_unit
                         AND collateral_sn = used_collat_sn) b
ON         (    a.eom_date = b.eom_date
            AND a.collat_item_combo_key = b.collat_item_combo_key
            AND a.collat_combo_key = b.collat_combo_key)
WHEN NOT MATCHED
THEN
   INSERT     (eom_date, collat_item_combo_key, collat_combo_key)
   VALUES     (b.eom_date, b.collat_item_combo_key, b.collat_combo_key);
END;

