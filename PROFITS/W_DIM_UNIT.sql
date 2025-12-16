create table W_DIM_UNIT
(
    CODE                INTEGER,
    ROW_EFFECTIVE_DATE  DATE,
    ROW_EXPIRATION_DATE DATE,
    ROW_CURRENT_FLAG    DECIMAL(15, 2) default 0,
    UNIT_NAME           VARCHAR(40),
    ENTRY_STATUS_NAME   VARCHAR(10),
    REGION_NAME         VARCHAR(40),
    EMAIL               VARCHAR(40)
);

create unique index PK_W_DIM_UNIT
    on W_DIM_UNIT (CODE, ROW_EFFECTIVE_DATE);

CREATE PROCEDURE W_DIM_UNIT ( )
  SPECIFIC SQL160620112632742
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_dim_unit
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (code) IN (SELECT t.code
                      FROM   w_dim_unit t
                             INNER JOIN
                             (SELECT code
                                    ,unit_name
                                    ,entry_status_name
                                    ,region_name
                                    ,email
                              FROM   w_dim_unit
                              WHERE  row_current_flag = 1
                              MINUS
                              SELECT code
                                    ,unit_name
                                    ,DECODE (
                                        unit.entry_status
                                       ,'1', 'Active'
                                       ,'Not Active')
                                        AS entry_status_name
                                    ,description region_name
                                    ,email
                              FROM   unit
                                     LEFT JOIN generic_detail
                                        ON     unit.fkgd_resides_in_re =
                                                  generic_detail.serial_num
                                           AND unit.fkgh_resides_in_re =
                                                  generic_detail.fk_generic_headpar)
                             s
                                ON (t.code = s.code));
INSERT INTO w_dim_unit (
               code
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag
              ,unit_name
              ,entry_status_name
              ,region_name
              ,email)
   SELECT code
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
         ,unit_name
         ,entry_status_name
         ,region_name
         ,email
   FROM   (SELECT code
                 ,unit_name
                 ,DECODE (unit.entry_status, '1', 'Active', 'Not Active')
                     entry_status_name
                 ,description region_name
                 ,email
           FROM   unit
                  LEFT JOIN generic_detail
                     ON     unit.fkgd_resides_in_re =
                               generic_detail.serial_num
                        AND unit.fkgh_resides_in_re =
                               generic_detail.fk_generic_headpar
           MINUS
           SELECT code
                 ,unit_name
                 ,entry_status_name
                 ,region_name
                 ,email
           FROM   w_dim_unit
           WHERE  row_current_flag = 1);
END;

