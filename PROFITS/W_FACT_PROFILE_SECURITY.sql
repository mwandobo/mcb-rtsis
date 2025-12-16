create table W_FACT_PROFILE_SECURITY
(
    PROFILE_ID          VARCHAR(8),
    WINDOW_CODE         VARCHAR(8),
    GUI_CODE            VARCHAR(8),
    ROW_EFFECTIVE_DATE  DATE,
    ROW_EXPIRATION_DATE DATE,
    ROW_CURRENT_FLAG    DECIMAL(15) default 0,
    SYSTEM_DESCRIPTION  VARCHAR(40)
);

create unique index PK_W_FACT_PROFILE_SECURITY
    on W_FACT_PROFILE_SECURITY (PROFILE_ID, WINDOW_CODE, GUI_CODE, ROW_EFFECTIVE_DATE);

CREATE PROCEDURE W_FACT_PROFILE_SECURITY ( )
  SPECIFIC SQL160620112707985
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_fact_profile_security
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (profile_id, window_code, gui_code) IN (SELECT t.profile_id
                                                         ,t.window_code
                                                         ,t.gui_code
                                                   FROM   w_fact_profile_security t
                                                          INNER JOIN
                                                          (SELECT profile_id
                                                                 ,window_code
                                                                 ,gui_code
                                                                 ,system_description
                                                           FROM   w_fact_profile_security
                                                           WHERE  row_current_flag =
                                                                     1
                                                           MINUS
                                                           SELECT p.fk_unit_categorid
                                                                     AS profile_id
                                                                 ,p.fk_sec_operatiofk
                                                                     AS window_code
                                                                 ,p.fk_sec_operatiofk1
                                                                     AS gui_code
                                                                 ,s.description
                                                                     system_description
                                                           FROM   profile_operation p
                                                                  JOIN
                                                                  generic_detail s
                                                                     ON     s.serial_num =
                                                                               UTILPKG.numtext (
                                                                                  SUBSTR (
                                                                                     p.fk_sec_operatiofk
                                                                                    ,1
                                                                                    ,2))
                                                                        AND s.fk_generic_headpar =
                                                                               'PRSYS')
                                                          s
                                                             ON (    t.profile_id =
                                                                        s.profile_id
                                                                 AND t.window_code =
                                                                        s.window_code
                                                                 AND t.gui_code =
                                                                        s.gui_code));
INSERT INTO w_fact_profile_security (
               profile_id
              ,window_code
              ,gui_code
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag
              ,system_description)
   SELECT profile_id
         ,window_code
         ,gui_code
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
         ,system_description
   FROM   (SELECT p.fk_unit_categorid AS profile_id
                 ,p.fk_sec_operatiofk AS window_code
                 ,p.fk_sec_operatiofk1 AS gui_code
                 ,s.description system_description
           FROM   profile_operation p
                  JOIN generic_detail s
                     ON     s.serial_num =
                               UTILPKG.numtext (
                                  SUBSTR (p.fk_sec_operatiofk, 1, 2))
                        AND s.fk_generic_headpar = 'PRSYS'
           MINUS
           SELECT profile_id
                 ,window_code
                 ,gui_code
                 ,system_description
           FROM   w_fact_profile_security
           WHERE  row_current_flag = 1);
END;

