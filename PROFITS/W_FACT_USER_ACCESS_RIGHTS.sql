create table W_FACT_USER_ACCESS_RIGHTS
(
    USER_CODE           VARCHAR(8),
    ROLE_CATEGORY_IND   VARCHAR(6),
    PROFILE_ID          VARCHAR(8),
    WINDOW_CODE         VARCHAR(8),
    GUI_CODE            VARCHAR(8),
    ROW_EFFECTIVE_DATE  DATE,
    ROW_EXPIRATION_DATE DATE,
    ROW_CURRENT_FLAG    DECIMAL(15) default 0
);

create unique index PK_W_FACT_USER_ACCESS_RIGHTS
    on W_FACT_USER_ACCESS_RIGHTS (USER_CODE, ROLE_CATEGORY_IND, PROFILE_ID, WINDOW_CODE, GUI_CODE, ROW_EFFECTIVE_DATE);

CREATE PROCEDURE W_FACT_USER_ACCESS_RIGHTS ( )
  SPECIFIC SQL160620112707986
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_fact_user_access_rights
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (user_code
           ,role_category_ind
           ,profile_id
           ,window_code
           ,gui_code) IN (SELECT t.user_code
                                ,t.role_category_ind
                                ,t.profile_id
                                ,t.window_code
                                ,t.gui_code
                          FROM   w_fact_user_access_rights t
                                 INNER JOIN
                                 ((SELECT user_code
                                         ,role_category_ind
                                         ,profile_id
                                         ,window_code
                                         ,gui_code
                                   FROM   w_fact_user_access_rights
                                   WHERE  row_current_flag = 1
                                   MINUS
                                   SELECT u.code user_code
                                         ,'Main' role_category_ind
                                         ,p.fk_unit_categorid profile_id
                                         ,p.fk_sec_operatiofk window_code
                                         ,p.fk_sec_operatiofk1 gui_code
                                   FROM   usr u, profile_operation p
                                   WHERE      u.fkucateg_owns_main =
                                                 p.fk_unit_categorid
                                          AND u.entry_status = '1'
                                   UNION ALL
                                   SELECT u.code user_code
                                         ,'Second' role_category_ind
                                         ,p.fk_unit_categorid profile_id
                                         ,p.fk_sec_operatiofk window_code
                                         ,p.fk_sec_operatiofk1 gui_code
                                   FROM   usr u, profile_operation p
                                   WHERE      u.fkucateg_owns_as_2 =
                                                 p.fk_unit_categorid
                                          AND u.entry_status = '1'
                                          AND LENGTH (
                                                 TRIM (u.fkucateg_owns_as_2))
                                                 IS NOT NULL
                                   UNION ALL
                                   SELECT u.code user_code
                                         ,'Third' role_category_ind
                                         ,p.fk_unit_categorid profile_id
                                         ,p.fk_sec_operatiofk window_code
                                         ,p.fk_sec_operatiofk1 gui_code
                                   FROM   usr u, profile_operation p
                                   WHERE      u.fkucateg_owns_as_3 =
                                                 p.fk_unit_categorid
                                          AND u.entry_status = '1'
                                          AND LENGTH (
                                                 TRIM (u.fkucateg_owns_as_3))
                                                 IS NOT NULL)) s
                                    ON (    t.user_code = s.user_code
                                        AND t.role_category_ind =
                                               s.role_category_ind
                                        AND t.profile_id = s.profile_id
                                        AND t.window_code = s.window_code
                                        AND t.gui_code = s.gui_code));
INSERT INTO w_fact_user_access_rights (
               user_code
              ,role_category_ind
              ,profile_id
              ,window_code
              ,gui_code
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag)
   SELECT user_code
         ,role_category_ind
         ,profile_id
         ,window_code
         ,gui_code
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
   FROM   ( (SELECT u.code user_code
                   ,'Main' role_category_ind
                   ,p.fk_unit_categorid profile_id
                   ,p.fk_sec_operatiofk window_code
                   ,p.fk_sec_operatiofk1 gui_code
             FROM   usr u, profile_operation p
             WHERE      u.fkucateg_owns_main = p.fk_unit_categorid
                    AND u.entry_status = '1'
             UNION ALL
             SELECT u.code user_code
                   ,'Second' role_category_ind
                   ,p.fk_unit_categorid profile_id
                   ,p.fk_sec_operatiofk window_code
                   ,p.fk_sec_operatiofk1 gui_code
             FROM   usr u, profile_operation p
             WHERE      u.fkucateg_owns_as_2 = p.fk_unit_categorid
                    AND u.entry_status = '1'
                    AND LENGTH (TRIM (u.fkucateg_owns_as_2)) IS NOT NULL
             UNION ALL
             SELECT u.code user_code
                   ,'Third' role_category_ind
                   ,p.fk_unit_categorid profile_id
                   ,p.fk_sec_operatiofk window_code
                   ,p.fk_sec_operatiofk1 gui_code
             FROM   usr u, profile_operation p
             WHERE      u.fkucateg_owns_as_3 = p.fk_unit_categorid
                    AND u.entry_status = '1'
                    AND LENGTH (TRIM (u.fkucateg_owns_as_3)) IS NOT NULL)
           MINUS
           SELECT user_code
                 ,role_category_ind
                 ,profile_id
                 ,window_code
                 ,gui_code
           FROM   w_fact_user_access_rights
           WHERE  row_current_flag = 1);
END;

