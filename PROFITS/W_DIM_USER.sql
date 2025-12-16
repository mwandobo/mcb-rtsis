create table W_DIM_USER
(
    CODE                CHAR(8),
    ROW_EFFECTIVE_DATE  DATE,
    ROW_EXPIRATION_DATE DATE,
    ROW_CURRENT_FLAG    SMALLINT default 0,
    BANK_EMPLOYEE_ID    CHAR(8)  default '0',
    NAME                VARCHAR(41),
    UNIT_CODE           INTEGER  default 0,
    ATM_FLAG            VARCHAR(7)
);

create unique index PK_W_DIM_USER
    on W_DIM_USER (CODE, ROW_EFFECTIVE_DATE);

CREATE PROCEDURE W_DIM_USER ( )
  SPECIFIC SQL160620112632743
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
UPDATE w_dim_user
SET    row_expiration_date =
          UTILPKG.add_days (
             (SELECT scheduled_date FROM bank_parameters)
            ,-1)
      ,row_current_flag = 0
WHERE      row_current_flag = 1
       AND (code) IN (SELECT t.code
                      FROM   w_dim_user t
                             INNER JOIN
                             (SELECT code
                                    ,bank_employee_id
                                    ,name
                                    ,unit_code
                                    ,atm_flag
                              FROM   w_dim_user
                              WHERE  row_current_flag = 1
                              MINUS
                              SELECT code
                                    ,bankemployee.id bank_employee_id
                                    ,   TRIM (bankemployee.first_name)
                                     || ' '
                                     || bankemployee.last_name
                                        name
                                    ,fk_unitcode unit_code
                                    ,DECODE (
                                        usr.atm_user
                                       ,'1', 'ATM'
                                       ,'Non-ATM')
                                        atm_flag
                              FROM   bankemployee
                                     JOIN usr
                                        ON usr.fk_bankemployeeid =
                                              bankemployee.id) s
                                ON (t.code = s.code));
INSERT INTO w_dim_user (
               code
              ,row_effective_date
              ,row_expiration_date
              ,row_current_flag
              ,bank_employee_id
              ,name
              ,unit_code
              ,atm_flag)
   SELECT code
         , (SELECT scheduled_date FROM bank_parameters) row_effective_date
         ,DATE '9999-12-31' row_expiration_date
         ,1 row_current_flag
         ,bank_employee_id
         ,name
         ,unit_code
         ,atm_flag
   FROM   (SELECT code
                 ,bankemployee.id bank_employee_id
                 ,   TRIM (bankemployee.first_name)
                  || ' '
                  || bankemployee.last_name
                     name
                 ,fk_unitcode unit_code
                 ,DECODE (usr.atm_user, '1', 'ATM', 'Non-ATM') atm_flag
           FROM   bankemployee
                  JOIN usr ON usr.fk_bankemployeeid = bankemployee.id
           MINUS
           SELECT code
                 ,bank_employee_id
                 ,name
                 ,unit_code
                 ,atm_flag
           FROM   w_dim_user
           WHERE  row_current_flag = 1);
END;

