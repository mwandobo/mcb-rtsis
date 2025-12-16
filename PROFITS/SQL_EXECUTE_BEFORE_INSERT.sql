CREATE PROCEDURE SQL_EXECUTE_BEFORE_INSERT (
    IN V_NEW_ROW_FK_DCD_PROJECT	DECIMAL(12, 0),
    IN V_NEW_ROW_TIMESTMP	TIMESTAMP,
    IN V_NEW_ROW_DATA_SQL	VARCHAR(4000),
    IN V_NEW_ROW_SQL_CRITERIA	VARCHAR(4000),
    IN V_NEW_ROW_MULTI_ROW	SMALLINT,
    IN V_NEW_ROW_FK_DCD_REPORT	DECIMAL(12, 0),
    IN V_NEW_ROW_FK_DCD_INFO	DECIMAL(15, 0),
    IN V_NEW_ROW_INTERNAL_SN	DECIMAL(15, 0),
    IN V_NEW_ROW_SHEET	INTEGER,
    OUT V_NEW_ROW_RESULT0	VARCHAR(4000) )
  SPECIFIC SQL160726185857124
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
  OLD SAVEPOINT LEVEL
BEGIN
DECLARE tmp decimal(10);
DECLARE l_statement VARCHAR (32672) ;
DECLARE res VARCHAR (5000);
DECLARE l_rec_number DECFLOAT;
DECLARE l_statement_type VARCHAR (32672);
DECLARE l_tab_name VARCHAR (30);
DECLARE l_dyn_sql VARCHAR (32672);
DECLARE l_statmnt2 VARCHAR(8000);
DECLARE l_stmt1 STATEMENT;
   IF V_NEW_ROW_fk_dcd_project > 0
   THEN
      SET l_tab_name = 'sql_rows';
      SELECT NVL (MAX (int_sn), 0) rec_number
      INTO l_rec_number
      FROM   sql_rows
      WHERE  timestmp = V_NEW_ROW_timestmp;
   ELSE
      SET l_tab_name = 'sql_list';
      SELECT NVL (MAX (intsn), 0) rec_number
      INTO l_rec_number
      FROM   sql_list
      WHERE  timestmp = V_NEW_ROW_timestmp;
   END IF;
  SET l_statement_type = LTRIM (V_NEW_ROW_data_sql);
  SET l_statement_type =
        SUBSTR (
  l_statement_type
          ,1
          ,INSTR (
  l_statement_type
             ,' '
             ,1
             ,1));
  SET l_statement_type = UPPER (TRIM (l_statement_type));
  SET l_statement_type = REPLACE(l_statement_type, CHR(10));
 
   IF (l_statement_type = 'SELECT'
        OR l_statement_type = 'WITH')
   THEN
      SET l_statement = V_NEW_ROW_data_sql || V_NEW_ROW_sql_criteria;
      SET l_dyn_sql  = 'DELETE FROM sql_rows_gtt';
      PREPARE dynsql_insert  FROM l_dyn_sql;
      EXECUTE dynsql_insert;
      SET l_dyn_sql  = 'INSERT INTO sql_rows_gtt (data0)' || chr(10) ||
        l_statement;
      PREPARE dynsql_insert FROM l_dyn_sql;
      EXECUTE dynsql_insert;
      IF V_NEW_ROW_multi_row = 1
      THEN
         IF V_NEW_ROW_fk_dcd_project > 0
         THEN
               INSERT INTO sql_rows (
                 timestmp
                ,int_sn
                ,fk_dcd_poject
                ,fk_dcd_report
                ,fk_dcd_info
                ,data0)
                 SELECT V_NEW_ROW_timestmp
                   ,ROW_NUMBER () OVER (ORDER BY 1) + l_rec_number
                   ,V_NEW_ROW_fk_dcd_project
                   ,V_NEW_ROW_fk_dcd_report
                   ,V_NEW_ROW_fk_dcd_info
                   ,t.data0
                 FROM sql_rows_gtt t;
         ELSE
            INSERT INTO sql_list (timestmp, intsn, VALUE)
            SELECT V_NEW_ROW_timestmp
             , ROW_NUMBER () OVER (ORDER BY 1) + l_rec_number
             , t.data0
             FROM sql_rows_gtt t;
         END IF;
      ELSE
        SET l_statmnt2 = 'SET ? = (' || l_statement || ')';
 
        PREPARE l_stmt1 FROM l_statmnt2;
        EXECUTE l_stmt1 into res;
        SET V_NEW_ROW_result0 = res;
      END IF;
   ELSE
    IF  l_statement_type IN ('CREATE'
                            ,'DROP'
                            ,'UPDATE'
                            ,'INSERT'
                            ,'DELETE'
                            ,'TRUNCATE'
                            ,'DECLARE'
                            ,'BEGIN'
                            ,'CALL')
       THEN
          PREPARE l_stmt1 FROM V_NEW_ROW_data_sql;
          EXECUTE l_stmt1;
          SET V_NEW_ROW_result0 = 'SUCCESS ' || l_statement_type;
     END IF;
   END IF;
END;

