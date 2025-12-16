CREATE PROCEDURE TOT_TEMP_GD (IN dept CHAR(1), OUT result INTEGER)
LANGUAGE SQL
BEGIN ATOMIC
   -- TEST PROC - PLAYING WITH HANDLERS
   DECLARE curval    INTEGER;
   DECLARE sum       INTEGER DEFAULT 0;
   DECLARE END_TABLE INTEGER DEFAULT 0;
   DECLARE mycursor  CURSOR  FOR select a2 from TEMP_GD where a1 = dept;
   DECLARE CONTINUE  HANDLER FOR NOT FOUND SET END_TABLE = 1;
   OPEN  mycursor;
   FETCH mycursor INTO curval;
   WHILE END_TABLE = 0 DO
      SET sum = sum + curval;
      FETCH mycursor INTO curval;
   END WHILE;
   CLOSE mycursor;
   call dbms_output.put_line(sum);
   SET result = sum;
END;

